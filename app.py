#!/usr/bin/env python3

"""
Extended SQLite MCP server for Claude Case File Management

This extension adds tools for importing Excel/CSV data into the SQLite database
and managing case files with comprehensive billing data integration.
"""

import os
import sys
import json
import sqlite3
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional, Union

# MCP server specific utilities
def respond(data):
    """Send a response back to the MCP client"""
    print(json.dumps(data), file=sys.stdout)
    sys.stdout.flush()

def respond_with_error(message, code=-1):
    """Send an error response back to the MCP client"""
    error_data = {
        "error": {
            "code": code,
            "message": message
        }
    }
    respond(error_data)

def process_mcp_request(event):
    """Process an MCP request event"""
    try:
        if event.get("type") != "function":
            respond_with_error("Unsupported event type")
            return

        function_name = event.get("name", "")
        params = event.get("parameters", {})
        
        # Map function names to handler functions
        function_map = {
            "check_database_health": handle_check_database_health,
            "initialize_database": handle_initialize_database,
            "import_excel_data": handle_import_excel_data,
            "get_case_files": handle_get_case_files,
            "get_case_file_entries": handle_get_case_file_entries,
            "generate_billing_report": handle_generate_billing_report
        }
        
        if function_name in function_map:
            function_map[function_name](params)
        else:
            respond_with_error(f"Unknown function: {function_name}")
    
    except Exception as e:
        respond_with_error(f"Error processing request: {str(e)}")

# Database validation and management functions
def db_health_check(db_path: str) -> Dict[str, Any]:
    """
    Perform a health check on the database to verify its existence,
    connectivity, and schema correctness.
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        Dict with health check results
    """
    results = {
        "database_exists": False,
        "can_connect": False,
        "schema_exists": False,
        "schema_valid": False,
        "discrepancies": [],
        "tables": []
    }
    
    # Check if database file exists
    if not os.path.exists(db_path):
        return results
    
    results["database_exists"] = True
    
    # Try to connect to the database
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        results["can_connect"] = True
        
        # Check for existing tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        results["tables"] = [table[0] for table in tables if table[0] != "sqlite_sequence"]
        
        # Check if the expected schema exists
        required_tables = ["clients", "case_files", "case_file_entries", "billing_entries"]
        results["schema_exists"] = all(table in results["tables"] for table in required_tables)
        
        if results["schema_exists"]:
            # Validate each table schema
            schema_valid = True
            
            # Define expected columns for each table
            expected_schema = {
                "clients": ["client_id", "client_name", "contact_info"],
                "case_files": ["case_id", "client_id", "case_name", "case_status"],
                "case_file_entries": [
                    "entry_id", "case_id", "type", "date", "title", "from_party", "to_party",
                    "cc_party", "content", "attachments", "synopsis", "comments",
                    "billing_start", "billing_stop", "billing_hrs"
                ],
                "billing_entries": [
                    "billing_id", "case_id", "entry_id", "billing_category",
                    "billing_start", "billing_stop", "billing_hours", "billing_description"
                ]
            }
            
            for table in required_tables:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()
                current_columns = [col[1] for col in columns]  # Column name is at index 1
                
                # Check for missing columns
                missing_columns = set(expected_schema[table]) - set(current_columns)
                extra_columns = set(current_columns) - set(expected_schema[table])
                
                if missing_columns or extra_columns:
                    schema_valid = False
                    if missing_columns:
                        results["discrepancies"].append({
                            "table": table,
                            "issue": "missing_columns",
                            "columns": list(missing_columns)
                        })
                    if extra_columns:
                        results["discrepancies"].append({
                            "table": table,
                            "issue": "extra_columns",
                            "columns": list(extra_columns)
                        })
            
            results["schema_valid"] = schema_valid
        
        conn.close()
    except sqlite3.Error as e:
        results["error"] = str(e)
    
    return results

def initialize_database(db_path: str) -> Dict[str, Any]:
    """
    Initialize the database with the required schema
    
    Args:
        db_path: Path to the SQLite database file
        
    Returns:
        Dict with initialization results
    """
    results = {"success": False, "message": ""}
    
    try:
        # Read the schema.sql file
        schema_dir = os.path.dirname(os.path.abspath(__file__))
        schema_path = os.path.join(schema_dir, "schema.sql")
        
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
        
        # Connect to the database and execute the schema
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.executescript(schema_sql)
        conn.commit()
        conn.close()
        
        results["success"] = True
        results["message"] = "Database initialized successfully"
    except Exception as e:
        results["message"] = f"Error initializing database: {str(e)}"
    
    return results

# Excel data import functions
def load_excel_to_db(file_path: str, db_path: str, case_id: int) -> Dict[str, Any]:
    """
    Load data from an Excel spreadsheet or CSV file into the database
    
    Args:
        file_path: Path to the Excel or CSV file
        db_path: Path to the SQLite database
        case_id: ID of the case to associate entries with
        
    Returns:
        Dict with import results
    """
    results = {
        "success": False,
        "message": "",
        "entries_added": 0,
        "errors": []
    }
    
    try:
        # Determine file type and load data
        file_ext = os.path.splitext(file_path.lower())[1]
        
        if file_ext in ['.xlsx', '.xls', '.xlsm']:
            df = pd.read_excel(file_path)
        elif file_ext == '.csv':
            df = pd.read_csv(file_path, delimiter='\t')
        else:
            results["message"] = f"Unsupported file type: {file_ext}"
            return results
        
        # Validate the data format
        validation = validate_case_file_data(df)
        if not validation["valid"]:
            results["message"] = "Data validation failed"
            results["errors"] = validation
            return results
        
        # Connect to the database
        conn = sqlite3.connect(db_path)
        
        # Process the data and insert into the database
        prepared_df = prepare_data_for_db(df, case_id)
        entries_added = insert_data_to_db(conn, prepared_df, case_id)
        
        conn.close()
        
        results["success"] = True
        results["message"] = f"Successfully imported {entries_added} entries"
        results["entries_added"] = entries_added
    except Exception as e:
        results["message"] = f"Error importing data: {str(e)}"
        results["errors"].append(str(e))
    
    return results

def validate_case_file_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate that the DataFrame conforms to the case file schema
    
    Args:
        df: DataFrame to validate
        
    Returns:
        Dict with validation results
    """
    required_columns = ['type', 'date', 'title', 'from', 'to', 'cc', 'content', 'attachments', 'synopsis', 'comments']
    
    validation = {
        "valid": True,
        "missing_columns": [],
        "invalid_entries": []
    }
    
    # Check for required columns
    for col in required_columns:
        if col not in df.columns:
            validation["valid"] = False
            validation["missing_columns"].append(col)
    
    if not validation["valid"]:
        return validation
    
    # Check entry types
    valid_types = [
        'email-type', 'doc-type', 'meeting-type', 'phone-call-type',
        'case-note-type', 'billing-type', 'other-type'
    ]
    
    for i, row in df.iterrows():
        entry_type = row['type']
        if pd.isna(entry_type) or entry_type not in valid_types:
            validation["valid"] = False
            validation["invalid_entries"].append({
                "row": i + 1,
                "issue": f"Invalid entry type: {entry_type}"
            })
    
    return validation

def prepare_data_for_db(df: pd.DataFrame, case_id: int) -> pd.DataFrame:
    """
    Prepare the DataFrame for database insertion, mapping column names
    and handling data conversion.
    
    Args:
        df: DataFrame to prepare
        case_id: Case ID to associate with the entries
        
    Returns:
        Prepared DataFrame
    """
    # Create a copy to avoid modifying the original
    prepared_df = df.copy()
    
    # Add case_id
    prepared_df['case_id'] = case_id
    
    # Map column names to match database schema
    column_mapping = {
        'from': 'from_party',
        'to': 'to_party',
        'cc': 'cc_party',
        'billing-start': 'billing_start',
        'billing-stop': 'billing_stop',
        'billing-hrs': 'billing_hrs'
    }
    
    prepared_df.rename(columns=column_mapping, inplace=True)
    
    # Convert date columns to proper datetime format if needed
    date_columns = ['date', 'billing_start', 'billing_stop']
    for col in date_columns:
        if col in prepared_df.columns:
            # Skip if already datetime or if column is empty
            if not pd.api.types.is_datetime64_dtype(prepared_df[col]):
                prepared_df[col] = pd.to_datetime(prepared_df[col], errors='coerce')
    
    return prepared_df

def insert_data_to_db(conn: sqlite3.Connection, df: pd.DataFrame, case_id: int) -> int:
    """
    Insert the prepared DataFrame into the database
    
    Args:
        conn: Database connection
        df: Prepared DataFrame
        case_id: Case ID for the entries
        
    Returns:
        Number of entries added
    """
    cursor = conn.cursor()
    entries_added = 0
    
    try:
        # Get the list of columns in the case_file_entries table
        cursor.execute("PRAGMA table_info(case_file_entries)")
        table_columns = [col[1] for col in cursor.fetchall()]
        
        # Filter DataFrame to only include columns that exist in the table
        df_columns = [col for col in df.columns if col in table_columns]
        filtered_df = df[df_columns]
        
        # Insert data into case_file_entries table
        for _, row in filtered_df.iterrows():
            columns = ", ".join(df_columns)
            placeholders = ", ".join(["?"] * len(df_columns))
            
            values = []
            for col in df_columns:
                val = row[col]
                # Convert timestamps to strings in the expected format
                if isinstance(val, pd.Timestamp):
                    val = val.strftime("%Y-%m-%d %H:%M:%S")
                values.append(val)
            
            cursor.execute(
                f"INSERT INTO case_file_entries ({columns}) VALUES ({placeholders})",
                values
            )
            entries_added += 1
            
            # Get the ID of the inserted entry
            entry_id = cursor.lastrowid
            
            # Create billing entry if this is a billing-type entry
            if row['type'] == 'billing-type':
                create_billing_entry(cursor, case_id, entry_id, row)
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    
    return entries_added

def create_billing_entry(cursor: sqlite3.Cursor, case_id: int, entry_id: int, row: pd.Series) -> None:
    """
    Create a billing entry for a billing-type case file entry
    
    Args:
        cursor: Database cursor
        case_id: Case ID
        entry_id: Entry ID of the case file entry
        row: Row data from the DataFrame
    """
    # Extract the billing category from the title
    title = row['title']
    if ':' in title:
        billing_category = title.split(':', 1)[1].strip()
    else:
        billing_category = title
    
    # Format datetime values
    billing_start = row['billing_start']
    if isinstance(billing_start, pd.Timestamp):
        billing_start = billing_start.strftime("%Y-%m-%d %H:%M:%S")
    
    billing_stop = row['billing_stop']
    if isinstance(billing_stop, pd.Timestamp):
        billing_stop = billing_stop.strftime("%Y-%m-%d %H:%M:%S")
    
    # Create the billing entry
    cursor.execute(
        """
        INSERT INTO billing_entries 
        (case_id, entry_id, billing_category, billing_start, billing_stop, billing_hours, billing_description)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            case_id, 
            entry_id, 
            billing_category, 
            billing_start, 
            billing_stop, 
            row['billing_hrs'], 
            row['content']
        )
    )

# MCP handler functions
def handle_check_database_health(params: Dict[str, Any]) -> None:
    """Handle database health check request"""
    db_path = params.get("db_path", "")
    
    if not db_path:
        respond_with_error("Database path must be provided")
        return
    
    results = db_health_check(db_path)
    respond(results)

def handle_initialize_database(params: Dict[str, Any]) -> None:
    """Handle database initialization request"""
    db_path = params.get("db_path", "")
    
    if not db_path:
        respond_with_error("Database path must be provided")
        return
    
    results = initialize_database(db_path)
    respond(results)

def handle_import_excel_data(params: Dict[str, Any]) -> None:
    """Handle Excel/CSV data import request"""
    file_path = params.get("file_path", "")
    db_path = params.get("db_path", "")
    case_id = params.get("case_id", 0)
    
    if not file_path or not db_path or not case_id:
        respond_with_error("File path, database path, and case ID must be provided")
        return
    
    results = load_excel_to_db(file_path, db_path, case_id)
    respond(results)

def handle_get_case_files(params: Dict[str, Any]) -> None:
    """Handle request to get list of case files"""
    db_path = params.get("db_path", "")
    
    if not db_path:
        respond_with_error("Database path must be provided")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(
            """
            SELECT cf.case_id, cf.case_name, cf.case_status, c.client_name,
                   COUNT(cfe.entry_id) as entry_count
            FROM case_files cf
            LEFT JOIN clients c ON cf.client_id = c.client_id
            LEFT JOIN case_file_entries cfe ON cf.case_id = cfe.case_id
            GROUP BY cf.case_id
            ORDER BY cf.case_id
            """,
            conn
        )
        conn.close()
        
        # Convert DataFrame to list of dictionaries
        case_files = df.to_dict('records')
        respond({"case_files": case_files})
    except Exception as e:
        respond_with_error(f"Error retrieving case files: {str(e)}")

def handle_get_case_file_entries(params: Dict[str, Any]) -> None:
    """Handle request to get case file entries"""
    db_path = params.get("db_path", "")
    case_id = params.get("case_id", 0)
    
    if not db_path or not case_id:
        respond_with_error("Database path and case ID must be provided")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(
            """
            SELECT * FROM case_file_entries
            WHERE case_id = ?
            ORDER BY date
            """,
            conn,
            params=(case_id,)
        )
        conn.close()
        
        # Convert DataFrame to list of dictionaries
        entries = df.to_dict('records')
        respond({"entries": entries})
    except Exception as e:
        respond_with_error(f"Error retrieving case file entries: {str(e)}")

def handle_generate_billing_report(params: Dict[str, Any]) -> None:
    """Handle request to generate a billing report"""
    db_path = params.get("db_path", "")
    case_id = params.get("case_id", 0)
    
    if not db_path or not case_id:
        respond_with_error("Database path and case ID must be provided")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        
        # Get case and client information
        case_info = pd.read_sql_query(
            """
            SELECT cf.case_id, cf.case_name, cf.case_status, 
                   c.client_id, c.client_name, c.contact_info
            FROM case_files cf
            LEFT JOIN clients c ON cf.client_id = c.client_id
            WHERE cf.case_id = ?
            """,
            conn,
            params=(case_id,)
        ).to_dict('records')[0]
        
        # Get billing entries
        billing_df = pd.read_sql_query(
            """
            SELECT be.*, cfe.date
            FROM billing_entries be
            JOIN case_file_entries cfe ON be.entry_id = cfe.entry_id
            WHERE be.case_id = ?
            ORDER BY cfe.date
            """,
            conn,
            params=(case_id,)
        )
        
        conn.close()
        
        # Process billing entries
        billing_entries = []
        for _, row in billing_df.iterrows():
            billing_entries.append({
                "billing_id": row["billing_id"],
                "date": row["date"],
                "category": row["billing_category"],
                "start_time": row["billing_start"],
                "end_time": row["billing_stop"],
                "hours": row["billing_hours"],
                "description": row["billing_description"]
            })
        
        # Calculate total hours
        total_hours = billing_df["billing_hours"].sum()
        
        respond({
            "case_info": case_info,
            "billing_entries": billing_entries,
            "total_hours": total_hours,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    except Exception as e:
        respond_with_error(f"Error generating billing report: {str(e)}")

# Main entry point
def main():
    """Main entry point for the MCP server"""
    # Wait for input on stdin
    for line in sys.stdin:
        try:
            event = json.loads(line)
            process_mcp_request(event)
        except json.JSONDecodeError:
            respond_with_error("Invalid JSON input")
        except Exception as e:
            respond_with_error(f"Unexpected error: {str(e)}")

if __name__ == "__main__":
    main()
