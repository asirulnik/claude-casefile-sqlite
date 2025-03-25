#!/usr/bin/env python3

"""
Excel and CSV loading functionality for the Case File Database

This module provides core functions to load data from Excel spreadsheets or CSV files
into the SQLite database in the proper case file format.
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Union, Tuple, Optional

# Import functionality from modules
from validation import validate_case_file_data
from data_processing import prepare_data_for_import

def identify_file_type(file_path: str) -> str:
    """
    Identify if the file is an Excel spreadsheet or CSV based on extension.
    
    Args:
        file_path: Path to the input file
        
    Returns:
        File type: 'excel', 'csv', or 'unknown'
    """
    _, ext = os.path.splitext(file_path.lower())
    
    if ext in ['.xlsx', '.xls', '.xlsm']:
        return 'excel'
    elif ext == '.csv':
        return 'csv'
    else:
        return 'unknown'

def load_data_to_dataframe(file_path: str) -> pd.DataFrame:
    """
    Load data from Excel or CSV file into a pandas DataFrame.
    
    Args:
        file_path: Path to the file to load
        
    Returns:
        DataFrame containing the data
    """
    file_type = identify_file_type(file_path)
    
    if file_type == 'excel':
        return pd.read_excel(file_path)
    elif file_type == 'csv':
        return pd.read_csv(file_path, delimiter='\t')
    else:
        raise ValueError(f"Unsupported file type: {file_path}")

def load_case_file_to_db(conn: sqlite3.Connection, df: pd.DataFrame, case_id: int) -> Dict[str, Any]:
    """
    Load case file data into the database.
    
    Args:
        conn: SQLite database connection
        df: DataFrame containing case file data
        case_id: ID of the case to associate entries with
        
    Returns:
        Dict with import results
    """
    results = {
        "success": False,
        "entries_added": 0,
        "billing_entries_added": 0,
        "errors": []
    }
    
    try:
        prepared_df = prepare_data_for_import(df, case_id)
        
        # Get list of columns from case_file_entries table
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(case_file_entries)")
        db_columns = [col[1] for col in cursor.fetchall()]
        
        # Filter DataFrame to include only columns that exist in the database
        columns_to_use = [col for col in prepared_df.columns if col in db_columns]
        filtered_df = prepared_df[columns_to_use]
        
        # Insert data into case_file_entries table
        filtered_df.to_sql('case_file_entries', conn, if_exists='append', index=False)
        
        # Get the IDs of the newly inserted entries
        cursor.execute("SELECT last_insert_rowid()")
        last_id = cursor.fetchone()[0]
        first_id = last_id - len(filtered_df) + 1
        
        results["entries_added"] = len(filtered_df)
        
        # Create billing entries for any rows of type 'billing-type'
        billing_entries = 0
        billing_rows = df[df['type'] == 'billing-type']
        
        for _, row in billing_rows.iterrows():
            # Find the index of this row in the original dataframe
            orig_idx = df.index.get_loc(row.name)
            entry_id = first_id + orig_idx
            
            # Extract billing category from title
            billing_category = ""
            if ':' in row['title']:
                billing_category = row['title'].split(':', 1)[1].strip()
            else:
                billing_category = row['title']
            
            # Convert fields to appropriate format
            billing_start = row.get('billing-start') if 'billing-start' in row else None
            billing_stop = row.get('billing-stop') if 'billing-stop' in row else None
            billing_hrs = row.get('billing-hrs') if 'billing-hrs' in row else None
            
            # Insert billing entry
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
                    billing_hrs,
                    row['content']
                )
            )
            billing_entries += 1
        
        conn.commit()
        results["billing_entries_added"] = billing_entries
        results["success"] = True
    except Exception as e:
        conn.rollback()
        results["errors"].append(str(e))
    
    return results

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
        # Load data from file
        df = load_data_to_dataframe(file_path)
        
        # Validate the data format
        validation = validate_case_file_data(df)
        if not validation["valid"]:
            results["message"] = "Data validation failed"
            results["errors"] = validation
            return results
        
        # Connect to the database and load data
        conn = sqlite3.connect(db_path)
        import_results = load_case_file_to_db(conn, df, case_id)
        conn.close()
        
        # Update results with import results
        results.update(import_results)
        if results["success"]:
            results["message"] = f"Successfully imported {results['entries_added']} entries"
    except Exception as e:
        results["message"] = f"Error importing data: {str(e)}"
        results["errors"].append(str(e))
    
    return results
