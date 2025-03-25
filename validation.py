#!/usr/bin/env python3

"""
Validation module for case file data

This module provides functions to validate case file data before importing
into the SQLite database.
"""

import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Union

# Valid entry types according to schema
VALID_ENTRY_TYPES = [
    'email-type',
    'doc-type',
    'meeting-type',
    'phone-call-type',
    'case-note-type',
    'billing-type',
    'other-type'
]

# Valid billing categories
BILLING_CATEGORIES = [
    'Hearing Preparation',
    'Hearing',
    'Hearing Notes & Follow-up',
    'Billing & Invoicing',
    'Draft correspondence',
    'Draft email',
    'Draft documents',
    'Draft records',
    'Review correspondence',
    'Review email',
    'Review documents',
    'Review records',
    'Client Interview Preparation',
    'Client Interview',
    'Client Interview Notes & Follow-up',
    'Client Meeting Preparation',
    'Client Meeting',
    'Client Meeting Notes & Follow-up',
    'Client Status Update Preparation',
    'Client Status Update',
    'Client Status Update Notes & Follow-up',
    'Conference with Attorney Preparation',
    'Conference with Attorney',
    'Conference with Attorney Notes & Follow-up',
    'Settlement Agreement Drafting',
    'Settlement Agreement Review & Analysis',
    'Court Appearance Preparation',
    'Court Appearance',
    'Court Appearance Notes & Follow-up',
    'Mediation Meeting Preparation',
    'Mediation Meeting',
    'Mediation Meeting Notes & Follow-up',
    'Discovery Drafting',
    'Discovery Review',
    'Discovery Production',
    'Legal Research',
    'Settlement Preparation',
    'Settlement',
    'Settlement Notes & Follow-up',
    'Mediation Preparation',
    'Mediation',
    'Mediation Notes & Follow-up',
    'Meeting with Opposing Counsel Preparation',
    'Meeting with Opposing Counsel',
    'Meeting with Opposing Counsel Notes & Follow-up',
    'Phone Call Preparation',
    'Phone Call',
    'Phone Call Notes & Follow-up',
    'Prepare Report',
    'Review Report',
    'Team/Case Strategy Meeting Preparation',
    'Team/Case Strategy Meeting',
    'Team/Case Strategy Meeting Notes & Follow-up',
    'Trial Preparation',
    'Travel Time'
]

# CPCS specific categories (limited set)
CPCS_BILLING_CATEGORIES = [
    'Client Interview',
    'Conference With Attorney',
    'Court Appearance',
    'Examine/Test Material',
    'Phone Calls',
    'Prepare Report',
    'Review Documents',
    'Travel Time'
]

def validate_case_file_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate that the provided DataFrame conforms to the case file schema.
    
    Args:
        df: DataFrame to validate
        
    Returns:
        Dict with validation results
    """
    required_columns = [
        'type', 'date', 'title', 'from', 'to', 'cc', 'content',
        'attachments', 'synopsis', 'comments'
    ]
    
    # Optional billing columns
    billing_columns = ['billing-start', 'billing-stop', 'billing-hrs']
    
    validation = {
        "valid": True,
        "missing_columns": [],
        "invalid_types": [],
        "date_format_errors": [],
        "billing_category_errors": [],
        "sample_errors": []
    }
    
    # Check required columns
    for col in required_columns:
        if col not in df.columns:
            validation["valid"] = False
            validation["missing_columns"].append(col)
    
    if not validation["valid"]:
        return validation
    
    # Check entry types
    invalid_types = []
    for idx, entry_type in enumerate(df['type']):
        if pd.isna(entry_type) or entry_type not in VALID_ENTRY_TYPES:
            invalid_types.append({
                "row": idx + 2,  # +2 for 1-based indexing and header row
                "type": str(entry_type) if not pd.isna(entry_type) else "NA"
            })
    
    if invalid_types:
        validation["valid"] = False
        validation["invalid_types"] = invalid_types
    
    # Check billing categories for billing-type entries
    billing_category_errors = []
    for idx, row in df.iterrows():
        if row['type'] == 'billing-type':
            title = row['title']
            if not pd.isna(title) and ':' in title:
                category = title.split(':', 1)[1].strip()
                
                # Check if it's a valid category based on the client type
                is_cpcs = False  # Assume not CPCS by default, could add logic to determine
                
                if is_cpcs and category not in CPCS_BILLING_CATEGORIES:
                    billing_category_errors.append({
                        "row": idx + 2,
                        "category": category,
                        "allowed_categories": CPCS_BILLING_CATEGORIES
                    })
                elif not is_cpcs and category not in BILLING_CATEGORIES:
                    billing_category_errors.append({
                        "row": idx + 2,
                        "category": category,
                        "allowed_categories": "General billing categories"
                    })
    
    if billing_category_errors:
        validation["valid"] = False
        validation["billing_category_errors"] = billing_category_errors
    
    # Check date formats and billing time consistency
    date_errors = []
    for idx, row in df.iterrows():
        try:
            # Skip empty values
            if pd.isna(row['date']):
                continue
                
            # If already a datetime object, it's valid
            if not isinstance(row['date'], (pd.Timestamp, datetime)):
                # Try to parse the date string
                datetime.strptime(str(row['date']), "%m/%d/%Y %I:%M %p")
            
            # For billing-type entries, check time sequence logic
            if row['type'] == 'billing-type':
                billing_start = row.get('billing-start')
                billing_stop = row.get('billing-stop')
                billing_hrs = row.get('billing-hrs')
                
                # If billing times are provided, validate them
                if not pd.isna(billing_start) and not pd.isna(billing_stop):
                    # Convert to datetime if they're strings
                    if not isinstance(billing_start, (pd.Timestamp, datetime)):
                        start_dt = pd.to_datetime(billing_start)
                    else:
                        start_dt = billing_start
                        
                    if not isinstance(billing_stop, (pd.Timestamp, datetime)):
                        stop_dt = pd.to_datetime(billing_stop)
                    else:
                        stop_dt = billing_stop
                    
                    # Check sequence: start must be before stop
                    if start_dt >= stop_dt:
                        date_errors.append({
                            "row": idx + 2,
                            "issue": "billing_sequence",
                            "start": str(billing_start),
                            "stop": str(billing_stop)
                        })
                    
                    # If billing hours provided, check consistency
                    if not pd.isna(billing_hrs):
                        # Calculate duration in hours
                        duration = (stop_dt - start_dt).total_seconds() / 3600
                        
                        # Allow for small rounding differences (0.01 hours = 36 seconds)
                        if abs(duration - float(billing_hrs)) > 0.01:
                            date_errors.append({
                                "row": idx + 2,
                                "issue": "billing_duration",
                                "calculated": duration,
                                "provided": billing_hrs
                            })
        except (ValueError, TypeError) as e:
            date_errors.append({
                "row": idx + 2,
                "date": str(row['date']) if not pd.isna(row['date']) else "NA",
                "error": str(e)
            })
    
    if date_errors:
        validation["valid"] = False
        validation["date_format_errors"] = date_errors
    
    # Limit sample errors to avoid overwhelming output
    validation["sample_errors"] = {
        "invalid_types": invalid_types[:5] if invalid_types else [],
        "date_format_errors": date_errors[:5] if date_errors else [],
        "billing_category_errors": billing_category_errors[:5] if billing_category_errors else []
    }
    
    return validation

def validate_time_sequence(start_time: Union[str, datetime], 
                          stop_time: Union[str, datetime]) -> Dict[str, Any]:
    """
    Validate that the start time is before the stop time
    
    Args:
        start_time: Start time (string or datetime)
        stop_time: Stop time (string or datetime)
        
    Returns:
        Dict with validation results
    """
    validation = {
        "valid": True,
        "error": None
    }
    
    try:
        # Convert to datetime if string
        if isinstance(start_time, str):
            start_dt = pd.to_datetime(start_time)
        else:
            start_dt = start_time
            
        if isinstance(stop_time, str):
            stop_dt = pd.to_datetime(stop_time)
        else:
            stop_dt = stop_time
        
        # Check sequence
        if start_dt >= stop_dt:
            validation["valid"] = False
            validation["error"] = "Start time must be before stop time"
    except Exception as e:
        validation["valid"] = False
        validation["error"] = f"Error parsing times: {str(e)}"
    
    return validation

def check_billing_duration(start_time: Union[str, datetime], 
                          stop_time: Union[str, datetime],
                          billing_hours: float) -> Dict[str, Any]:
    """
    Check that the billing hours matches the duration between start and stop times
    
    Args:
        start_time: Start time (string or datetime)
        stop_time: Stop time (string or datetime)
        billing_hours: Billing hours value
        
    Returns:
        Dict with check results
    """
    validation = {
        "valid": True,
        "calculated_hours": None,
        "difference": None,
        "error": None
    }
    
    try:
        # Convert to datetime if string
        if isinstance(start_time, str):
            start_dt = pd.to_datetime(start_time)
        else:
            start_dt = start_time
            
        if isinstance(stop_time, str):
            stop_dt = pd.to_datetime(stop_time)
        else:
            stop_dt = stop_time
        
        # Calculate duration in hours
        calculated_hours = (stop_dt - start_dt).total_seconds() / 3600
        validation["calculated_hours"] = calculated_hours
        
        # Compare with provided billing hours (allow for small rounding differences)
        difference = abs(calculated_hours - billing_hours)
        validation["difference"] = difference
        
        if difference > 0.01:  # 0.01 hours = 36 seconds
            validation["valid"] = False
            validation["error"] = f"Duration mismatch: calculated={calculated_hours:.2f}, provided={billing_hours}"
    except Exception as e:
        validation["valid"] = False
        validation["error"] = f"Error calculating duration: {str(e)}"
    
    return validation