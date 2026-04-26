"""
Schemas module.

Contains data models, parsing helpers, and data formatting functions.
"""
import csv

import apache_beam as beam

def parse_patient(line):
    """Turn a CSV line into a patient dictionary, with DQ validation."""
    try:
        row = next(csv.reader([line]))
        
        # Validation checks
        if not row[0] or not row[0].isdigit():
            raise ValueError("Invalid subject_id")
            
        gender = row[1].strip().upper()
        if gender not in ["M", "F"]:
            raise ValueError(f"Invalid gender: {gender}")
            
        age = int(row[2])
        if age < 0 or age > 120:
            raise ValueError(f"Invalid age: {age}")
            
        patient = {
            "subject_id": row[0],
            "gender":     gender,
            "age":        age,
            "dod":        row[5].strip()
        }
        yield patient
    except Exception as e:
        # Route to Dead-Letter Queue
        yield beam.pvalue.TaggedOutput("invalid", f"Error: {e} | Line: {line}")

def parse_diagnosis(line):
    """Pull out subject_id and icd_code from a diagnosis row, with DQ validation."""
    try:
        row = next(csv.reader([line]))
        
        if not row[0] or not row[0].isdigit():
            raise ValueError("Invalid subject_id")
        if not row[3]:
            raise ValueError("Missing icd_code")
            
        diagnosis = {"subject_id": row[0], "icd_code": row[3].strip()}
        yield diagnosis
    except Exception as e:
        yield beam.pvalue.TaggedOutput("invalid", f"Error: {e} | Line: {line}")

def parse_lookup(line):
    """Return (icd_code, long_title) for the lookup dictionary."""
    row = next(csv.reader([line]))
    return (row[0], row[2])

def format_top_diagnosis(kv, lookup_dict):
    """Format one top-diagnosis entry into a readable string."""
    code, count = kv
    name = lookup_dict.get(code, "Unknown")
    return f"    {code:>8s}  |  {count:>4d} times  |  {name}"
