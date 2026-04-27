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

        if len(row) < 6:
            raise ValueError(f"Expected 6 columns, got {len(row)}")

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
    """Pull out subject_id, ICD code, and ICD version, with DQ validation."""
    try:
        row = next(csv.reader([line]))

        if len(row) < 5:
            raise ValueError(f"Expected 5 columns, got {len(row)}")

        if not row[0] or not row[0].isdigit():
            raise ValueError("Invalid subject_id")
        if not row[3]:
            raise ValueError("Missing icd_code")
        if row[4] not in ["9", "10"]:
            raise ValueError(f"Invalid icd_version: {row[4]}")
            
        diagnosis = {
            "subject_id": row[0],
            "icd_code": row[3].strip(),
            "icd_version": row[4].strip()
        }
        yield diagnosis
    except Exception as e:
        yield beam.pvalue.TaggedOutput("invalid", f"Error: {e} | Line: {line}")

def parse_lookup(line):
    """Return ((icd_code, icd_version), long_title), with DQ validation."""
    try:
        row = next(csv.reader([line]))

        if len(row) < 3:
            raise ValueError(f"Expected 3 columns, got {len(row)}")
        if not row[0]:
            raise ValueError("Missing icd_code")
        if row[1] not in ["9", "10"]:
            raise ValueError(f"Invalid icd_version: {row[1]}")
        if not row[2]:
            raise ValueError("Missing long_title")

        yield ((row[0].strip(), row[1].strip()), row[2].strip())
    except Exception as e:
        yield beam.pvalue.TaggedOutput("invalid", f"Error: {e} | Line: {line}")

def format_top_diagnosis(kv, lookup_dict):
    """Format one top-diagnosis entry into a readable string."""
    (code, version), count = kv
    name = lookup_dict.get((code, version), "Unknown")
    return f"    {code:>8s}  |  ICD-{version:<2s}  |  {count:>4d} times  |  {name}"