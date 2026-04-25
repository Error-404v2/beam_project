"""
Schemas module.

Contains data models, parsing helpers, and data formatting functions.
"""
import csv

def parse_patient(line):
    """Turn a CSV line into a patient dictionary."""
    row = next(csv.reader([line]))
    return {
        "subject_id": row[0],
        "gender":     row[1],
        "age":        int(row[2]),
        "dod":        row[5]       # empty string = alive
    }

def parse_diagnosis(line):
    """Pull out subject_id and icd_code from a diagnosis row."""
    row = next(csv.reader([line]))
    return {"subject_id": row[0], "icd_code": row[3]}

def parse_lookup(line):
    """Return (icd_code, long_title) for the lookup dictionary."""
    row = next(csv.reader([line]))
    return (row[0], row[2])

def format_top_diagnosis(kv, lookup_dict):
    """Format one top-diagnosis entry into a readable string."""
    code, count = kv
    name = lookup_dict.get(code, "Unknown")
    return f"    {code:>8s}  |  {count:>4d} times  |  {name}"
