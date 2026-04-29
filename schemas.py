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

def parse_transfer(line):
    """Turn a transfer row into a contact/admission event."""
    try:
        row = next(csv.reader([line]))

        if len(row) < 7:
            raise ValueError(f"Expected 7 columns, got {len(row)}")
        if not row[0] or not row[0].isdigit():
            raise ValueError("Invalid subject_id")
        if not row[3]:
            raise ValueError("Missing eventtype")
        if not row[5]:
            raise ValueError("Missing intime")

        yield {
            "subject_id": row[0].strip(),
            "hadm_id": row[1].strip(),
            "transfer_id": row[2].strip(),
            "eventtype": row[3].strip(),
            "careunit": row[4].strip(),
            "intime": row[5].strip(),
            "outtime": row[6].strip(),
        }
    except Exception as e:
        yield beam.pvalue.TaggedOutput("invalid", f"Error: {e} | Line: {line}")


def parse_procedure(line):
    """Pull out a patient procedure code."""
    try:
        row = next(csv.reader([line]))

        if len(row) < 6:
            raise ValueError(f"Expected 6 columns, got {len(row)}")
        if not row[0] or not row[0].isdigit():
            raise ValueError("Invalid subject_id")
        if not row[1]:
            raise ValueError("Missing hadm_id")
        if not row[4]:
            raise ValueError("Missing icd_code")
        if row[5] not in ["9", "10"]:
            raise ValueError(f"Invalid icd_version: {row[5]}")

        yield {
            "subject_id": row[0].strip(),
            "hadm_id": row[1].strip(),
            "icd_code": row[4].strip(),
            "icd_version": row[5].strip(),
        }
    except Exception as e:
        yield beam.pvalue.TaggedOutput("invalid", f"Error: {e} | Line: {line}")


def parse_prescription(line):
    """Pull out a patient medication order."""
    try:
        row = next(csv.reader([line]))

        if len(row) < 10:
            raise ValueError(f"Expected at least 10 columns, got {len(row)}")
        if not row[0] or not row[0].isdigit():
            raise ValueError("Invalid subject_id")
        if not row[9]:
            raise ValueError("Missing drug")

        yield {
            "subject_id": row[0].strip(),
            "hadm_id": row[1].strip(),
            "drug": row[9].strip(),
            "drug_normalized": row[9].strip().lower(),
        }
    except Exception as e:
        yield beam.pvalue.TaggedOutput("invalid", f"Error: {e} | Line: {line}")


def parse_lab_event(line):
    """Return numeric lab history rows for patient-level trends."""
    try:
        row = next(csv.reader([line]))

        if len(row) < 16:
            raise ValueError(f"Expected 16 columns, got {len(row)}")
        if not row[1] or not row[1].isdigit():
            raise ValueError("Invalid subject_id")
        if not row[4]:
            raise ValueError("Missing itemid")
        if not row[6]:
            raise ValueError("Missing charttime")
        if not row[9]:
            raise ValueError("Missing valuenum")

        yield {
            "subject_id": row[1].strip(),
            "hadm_id": row[2].strip(),
            "itemid": row[4].strip(),
            "charttime": row[6].strip(),
            "value": row[8].strip(),
            "valuenum": float(row[9]),
            "valueuom": row[10].strip(),
            "flag": row[13].strip(),
        }
    except Exception as e:
        yield beam.pvalue.TaggedOutput("invalid", f"Error: {e} | Line: {line}")


def parse_lab_lookup(line):
    """Return itemid -> lab label metadata."""
    try:
        row = next(csv.reader([line]))

        if len(row) < 4:
            raise ValueError(f"Expected 4 columns, got {len(row)}")
        if not row[0]:
            raise ValueError("Missing itemid")
        if not row[1]:
            raise ValueError("Missing label")

        yield (row[0].strip(), {
            "label": row[1].strip(),
            "fluid": row[2].strip(),
            "category": row[3].strip(),
        })
    except Exception as e:
        yield beam.pvalue.TaggedOutput("invalid", f"Error: {e} | Line: {line}")
