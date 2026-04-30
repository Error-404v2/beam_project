"""
Pipeline module.

Defines the Beam graph: read -> transform -> write.
"""
import logging
import datetime
import json
import apache_beam as beam
from config import (
    BQ_ADMISSIONS_TIMELINE_TABLE,
    BQ_LAB_HISTORY_TABLE,
    BQ_PATIENT_DASHBOARD_TABLE,
    BQ_SUMMARY_TABLE,
    BQ_TOP_DIAG_TABLE,
)
from schemas import (
    parse_diagnosis,
    parse_lab_event,
    parse_lab_lookup,
    parse_lookup,
    parse_patient,
    parse_prescription,
    parse_procedure,
    parse_transfer,
)
from transforms import CountFn, AverageFn, MinMaxFn

RUN_TIMESTAMP = datetime.datetime.utcnow().isoformat()
LOCAL_RUN_ID = RUN_TIMESTAMP.replace("-", "").replace(":", "").replace(".", "")


def first_or_default(values, default=0):
    """Return the first grouped singleton value, or a default."""
    return values[0] if values else default


def build_pipeline(p, input_paths):
    """Build the profiling and patient-dashboard pipeline."""

    # --- 1. READ & PARSE ---
    patients_parsed = (
        p | "Read Patients" >> beam.io.ReadFromText(input_paths["patients"], skip_header_lines=1)
          | "Parse Patient" >> beam.FlatMap(parse_patient).with_outputs("invalid")
    )
    patients = patients_parsed[None]
    
    diagnoses_raw = (
        p | "Read Diagnoses" >> beam.io.ReadFromText(input_paths["diagnoses"], skip_header_lines=1)
          | "Parse Diagnosis" >> beam.FlatMap(parse_diagnosis).with_outputs("invalid")
    )[None]

    # Deduplicate Diagnoses per Patient
    diagnoses = (
        diagnoses_raw
        | "Extract Patient-Diag Pair" >> beam.Map(lambda d: (d["subject_id"], d["icd_code"], d["icd_version"]))
        | "Deduplicate Per Patient" >> beam.Distinct()
        | "Reconstruct Dict" >> beam.Map(lambda t: {"subject_id": t[0], "icd_code": t[1], "icd_version": t[2]})
    )

    lookup_parsed = (
        p | "Read Diagnosis Lookup" >> beam.io.ReadFromText(input_paths["diagnosis_lookup"], skip_header_lines=1)
          | "Parse Lookup" >> beam.FlatMap(parse_lookup).with_outputs("invalid")
    )
    lookup = lookup_parsed[None]

    transfers = (
        p | "Read Transfers" >> beam.io.ReadFromText(input_paths["transfers"], skip_header_lines=1)
          | "Parse Transfers" >> beam.FlatMap(parse_transfer).with_outputs("invalid")
    )[None]

    procedures = (
        p | "Read Procedures" >> beam.io.ReadFromText(input_paths["procedures"], skip_header_lines=1)
          | "Parse Procedures" >> beam.FlatMap(parse_procedure).with_outputs("invalid")
    )[None]

    prescriptions = (
        p | "Read Prescriptions" >> beam.io.ReadFromText(input_paths["prescriptions"], skip_header_lines=1)
          | "Parse Prescriptions" >> beam.FlatMap(parse_prescription).with_outputs("invalid")
    )[None]

    lab_events = (
        p | "Read Lab Events" >> beam.io.ReadFromText(input_paths["labs"], skip_header_lines=1)
          | "Parse Numeric Lab Events" >> beam.FlatMap(parse_lab_event).with_outputs("invalid")
    )[None]

    lab_lookup = (
        p | "Read Lab Lookup" >> beam.io.ReadFromText(input_paths["lab_lookup"], skip_header_lines=1)
          | "Parse Lab Lookup" >> beam.FlatMap(parse_lab_lookup).with_outputs("invalid")
    )[None]

    # --- 2. CALCULATE METRICS ---
    total_diagnoses = diagnoses | "Count Unique Diagnoses" >> beam.CombineGlobally(CountFn())
    unique_codes = (
        diagnoses
        | "Get Codes" >> beam.Map(lambda d: (d["icd_code"], d["icd_version"]))
        | "Distinct Codes" >> beam.Distinct()
        | "Count Codes" >> beam.CombineGlobally(CountFn())
    )

    top_10_raw = (
        diagnoses
        | "Diag KV" >> beam.Map(lambda d: ((d["icd_code"], d["icd_version"]), 1))
        | "Sum Codes" >> beam.CombinePerKey(sum)
        | "Top 10 Raw" >> beam.combiners.Top.Of(10, key=lambda kv: (kv[1], kv[0][0], kv[0][1]))
        | "Flatten Top" >> beam.FlatMap(lambda x: x)
    )

    # --- 3. FORMAT FOR BIGQUERY ---
    def prepare_summary(element, td, uc):
        stats = [
            ("total_diagnoses", float(td)),
            ("unique_codes", float(uc)),
        ]
        return [
            {"timestamp": RUN_TIMESTAMP, "stat_name": name, "stat_value": value}
            for name, value in stats
        ]

    summary_bq = (
        p | "Trigger Summary" >> beam.Create([None])
          | "Prepare BQ Summary" >> beam.FlatMap(
              prepare_summary,
              td=beam.pvalue.AsSingleton(total_diagnoses),
              uc=beam.pvalue.AsSingleton(unique_codes)
          )
    )

    def prepare_top_bq(kv, lookup_dict):
        (code, version), count = kv
        name = lookup_dict.get((code, version), "Unknown")
        return {
            "timestamp": RUN_TIMESTAMP,
            "icd_code": code,
            "icd_version": version,
            "count": count,
            "description": name
        }

    top_10_bq = (
        top_10_raw | "Prepare BQ Top 10" >> beam.Map(
            prepare_top_bq,
            lookup_dict=beam.pvalue.AsDict(lookup)
        )
    )

    # --- 4. PATIENT-CENTRIC DASHBOARD MARTS ---
    patient_by_id = patients | "Patient KV" >> beam.Map(lambda p: (p["subject_id"], p))

    admission_pairs = (
        transfers
        | "Transfer Admission Pair" >> beam.Filter(lambda t: t["hadm_id"] != "")
        | "Distinct Patient Admission" >> beam.Map(lambda t: (t["subject_id"], t["hadm_id"]))
        | "Unique Patient Admissions" >> beam.Distinct()
    )

    admission_counts = (
        admission_pairs
        | "Admission Count One" >> beam.Map(lambda pair: (pair[0], 1))
        | "Admission Count Per Patient" >> beam.CombinePerKey(sum)
    )

    first_contacts = (
        transfers
        | "Patient Contact Time" >> beam.Map(lambda t: (t["subject_id"], t["intime"]))
        | "First Contact Per Patient" >> beam.CombinePerKey(min)
    )

    procedure_counts = (
        procedures
        | "Procedure Identity" >> beam.Map(
            lambda proc: (proc["subject_id"], proc["hadm_id"], proc["icd_code"], proc["icd_version"])
        )
        | "Unique Procedures" >> beam.Distinct()
        | "Procedure Count One" >> beam.Map(lambda proc: (proc[0], 1))
        | "Procedure Count Per Patient" >> beam.CombinePerKey(sum)
    )

    med_counts = (
        prescriptions
        | "Medication Identity" >> beam.Map(lambda med: (med["subject_id"], med["drug_normalized"]))
        | "Unique Medications" >> beam.Distinct()
        | "Medication Count One" >> beam.Map(lambda med: (med[0], 1))
        | "Medication Count Per Patient" >> beam.CombinePerKey(sum)
    )

    diagnosis_counts = (
        diagnoses
        | "Diagnosis Count One" >> beam.Map(lambda diag: (diag["subject_id"], 1))
        | "Diagnosis Count Per Patient" >> beam.CombinePerKey(sum)
    )

    def prepare_patient_dashboard(kv):
        subject_id, grouped = kv
        if not grouped["patient"]:
            return []

        patient = first_or_default(grouped["patient"], {})
        dod = patient.get("dod", "")
        return [{
            "subject_id": subject_id,
            "gender": patient.get("gender", ""),
            "age": patient.get("age", 0),
            "is_alive": dod == "",
            "dod": dod or None,
            "first_contact": first_or_default(grouped["first_contact"], ""),
            "admission_count": first_or_default(grouped["admission_count"]),
            "procedure_count": first_or_default(grouped["procedure_count"]),
            "unique_med_count": first_or_default(grouped["med_count"]),
            "unique_diagnosis_count": first_or_default(grouped["diagnosis_count"]),
        }]

    patient_dashboard = (
        {
            "patient": patient_by_id,
            "first_contact": first_contacts,
            "admission_count": admission_counts,
            "procedure_count": procedure_counts,
            "med_count": med_counts,
            "diagnosis_count": diagnosis_counts,
        }
        | "Join Patient Dashboard Metrics" >> beam.CoGroupByKey()
        | "Prepare Patient Dashboard Rows" >> beam.FlatMap(prepare_patient_dashboard)
    )

    transfer_by_admission = (
        transfers
        | "Transfers With Admission" >> beam.Filter(lambda t: t["hadm_id"] != "")
        | "Admission Transfer KV" >> beam.Map(lambda t: ((t["subject_id"], t["hadm_id"]), t["intime"]))
        | "Admission Transfer Times" >> beam.GroupByKey()
    )

    def prepare_admission_timeline(kv):
        (subject_id, hadm_id), times_iter = kv
        times = list(times_iter)
        first_contact = min(times)
        return {
            "subject_id": subject_id,
            "hadm_id": hadm_id,
            "first_contact": first_contact,
            "admission_year": int(first_contact[:4]),
            "transfer_event_count": len(times),
        }

    admissions_timeline = (
        transfer_by_admission
        | "Prepare Admission Timeline Rows" >> beam.Map(prepare_admission_timeline)
    )

    def prepare_lab_history(kv):
        itemid, grouped = kv
        metadata = first_or_default(grouped["lookup"], {})
        for lab in grouped["lab"]:
            yield {
                "subject_id": lab["subject_id"],
                "hadm_id": lab["hadm_id"],
                "itemid": itemid,
                "lab_label": metadata.get("label", "Unknown"),
                "lab_fluid": metadata.get("fluid", ""),
                "lab_category": metadata.get("category", ""),
                "charttime": lab["charttime"],
                "value": lab["value"],
                "valuenum": lab["valuenum"],
                "valueuom": lab["valueuom"],
                "is_abnormal": lab["flag"].lower() == "abnormal",
                "flag": lab["flag"],
            }

    lab_history = (
        {
            "lab": lab_events | "Lab Events By Item" >> beam.Map(lambda lab: (lab["itemid"], lab)),
            "lookup": lab_lookup,
        }
        | "Join Lab Events Lookup" >> beam.CoGroupByKey()
        | "Prepare Lab History Rows" >> beam.FlatMap(prepare_lab_history)
    )

    # --- 5. SINK OUTPUTS ---
    logging.info("Configuring sinks for profiling and patient dashboard outputs.")
    
    summary_schema = "timestamp:TIMESTAMP,stat_name:STRING,stat_value:FLOAT"

    patient_dashboard_schema = (
        "subject_id:STRING,gender:STRING,age:INTEGER,is_alive:BOOLEAN,dod:DATE,"
        "first_contact:TIMESTAMP,admission_count:INTEGER,procedure_count:INTEGER,"
        "unique_med_count:INTEGER,unique_diagnosis_count:INTEGER"
    )

    admissions_timeline_schema = (
        "subject_id:STRING,hadm_id:STRING,first_contact:TIMESTAMP,"
        "admission_year:INTEGER,transfer_event_count:INTEGER"
    )

    lab_history_schema = (
        "subject_id:STRING,hadm_id:STRING,itemid:STRING,lab_label:STRING,lab_fluid:STRING,"
        "lab_category:STRING,charttime:TIMESTAMP,value:STRING,valuenum:FLOAT,valueuom:STRING,"
        "is_abnormal:BOOLEAN,flag:STRING"
    )

    summary_bq | "Write Summary to BQ" >> beam.io.WriteToBigQuery(
        BQ_SUMMARY_TABLE,
        schema=summary_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    top_10_bq | "Write Top Diags to BQ" >> beam.io.WriteToBigQuery(
        BQ_TOP_DIAG_TABLE,
        schema="timestamp:TIMESTAMP,icd_code:STRING,icd_version:STRING,count:INTEGER,description:STRING",
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    patient_dashboard | "Write Patient Dashboard to BQ" >> beam.io.WriteToBigQuery(
        BQ_PATIENT_DASHBOARD_TABLE,
        schema=patient_dashboard_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    admissions_timeline | "Write Admissions Timeline to BQ" >> beam.io.WriteToBigQuery(
        BQ_ADMISSIONS_TIMELINE_TABLE,
        schema=admissions_timeline_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    lab_history | "Write Lab History to BQ" >> beam.io.WriteToBigQuery(
        BQ_LAB_HISTORY_TABLE,
        schema=lab_history_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )
