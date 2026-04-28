"""
Pipeline module.

Defines the Beam graph: read -> transform -> write.
"""
import logging
import datetime
import apache_beam as beam
from config import BQ_SUMMARY_TABLE, BQ_TOP_DIAG_TABLE
from schemas import parse_patient, parse_diagnosis, parse_lookup, format_top_diagnosis
from transforms import CountFn, AverageFn, MinMaxFn

def build_pipeline(p, input_paths):
    """Builds the profiling pipeline with patient-level diagnosis deduplication and BQ sinking."""

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
        p | "Read Lookup" >> beam.io.ReadFromText(input_paths["lookup"], skip_header_lines=1)
          | "Parse Lookup" >> beam.FlatMap(parse_lookup).with_outputs("invalid")
    )
    lookup = lookup_parsed[None]

    # --- 2. CALCULATE METRICS ---
    total_patients = patients | "Count Patients" >> beam.CombineGlobally(CountFn())
    total_diagnoses = diagnoses | "Count Unique Diagnoses" >> beam.CombineGlobally(CountFn())
    unique_codes = (
        diagnoses
        | "Get Codes" >> beam.Map(lambda d: (d["icd_code"], d["icd_version"]))
        | "Distinct Codes" >> beam.Distinct()
        | "Count Codes" >> beam.CombineGlobally(CountFn())
    )

    gender_counts = (
        patients | "Get Gender" >> beam.Map(lambda p: (p["gender"], 1))
                 | "Sum Gender" >> beam.CombinePerKey(sum)
    )

    ages = patients | "Get Age" >> beam.Map(lambda p: p["age"])
    age_avg = ages | "Avg Age" >> beam.CombineGlobally(AverageFn())
    age_range = ages | "MinMax Age" >> beam.CombineGlobally(MinMaxFn())

    top_10_raw = (
        diagnoses
        | "Diag KV" >> beam.Map(lambda d: ((d["icd_code"], d["icd_version"]), 1))
        | "Sum Codes" >> beam.CombinePerKey(sum)
        | "Top 10 Raw" >> beam.combiners.Top.Of(10, key=lambda kv: (kv[1], kv[0][0], kv[0][1]))
        | "Flatten Top" >> beam.FlatMap(lambda x: x)
    )

    diag_per_patient = (
        diagnoses | "Diag Per Patient" >> beam.Map(lambda d: (d["subject_id"], 1))
                  | "Sum Per Patient" >> beam.CombinePerKey(sum)
                  | "Values Only" >> beam.Values()
                  | "Avg Diag" >> beam.CombineGlobally(AverageFn())
    )

    alive = patients | "Filt Alive" >> beam.Filter(lambda p: p["dod"] == "") | "Cnt Alive" >> beam.CombineGlobally(CountFn())
    deceased = patients | "Filt Dec" >> beam.Filter(lambda p: p["dod"] != "") | "Cnt Dec" >> beam.CombineGlobally(CountFn())

    # --- 3. FORMAT FOR BIGQUERY ---
    def prepare_summary(element, tp, td, uc, gc_list, aa, ar, dpp, al, dec):
        # Convert gender counts list to a dictionary for easier access
        gc_dict = dict(gc_list)
        return [{
            "timestamp": datetime.datetime.now().isoformat(),
            "total_patients": tp,
            "total_diagnoses": td,
            "unique_codes": uc,
            "avg_age": aa,
            "min_age": int(ar["min"]),
            "max_age": int(ar["max"]),
            "avg_diag_per_patient": dpp,
            "female_count": gc_dict.get('F', 0),
            "male_count": gc_dict.get('M', 0),
            "alive_count": al,
            "deceased_count": dec,
            "mortality_rate": round(dec / (al + dec) * 100, 2) if (al + dec) else 0
        }]

    summary_bq = (
        p | "Trigger Summary" >> beam.Create([None])
          | "Prepare BQ Summary" >> beam.FlatMap(
              prepare_summary,
              tp=beam.pvalue.AsSingleton(total_patients),
              td=beam.pvalue.AsSingleton(total_diagnoses),
              uc=beam.pvalue.AsSingleton(unique_codes),
              gc_list=beam.pvalue.AsList(gender_counts),
              aa=beam.pvalue.AsSingleton(age_avg),
              ar=beam.pvalue.AsSingleton(age_range),
              dpp=beam.pvalue.AsSingleton(diag_per_patient),
              al=beam.pvalue.AsSingleton(alive),
              dec=beam.pvalue.AsSingleton(deceased)
          )
    )

    def prepare_top_bq(kv, lookup_dict):
        (code, version), count = kv
        name = lookup_dict.get((code, version), "Unknown")
        return {
            "timestamp": datetime.datetime.now().isoformat(),
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

    # --- 4. SINK TO BIGQUERY ---
    logging.info("Configuring unconditional BigQuery sinks for summary and top diagnoses.")
    
    summary_schema = (
        "timestamp:TIMESTAMP,total_patients:INTEGER,total_diagnoses:INTEGER,unique_codes:INTEGER,"
        "avg_age:FLOAT,min_age:INTEGER,max_age:INTEGER,avg_diag_per_patient:FLOAT,"
        "female_count:INTEGER,male_count:INTEGER,alive_count:INTEGER,deceased_count:INTEGER,mortality_rate:FLOAT"
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
