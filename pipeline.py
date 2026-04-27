"""
Pipeline module.

Defines the Beam graph: read -> transform -> write.
"""
import apache_beam as beam
from config import (PATIENTS_CSV,DIAGNOSES_CSV,LOOKUP_CSV,DLQ_PATIENTS_FILE,DLQ_DIAGNOSES_FILE,DLQ_LOOKUP_FILE)
from schemas import parse_patient, parse_diagnosis, parse_lookup, format_top_diagnosis
from transforms import CountFn, AverageFn, MinMaxFn

def build_pipeline(p, output_file):
    """Builds the full profiling pipeline and writes results."""

    # Read and parse the three CSV files.
    patients_parsed = (
        p
        | "Read Patients" >> beam.io.ReadFromText(PATIENTS_CSV, skip_header_lines=1)
        | "Parse Patient" >> beam.FlatMap(parse_patient).with_outputs("invalid")
    )
    patients = patients_parsed[None]
    dlq_patients = patients_parsed.invalid

    dlq_patients | "Write DLQ Patients" >> beam.io.WriteToText(DLQ_PATIENTS_FILE, shard_name_template="")

    diagnoses_parsed = (
        p
        | "Read Diagnoses" >> beam.io.ReadFromText(DIAGNOSES_CSV, skip_header_lines=1)
        | "Parse Diagnosis" >> beam.FlatMap(parse_diagnosis).with_outputs("invalid")
    )
    diagnoses = diagnoses_parsed[None]
    dlq_diagnoses = diagnoses_parsed.invalid

    dlq_diagnoses | "Write DLQ Diagnoses" >> beam.io.WriteToText(DLQ_DIAGNOSES_FILE, shard_name_template="")

    lookup_parsed = (
        p
        | "Read Lookup" >> beam.io.ReadFromText(LOOKUP_CSV, skip_header_lines=1)
        | "Parse Lookup" >> beam.FlatMap(parse_lookup).with_outputs("invalid")
    )
    lookup = lookup_parsed[None]
    dlq_lookup = lookup_parsed.invalid

    dlq_lookup | "Write DLQ Lookup" >> beam.io.WriteToText(DLQ_LOOKUP_FILE, shard_name_template="")

    # Section 1: Dataset overview.
    total_patients = patients | "Count Patients" >> beam.CombineGlobally(CountFn())
    total_diagnoses = diagnoses | "Count Diagnoses" >> beam.CombineGlobally(CountFn())

    unique_codes = (
        diagnoses
        | "Get Code Version" >> beam.Map(lambda d: (d["icd_code"], d["icd_version"]))
        | "Deduplicate Codes" >> beam.Distinct()
        | "Count Unique Codes" >> beam.CombineGlobally(CountFn())
    )

    # Section 2: Gender distribution.
    gender_counts = (
        patients
        | "Get Gender" >> beam.Map(lambda p: (p["gender"], 1))
        | "Sum Genders" >> beam.CombinePerKey(sum)
    )

    # Section 3: Age stats.
    ages = patients | "Get Age" >> beam.Map(lambda p: p["age"])
    age_avg = ages | "Avg Age" >> beam.CombineGlobally(AverageFn())
    age_range = ages | "MinMax Age" >> beam.CombineGlobally(MinMaxFn())

    # Section 4: Top 10 diagnoses.
    top_10 = (
        diagnoses
        | "Diag Code Version" >> beam.Map(lambda d: ((d["icd_code"], d["icd_version"]), 1))
        | "Count Per Code" >> beam.CombinePerKey(sum)
        | "Top 10" >> beam.combiners.Top.Of(10, key=lambda kv: (kv[1], kv[0][0], kv[0][1]))
        | "Flatten Top" >> beam.FlatMap(lambda x: x)
        | "Format Top" >> beam.Map(
            format_top_diagnosis,
            lookup_dict=beam.pvalue.AsDict(lookup),
        )
    )

    # Section 5: Average diagnoses per patient.
    diag_per_patient = (
        diagnoses
        | "Diag Per Patient KV" >> beam.Map(lambda d: (d["subject_id"], 1))
        | "Sum Per Patient" >> beam.CombinePerKey(sum)
        | "Get Counts Only" >> beam.Map(lambda kv: kv[1])
        | "Avg Diag Per Patient" >> beam.CombineGlobally(AverageFn())
    )

    # Section 6: Mortality.
    alive = (
        patients
        | "Filter Alive" >> beam.Filter(lambda p: p["dod"] == "")
        | "Count Alive" >> beam.CombineGlobally(CountFn())
    )
    deceased = (
        patients
        | "Filter Deceased" >> beam.Filter(lambda p: p["dod"] != "")
        | "Count Deceased" >> beam.CombineGlobally(CountFn())
    )

    def build_report(_, tp, td, uc, genders, aa, ar, top, dpp, al, dec):
        """Combine all computed stats into one text report."""
        lines = []
        lines.append("=" * 60)
        lines.append("        DATA PROFILING REPORT  -  MIMIC-IV Demo")
        lines.append("=" * 60)

        lines.append("")
        lines.append("1. DATASET OVERVIEW")
        lines.append(f"  Total patients                 : {tp}")
        lines.append(f"  Total diagnosis records        : {td}")
        lines.append(f"  Unique ICD code/version pairs  : {uc}")

        lines.append("")
        lines.append("2. GENDER DISTRIBUTION")
        for gender, count in sorted(genders):
            label = "Female" if gender == "F" else "Male"
            lines.append(f"  {label:8s}: {count}")

        lines.append("")
        lines.append("3. AGE DISTRIBUTION")
        lines.append(f"  Minimum age : {ar['min']}")
        lines.append(f"  Maximum age : {ar['max']}")
        lines.append(f"  Average age : {aa}")

        lines.append("")
        lines.append("4. TOP 10 DIAGNOSES")
        lines.append(f"    {'Code':>8s}  |  {'Version':>7s}  |  {'Freq':>4s}       |  Description")
        lines.append("    " + "-" * 72)
        for row in top:
            lines.append(row)

        lines.append("")
        lines.append("5. DIAGNOSES PER PATIENT")
        lines.append(f"  Average diagnoses per patient: {dpp}")

        lines.append("")
        lines.append("6. MORTALITY INSIGHT")
        lines.append(f"  Alive    : {al}")
        lines.append(f"  Deceased : {dec}")
        pct = round(dec / (al + dec) * 100, 1) if (al + dec) else 0
        lines.append(f"  Mortality rate: {pct}%")

        lines.append("")
        lines.append("=" * 60)
        return lines

    report = (
        p
        | "Start" >> beam.Create(["trigger"])
        | "Build Report" >> beam.FlatMap(
            build_report,
            tp=beam.pvalue.AsSingleton(total_patients),
            td=beam.pvalue.AsSingleton(total_diagnoses),
            uc=beam.pvalue.AsSingleton(unique_codes),
            genders=beam.pvalue.AsList(gender_counts),
            aa=beam.pvalue.AsSingleton(age_avg),
            ar=beam.pvalue.AsSingleton(age_range),
            top=beam.pvalue.AsList(top_10),
            dpp=beam.pvalue.AsSingleton(diag_per_patient),
            al=beam.pvalue.AsSingleton(alive),
            dec=beam.pvalue.AsSingleton(deceased),
        )
    )

    report | "Print" >> beam.Map(print)
    report | "Write Report" >> beam.io.WriteToText(output_file, shard_name_template="")