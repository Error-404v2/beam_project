# Hospital Data Profiling with Apache Beam

A batch data pipeline built with **Apache Beam**. It reads hospital records from the
[MIMIC-IV Clinical Database Demo](https://physionet.org/content/mimic-iv-demo/2.2/)
and generates a plain-text data profiling report.

## What It Does

The project contains one profiling pipeline:

| Entry Point | Output | Purpose |
| --- | --- | --- |
| `main.py` | `output/profiling_report.txt` | Generates a six-section profiling report for the MIMIC-IV demo hospital data |

### Profiling Report Sections

1. **Dataset Overview** - total patients, diagnosis records, and unique ICD code/version pairs
2. **Gender Distribution** - count of male vs female patients
3. **Age Distribution** - minimum, maximum, and average patient age
4. **Top 10 Diagnoses** - most frequent ICD code/version pairs with readable names
5. **Diagnoses per Patient** - average number of diagnoses per patient
6. **Mortality Insight** - alive vs deceased count and mortality rate

## Project Structure

```text
main.py                 # Entry point for the profiling pipeline
pipeline.py             # Apache Beam graph
schemas.py              # CSV parsers and output formatting helpers
transforms.py           # Reusable CombineFn implementations
config.py               # Input and output paths
requirements.txt        # Python dependencies
input/data/             # MIMIC-IV demo CSV files
output/                 # Generated reports and dead-letter queue files
```

## Dataset

The pipeline uses the **MIMIC-IV Clinical Database Demo v2.2**, which contains
de-identified hospital records for 100 patients. It reads three CSV files:

- `patients.csv` - patient demographics: gender, anchor age, and date of death
- `diagnoses_icd.csv` - diagnosis records linked to hospital admissions
- `d_icd_diagnoses.csv` - lookup table mapping ICD code/version pairs to readable names

ICD code and ICD version are treated together as the lookup key. This avoids mixing
ICD-9 and ICD-10 descriptions when the same code text exists in both versions.

## Setup

The project is tested with the `DataEng` conda environment.

```bash
conda activate DataEng
python -m pip install -r requirements.txt
```

## How to Run

```bash
python main.py
```

The run writes:

- `output/profiling_report.txt`
- `output/dlq_patients.txt`
- `output/dlq_diagnoses.txt`
- `output/dlq_lookup.txt`

The DLQ files are empty when all parsed rows pass validation.

## Beam Concepts Used

| Concept | What It Does |
| --- | --- |
| `ReadFromText` | Reads CSV files line by line |
| `FlatMap` | Parses records and routes invalid rows to tagged DLQ outputs |
| `Map` | Transforms each element |
| `Filter` | Keeps elements that match a condition |
| `CombinePerKey(sum)` | Groups by key and sums values |
| `CombineGlobally` | Aggregates all elements into one result |
| `CombineFn` | Custom aggregation logic: `CountFn`, `AverageFn`, `MinMaxFn` |
| `Top.Of` | Finds the top N elements with a deterministic tie-breaker |
| `Distinct` | Removes duplicate elements |
| `Side Input (AsDict)` | Passes the ICD lookup table to a transform |
| `WriteToText` | Writes the report and DLQ files |

## Sample Output

```text
============================================================
        DATA PROFILING REPORT  -  MIMIC-IV Demo
============================================================

1. DATASET OVERVIEW
  Total patients                 : 100
  Total diagnosis records        : 4506
  Unique ICD code/version pairs  : 1474

2. GENDER DISTRIBUTION
  Female  : 43
  Male    : 57

3. AGE DISTRIBUTION
  Minimum age : 21
  Maximum age : 91
  Average age : 61.8

4. TOP 10 DIAGNOSES
        Code  |  Version  |  Freq       |  Description
    ------------------------------------------------------------------------
        4019  |  ICD-9   |    68 times  |  Unspecified essential hypertension
        E785  |  ICD-10  |    57 times  |  Hyperlipidemia, unspecified
        2724  |  ICD-9   |    55 times  |  Other and unspecified hyperlipidemia
        E039  |  ICD-10  |    47 times  |  Hypothyroidism, unspecified
        Z794  |  ICD-10  |    37 times  |  Long term (current) use of insulin

5. DIAGNOSES PER PATIENT
  Average diagnoses per patient: 45.1

6. MORTALITY INSIGHT
  Alive    : 69
  Deceased : 31
  Mortality rate: 31.0%

============================================================
```
