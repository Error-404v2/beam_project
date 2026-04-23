# Hospital Data Profiling with Apache Beam

A batch data pipeline built with **Apache Beam** that reads hospital records from the [MIMIC-IV Clinical Database Demo](https://physionet.org/content/mimic-iv-demo/2.2/) and generates a data profiling report.

## What It Does

The project contains two pipelines:

| Pipeline | Entry Point | Output | Purpose |
|----------|-------------|--------|---------|
| Top Diagnoses | `main.py` | `output/top_diagnoses.txt` | Finds the top 5 most frequent diagnosis codes |
| Data Profiling | `profiling.py` | `output/profiling_report.txt` | Full profiling report with 6 sections |

### Profiling Report Sections

1. **Dataset Overview** — total patients, total diagnosis records, unique ICD codes
2. **Gender Distribution** — count of male vs female patients
3. **Age Distribution** — min, max, and average patient age
4. **Top 10 Diagnoses** — most frequent ICD codes with readable names
5. **Diagnoses per Patient** — average number of diagnoses per patient
6. **Mortality Insight** — alive vs deceased count and mortality rate

## Project Structure

```
├── main.py                  # Entry point for the top diagnoses pipeline
├── pipeline.py              # Pipeline logic for top diagnoses
├── profiling.py             # Entry point for the profiling pipeline
├── profiling_pipeline.py    # Pipeline logic for data profiling
├── transforms.py            # Reusable custom PTransform
├── requirements.txt
├── input/
│   └── data/                # MIMIC-IV demo CSV files
└── output/
    ├── top_diagnoses.txt
    └── profiling_report.txt
```

## Dataset

Uses the **MIMIC-IV Clinical Database Demo v2.2**, which contains de-identified hospital records for 100 patients. The pipeline reads three CSV files:

- `patients.csv` — patient demographics (gender, age, date of death)
- `diagnoses_icd.csv` — diagnosis records linked to hospital admissions
- `d_icd_diagnoses.csv` — lookup table mapping ICD codes to readable names

## Setup

```bash
pip install apache-beam
```

## How to Run

```bash
# Run the profiling pipeline
python profiling.py

# Run the top diagnoses pipeline
python main.py
```

## Beam Concepts Used

| Concept | What It Does |
|---------|-------------|
| `ReadFromText` | Reads CSV files line by line |
| `Map` | Transforms each element (1 input → 1 output) |
| `FlatMap` | Transforms each element (1 input → 0 or more outputs) |
| `Filter` | Keeps elements that match a condition |
| `CombinePerKey(sum)` | Groups by key and sums values |
| `CombineGlobally` | Aggregates all elements into one result |
| `CombineFn` | Custom aggregation logic (CountFn, AverageFn, MinMaxFn) |
| `Top.Of` | Finds the top N elements |
| `Distinct` | Removes duplicate elements |
| `Side Input (AsDict)` | Passes a lookup table to a transform |
| `WriteToText` | Writes results to a text file |

## Sample Output

```
============================================================
        DATA PROFILING REPORT  -  MIMIC-IV Demo
============================================================

1. DATASET OVERVIEW
  Total patients        : 100
  Total diagnosis records: 4506
  Unique ICD codes used  : 1472

2. GENDER DISTRIBUTION
  Female  : 43
  Male    : 57

3. AGE DISTRIBUTION
  Minimum age : 21
  Maximum age : 91
  Average age : 61.8

4. TOP 10 DIAGNOSES
        Code  |  Freq       |  Description
    --------------------------------------------------
        4019  |    68 times  |  Unspecified essential hypertension
        E785  |    57 times  |  Hyperlipidemia, unspecified
        2724  |    55 times  |  Other and unspecified hyperlipidemia
        E039  |    47 times  |  Hypothyroidism, unspecified
        Z794  |    37 times  |  Long term (current) use of insulin

5. DIAGNOSES PER PATIENT
  Average diagnoses per patient: 45.1

6. MORTALITY INSIGHT
  Alive    : 69
  Deceased : 31
  Mortality rate: 31.0%

============================================================
```
