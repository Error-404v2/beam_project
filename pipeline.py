import apache_beam as beam
import csv

def extract_icd_code(line):
    # diagnoses_icd.csv format: subject_id,hadm_id,seq_num,icd_code,icd_version
    reader = csv.reader([line])
    for row in reader:
        if len(row) >= 4:
            return [row[3]]
    return []

def extract_lookup(line):
    # d_icd_diagnoses.csv format: icd_code,icd_version,long_title
    reader = csv.reader([line])
    for row in reader:
        if len(row) >= 3:
            return [(row[0], row[2])]
    return []

def format_result(kv, lookup_dict):
    code, count = kv
    # Try to find the name in the dictionary, otherwise use "Unknown"
    name = lookup_dict.get(code, "Unknown Diagnosis")
    return f"Diagnosis ICD Code: {code} ({name}) - Count: {count}"

def build_pipeline(p):
    
    # --- Branch A: Read the Dictionary ---
    lookup_data = (
        p 
        | "Read Lookup CSV" >> beam.io.ReadFromText(
            "input/data/mimic-iv-clinical-database-demo-2.2/hosp/d_icd_diagnoses.csv",
            skip_header_lines=1
        )
        # Parse into (icd_code, long_title) Key-Value pairs
        | "Parse Lookup" >> beam.FlatMap(extract_lookup)
    )

    # --- Branch B: Process the Data ---
    (
        p
        # 1. Read the data
        | "Read Diagnoses CSV" >> beam.io.ReadFromText(
            "input/data/mimic-iv-clinical-database-demo-2.2/hosp/diagnoses_icd.csv",
            skip_header_lines=1
        )
        
        # 2. Extract the diagnosis code (FlatMap flattens lists so we safely skip bad lines)
        | "Extract ICD Code" >> beam.FlatMap(extract_icd_code)
        
        # 3. Create Key-Value pairs: (ICD_Code, 1)
        | "Map to Tuple" >> beam.Map(lambda code: (code, 1))
        
        # 4. Sum up the 1s per Key (ICD Code)
        | "Count Diagnoses" >> beam.CombinePerKey(sum)
        
        # 5. Find the Top 5 by count. Top.Of returns a single list.
        | "Find Top 5" >> beam.combiners.Top.Of(5, key=lambda kv: kv[1])
        
        # 6. Flatten that list back into individual elements
        | "Flatten List" >> beam.FlatMap(lambda x: x)
        
        # 7. Format the output string using the lookup dictionary as a Side Input!
        | "Format Output" >> beam.Map(format_result, lookup_dict=beam.pvalue.AsDict(lookup_data))
        
        # 8. Write Results
        | "Write Results" >> beam.io.WriteToText("output/top_diagnoses", file_name_suffix=".txt")
    )