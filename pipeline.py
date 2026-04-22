import apache_beam as beam

def extract_icd_code(line):
    # CSV format: subject_id,hadm_id,seq_num,icd_code,icd_version
    fields = line.split(",")
    # If the line doesn't have enough fields, safely skip it (return empty list)
    if len(fields) < 5:
        return []
    
    icd_code = fields[3]
    return [icd_code] # Returning a list allows FlatMap to emit 0 or 1 element safely

def build_pipeline(p):
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
        
        # 5. Find the Top 5 by count. Top.Of returns a single list with the top N elements.
        | "Find Top 5" >> beam.combiners.Top.Of(5, key=lambda kv: kv[1])
        
        # 6. Because Top.Of returns a list like [[('4019', 500), ('2724', 300)]], 
        # we flat_map to turn that single list element back into 5 separate elements.
        | "Flatten List" >> beam.FlatMap(lambda x: x)
        
        # 7. Format the output string
        | "Format Output" >> beam.Map(lambda kv: f"Diagnosis ICD Code: {kv[0]} - Count: {kv[1]}")
        
        # 8. Write to a text file
        | "Write Results" >> beam.io.WriteToText("output/top_diagnoses", file_name_suffix=".txt")
    )