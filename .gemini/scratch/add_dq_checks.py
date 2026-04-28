import nbformat

notebook_path = "eda_profiling.ipynb"

with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = nbformat.read(f, as_version=4)

# Find the end of Section 2 (which is right before "## 3. Feature Engineering & Dataset Joining")
insert_idx = -1
for i, cell in enumerate(nb.cells):
    if cell.cell_type == 'markdown' and '3. Feature Engineering' in cell.source:
        insert_idx = i
        break

if insert_idx != -1:
    new_cells = []
    
    new_cells.append(nbformat.v4.new_markdown_cell(
        "### 2c. Explicit Data Quality Validation\n"
        "To ensure pipeline robustness, we programmatically check for anomalies such as invalid categorical values, out-of-bounds numerics, and referential integrity issues."
    ))
    
    new_cells.append(nbformat.v4.new_code_cell(
        "# 1. Invalid Categorical Values: Gender\n"
        "invalid_genders = patients_df[~patients_df['gender'].isin(['M', 'F'])]\n"
        "print(f\"Records with invalid gender: {len(invalid_genders)}\")\n"
        "\n"
        "# 2. Out-of-Bounds Numerics: Age\n"
        "invalid_ages = patients_df[(patients_df['anchor_age'] < 0) | (patients_df['anchor_age'] > 120)]\n"
        "print(f\"Records with invalid age (<0 or >120): {len(invalid_ages)}\")\n"
        "\n"
        "# 3. Referential Integrity: Orphaned Diagnoses\n"
        "# Check if there are any diagnoses associated with a patient not in our patients_df\n"
        "orphaned_diagnoses = diagnoses_df[~diagnoses_df['subject_id'].isin(patients_df['subject_id'])]\n"
        "print(f\"Diagnoses without a matching patient record: {len(orphaned_diagnoses)}\")\n"
        "\n"
        "# 4. Check for duplicates\n"
        "print(f\"Duplicate patient records: {patients_df.duplicated().sum()}\")\n"
        "print(f\"Duplicate diagnosis records: {diagnoses_df.duplicated().sum()}\")\n"
    ))
    
    nb.cells = nb.cells[:insert_idx] + new_cells + nb.cells[insert_idx:]
    
    with open(notebook_path, 'w', encoding='utf-8') as f:
        nbformat.write(nb, f)
    
    print("Added DQ checks successfully.")
else:
    print("Could not find insertion point.")
