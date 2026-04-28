import nbformat

notebook_path = "eda_profiling.ipynb"

# Read the notebook
with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = nbformat.read(f, as_version=4)

# 1. Add .head() to the data loading cell
for cell in nb.cells:
    if cell.cell_type == 'code' and 'patients_df = pd.read_csv' in cell.source:
        if 'display(patients_df.head())' not in cell.source:
            cell.source += "\n\n# Display the first few rows of each dataset\n"
            cell.source += "display(patients_df.head())\n"
            cell.source += "display(diagnoses_df.head())\n"
            cell.source += "display(lookup_df.head())"
        break

# 2. Remove the correlation section
# Find the index of the Markdown cell "## 6. Feature Correlations"
corr_idx = -1
for i, cell in enumerate(nb.cells):
    if cell.cell_type == 'markdown' and '6. Feature Correlations' in cell.source:
        corr_idx = i
        break

if corr_idx != -1:
    # Delete from corr_idx to the end
    nb.cells = nb.cells[:corr_idx]

# 3. Add Number of Visits analysis
new_cells = []

new_cells.append(nbformat.v4.new_markdown_cell(
    "## 6. Hospital Visits & Mortality\n"
    "We can determine the total number of hospital visits per patient by counting the unique Hospital Admission IDs (`hadm_id`). Let's see how the number of visits relates to mortality."
))

new_cells.append(nbformat.v4.new_code_cell(
    "# Calculate total unique hospital visits per patient\n"
    "visits_df = diagnoses_df.groupby('subject_id')['hadm_id'].nunique().reset_index(name='total_visits')\n"
    "\n"
    "# Merge into our main dataframe\n"
    "df_merged = pd.merge(df_merged, visits_df, on='subject_id', how='left')\n"
    "\n"
    "plt.figure(figsize=(8, 6))\n"
    "sns.boxplot(data=df_merged, x='is_deceased', y='total_visits', palette='Set2')\n"
    "plt.title('Number of Hospital Visits vs. Mortality', fontsize=15, fontweight='bold')\n"
    "plt.xlabel('Deceased (True/False)')\n"
    "plt.ylabel('Total Hospital Visits')\n"
    "plt.show()\n"
    "\n"
    "display(df_merged.groupby('is_deceased')['total_visits'].describe())"
))

nb.cells.extend(new_cells)

# Write the notebook back
with open(notebook_path, 'w', encoding='utf-8') as f:
    nbformat.write(nb, f)

print("Notebook updated successfully.")
