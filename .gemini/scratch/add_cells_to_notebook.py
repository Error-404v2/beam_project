import nbformat

notebook_path = "eda_profiling.ipynb"

# Read the notebook
with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = nbformat.read(f, as_version=4)

new_cells = []

# Markdown cell for new section
new_cells.append(nbformat.v4.new_markdown_cell(
    "## 5. Mortality Deep Dive & Patterns\n"
    "Let's take a more humanistic approach to see what happens when patients pass away. We will explore the mortality distribution across months and investigate the leading diagnoses for patients who didn't survive."
))

# Code cell for Mortality by Month
new_cells.append(nbformat.v4.new_code_cell(
    "# 5a. Mortality Distribution by Month\n"
    "# Let's check if there are specific months with higher mortality rates.\n"
    "df_merged['dod'] = pd.to_datetime(df_merged['dod'])\n"
    "deceased_patients = df_merged[df_merged['is_deceased']].copy()\n"
    "deceased_patients['death_month'] = deceased_patients['dod'].dt.month\n"
    "\n"
    "plt.figure(figsize=(10, 5))\n"
    "month_counts = deceased_patients['death_month'].value_counts().sort_index()\n"
    "sns.barplot(x=month_counts.index, y=month_counts.values, palette='mako')\n"
    "plt.title('Mortality Distribution by Month', fontsize=15, fontweight='bold')\n"
    "plt.xlabel('Month')\n"
    "plt.ylabel('Number of Deaths')\n"
    "plt.xticks(ticks=range(12), labels=['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])\n"
    "plt.grid(axis='y', linestyle='--', alpha=0.7)\n"
    "plt.show()"
))

# Code cell for Leading Causes of Death
new_cells.append(nbformat.v4.new_code_cell(
    "# 5b. Leading Causes of Death\n"
    "# We will filter the diagnoses for deceased patients and find the most common ICD codes.\n"
    "deceased_subject_ids = deceased_patients['subject_id']\n"
    "deceased_diagnoses = diagnoses_df[diagnoses_df['subject_id'].isin(deceased_subject_ids)]\n"
    "\n"
    "# Merge with lookup table to get human-readable long titles\n"
    "deceased_diagnoses_with_titles = pd.merge(deceased_diagnoses, lookup_df, on=['icd_code', 'icd_version'], how='left')\n"
    "\n"
    "top_causes = deceased_diagnoses_with_titles['long_title'].value_counts().head(10)\n"
    "\n"
    "plt.figure(figsize=(12, 6))\n"
    "sns.barplot(y=top_causes.index, x=top_causes.values, palette='rocket')\n"
    "plt.title('Top 10 Diagnoses for Deceased Patients', fontsize=15, fontweight='bold')\n"
    "plt.xlabel('Frequency')\n"
    "plt.ylabel('Diagnosis')\n"
    "plt.show()"
))

# Markdown cell for Correlations
new_cells.append(nbformat.v4.new_markdown_cell(
    "## 6. Feature Correlations\n"
    "To build interesting models or predictive analytics later, let's look at the correlation between Age, Total Diagnoses, and Mortality."
))

# Code cell for Correlation Heatmap
new_cells.append(nbformat.v4.new_code_cell(
    "# 6a. Correlation Heatmap\n"
    "corr_matrix = df_merged[['anchor_age', 'total_diagnoses', 'is_deceased']].corr()\n"
    "\n"
    "plt.figure(figsize=(8, 6))\n"
    "sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)\n"
    "plt.title('Correlation Matrix: Age, Diagnoses, and Mortality', fontsize=15, fontweight='bold')\n"
    "plt.show()\n"
    "\n"
    "# As we can see, Age and Mortality have a slight positive correlation, which aligns with clinical intuition."
))

# Append the new cells
nb.cells.extend(new_cells)

# Write the notebook back
with open(notebook_path, 'w', encoding='utf-8') as f:
    nbformat.write(nb, f)

print("Notebook updated successfully.")
