import nbformat
import re

notebook_path = "eda_profiling.ipynb"

with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = nbformat.read(f, as_version=4)

for cell in nb.cells:
    if cell.cell_type == 'code':
        # Boxplot 1
        cell.source = re.sub(
            r'sns\.boxplot\(\s*data=df_merged,\s*x="is_deceased",\s*y="total_diagnoses",\s*palette="Pastel1"\s*\)',
            'sns.boxplot(data=df_merged, x="is_deceased", y="total_diagnoses", hue="is_deceased", palette="Pastel1", legend=False)',
            cell.source
        )
        
        # Boxplot 2
        cell.source = re.sub(
            r"sns\.boxplot\(\s*data=df_merged,\s*x='is_deceased',\s*y='total_visits',\s*palette='Set2'\s*\)",
            "sns.boxplot(data=df_merged, x='is_deceased', y='total_visits', hue='is_deceased', palette='Set2', legend=False)",
            cell.source
        )
        
        # Barplot
        cell.source = re.sub(
            r"sns\.barplot\(\s*data=top_causes,\s*y='long_title',\s*x='count',\s*palette='Reds_r'\s*\)",
            "sns.barplot(data=top_causes, y='long_title', x='count', hue='long_title', palette='Reds_r', legend=False)",
            cell.source
        )

with open(notebook_path, 'w', encoding='utf-8') as f:
    nbformat.write(nb, f)
print("Fixed")
