import nbformat
import re

notebook_path = "eda_profiling.ipynb"

with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = nbformat.read(f, as_version=4)

for cell in nb.cells:
    if cell.cell_type == 'code':
        if "month_counts = deceased_patients['death_month'].value_counts().sort_index()" in cell.source:
            cell.source = cell.source.replace(
                "month_counts = deceased_patients['death_month'].value_counts().sort_index()",
                "month_counts = deceased_patients['death_month'].value_counts().reindex(range(1, 13), fill_value=0)"
            )

with open(notebook_path, 'w', encoding='utf-8') as f:
    nbformat.write(nb, f)
print("Fixed month counts!")
