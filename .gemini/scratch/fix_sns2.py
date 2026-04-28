import nbformat
import re

notebook_path = "eda_profiling.ipynb"

with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = nbformat.read(f, as_version=4)

for cell in nb.cells:
    if cell.cell_type == 'code':
        # Fix cell 5a
        cell.source = re.sub(
            r"sns\.barplot\(x=month_counts\.index, y=month_counts\.values, palette='mako'\)",
            "sns.barplot(x=month_counts.index, y=month_counts.values, hue=month_counts.index, palette='mako', legend=False)",
            cell.source
        )
        
        # Fix cell 5b
        cell.source = re.sub(
            r"sns\.barplot\(y=top_causes\.index, x=top_causes\.values, palette='rocket'\)",
            "sns.barplot(y=top_causes.index, x=top_causes.values, hue=top_causes.index, palette='rocket', legend=False)",
            cell.source
        )

with open(notebook_path, 'w', encoding='utf-8') as f:
    nbformat.write(nb, f)
print("Fixed remaining warnings")
