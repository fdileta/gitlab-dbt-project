
import os
import glob 

DAILY_NOTEBOOKS_PATH = "../../sales_analytics_notebooks/daily/"

def get_daily_notebooks(path):
    notebooks = []
    fileNames = []
    for file in os.listdir(path):
        filename = os.fsdecode(file)
        if filename.endswith(".ipynb"):
            notebooks.append(filename)
            fileNames.append(os.path.splitext(filename)[0])
        else:
            continue
    return dict(zip(notebooks, fileNames))

daily_notebooks = get_daily_notebooks(DAILY_NOTEBOOKS_PATH)

for key, val in daily_notebooks.items():
    print(str(key), str(val))
