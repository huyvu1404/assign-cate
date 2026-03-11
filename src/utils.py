import pandas as pd

def read_files(files, sheet_name=None):
    dfs = []
    for file in files:
        if sheet_name:
            df = pd.read_excel(file, sheet_name=sheet_name)
        else:
            df = pd.read_excel(file)
        dfs.append(df)
    
    df = pd.concat(dfs, ignore_index=True)
    return df