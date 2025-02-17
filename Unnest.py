import pandas as pd
import ast

def read_in(df):
    """We adjust this to read in the specific file we are working with and setting up the ETL job for"""
    df = pd.read_csv()
    return df

def fully_expand_nested_dataframe(df):
    def safe_eval(val):
        if isinstance(val, str) and (val.startswith("{") or val.startswith("[")):
            try:
                return ast.literal_eval(val)
            except (ValueError, SyntaxError):
                return val 
        return val
    for col in df.columns:
        df[col] = df[col].apply(safe_eval)

    def expand_column(df, col):
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            expanded = df[col].apply(pd.Series)
            expanded.columns = [f"{col}_{subcol}" for subcol in expanded.columns]
            df = df.drop(columns=[col]).join(expanded)
        elif df[col].apply(lambda x: isinstance(x, list)).any():
            df = df.explode(col, ignore_index=True)
            df = expand_column(df, col)
        return df

    while any(df[col].apply(lambda x: isinstance(x, (dict, list))).any() for col in df.columns):
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df = expand_column(df, col)

    return df

df = fully_expand_nested_dataframe(df)