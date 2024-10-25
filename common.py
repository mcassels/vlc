import pandas
from typing import Any
import re

def check_duplicates(df: pandas.DataFrame):
    # No duplicate names
    df['full_name'] = df.apply(lambda x: f"{x['FirstName']} {x['LastName']}", axis=1)
    assert df['full_name'].nunique() == len(df)
    # no duplicate emails
    assert len(df[~pandas.isna(df['EmailAddress'])]) == df[~pandas.isna(df['EmailAddress'])]['EmailAddress'].nunique()
    # no duplicate phone numbers
    assert len(df[~pandas.isna(df['HomePhone'])]) == df[~pandas.isna(df['HomePhone'])]['HomePhone'].nunique()

def clean_phone_number(phone: Any) -> str|None:
    if phone is None or pandas.isna(phone):
        return None

    return re.sub(r'\D', '', str(phone))

def clean_phone_numbers(df: pandas.DataFrame) -> pandas.DataFrame:
    df['HomePhone'] = df['HomePhone'].apply(clean_phone_number)
    df['CellPhone'] = df['CellPhone'].apply(clean_phone_number)
    df['WorkPhone'] = df['WorkPhone'].apply(clean_phone_number)
    return df

def format_date(date: Any) -> str:
    if date is None or pandas.isna(date):
        return "01/01/1999"
    return date.strftime("%m/%d/%Y")

def clean_date_joined(df: pandas.DataFrame) -> pandas.DataFrame:
    df['DateJoined'] = pandas.to_datetime(df['DateJoined'])
    df['DateJoined'] = df['DateJoined'].apply(format_date)
    return df

def please_update_if_empty(df: pandas.DataFrame, colName: str) -> pandas.DataFrame:
    df[colName] = df[colName].apply(lambda x: x if x is not None and not pandas.isna(x) else "please update")
    return df

def assert_col_valid(df: pandas.DataFrame, col: str, valid_vals: str):
    num_invalid = len(df[(~pandas.isna(df[col])) & (~df[col].isin(valid_vals))])
    assert num_invalid == 0


valid_neighbourhoods = [
    "Victoria",
    "Oak Bay",
    "Esquimalt/Vic West",
    "WestShore",
    "Sooke",
    "S. Saanich",
    "Central Saanich",
    "N. Saanich",
    "Other",
]

