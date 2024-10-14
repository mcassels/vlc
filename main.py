from pandas import read_csv
from os import scandir
import datefinder
from datetime import datetime
from thefuzz import fuzz
from typing import NamedTuple, Union, Any
import pandas
import re

class Expiry(NamedTuple):
    name: str
    expiry: Union[datetime, None]

def get_crc_expiries() -> list[Expiry]:
    volunteers = []
    for f in scandir("data/aa ACCEPTED - OK ACTIVE VOLUNTEERS"):
        if not f.is_dir():
            continue

        volunteer = f.name.split(' - ')[0]
        if not volunteer:
            print(f"Skipping {f.name}; could not extract volunteer name")

        expiry = None
        for subf in scandir(f.path):
            if not subf.is_file():
                continue

            name = subf.name.lower()
            split = name.split(' exp ')
            if len(split) != 2:
                continue
            dates = list(datefinder.find_dates(split[1]))
            if len(dates) != 1:
                continue
            expiry = dates[0]

        volunteers.append(Expiry(volunteer, expiry))
    return volunteers

def find_closest(name: str, expiries: list[Expiry]) -> tuple[int, Expiry]:
    ratios = [(fuzz.ratio(name, e.name), e) for e in expiries]
    ratios.sort(key=lambda x: x[0], reverse=True)
    return ratios[0]

def get_all_names_ordered():
    df = read_csv("data/volunteers.csv")
    df['name'] = df.apply(lambda x: f"{x.FirstName} {x.LastNameNew}", axis=1)
    return list(df['name'])

def get_row_crc_expiry(name: str, expiries: list[Expiry]) -> Union[str, None]:
    [ratio, expiry] = find_closest(name, expiries)
    if expiry.expiry is not None and ratio > 80:
        return expiry.expiry.strftime("%m/01/%Y")
    return None

def add_crc_columns(df: pandas.DataFrame) -> pandas.DataFrame:
    expiries = get_crc_expiries()
    df['CRC Expiry'] = df.apply(lambda x: get_row_crc_expiry(f"{x.FirstName} {x.LastNameNew}", expiries), axis=1)
    df['Qualification: CRC'] = df['CRC Expiry'].apply(lambda x: "Yes" if x is not None else "No")
    return df

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

"""
Data cleaning steps:
1. add CRC information
2. replace LastName with LastNameNew
3. Format phone numbers
4. Fill in date joined with dummy 01-01-1999 values and format date joined
5. Fill in legal first name with first name
6. Fill in all required fields with "please update"
7. Write out file with correct column order.

Data validation steps:
1. Check for valid values for multiple choice fields
2. Check for duplicates

"""

def main():
    df = pandas.read_excel("data/vlc_import_file.xlsx")
    df = add_crc_columns(df)
    df['LastName'] = df['LastNameNew']
    df = clean_phone_numbers(df)
    df = clean_date_joined(df)
    df['LegalFirstName'] = df.apply(lambda x: x['LegalFirstName'] if x['LegalFirstName'] is not None and not pandas.isna(x['LegalFirstName']) else x['FirstName'], axis=1)

    required_columns = [
        "FirstName",
        "LastName",
        "Address1",
        "City",
        "Province",
        "Country",
        "PostalCode",
    ]
    for col in required_columns:
        df = please_update_if_empty(df, col)

    output_column_order = [
        "Salutation",
        "FirstName",
        "LastName",
        "MiddleName",
        "Suffix",
        "Pronouns",
        "LegalFirstName",
        "Address1",
        "Address2",
        "City",
        "Province",
        "Country",
        "PostalCode",
        "HomePhone",
        "WorkPhone",
        "WorkPhoneExt",
        "CellPhone",
        "EmailAddress",
        "SecondaryEmailAddress",
        "Birthday",
        "DateJoined",
        "VolunteerStatus",
        "Preferred Learner Age Group",
        "Preferred Tutoring Format",
        "Neighbourhood",
        "Qualification: CRC",
        "CRC Expiry",
    ]
    df = df[output_column_order]
    df.to_excel("formatted_import_file.xlsx", index=False)


if __name__ == '__main__':
    main()