from pandas import read_csv
from os import scandir
import datefinder
from datetime import datetime
from thefuzz import fuzz
from typing import NamedTuple, Union, List
import pandas

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

"""
TODO:
1. add CRC information
2. replace LastName with LastNameNew
3. Format phone numbers
4. Fill in date joined with dummy 01-01-1999 values and format date joined
5. Fill in legal first name with first name
6. Fill in all required fields with "please update"
7. Write out file with correct column order.

Also:
- Check for duplicates
- Check for valid values for all multiple choice fields

"""

def main():
    df = pandas.read_excel("data/vlc_import_file.xlsx")
    df = add_crc_columns(df)


if __name__ == '__main__':
    main()