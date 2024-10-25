from pandas import read_csv
from os import scandir
import datefinder
from datetime import datetime
from thefuzz import fuzz
from typing import NamedTuple, Union, Any
import pandas
import re
from common import check_duplicates, clean_phone_numbers, clean_date_joined, please_update_if_empty, assert_col_valid, valid_neighbourhoods

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

def data_validation(df: pandas.DataFrame):
    valid_tutoring_formats = [
        "in person (in public)",
        "remote (online)",
        "either / both",
        "n/a (non-tutor volunteers)",
    ]
    valid_learner_age_groups = [
        "child/youth (18 or younger)",
        "adult (19 or older)",
        "either / both",
        "n/a (non-tutor volunteers)",
    ]
    valid_volunteer_statuses = [
        "Active",
        "Inactive",
        "Applicant",
        "In Process",
        "Accepted",
        "Inactive - Short term",
        "Inactive - Long Term",
        "Archived - Didn't Start",
        "Archived - Rejected",
        "Archived - Dismissed",
        "Archived - Moved",
        "Archived - Quit",
        "Archived - Deceased",
        "Archived - Other",
    ]
    assert_col_valid(df, "VolunteerStatus", valid_volunteer_statuses)
    assert_col_valid(df, "Preferred Tutoring Format", valid_tutoring_formats)
    assert_col_valid(df, "Preferred Learner Age Group", valid_learner_age_groups)
    assert_col_valid(df, "Neighbourhood", valid_neighbourhoods)

def write_output(df: pandas.DataFrame):
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
    df.to_excel("data/formatted_import_file.xlsx", index=False)


# valid_tutoring_formats = [
#     "in person (in public)",
#     "remote (online)",
#     "either / both",
#     "n/a (non-tutor volunteers)",
# ]
def clean_tutoring_format(format: Any) -> str|None:
    if pandas.isna(format) or format is None:
        return None
    if "online" in format:
        return "remote (online)"
    if "person" in format:
        return "in person (in public)"
    if "either" in format:
        return "either / both"
    if "n/a" in format:
        return "n/a (non-tutor volunteers)"
    return format

# valid_learner_age_groups = [
#     "child/youth (18 or younger)",
#     "adult (19 or older)",
#     "either / both",
#     "n/a (non-tutor volunteers)",
# ]
def clean_learner_age_group(format: Any) -> str|None:
    if pandas.isna(format) or format is None:
        return None
    if "child" in format:
        return "child/youth (18 or younger)"
    if "adult" in format:
        return "adult (19 or older)"
    if "either" in format:
        return "either / both"
    if "n/a" in format:
        return "n/a (non-tutor volunteers)"
    return format

def clean_neighbourhood(val: Any) -> str|None:
    if pandas.isna(format) or format is None:
        return None
    no_whitespace_val = re.sub(r'\s', '', str(val))
    for neighbourhood in valid_neighbourhoods:
        if re.sub(r'\s', '', neighbourhood) == no_whitespace_val:
            return neighbourhood
    return val

"""
Data cleaning steps:
1. add CRC information
2. replace LastName with LastNameNew
3. Format phone numbers
4. Fill in date joined with dummy 01-01-1999 values and format date joined
5. Fill in legal first name with first name
6. Fill in all required fields with "please update"
7. Clean categorical column values
8. Enforce that categorical columns have valid values
9. Check for duplicate names
10. Write out file with correct column order.
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

    for col in [
        "Preferred Learner Age Group",
        "Preferred Tutoring Format",
    ]:
        df[col] = df[col].apply(lambda x: x.lower() if not pandas.isna(x) and x is not None else x)

    df["Preferred Learner Age Group"] = df["Preferred Learner Age Group"].apply(clean_learner_age_group)
    df["Preferred Tutoring Format"] = df["Preferred Tutoring Format"].apply(clean_tutoring_format)
    df["Neighbourhood"] = df["Neighbourhood"].apply(clean_neighbourhood)

    data_validation(df)
    check_duplicates(df)
    write_output(df)


if __name__ == '__main__':
    main()