from typing import Any
import pandas
import re
import geopandas
from common import check_duplicates, clean_phone_numbers, clean_date_joined, please_update_if_empty, assert_col_valid, valid_neighbourhoods, fix_mojibake


def validate_statuses(df: pandas.DataFrame):
    valid_leaner_statuses = [
        "Applicant",
        "In Process",
        "Accepted",
        "Inactive",
        "Archived",
    ]
    assert_col_valid(df, "ClientStatus", valid_leaner_statuses)


valid_tutoring_formats = [
    "in person (in public)",
    "remote (online)",
    "either / both",
]
def clean_tutoring_format(raw_format: Any) -> str|None:
    if pandas.isna(raw_format) or raw_format is None:
        return None
    format = raw_format.lower()
    if "online" in format:
        return "remote (online)"
    if "person" in format:
        return "in person (in public)"
    if "either" or "preference" in format:
        return "either / both"
    return None

# "Neighbourhood" options for VLC:
#
# Victoria
# Oak Bay
# Esquimalt/Vic West
# WestShore (Colwood, Langford, View Royal, Highlands, Metchosin)
# Sooke
# S. Saanich (Uptown, Gordon Head, Royal Oak, Cordova Bay)
# Central Saanich (Saanichton, Brentwood Bay)
# N. Saanich (incl Sidney)
# Other
valid_neighbourhoods = [
    "Victoria", # same
    "Oak Bay", # same
    "Esquimalt/Vic West",
    "WestShore",
    "Sooke",
    "S. Saanich",
    "Central Saanich",
    "N. Saanich",
    "Other",
]

def get_learner_neighbourhood(admin_area: str|None) -> str|None:
    if pandas.isna(admin_area) or admin_area is None:
        return None
    if admin_area in valid_neighbourhoods:
        return admin_area
    if admin_area == "Esquimalt":
        return "Esquimalt/Vic West"
    if admin_area in ["Langford", "Colwood", "View Royal", "Highlands", "Metchosin"]:
        return "WestShore"
    if admin_area == "Saanich":
        return "S. Saanich"
    if admin_area == "Central Saanich":
        return "Central Saanich"
    if admin_area in ["North Saanich", "Sidney"]:
        return "N. Saanich"
    return "Other"



# Municipality shapes were requested from https://catalogue.data.gov.bc.ca/dataset/municipalities-legally-defined-administrative-areas-of-bc
def get_learners_with_neighbourhoods() -> pandas.DataFrame:
    municipalities = geopandas.read_file("data/learner_intake/ABMS_MUNICIPALITIES_SP.geojson", driver="GeoJSON")
    learners = geopandas.read_file("data/learner_intake/may_migration/learners_with_geometry.geojson", driver="GeoJSON")

    gdf = geopandas.sjoin(learners, municipalities, how="left")
    gdf["neighbourhood"] = gdf['ADMIN_AREA_ABBREVIATION'].apply(get_learner_neighbourhood)
    return gdf

def clean_birthdate(birthdate: Any) -> str|None:
    if pandas.isna(birthdate) or birthdate is None:
        return None
    try:
        return pandas.to_datetime(birthdate).strftime("%m/%d/%Y")
    except Exception as e:
        print(f"Error parsing birthdate: {birthdate}. Using None.")
        return None


def extract_postal_code(address: str|None) -> str|None:
    if pandas.isna(address) or address is None:
        return None
    postal_code = re.search(r'[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d', address)
    if postal_code is None:
        return None
    code = postal_code.group(0).upper()
    return code.replace(" ", "")

def extract_country(address: str|None) -> str|None:
    if pandas.isna(address) or address is None:
        return None
    return "CANADA"

def extract_province(address: str|None) -> str|None:
    if pandas.isna(address) or address is None:
        return None
    return "BC"

def extract_city(address: str|None) -> str|None:
    if pandas.isna(address) or address is None:
        return None
    for city in ["Victoria", "Oak Bay", "Esquimalt", "Langford", "Colwood", "Sooke", "Saanich", "Sidney"]:
        if city.lower() in address.lower():
            return city
    return "Victoria"

def extract_address1(address: str|None) -> str|None:
    if pandas.isna(address) or address is None:
        return None
    postal_code = re.search(r'[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d', address)
    if postal_code is not None:
        address = address.replace(postal_code.group(0), "")
    city = extract_city(address)
    if city is not None:
        address = address.replace(city, "")
        address = address.replace(city.upper(), "")
        address = address.replace(city.lower(), "")
    province = re.search(r' [Bb].?[Cc]', address)
    if province is not None:
        address = address.replace(province.group(0), "")
    address = address.strip('., ')
    if address == "":
        return None
    return address

def get_notes_on_participation(row: pandas.Series) -> str|None:
    """
    If birthdate is missing, add age at intake to the note.
    """
    if isinstance(row["Birthday"], str) and len(row["Birthday"]) > 0:
        return None
    if isinstance(row["age"], str) and len(row["age"]) > 0:
        return f"Age at intake: {row['age']}"
    return None


def fill_in_preferred_name(row: pandas.Series) -> str|None:
    if (pandas.isna(row['preferred_name']) or row['preferred_name'] is None or row['preferred_name'] == "") and isinstance(row['full_legal_name'], str):
          return row['full_legal_name'].split(" ")[0]
    return row['preferred_name']


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
    "ClientStatus",
    "Tutoring Format",
    "Neighbourhood",
    "Parent/Guardian",
    "Notes on Participation - PRIVATE",
]

"""
Data cleaning steps:
1. Add neighbourhood based on address
2. Clean/format the "tutoring format" column values to contain only "online", "in-person", or "either"
3. Use "preferred name" for "FirstName"
4. Use the first word from "Full Legal Name" for "LegalFirstName"
5. Use the remainder of "Full Legal Name", after the first word is removed, for "LastName"
6. Format phone numbers
7. Format dates (intake date and birthdate)
8. Extract address column into Address1, City, Province, Country, PostalCode
9. Replace empty required fields with "please update"
10. Check for duplicate names, emails, or phone numbers
11. Check that categorical columns only contain valid values
12. Output file with correct column names and order
"""
def main():
    df = get_learners_with_neighbourhoods()
    df = df[~pandas.isna(df['full_legal_name'])]
    # This column is called "Would you prefer to meet your tutor online or in person?"
    df['Tutoring Format'] = df['tutoring_method'].apply(clean_tutoring_format)
    df['LegalFirstName'] = df['full_legal_name'].apply(lambda x: x.split(" ")[0])
    df['FirstName'] = df.apply(fill_in_preferred_name, axis=1)
    df['LastName'] = df['full_legal_name'].apply(lambda x: " ".join(x.split(" ")[1:]))

    df = df.rename(
        columns={
            'pronouns': 'Pronouns',
            'intake_date': 'DateJoined',
            'email': 'EmailAddress',
            'status': 'ClientStatus',
            'phone': 'HomePhone',
            'neighbourhood': 'Neighbourhood',
            'parent_guardian': 'your name:',
        }
    )
    for column in output_column_order:
        if column not in df.columns:
            df[column] = None

    df = clean_phone_numbers(df)
    df = clean_date_joined(df)
    df['Birthday'] = df['birthdate'].apply(clean_birthdate)
    df["Notes on Participation - PRIVATE"] = df.apply(get_notes_on_participation, axis=1)

    # address parts
    df['PostalCode'] = df['address'].apply(extract_postal_code)
    df['Country'] = df['address'].apply(extract_country)
    df['Province'] = df['address'].apply(extract_province)
    df['City'] = df['address'].apply(extract_city)
    df['Address1'] = df['address'].apply(extract_address1)

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

    check_duplicates(df)
    validate_statuses(df)

    df[output_column_order].to_excel("data/learner_intake/may_migration/learner_import_file.xlsx", index=False)


if __name__ == '__main__':
    main()