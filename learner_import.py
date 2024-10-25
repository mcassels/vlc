from pandas import read_csv
from os import scandir
import datefinder
from datetime import datetime
from thefuzz import fuzz
from typing import NamedTuple, Union, Any
import pandas
import re
from dotenv import load_dotenv
import googlemaps
import os
from shapely.geometry import Point
import geopandas
from common import check_duplicates, clean_phone_numbers, clean_date_joined, please_update_if_empty, assert_col_valid, valid_neighbourhoods


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


valid_tutoring_formats = [
    "in person (in public)",
    "remote (online)",
    "either / both",
]
def clean_tutoring_format(format: Any) -> str|None:
    if pandas.isna(format) or format is None:
        return None
    if "online" in format:
        return "remote (online)"
    if "person" in format:
        return "in person (in public)"
    if "either" in format:
        return "either / both"
    return format

def geolocate_address(address: str|None) -> Point|None:
    if address is None or pandas.isna(address):
        return None
    gmaps = googlemaps.Client(key=os.environ['GOOGLE_MAPS_KEY'])
    res = gmaps.geocode(address)
    if len(res) == 0:
        return None
    loc = res[0]['geometry']['location']
    return Point(loc['lng'], loc['lat'])

# Run this first, just once to geolocate the learners,
# so that we only have to call the google maps api once.
def write_learners_with_geometries():
    load_dotenv()
    df = pandas.read_excel("data/learner_intake/adult_learner_import_1.xlsx")
    df["geometry"] = df["address"].apply(lambda x: geolocate_address(x))
    gdf = geopandas.GeoDataFrame(df, geometry="geometry")
    gdf.to_file("data/learner_intake/learners_with_geometry_2.geojson", driver="GeoJSON")

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
    learners = geopandas.read_file("data/learner_intake/learners_with_geometry_2.geojson", driver="GeoJSON")

    gdf = geopandas.sjoin(learners, municipalities, how="left")
    gdf["neighbourhood"] = gdf['ADMIN_AREA_ABBREVIATION'].apply(get_learner_neighbourhood)
    return gdf

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
    "Age at Intake",
    "Tutoring Format",
    "Neighbourhood",
    "Parent/Guardian",
    "Emergency Contact",
]

"""
Data cleaning steps:
1. Add neighbourhood based on address
2. Clean/format the "tutoring format" column values to contain only "online", "in-person", or "either"
3. Use "preferred name" for "FirstName"
4. Use the first word from "Full Legal Name" for "LegalFirstName"
5. Use the remainder of "Full Legal Name", after the first word is removed, for "LastName"
6. Format phone numbers
7. Format dates (intake date and birtdate)
8. Format phone numbers
9. Replace empty required fields with "please update"
10. Check for duplicate names, emails, or phone numbers
11. Check that categorical columns only contain valid values
12. Output file with correct column names and order
"""
def main():
    get_learners_with_neighbourhoods()
    # df = pandas.read_excel("data/learner_intake/adult_learner_import_1.xlsx")
    # address = df.iloc[0]['address']
    # print(address)
    # gmaps = googlemaps.Client(key=os.environ['GOOGLE_MAPS_KEY'])
    # geocode_result = gmaps.geocode(address)
    # loc = geocode_result[0]['geometry']['location']
    # print(loc)


if __name__ == '__main__':
    main()