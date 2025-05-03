import pandas
from dotenv import load_dotenv
import googlemaps
import os
from shapely.geometry import Point
import geopandas
from typing import Any

from common import fix_mojibake


def geolocate_address(address: str|None) -> Point|None:
    if address is None or pandas.isna(address) or address == "please update":
        return None
    gmaps = googlemaps.Client(key=os.environ['GOOGLE_MAPS_KEY'])
    address = fix_mojibake(address)
    print("Geolocating address: ", address)
    res = gmaps.geocode(address)
    if len(res) == 0:
        return None
    loc = res[0]['geometry']['location']
    return Point(loc['lng'], loc['lat'])

def clean_cell(cell: Any) -> str|None:
    if isinstance(cell, str):
        return fix_mojibake(cell)
    return cell

# Run this first, just once to geolocate the learners,
# so that we only have to call the google maps api once.
def main():
    load_dotenv()
    df = pandas.read_csv("data/learner_intake/may_migration/combined_data_cleaned.csv", encoding="latin1")
    df = df.map(clean_cell)
    df["geometry"] = df["address"].apply(lambda x: geolocate_address(x))
    gdf = geopandas.GeoDataFrame(df, geometry="geometry")
    gdf.to_file("data/learner_intake/may_migration/learners_with_geometry.geojson", driver="GeoJSON")


if __name__ == '__main__':
    main()