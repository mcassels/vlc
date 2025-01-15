import geopandas
from shapely.ops import unary_union
from learner_import import get_learner_neighbourhood


def main():
  df = geopandas.read_file("data/learner_intake/ABMS_MUNICIPALITIES_SP.geojson", driver="GeoJSON")
  df["neighbourhood"] = df['ADMIN_AREA_ABBREVIATION'].apply(get_learner_neighbourhood)
  df = df[df["neighbourhood"] != "Other"]

  df = df[["neighbourhood", "geometry"]]
  df = df.dissolve(by="neighbourhood")
  df.to_file("neighbourhoods.geojson", driver="GeoJSON")

if __name__ == '__main__':
  main()