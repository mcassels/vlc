from pandas import read_csv
from os import scandir
import datefinder
from datetime import datetime
from thefuzz import fuzz
from typing import NamedTuple, Union, List

class Expiry(NamedTuple):
    name: str
    expiry: datetime

def get_crc_expiries() -> list[Expiry]:
    volunteers = []
    for f in scandir("data/aa ACCEPTED - OK ACTIVE VOLUNTEERS"):
        if not f.is_dir():
            continue

        volunteer = f.name.split(' - ')[0]
        if not volunteer:
            print(f"Skipping {f.name}; could not extract volunteer name")

        rest = f.name.removeprefix(volunteer + ' - ')
        dates = list(datefinder.find_dates(rest))
        if len(dates) != 1:
            print(f"Skipping {f.name}; could not extract expiry date")
            continue

        volunteers.append(Expiry(volunteer, dates[0]))
    return volunteers

def find_closest(name: str, expiries: list[Expiry]) -> tuple[int, Expiry]:
    ratios = [(fuzz.ratio(name, e.name), e) for e in expiries]
    ratios.sort(key=lambda x: x[0], reverse=True)
    return ratios[0]

def get_all_names_ordered():
    df = read_csv("data/volunteers.csv")
    df['name'] = df.apply(lambda x: f"{x.FirstName} {x.LastNameNew}", axis=1)
    return list(df['name'])

def main():
    expiries = get_crc_expiries()
    names = get_all_names_ordered()
    ordered_expiries = []
    for name in names:
        [ratio, expiry] = find_closest(name, expiries)
        formatted_date = expiry.expiry.strftime("%m/01/%Y")
        print(f"name: {name}, expiry name: {expiry.name} expiry: {formatted_date}, ratio: {ratio}")
        ordered_expiries.append({ "name": name, "expiry_name": expiry.name, "crc_expiry_date": expiry.expiry.strftime("%m/01/%YYYY") })

if __name__ == '__main__':
    main()