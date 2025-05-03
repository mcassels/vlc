import pandas
import re

def split_on_non_letters(s):
    return re.split(r'[^a-zA-Z]+', s)

def split_pronouns_from_preferred_name(preferred_name: str|None) -> tuple[str|None, str|None]:
    if pandas.isna(preferred_name) or preferred_name is None or preferred_name == "":
        return None, None
    lowered = preferred_name.lower()
    if lowered == "n/a" or lowered == "no preference":
        return None, None
    split = split_on_non_letters(preferred_name)
    pronouns = None
    name = None
    for word in split:
        lowered_word = word.lower()
        if lowered_word in ["he", "him", "his"]:
            pronouns = "he/him"
        elif lowered_word in ["she", "her", "hers"]:
            pronouns = "she/her"
        else:
            name = word
    return name, pronouns


def fill_in_preferred_name(row: pandas.Series) -> str|None:
    if (pandas.isna(row['preferred_name']) or row['preferred_name'] is None or row['preferred_name'] == "") and isinstance(row['full_legal_name'], str):
          return row['full_legal_name'].split(" ")[0]
    return row['preferred_name']

"""
Output columns:
intake_date	full_legal_name	preferred_name	pronouns	address	email	phone	birthdate	age	tutoring_method	status parent_guardian
"""
def main():
    df = pandas.read_csv("data/learner_intake/may_migration/child_raw_data.csv", encoding="latin1")
    df[['preferred_name', 'pronouns']] = df['preferred_name'].apply(lambda x: split_pronouns_from_preferred_name(x)).apply(pandas.Series)

    df["preferred_name"] = df.apply(fill_in_preferred_name, axis=1)
    df.to_csv("data/learner_intake/may_migration/child_data_cleaned.csv", index=False)

if __name__ == '__main__':
    main()