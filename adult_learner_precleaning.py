import pandas

"""
Output columns:
intake_date	full_legal_name	preferred_name	pronouns	address	email	phone	birthdate	age	tutoring_method	status parent_guardian
"""
def main():
    df = pandas.read_csv("data/learner_intake/may_migration/adult_raw_data.csv", encoding="latin1")
    df.to_csv("data/learner_intake/may_migration/adult_data_cleaned.csv", index=False)

if __name__ == '__main__':
    main()