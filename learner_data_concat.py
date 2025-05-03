import pandas

"""
Output columns:
intake_date	full_legal_name	preferred_name	pronouns	address	email	phone	birthdate	age	tutoring_method	status parent_guardian
"""
def main():
    df1 = pandas.read_csv("data/learner_intake/may_migration/adult_data_cleaned.csv", encoding="latin1")
    df2 = pandas.read_csv("data/learner_intake/may_migration/child_data_cleaned.csv", encoding="latin1")
    combined = pandas.concat([df1, df2], ignore_index=True)
    combined.to_csv("data/learner_intake/may_migration/combined_data_cleaned.csv", index=False)

if __name__ == '__main__':
    main()