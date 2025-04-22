import pandas

"""
Output columns:
intake_date	full_legal_name	preferred_name	pronouns	address	email	phone	birthdate	age	tutoring_method	status
"""
def main():
    df = pandas.read_excel("data/learner_intake/learner_import_file_tidied.xlsx")

if __name__ == '__main__':
    main()