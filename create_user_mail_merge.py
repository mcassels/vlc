import pandas
import math

def clean_email_address(email: str) -> str:
    if (type(email) is not str and math.isnan(email)):
        return ""
    return str(email).lower().strip()

def main():
    user_subset = pandas.read_excel("data/mail_merge/initial_users.xlsx")
    user_subset["EmailAddress"] = user_subset["Please add your email address:\n"].apply(lambda x: clean_email_address(x))
    user_subset = user_subset[user_subset["EmailAddress"] != ""]

    users = pandas.read_csv("data/mail_merge/volunteer_user_names_passwords.csv")
    users["EmailAddress"] = users["EmailAddress"].apply(lambda x: clean_email_address(x))

    joined = users.merge(user_subset[["EmailAddress"]], on="EmailAddress", how="inner")
    print("subset:", len(user_subset))
    print("users:", len(users))
    print("joined:", len(joined))

    subset_emails = set(user_subset["EmailAddress"])
    user_emails = set(users["EmailAddress"])
    print("subset - user:", subset_emails - user_emails)

    joined.to_csv("data/mail_merge/mail_merge_input.csv", index=False)



if __name__ == '__main__':
    main()