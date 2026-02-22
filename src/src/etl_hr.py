import pandas as pd


def extract():
    return pd.read_csv("data/raw/hr_data_raw.csv")


def transform(df):
    df = df.copy()

    df["DOB"] = pd.to_datetime(df["DOB"], errors="coerce", format="mixed")
    invalid_dob = df["DOB"].isna().sum()
    print(f"[INFO] Dates de naissance invalides (NaT) : {invalid_dob}")
    df["Age"] = (pd.Timestamp("today") - df["DOB"]).dt.days // 365

    df = df[
        [
            "EmpID",
            "Department",
            "Position",
            "Gender",
            "Salary",
            "PerformanceScore",
            "Termd",
            "Age",
            "Absences",
            "DaysLateLast30",
        ]
    ]

    return df


def load(df):
    df.to_csv("data/processed/employee_hr_clean.csv", index=False)


if __name__ == "__main__":
    df_raw = extract()
    df_clean = transform(df_raw)
    load(df_clean)
    print("ETL pipeline executed successfully.")
