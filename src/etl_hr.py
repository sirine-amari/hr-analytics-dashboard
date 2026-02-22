import pandas as pd


def extract():
    return pd.read_csv("data/raw/hr_data_raw.csv")


def transform(df):
    df = df.copy()

    df["DOB"] = pd.to_datetime(df["DOB"], errors="coerce", format="mixed")
    invalid_dob = df["DOB"].isna().sum()
    print(f"[INFO] Dates de naissance invalides (NaT) : {invalid_dob}")
    df["Age"] = pd.Timestamp.today().year - df["DOB"].dt.year
    df = df[(df["Age"] > 0) & (df["Age"] < 80)]
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

    # KPI globaux

    headcount = len(df)
    attrition_rate = df["Termd"].mean() * 100
    avg_salary = df["Salary"].mean()

    print(f"[KPI] Effectif total : {headcount}")
    print(f"[KPI] Taux d'attrition (%) : {attrition_rate:.2f}")
    print(f"[KPI] Salaire moyen : {avg_salary:.2f}")

    return df


def load(df):
    df.to_csv("data/processed/employee_hr_clean.csv", index=False)


def create_kpi_tables(df):

    # Attrition par département
    attrition_by_dept = df.groupby("Department")["Termd"].mean().reset_index()
    attrition_by_dept["AttritionRate"] = attrition_by_dept["Termd"] * 100
    attrition_by_dept.drop(columns=["Termd"], inplace=True)

    attrition_by_dept.to_csv("data/processed/attrition_by_department.csv", index=False)

    print("[INFO] Table attrition par département créée.")


if __name__ == "__main__":
    df_raw = extract()  # Extraction des données
    df_clean = transform(df_raw)  # Transformation
    load(df_clean)  # Sauvegarde du dataset
    create_kpi_tables(df_clean)  # Création des tables KPI
    print("ETL pipeline executed successfully.")
