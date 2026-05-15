import pandas as pd
import sqlite3

base = r"data\raw\charter accountability\GA"

df = pd.read_csv(fr"{base}\local_charter_dataset.csv", dtype=str)

# 1) authorizers file
auth = (
    df[["authorizer"]]
    .dropna()
    .drop_duplicates()
    .rename(columns={"authorizer": "authorizer_name"})
)
auth.to_csv(fr"{base}\ga_authorizers.csv", index=False)

# 2) links file
conn = sqlite3.connect(r"data\cd_command_center.sqlite")
schools = pd.read_sql_query(
    "SELECT nces_id, school_name FROM schools WHERE state='GA' AND is_charter=1",
    conn,
)
conn.close()

links = (
    df[["school_name", "authorizer"]]
    .dropna()
    .rename(columns={"authorizer": "authorizer_name"})
)

merged = links.merge(schools, on="school_name", how="left")
merged["school_year"] = "2023-24"

out = merged.rename(columns={"nces_id": "nces_school_id"})[
    ["nces_school_id", "authorizer_name", "school_year"]
]
out.to_csv(fr"{base}\ga_school_authorizer_links.csv", index=False)

print("Created: ga_authorizers.csv and ga_school_authorizer_links.csv")
print("Authorizers:", len(auth))
print("Links:", len(out))
print("Missing NCES IDs:", out["nces_school_id"].isna().sum())
