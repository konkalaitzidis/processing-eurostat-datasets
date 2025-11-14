import pandas as pd
import re
import os

# Folder where Excel files are located
folder_path = "data"

# Define the exact order of files to process
ordered_files = [
    "countries_2000_W1-to_latest_available_dataset.xlsx",
    "NUTS1_2000_W1-to_latest_available_dataset.xlsx",
    "NUTS2_2000_W1-to_latest_available_dataset.xlsx",
    "NUTS3_2000_W1-2004_W53.xlsx",
    "NUTS3_2005_W1-2009_W53.xlsx",
    "NUTS3_2010_W1-2014_W52.xlsx",
    "NUTS3_2015_W1-2019_W52.xlsx",
    "NUTS3_2020_W1-to_latest_available_dataset.xlsx"
]

# Helper function to detect NUTS type from filename
def detect_nuts_type(filename: str):
    name = filename.lower()
    if "nuts3" in name:
        return "nuts3"
    elif "nuts2" in name:
        return "nuts2"
    elif "nuts1" in name:
        return "nuts1"
    elif "nuts0" in name:
        return "nuts0"
    elif "countries" in name:
        return "country"
    else:
        return "unknown"

# Function to read and transform one file
def process_file(file_path):
    try:
        # Detect NUTS type
        nuts_type = detect_nuts_type(os.path.basename(file_path))

        # Read the file to detect the header row
        df_preview = pd.read_excel(file_path, header=None)
        header_row = None
        for i in range(5):  # look at first 5 rows to find GEO headers
            if df_preview.iloc[i].astype(str).str.contains("GEO").any():
                header_row = i
                break
        if header_row is None:
            header_row = 0

        # Read with the correct header row
        df = pd.read_excel(file_path, header=header_row)

        # Clean column names
        df.columns = df.columns.astype(str).str.strip()

        # Find nuts and country columns
        nuts_col = next((c for c in df.columns if "Code" in c or "code" in c), None)
        country_col = next((c for c in df.columns if "Label" in c or "label" in c), None)

        if nuts_col is None or country_col is None:
            raise ValueError(f"Could not find NUTS or country columns in {file_path}")

        # Rename and melt
        df = df.rename(columns={nuts_col: "nuts", country_col: "country"})
        df_long = df.melt(id_vars=["nuts", "country"], var_name="time", value_name="number_of_death")

        # Drop missing values
        df_long = df_long.dropna(subset=["number_of_death"])

        # Extract year and week
        df_long[["year", "week"]] = df_long["time"].astype(str).str.extract(r"(\d{4})-W(\d{2})")

        # Add NUTS type
        df_long["nuts_type"] = nuts_type

        # Keep only needed columns
        df_long = df_long[["nuts_type", "nuts", "country", "year", "week", "number_of_death"]]

        print(f"✅ Processed {os.path.basename(file_path)} ({len(df_long)} rows)")
        return df_long

    except Exception as e:
        print(f"⚠️ Skipped {os.path.basename(file_path)} due to error: {e}")
        return pd.DataFrame()

# Process all files in the defined order
all_dfs = []
for filename in ordered_files:
    file_path = os.path.join(folder_path, filename)
    if os.path.exists(file_path):
        df = process_file(file_path)
        all_dfs.append(df)
    else:
        print(f"⚠️ File not found: {filename} — skipped.")

# Combine and export (as CSV to avoid Excel’s row limit)
if all_dfs:
    df_merged = pd.concat(all_dfs, ignore_index=True)
    output_path = "eurostat_mortality_all_levels.csv"
    df_merged.to_csv(output_path, index=False)
    print(f"\n🎯 All done! Combined file saved as: {output_path}")
    print(f"Total rows: {len(df_merged)}")
else:
    print("❌ No valid dataframes were created — check your source files.")
