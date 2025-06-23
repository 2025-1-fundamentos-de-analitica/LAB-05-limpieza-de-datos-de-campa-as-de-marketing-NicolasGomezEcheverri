import os
import pandas as pd

def clean_campaign_data():
    input_path = "files/input"
    output_path = "files/output"
    os.makedirs(output_path, exist_ok=True)

    all_data = []
    for file in os.listdir(input_path):
        if file.endswith(".zip"):
            path = os.path.join(input_path, file)
            df = pd.read_csv(path, compression="zip")
            all_data.append(df)

    df = pd.concat(all_data, ignore_index=True)

    # CLIENT CSV
    client = df[[
        "client_id", "age", "job", "marital", "education", "credit_default", "mortgage"
    ]].copy()
    client["job"] = client["job"].str.replace(".", "", regex=False).str.replace("-", "_", regex=False)
    client["education"] = client["education"].str.replace(".", "_", regex=False)
    client["education"] = client["education"].replace("unknown", pd.NA)
    client["credit_default"] = client["credit_default"].apply(lambda x: 1 if x == "yes" else 0)
    client["mortgage"] = client["mortgage"].apply(lambda x: 1 if x == "yes" else 0)
    client.to_csv(os.path.join(output_path, "client.csv"), index=False)

    # CAMPAIGN CSV
    campaign = df[[
        "client_id", "number_contacts", "contact_duration",
        "previous_campaign_contacts", "previous_outcome",
        "campaign_outcome", "day", "month"
    ]].copy()

    campaign["previous_outcome"] = campaign["previous_outcome"].apply(lambda x: 1 if x == "success" else 0)
    campaign["campaign_outcome"] = campaign["campaign_outcome"].apply(lambda x: 1 if x == "yes" else 0)

    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }

    campaign["month"] = campaign["month"].str.lower().map(month_map)
    campaign["day"] = campaign["day"].astype(str).str.zfill(2)
    campaign["last_contact_date"] = "2022-" + campaign["month"] + "-" + campaign["day"]
    campaign.drop(columns=["day", "month"], inplace=True)
    campaign.to_csv(os.path.join(output_path, "campaign.csv"), index=False)

    # ECONOMICS CSV
    economics = df[[
        "client_id", "cons_price_idx", "euribor_three_months"
    ]].copy()
    economics.to_csv(os.path.join(output_path, "economics.csv"), index=False)
