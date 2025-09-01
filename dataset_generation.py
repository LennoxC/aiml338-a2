import os
import pandas as pd
from datetime import date, timedelta


def get_electricityprice_data(start_date, end_date, poc=None):
    # Ensure ./data folder exists
    os.makedirs("./data", exist_ok=True)

    filename = ""
    if poc is None:
        filename = f"{start_date.isoformat()}_{end_date.isoformat()}.csv"
    else:
        filename = f"{start_date.isoformat()}_{end_date.isoformat()}_poc{"_".join(poc)}.csv"

    filepath = os.path.join("data", filename)

    # load the data if the dataset already exists
    if os.path.exists(filepath):
        return pd.read_csv(filepath)

    print("Fetching data from web")
    df_accum = pd.DataFrame()

    current_date = start_date
    while current_date <= end_date:
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")
        day = current_date.strftime("%d")

        url = f"https://www.emi.ea.govt.nz/Wholesale/Datasets/DispatchAndPricing/DispatchEnergyPrices/{year}/{year}{month}{day}_DispatchEnergyPrices.csv"

        try:
            df_temp = pd.read_csv(url)

            if poc is not None:
                df_temp = df_temp[df_temp["PointOfConnection"].isin(poc)]

            df_accum = pd.concat([df_accum, df_temp], axis=0, ignore_index=True)
        except Exception as e:
            print(f"Failed to download {url}: {e}")

        current_date += timedelta(days=1)

    # Save the dataset for next time
    df_accum.to_csv(filepath, index=False)
    print(f"Saved dataset to {filepath}")

    return df_accum

#df_raw = get_electricityprice_data(start_date = date(2025, 8, 1), end_date = date(2025, 8, 10))