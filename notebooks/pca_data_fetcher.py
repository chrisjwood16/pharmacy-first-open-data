import pandas as pd
import bsa_utils

# This fetches csv files for Pharmacy and appliance contractor dispensing data from https://www.nhsbsa.nhs.uk/prescription-data/dispensing-data/dispensing-contractors-data for Feb 24 (first month with Pharmacy First data) onwards.

def fetch_pca_data():
    dataset_id = "prescription-cost-analysis-pca-monthly-data" # Dateset ID
    date_from = "202402" # Can be either "YYYYMM" or "earliest", default="earliest"
    date_to = "" # Can be either "YYYYMM" or "latest", default="latest"
    sql = (
        "SELECT * "
        "{FROM_TABLE} "
        "WHERE PHARMACY_ADVANCED_SERVICE = 'Pharmacy First Clinical Pathways'"
    )

    # Fetch data using BSA API
    df=bsa_utils.fetch_data(resource=dataset_id, date_from=date_from, date_to=date_to, sql=sql)

    # Save data to file
    df.to_csv("../data/pca_data.csv", index=False)

    # Convert YEAR_MONTH from YYYYMM to a string and then to datetime, setting day as 01
    df['YEAR_MONTH'] = pd.to_datetime(df['YEAR_MONTH'].astype(str) + '01', format='%Y%m%d')

    # Format the datetime object to YYYY-MM-DD
    #df['YEAR_MONTH'] = df['YEAR_MONTH'].dt.strftime('%Y-%m-%d')

    return df