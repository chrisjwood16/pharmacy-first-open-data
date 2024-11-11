import pandas as pd
import requests
from bs4 import BeautifulSoup

# This fetches csv files for Pharmacy and appliance contractor dispensing data from https://www.nhsbsa.nhs.uk/prescription-data/dispensing-data/dispensing-contractors-data for Feb 24 (first month with Pharmacy First data) onwards.

def extract_yyyymm_from_url(url):
    """
    Extracts the YYYYMM part from a URL that contains a month abbreviation and a 2-digit year part.
    The month abbreviation is expected to be one of the following:
    - Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec
    - January, February, March, April, May, June, July, August, September, October, November, December
    The year part is expected to be a 2-digit year.
    The month abbreviation and the year part are expected to be separated by '%20'.
    The month abbreviation is expected to be followed by the year part in the URL.
    If the month abbreviation and the year part are not found in the URL, None is returned.
    If the month abbreviation and the year part are found in the URL, the YYYYMM part is returned.
    """
    parts = url.split('%20')
    month_mapping = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06',
        'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12',
        'January': '01', 'February': '02', 'March': '03', 'April': '04', 'May': '05', 'June': '06',
        'July': '07', 'August': '08', 'September': '09', 'October': '10', 'November': '11', 'December': '12'
    }

    # Extract the month abbreviation and the year part from the URL
    for i, part in enumerate(parts):
        if part in month_mapping:
            month_str = month_mapping[part]
            year_str = parts[i + 1][:2]
            break
    else:
        return None

    # Convert 2-digit year to 4-digit year
    year_number_str = f'20{year_str}'

    return year_number_str + month_str

def fetch_contractor_data():
    url = "https://www.nhsbsa.nhs.uk/prescription-data/dispensing-data/dispensing-contractors-data"
    base_url = "https://www.nhsbsa.nhs.uk"

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    csv_links = []

    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.endswith('.csv') and 'Dispensing%20Data' in href:
            full_url = base_url + href
            csv_links.append(full_url)



    link_list=[]

    for link in csv_links:
        yyyymm = extract_yyyymm_from_url(link)
        #print(f"Full URL: {link}, Extracted YYYYMM: {yyyymm}")
        link_list.append({'month': yyyymm, 'url': link})

    df = pd.DataFrame(link_list)

    # Convert the 'month' column to datetime
    df['month'] = pd.to_datetime(df['month'], format='%Y%m')

    # Filter the DataFrame to only include months on or after February 2024 as this is when Pharmacy First columns first appear
    filtered_df = df[df['month'] >= '2024-02-01']

    # List to hold the resulting dataframes
    df_list = []

    # Iterate through the filtered_df
    for index, row in filtered_df.iterrows():
        #print(f"Month: {row['month']}, URL: {row['url']}")
        df_fetched = pd.read_csv(row['url'])
        df_fetched['month'] = row['month']
        df_list.append(df_fetched)

    # Combine all dataframes in df_list into a single dataframe
    combined_df = pd.concat(df_list, ignore_index=True)

    # Normalise column names to remove gaps around hyphen (NumberofPharmacyFirstClinicalPathwaysConsultations -AcuteSoreThroat column name has an additional space) 
    combined_df.columns = combined_df.columns.str.replace(r'\s*-\s*', '-', regex=True)

    # Save the combined dataframe to a CSV file
    combined_df.to_csv("../data/contractor_data.csv", index=False)

    return combined_df