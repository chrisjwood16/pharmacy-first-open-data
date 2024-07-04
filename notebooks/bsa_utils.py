import grequests
import pandas as pd
import re
import requests
import warnings
import urllib.parse
from datetime import datetime

warnings.simplefilter("ignore", category=UserWarning)

base_endpoint = 'https://opendata.nhsbsa.net/api/3/action/'
package_list_method = 'package_list'     # List of data-sets in the portal
package_show_method = 'package_show?id=' # List all resources of a data-set
action_method = 'datastore_search_sql?'  # SQL action method

# Function to convert YYYYMM to YYYY-MM-DD
def convert(date_str):
    return datetime.strptime(date_str, "%Y%m").strftime("%Y-%m-%d")

def show_available_datasets():
    # Extract list of datasets
    datasets_response = requests.get(base_endpoint +  package_list_method).json()
    
    # Get as a list
    dataset_list=datasets_response['result']
    
    # Excluse FOIs from the results
    list_to_exclude=["foi"]
    filtered_list = [item for item in dataset_list if not any(item.startswith(prefix) for prefix in list_to_exclude)]
    
    # Print available datasets
    for item in filtered_list:
        print (item)

def resource_name_list_filter(resource, date_from="earliest", date_to="latest"):
    metadata_repsonse  = requests.get(f"{base_endpoint}" \
                                      f"{package_show_method}" \
                                      f"{resource}").json()
    
    resources_table  = pd.json_normalize(metadata_repsonse['result']['resources'])
    
    # Extract date from bq_table_name and add this as a new column date
    resources_table['date'] = pd.to_datetime(resources_table['bq_table_name'].str.extract(r'(\d{6})')[0], format='%Y%m', errors='coerce')
    
    # Set date filters based on input
    if date_from == "earliest" or date_from == "":
        date_from = resources_table['date'].min()
    else:
        date_from = convert(date_from)  # Convert from YYYYMM to YYYY-MM-DD
    
    if date_to == "latest" or date_to=="":
        date_to = resources_table['date'].max()
    else:
        date_to = convert(date_to)  # Convert from YYYYMM to YYYY-MM-DD

    # Filter the DataFrame
    filtered_df = resources_table[(resources_table['date'] >= date_from) & (resources_table['date'] <= date_to)]
    
    # Extract the 'bq_table_name' column as a list
    bq_table_name_list = filtered_df['bq_table_name'].tolist()

    # Return list of resource names
    return bq_table_name_list

def async_query(resource_id, sql):
    placeholder = "{FROM_TABLE}"
    if placeholder not in sql:
        raise ValueError(f"Placeholder {placeholder} not found in the SQL query.")
    sql = sql.replace(placeholder, f"FROM `{resource_id}`")
    return sql

def fetch_data(resource, sql, date_from, date_to):
    # Create blank list to append each URL to call
    async_api_calls = []

    # Create blank list of resources for error handling
    resource_names = []
    
    # Get a list of resources matching criteria (months to include)
    resource_name_list=resource_name_list_filter(resource, date_from, date_to)
    
    # Generate a list of API calls (and resource_names for error handling)
    for resource_id in resource_name_list:
        resource_names.append(resource_id)
        async_api_calls.append(
            f"{base_endpoint}" \
            f"{action_method}" \
            "resource_id=" \
            f"{resource_id}" \
            "&" \
            "sql=" \
            f"{urllib.parse.quote(async_query(resource_id, sql))}" # Encode spaces in the url 
        )
    
    # Process API calls
    dd = (grequests.get(u) for u in async_api_calls)
    res = grequests.map(dd)
    
    # Check all API calls ran OK
    all_ok = all(x.ok for x in res)
    any_ok = any(x.ok for x in res)
        
    if all_ok:
        print("All API calls ran successfully")
    else:
        # Print information about successful and failed API calls
        for idx, response in enumerate(res):
            if response.ok:
                print(f"API call for resource {resource_names[idx]} succeeded")
            else:
                print(f"API call for resource {resource_names[idx]} failed with status code {response.status_code}")

    if not all_ok:
        raise ValueError(f"API call error.")
    
    # Initialize an empty list to store the temporary dataframes
    dataframes = []
    
    for x in res:
        # Grab the response JSON as a temporary list
        tmp_response = x.json()
        
        # Extract records in the response to a temporary dataframe
        tmp_df = pd.json_normalize(tmp_response['result']['result']['records'])
        
        # Append the temporary dataframe to the list
        dataframes.append(tmp_df)
    
    # Concatenate all dataframes in the list into a single dataframe
    async_df = pd.concat(dataframes, ignore_index=True)

    # Return dataframe
    return async_df