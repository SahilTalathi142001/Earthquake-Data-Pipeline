import requests
import json

# Construct the API URL with start and end dates
url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}"

# Fetch data from the API
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    data = data['features']
    
    # Save the data to a JSON file
    file_path = f'/lakehouse/default/Files/{start_date}_earthquake_data.json'
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data successfully saved to {file_path}")
else:
    print("Failed to fetch data. Status code:", response.status_code)
