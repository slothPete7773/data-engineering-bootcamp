import json
import requests
import csv
DATA = "order-items"
url = f"http://34.87.139.82:8000/{DATA}/"
response = requests.get(url)
print(response)
fetched = response.json()

with open(f"/data/{DATA}.csv", "w") as f:
    writer = csv.writer(f)
    header = fetched[0].keys()
    writer.writerow(header)
    for each in fetched:
        writer.writerow(each.values())