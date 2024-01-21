import polygon
import json
import requests
import csv
import os
import dotenv

dotenv.load_dotenv('.env')

api_key = os.getenv('API_KEY')
data_dir = os.getenv('DATA_DIR')

client = polygon.RESTClient(api_key=api_key)

results = []

ticker_idx = 0
tickers_url = f'https://api.polygon.io/v3/reference/tickers?active=true&limit=1000&apiKey={api_key}'
while True:
    ticker_idx += 1
    response = json.loads(requests.get(tickers_url + f'&apiKey={api_key}').text)
    with open(f'tickers/{ticker_idx}.json', 'w') as f:
        json.dump(response, f)
    for result in response['results']:
        results.append(result)
    if not 'next_url' in response:
        break
    tickers_url = response['next_url']

with open(f'tickers.csv', 'w') as f:
    csv_writer = csv.writer(f)
    count = 0
    for result in results:
        if count == 0:
            header = result.keys()
            csv_writer.writerow(header)
        count += 1
        csv_writer.writerow(result.values())

print(count)
