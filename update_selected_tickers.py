import datetime

import polygon
import json
import requests
import csv

import os
import dotenv

import argparse

dotenv.load_dotenv('.env')

api_key = os.getenv('API_KEY')
client = polygon.RESTClient(api_key=api_key)

results = []

with open('selected.csv', 'r') as f:
    csv_reader = csv.reader(f, delimiter=',')
    lines = 0
    for row in csv_reader:
        if lines > 0:
            stock = row[0]
            aggs_url = f'https://api.polygon.io/v2/aggs/ticker/{stock}/prev?adjusted=true&apiKey={api_key}'
            aggs = json.loads(requests.get(aggs_url).text)
            result = {
                'ticker': stock,
                'exchange': '',
                'open': aggs['results'][0]['o'],
                'high': aggs['results'][0]['h'],
                'low': aggs['results'][0]['l'],
                'close': aggs['results'][0]['c'],
                'volume': aggs['results'][0]['v'],
                'last_updated_utc': datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            }
            details_url = f'https://api.polygon.io/v3/reference/tickers/{stock}?apiKey={api_key}'
            details = json.loads(requests.get(details_url).text)
            if 'results' in details and 'primary_exchange' in details['results']:
                result['exchange'] = details['results']['primary_exchange']
            results.append(result)
        lines += 1

def by_volume(row):
    return row['volume']

list.sort(results, key=by_volume, reverse=True)

with open(f'selected.csv', 'w') as f:
    csv_writer = csv.writer(f)
    count = 0
    for result in results:
        if count == 0:
            header = result.keys()
            csv_writer.writerow(header)
        count += 1
        csv_writer.writerow(result.values())
