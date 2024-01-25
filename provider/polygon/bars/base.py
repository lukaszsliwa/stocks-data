import glob
import tqdm
import json
import datetime
import csv
import os
import requests

class Base:
    def __init__(self, progressbar=True, api_key=None, data_dir=None, tickers_file=None, ticker=None, timeframe=None,
                 from_date=None, to_date=None, force=None, limit=50000, tickers=list()):
        self.api_key = api_key
        self.data_dir = data_dir
        self.tickers_file = tickers_file
        self.ticker = ticker
        self.tickers = tickers
        self.timeframe = timeframe
        self.multiplier = int(timeframe[:-1])
        self.from_date = from_date
        self.to_date = to_date
        self.force = force
        self.limit = limit
        self.progressbar = progressbar

        if timeframe[-1] == 'd':
            self.frame = 'day'
        elif timeframe[-1] == 'm':
            self.frame = 'minute'
        elif timeframe[-1] == 'h':
            self.frame = 'hour'

        if self.tickers:
            self.tickers = self.tickers.split(',')

        if self.tickers_file:
            with open(self.tickers_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row['last_updated_utc'] is not None and datetime.datetime.strptime(row['last_updated_utc'],
                                                                                          "%Y-%m-%dT%H:%M:%SZ").year == datetime.datetime.now().year:
                        self.tickers.append(row['ticker'])

        if self.ticker:
            self.tickers = [self.ticker]

    def perform(self):
        start = datetime.datetime.strptime(self.from_date, '%Y-%m-%d')
        end = datetime.datetime.strptime(self.to_date, '%Y-%m-%d')

        from_ = start.strftime('%Y-%m-%d')
        to_ = end.strftime('%Y-%m-%d')

        if self.progressbar:
            self.progressbar = tqdm.tqdm(total=len(self.tickers))

        for ticker in self.tickers:
            if self.progressbar:
                self.progressbar.set_description(f'{ticker} {self.timeframe}')

            ticker_path = f'{self.data_dir}/{self.timeframe}/{ticker}/{from_}/{to_}'.replace(':', '_')
            os.makedirs(ticker_path, exist_ok=True)

            if not self.force and os.path.exists(ticker_path):
                continue

            aggs_url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{self.multiplier}/{self.frame}/{from_}/{to_}?adjusted=true&sort=asc&limit={self.limit}&apiKey={self.api_key}'

            index = 0
            while True:
                response = json.loads(requests.get(aggs_url).text)
                with open(f'{ticker_path}/{index}.json', 'w') as f:
                    json.dump(response, f)

                if 'next_url' in response:
                    aggs_url = response['next_url'] + f'&adjusted=true&sort=asc&limit={self.limit}&apiKey={self.api_key}'
                else:
                    break
                index += 1

            if self.progressbar:
                self.progressbar.update()

    def save(self):
        for ticker in self.tickers:
            save_path = f'{self.data_dir}/{self.timeframe}/{ticker}.csv'.replace(':', '_')

            results = []

            with open(save_path, 'w') as csv:
                csv.write("datetime,date,time,high,low,open,close,volume\n")
                paths = glob.glob(f'{self.data_dir}/{self.timeframe}/{ticker}/**/*.json'.replace(':', '_'), recursive=True)

                if self.progressbar:
                    self.progressbar = tqdm.tqdm(total=len(paths), desc=save_path)

                for path in paths:
                    if self.progressbar:
                        self.progressbar.update()

                    with open(path.replace(':', '_')) as file:
                        data = json.load(file)
                        if 'results' in data:
                            for result in data['results']:
                                datetime_ = datetime.datetime.fromtimestamp(int(result['t']) / 1000)
                                date = datetime_.strftime('%Y-%m-%d')
                                time = datetime_.strftime('%H:%M:%S')
                                results.append([str(datetime_), str(date), str(time), str(result['h']), str(result['l']), str(result['o']), str(result['c']), str(result['v'])])
                def sort_by(row):
                    return datetime.datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
                for result in sorted(results, key=sort_by):
                    csv.write(f"{','.join(result)}\n")
