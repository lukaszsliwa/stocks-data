import datetime
import csv
import glob
import tqdm
import json

class Base:
    def __init__(self, progressbar=True, api_key=None, data_dir=None, tickers_file=None, ticker=None, timeframe=None,
                 from_year=None, to_year=None, force=None, limit=5000, tickers=list()):
        self.api_key = api_key
        self.data_dir = data_dir
        self.tickers_file = tickers_file
        self.ticker = ticker
        self.tickers = tickers
        self.timeframe = timeframe
        self.multiplier = int(timeframe[:-1])
        self.from_year = from_year
        self.to_year = to_year
        self.force = force
        self.limit = limit
        self.progressbar = progressbar

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

import bars

class Get:
    def __init__(self, timeframe=None, progressbar=True, api_key=None, data_dir=None, tickers_file=None, ticker=None,
                 from_year=None, to_year=None, force=None, limit=5000):
        self.api_key = api_key
        self.data_dir = data_dir
        self.tickers_file = tickers_file
        self.ticker = ticker
        self.tickers = []
        self.from_year = from_year
        self.to_year = to_year
        self.force = force
        self.limit = limit
        self.timeframe = timeframe
        self.progressbar = progressbar

    def create(self):
        params = {
            'from_year': self.from_year, 'to_year': self.to_year, 'ticker': self.ticker, 'force': self.force, 'tickers': self.tickers,
            'api_key': self.api_key, 'data_dir': self.data_dir, 'tickers_file': self.tickers_file,
            'limit': self.limit, 'progressbar': self.progressbar, 'timeframe': self.timeframe
        }
        frame = self.timeframe[-1]
        if frame == 'd':
            return bars.day.Get(**params)
        elif frame == 'm':
            return bars.minute.Get(**params)
        elif frame == 'h':
            return bars.hour.Get(**params)

    def perform(self):
        object = self.create()
        object.perform()
        object.save()
