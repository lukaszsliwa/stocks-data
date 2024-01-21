import json
import requests
import calendar
import datetime
import dotenv
import os
import argparse
import tqdm
import bars

class Get(bars.Base):
    def perform(self):
        if self.progressbar:
            self.progressbar = tqdm.tqdm(total=len(self.tickers), desc=f"Downloading {len(self.tickers)} ticker(s)")

        for ticker in self.tickers:
            ticker_path = f'{self.data_dir}/{self.timeframe}/{ticker}'.replace(':', '_')
            os.makedirs(ticker_path, exist_ok=True)

            ticker_path = f'{self.data_dir}/{self.timeframe}/{ticker}/all.json'.replace(':', '_')

            if self.progressbar:
                self.progressbar.set_description(f'{ticker} {self.timeframe}')
                self.progressbar.update()

            if not self.force and os.path.exists(ticker_path):
                continue

            from_ = datetime.date.fromisoformat(f'{self.from_year}-01-01')
            lastday = calendar.monthrange(int(self.to_year), 12)[1]
            to_   = datetime.date.fromisoformat(f'{self.to_year}-12-{lastday}')

            aggs_url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{self.multiplier}/day/{from_}/{to_}?adjusted=true&sort=asc&limit={self.limit}&apiKey={self.api_key}'

            response = json.loads(requests.get(aggs_url).text)
            with open(ticker_path, 'w') as f:
                json.dump(response, f)

if __name__ == '__main__':
    dotenv.load_dotenv('../../.env')

    api_key = os.getenv('API_KEY')
    data_dir = os.getenv('DATA_DIR')

    parser = argparse.ArgumentParser(description='Get day bars')
    parser.add_argument('--tickers-file', default=None, help='Tickers file')
    parser.add_argument('--tickers', default=None, help='Tickers')
    parser.add_argument('--ticker', default=None, help='Ticker')
    parser.add_argument('--multiplier', default=1, help='Multiplier')
    parser.add_argument('--from-year', default=2019, help='From year')
    parser.add_argument('--to-year', default=2024, help='To year')
    parser.add_argument('--force', action='store_true', help='Force download')
    args = parser.parse_args()

    Get(
        api_key=api_key,
        data_dir=data_dir,
        tickers_file=args.tickers_file,
        ticker=args.ticker,
        timeframe=f'{args.multiplier}d',
        from_year=args.from_year,
        to_year=args.to_year,
        force=args.force,
        limit=5000
    ).perform()
