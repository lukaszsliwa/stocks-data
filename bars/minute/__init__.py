import json
import requests
import calendar
import datetime
import os

import dotenv
import argparse
import tqdm
import bars

class Get(bars.Base):
    def perform(self):
        progress_max = (int(self.to_year) - int(self.from_year) + 1) * 12

        for ticker in self.tickers:
            if self.progressbar:
                self.progressbar = tqdm.tqdm(total=progress_max, desc=f"{ticker}")
        
            for year in reversed(range(int(self.from_year), int(self.to_year) + 1)):
                ticker_path = f'{self.data_dir}/{self.timeframe}/{ticker}/{year}'.replace(':', '_')
                os.makedirs(ticker_path, exist_ok=True)
        
                for month in range(1, 13):
                    if self.progressbar:
                        self.progressbar.set_description(f'{ticker} {self.timeframe} ({"%02d" % (month,)}/{year})')
                        self.progressbar.update()
        
                    if not self.force and os.path.exists(f'{ticker_path}/{"%02d" % (month,)}.json'):
                        break
        
                    from_ = datetime.date.fromisoformat(f'{year}-{"%02d" % (month,)}-01')
                    lastday = calendar.monthrange(year, month)[1]
                    to_   = datetime.date.fromisoformat(f'{year}-{"%02d" % (month,)}-{lastday}')
        
                    aggs_url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{self.multiplier}/minute/{from_}/{to_}?adjusted=true&sort=asc&limit={self.limit}&apiKey={self.api_key}'
        
                    response = json.loads(requests.get(aggs_url).text)
                    with open(f'{ticker_path}/{"%02d" % (month,)}.json', 'w') as f:
                        json.dump(response, f)

if __name__ == '__main__':
    dotenv.load_dotenv('.env')

    api_key = os.getenv('API_KEY')
    data_dir = os.getenv('DATA_DIR')

    parser = argparse.ArgumentParser(description='Get minute bars')
    parser.add_argument('--tickers-file', default=None, help='Tickers file')
    parser.add_argument('--tickers', help='Tickers')
    parser.add_argument('--ticker', help='Ticker')
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
        timeframe=f'{args.multiplier}m',
        from_year=args.from_year,
        to_year=args.to_year,
        force=args.force,
        limit=5000
    ).perform()
    