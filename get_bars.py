import datetime
import os
import argparse
import dotenv
import provider.polygon.bars as bars
import dateutil.relativedelta

dotenv.load_dotenv('.env')

api_key = os.getenv('API_KEY')
data_dir = os.getenv('DATA_DIR')

parser = argparse.ArgumentParser(description='Get bars')
parser.add_argument('--ticker', help='Ticker')
parser.add_argument('--tickers', default=None, help='Tickers')
parser.add_argument('--timeframe', default='1-59m,1-24h,1-30d', help='Timeframe')
parser.add_argument('--from-date', default=None, help='From')
parser.add_argument('--to-date', default=None, help='To')
parser.add_argument('--force', help='Force', action='store_true')
args = parser.parse_args()

if args.from_date is None:
    args.from_date = (datetime.datetime.now() - dateutil.relativedelta.relativedelta(years=5)).strftime('%Y-%m-%d')

if args.to_date is None:
    args.to_date = datetime.datetime.today().strftime('%Y-%m-%d')

tickers = []
if args.ticker:
    tickers = [args.ticker]

if args.tickers:
    tickers = args.tickers.split(',')

for ticker in tickers:
    for timeframes_str in args.timeframe.split(','):
        timeframes = [timeframes_str]
        if '-' in timeframes_str:
            timeframes = []
            frame = timeframes_str[-1]
            timeframe_from, timeframe_to = timeframes_str.replace(frame, '').split('-')
            timeframe_from = int(timeframe_from)
            timeframe_to = int(timeframe_to)
            for timeframe in range(timeframe_from, timeframe_to + 1):
                timeframes.append(f'{timeframe}{frame}')

        for timeframe in timeframes:
            bars.Get(
                data_dir=data_dir,
                api_key=api_key,
                timeframe=timeframe,
                ticker=ticker,
                from_date=args.from_date,
                to_date=args.to_date,
                force=args.force
            ).perform()
