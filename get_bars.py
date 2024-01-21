import datetime
import os
import argparse
import dotenv
import bars.day
import bars.hour
import bars.minute

dotenv.load_dotenv('.env')

api_key = os.getenv('API_KEY')
data_dir = os.getenv('DATA_DIR')

parser = argparse.ArgumentParser(description='Get bars')
parser.add_argument('--ticker', help='Ticker')
parser.add_argument('--timeframe', default='1-59m,1-24h,1-14d', help='Timeframe')
parser.add_argument('--from-year', default=datetime.datetime.now().year-5, help='From year')
parser.add_argument('--to-year', default=datetime.datetime.now().year, help='To year')
parser.add_argument('--force', help='Force', action='store_true')
args = parser.parse_args()

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
            ticker=args.ticker,
            from_year=args.from_year,
            to_year=args.to_year,
            force=args.force
        ).perform()
