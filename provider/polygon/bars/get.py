from provider.polygon.bars import Base

class Get:
    def __init__(self, timeframe=None, progressbar=True, api_key=None, data_dir=None, tickers_file=None, ticker=None,
                 from_date=None, to_date=None, force=None, limit=50000):
        self.api_key = api_key
        self.data_dir = data_dir
        self.tickers_file = tickers_file
        self.ticker = ticker
        self.tickers = []
        self.from_date = from_date
        self.to_date = to_date
        self.force = force
        self.limit = limit
        self.timeframe = timeframe
        self.progressbar = progressbar

    def create(self):
        params = {
            'from_date': self.from_date, 'to_date': self.to_date, 'ticker': self.ticker, 'force': self.force, 'tickers': self.tickers,
            'api_key': self.api_key, 'data_dir': self.data_dir, 'tickers_file': self.tickers_file,
            'limit': self.limit, 'progressbar': self.progressbar, 'timeframe': self.timeframe
        }
        return Base(**params)

    def perform(self):
        object = self.create()
        object.perform()
        object.save()
