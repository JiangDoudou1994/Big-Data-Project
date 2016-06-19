import datetime
from utils import date_helper


class PlaceHolder:

    # MUST ADD PLACEHOLDER TO HERE
    def __dir__(self):
        return ['FDM', 'NOW']

# current time
    def now(self, value):
        return datetime.datetime.now().strftime('%m/%d/%Y')

# first day of month
    def fdm(self, value):
        #        time = date_helper.parse(value)
        return datetime.date(value.year, value.month, 1).strftime('%m/%d/%Y')
