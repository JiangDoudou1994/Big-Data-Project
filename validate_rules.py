import datetime
import dateutil.parser as par


def parse(date):
    kwargs = {}
    parsedate = par.parse(date, **kwargs)
    return parsedate


class MetaFileHandler:

    def validate_oldest(self, time, oldest='01-01-1890'):
        oldest_time = parse(oldest)
        if time < oldest_time:
            return 'Earlier than odest time; '
        else:
            return ''

    def validate_now(self, time, now='16-06-2016'):
        now = parse(now)
        if time > now:
            return 'Greater than now; '
        else:
            return ''

    def validate_weekdays(self, time, weekdays=[0, 6]):
        if time.weekday() in weekdays:
            return 'Date is in invalid weekdays; '
        else:
            return ''

    def validate_fdm(self, time, day_add=5):
        day_of_time = int(time.strftime('%d'))
        if day_of_time <= day_add:
            return 'Date is earlier than 5th; '
        else:
            return ''

    def validate_fmm(self, time):
        day_of_time = int(time.strftime('%d'))
        weekday_of_time = time.weekday()
        if day_of_time <= 7:
            if weekday_of_time + 1 >= day_of_time:
                return 'Date is earlier than FMM; '
            else:
                return ''
        else:
            return ''
