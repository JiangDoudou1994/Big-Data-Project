import datetime


class Function():

	
    # All function need list at here to let rule_parse extract
    def __dir__(self):
        return ['date_add', 'day_of_week']

    # Add number of days to special day
    def date_add(self, date_type, num, value):
        result = 'xxx'
        if date_type == 'D':
            date = datetime.datetime.strptime(value, "%m/%d/%Y")
            result = date + datetime.timedelta(days=int(num))

        return result.strftime("%m/%d/%Y")
    # Get week day of special date
    def day_of_week(self, value):
        return str(value.weekday())
