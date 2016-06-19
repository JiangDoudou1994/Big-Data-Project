import datetime

class Function():

    def __dir__(self):
        return ['date_add','day_of_week']

    def date_add(self,date_type,num,value):
        result='xxx'
        if date_type=='D':
           date=datetime.datetime.strptime(value,"%m/%d/%Y")
           result=date+datetime.timedelta(days=int(num))
            
        return result.strftime("%m/%d/%Y")

    def day_of_week(self,value):
       date=datetime.datetime.strptime(value,"%m/%d/%Y")
       return date.weekday()

