import datetime
import re
from utils import date_helper 

class Validate:
    def validate(self,value,rule):
    #    value=datetime.datetime.strptime(value,"%m/%d/%Y")
        value=date_helper.parse(value)
        try:
            compare=rule.split(' ')[0]
            target=rule.split(' ')[1]
            target=datetime.datetime.strptime(target,"%m/%d/%Y")
            if(re.match('[>=<]+',compare)):
                
                return eval('value {0} target'.format(compare))
        except Exception as error:
            print error

