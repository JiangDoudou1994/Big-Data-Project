import datetime
import re
import date_helper 

class Validate:
    # transfer rule to python to compare
    def validate(self,value,rule):
        value=date_helper.parse(value)
        try:
            compare=rule.split(' ')[0]
            target=rule.split(' ')[1]
            target=datetime.datetime.strptime(target,"%m/%d/%Y")
            if(re.match('[>=<]+',compare)):
                
                return eval('value {0} target'.format(compare))
        except Exception as error:
            print error

