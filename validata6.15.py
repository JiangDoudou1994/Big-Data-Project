from pyspark import SparkContext
sc=SparkContext(appName="validata6.7")

import datetime
import dateutil.parser as par

def parse(date):
    kwargs={}
    parsedate=par.parse(date,**kwargs)
    return parsedate

class MetaFileHandler:

    def __init__(self,file_name):
        self.file_name=file_name
        self.rules=['<= #NOW','!IN (DayOfWeek(),(1,7))']
        self.NOW=datetime.datetime.now()

    def meta_kv_mapper(self,x):
        x=x.strip().split(' ')
        return x

    def validate_on_rules(self,rule,time):
        err = ''
        if rule == '<= #NOW':
            if time > self.NOW:
                err = 'Greater than now; '
        elif rule == '!IN (DayOfWeek(),(1,7))':
            if time.weekday() == 0 or time.weekday() == 6:
                err = 'Date is monsday or sunday;'
        return err

    def meta_validate_fields(self,x):
        err=''
        time=parse(x[2])
        for rule in self.rules:
            err = err + self.validate_on_rules(rule,time)
        x[2]=time.strftime('%d-%m-%Y')
        return x[0],x[1],x[2],err

    def meta_to_str(self,x):
        return (' ').join((x[0],x[1],x[2],x[3],'\n'))

meta = MetaFileHandler('testdata.txt')

rdd = sc.textFile(meta.file_name)

rdd_kv = rdd.map(lambda x: meta.meta_kv_mapper(x))

rdd_kv_invalid = rdd_kv.map(lambda x: meta.meta_validate_fields(x)).filter(lambda x: x[3] > '')

rdd_to_str = rdd_kv_invalid.map(lambda x: meta.meta_to_str(x))

f = open('err_data.txt','w')

data = rdd_to_str.reduce(lambda a,b: a+b)

f.write(data)
