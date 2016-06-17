from pyspark import SparkContext
import datetime
import dateutil.parser as par

def parse(date):
    kwargs={}
    parsedate=par.parse(date,**kwargs)
    return parsedate

class MetaFileHandler:

    def __init__(self,file_name):
        self.file_name = file_name
	self.rules = ['>= 01/01/1890','<= #NOW','!IN (DayOfWeek(),(1,7))',
	            '#FDM,DateAdd(D,5,#FDM','>= #FMM']

    def meta_kv_mapper(self,x):
        x = x.strip().split(' ')
	return x

    def validate_oldest(self,time):
        oldest_time = parse('01-01-1890')
	if time < oldest_time:
	    return 'Earlier than odest time'
	else:
	    return ''

    def validate_now(self,time):
        now = parse('16-06-2016')
	if time > now:
	    return 'Greater than now'
	else:
	    return ''

    def validate_mon_or_sun(self,time):
        if time.weekday() == 0 or time.weekday() == 6:
            return 'Date is monday or sunday;'
	else:
	    return ''

    def validate_fdm(self,time):
        day_of_time = time.strftime('%d')
	if day_of_time <= '5':
	    return 'Date is earlier than 5th;'
	else:
	    return ''

    def validate_fmm(self,time):
        day_of_time = int(time.strftime('%d'))
	weekday_of_time = time.weekday()
	if day_of_time <= 7:
	    if weekday_of_time <= day_of_time-1 and weekday_of_time >= 0:
	        return 'Date is earlier than FMM'
	    else:
	        return ''
	else:
	    return ''

    def validate_on_rules(self,rule,time):
        err = ''
	if rule == '>= 01/01/1890':
	    err = self.validate_oldest(time)
	elif rule == '<= #NOW':
	    err = self.validate_now(time)
	elif rule == '!IN (DayOfWeek(),(1,7))':
	    err = self.validate_mon_or_sun(time)
	elif rule == '#FDM,DateAdd(D,5,#FDM':
	    err = self.validate_fdm(time)
	elif rule == '>= #FMM':
	    err = self.validate_fmm(time)
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

sc = SparkContext(appName="validata")
meta = MetaFileHandler('testdata.txt')
rdd = sc.textFile(meta.file_name)
rdd_kv = rdd.map(lambda x: meta.meta_kv_mapper(x))
rdd_kv_invalid = rdd_kv.map(lambda x: meta.meta_validate_fields(x)).filter(lambda x: x[3] > '')
rdd_to_str = rdd_kv_invalid.map(lambda x: meta.meta_to_str(x))
now=datetime.datetime.now()
rdd_to_str.saveAsTextFile('error_data_{:%H_%M}.txt'.format(now))
