from pyspark import SparkContext
sc=SparkContext(appName="validation")

class MetaFileHandler:

    def __init__(self, file_name):
        self.file_name=file_name

    def meta_split(self,x):
        x = x.strip().split(',')
        if x[0] == 'Year' or len(x) < 10:
            x = 'unuseful'
        return x

    def meta_kv_mapper(self, x):
        return x[0],x[1],x[2],x[13]

    def meta_validate_fields(self, x):
        err=''
        if x[0] < '2015':
            err = err + 'date is too old'
        return (',').join((x[0],x[1],x[2],x[3],err))+'\n'

meta = MetaFileHandler('Emergency_Department_Encounters_by_Expected_Payer__2005_-_2015.csv')

rdd=sc.textFile(meta.file_name)

rdd_split = rdd.map(lambda x: meta.meta_split(x)).filter(lambda x: x != 'unuseful')

rdd_kv = rdd_split.map(lambda x: meta.meta_kv_mapper(x))

rdd_kv_invalid = rdd_kv.map(lambda x: meta.meta_validate_fields(x)).filter(lambda x: x[4] > '')

f = open('err_data.txt','w')

data = rdd_kv_invalid.reduce(lambda a,b: a+b)

f.write(data)
