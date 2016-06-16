from pyspark import SparkContext
sc=SparkContext(appName="cdc")

import datetime
import dateutil.parser as par

def parse(date):
    kwargs={}
    parsedate=par.parse(date,**kwargs)
    return parsedate

class MetaFileHandler:
    
    def __init__(self, file_name):
        self.file_name=file_name
        
    def meta_kv_mapper(self, x):
        return (x[0], x[2]),(x[1],x[4])
        
    def handle_data(self,x):
        time1=parse(x[1][0])
        time2=parse(x[1][1])
        x[1][0]=time1.strftime('%d-%m-%Y')
        x[1][1]=time2.strftime('%d-%m-%Y')
        return (x[0][0].split('.')[0],x[0][1].split('.')[0]),(x[1][0],x[1][1])

meta1 = MetaFileHandler('testdata1.txt')
meta2 = MetaFileHandler('testdata2.txt')
rdd1 = sc.textFile(meta1.file_name)
rdd2 = sc.textFile(meta2.file_name)
rdd1_kv = rdd1.map(lambda x: meta1.meta_kv_mapper(x))
rdd2_kv = rdd2.map(lambda x: meta2.meta_kv_mapper(x))
rdd1_handle = rdd1_kv.map(lambda x: meta1.handle_data(x))
rdd2_handle = rdd2_kv.map(lambda x: meta2.handle_data(x))
rdd_combo = rdd1_handle.fullOuterJoin(rdd2_handle)
rdd_combo.collect()
rdd_del = rdd_combo.filter(lambda x: x[1][0] == None)
rdd_add = rdd_combo.filter(lambda x: x[1][1] == None)
rdd_chg = rdd_combo.filter(lambda x: x[1][0] != None and x[1][1] != None and x[1][0] != x[1][1])
rdd_del.collect()
rdd_add.collect()
print rdd_chg.collect()
