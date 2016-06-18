from pyspark import SparkContext

import datetime
from utils import date_helper

class MetaFileHandler:

    def __init__(self, file_name):
        self.file_name = file_name

    def meta_kv_mapper(self, x):
        x = x.split(' ')
        return (x[0], x[2]), (x[1], x[4])

    def handle_data(self, x):
        time1 = date_helper.parse(x[1][0])
        time2 = date_helper.parse(x[1][1])
        x10 = time1.strftime('%m-%d-%Y')
        x11 = time2.strftime('%m-%d-%Y')
        return (x[0][0].split('.')[0], x[0][1].split('.')[0]), (x10, x11)

sc = SparkContext(appName="cdc")

meta1 = MetaFileHandler('testdata1.txt')
meta2 = MetaFileHandler('testdata2.txt')
rdd1 = sc.textFile(meta1.file_name)
rdd2 = sc.textFile(meta2.file_name)
rdd1_kv = rdd1.map(lambda x: meta1.meta_kv_mapper(x))
rdd2_kv = rdd2.map(lambda x: meta2.meta_kv_mapper(x))
rdd1_handle = rdd1_kv.map(lambda x: meta1.handle_data(x))
rdd2_handle = rdd2_kv.map(lambda x: meta2.handle_data(x))
rdd_combo = rdd1_handle.fullOuterJoin(rdd2_handle)
rdd_del = rdd_combo.filter(lambda x: x[1][0] == None)
rdd_add = rdd_combo.filter(lambda x: x[1][1] == None)
rdd_chg = rdd_combo.filter(lambda x: x[1][0] != None and x[1][
                           1] != None and x[1][0] != x[1][1])

now = datetime.datetime.now()
rdd_del.saveAsTextFile('cdc_del.txt_{:%H_%M}'.format(now))
rdd_add.saveAsTextFile('cdc_add.txt_{:%H_%M}'.format(now))
rdd_chg.saveAsTextFile('cdc_chg.txt_{:%H_%M}'.format(now))
