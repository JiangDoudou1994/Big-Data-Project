from pyspark import SparkContext
import sys
import datetime
import date_helper
from metafile_handler import MetaFileHandler

argv=sys.argv

sc = SparkContext(appName="cdc")
meta_data1 = sc.textFile(argv[1])
meta_data2 = sc.textFile(argv[2])
meta_handler1=MetaFileHandler(meta_data1)
meta_handler2=MetaFileHandler(meta_data2)

rdd1 = sc.textFile(argv[3])
rdd2 = sc.textFile(argv[4])
rdd1_kv = rdd1.map(lambda x: meta_handler1.meta_kv_mapper(x))
rdd2_kv = rdd2.map(lambda x: meta_handler2.meta_kv_mapper(x))
rdd1_handle = rdd1_kv.map(lambda x: meta_handler1.handle_data(x))
rdd2_handle = rdd2_kv.map(lambda x: meta_handler2.handle_data(x))
rdd_combo = rdd1_handle.fullOuterJoin(rdd2_handle)
rdd_del = rdd_combo.filter(lambda x: x[1][0] == None)
rdd_add = rdd_combo.filter(lambda x: x[1][1] == None)
rdd_chg = rdd_combo.filter(lambda x: x[1][0] != None and x[1][
                           1] != None and meta_handler1.compare(x[1]))

now = datetime.datetime.now()
rdd_del.saveAsTextFile('cdc_del.txt_{:%H_%M}'.format(now))
rdd_add.saveAsTextFile('cdc_add.txt_{:%H_%M}'.format(now))
rdd_chg.saveAsTextFile('cdc_chg.txt_{:%H_%M}'.format(now))
