from pyspark import SparkContext
import sys
import glob
import datetime
import date_helper
from metafile_handler import MetaFileHandler



argv=sys.argv
partitions=int(argv[5])

sc = SparkContext(appName="cdc")
for src in glob.glob('*.py'):
    sc.addPyFile(src)
sc.addPyFile('spooky.egg')

meta_data1 = sc.textFile(argv[1])
meta_data2 = sc.textFile(argv[2])
meta_handler1=MetaFileHandler(meta_data1)
meta_handler2=MetaFileHandler(meta_data2)

rdd1 = sc.textFile(argv[3],partitions)
rdd2 = sc.textFile(argv[4],partitions)

# find key columns from meta file and generate to key-value map of rdd
rdd1_kv = rdd1.map(lambda x: meta_handler1.meta_kv_mapper(x)).partitionBy(partitions,lambda x:meta_handler1.hash_key(x[0]))
rdd2_kv = rdd2.map(lambda x: meta_handler2.meta_kv_mapper(x)).partitionBy(partitions,lambda x:meta_handler1.hash_key(x[0]))

# covert format to same 
rdd1_handle = rdd1_kv.map(lambda x: meta_handler1.handle_data(x))
rdd2_handle = rdd2_kv.map(lambda x: meta_handler2.handle_data(x))

# compare with full outer join, 
rdd_combo = rdd1_handle.fullOuterJoin(rdd2_handle)
rdd_del = rdd_combo.filter(lambda x: x[1][0] == None)
rdd_add = rdd_combo.filter(lambda x: x[1][1] == None)
rdd_chg = rdd_combo.filter(lambda x: x[1][0] != None and x[1][
                           1] != None and meta_handler1.compare(x[1]))

now = datetime.datetime.now()
try:
    rdd_del.saveAsTextFile('cdc_del.txt_{:%H_%M}'.format(now))
    rdd_add.saveAsTextFile('cdc_add.txt_{:%H_%M}'.format(now))
    rdd_chg.saveAsTextFile('cdc_chg.txt_{:%H_%M}'.format(now))
except Exception as error:
    print error.args
