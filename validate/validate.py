import sys
from pyspark import SparkContext
import datetime
from metafile_handler import MetaFileHandler


argv=sys.argv

sc = SparkContext(appName="validata")
meta_data = sc.textFile(argv[0])

meta_handler = MetaFileHandler(meta_data)
rdd = sc.textFile(argv[1])
rdd_kv = rdd.map(lambda x: meta_handler.meta_kv_mapper(x))
rdd_kv_invalid = rdd_kv.map(
    lambda x: meta_handler.meta_validate_fields(x)).filter(lambda x: x[1] > '')
rdd_to_str = rdd_kv_invalid.map(
    lambda x: meta_handler.meta_to_str(x))  
now = datetime.datetime.now()
rdd_to_str.saveAsTextFile('validate_error_{:%H_%M}.txt'.format(now))
