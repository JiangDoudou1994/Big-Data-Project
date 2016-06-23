import sys
import glob
from pyspark import SparkContext
import datetime
from metafile_handler import MetaFileHandler


argv = sys.argv
partitions = int(argv[3])

sc = SparkContext(appName="validate")
# load required files
for src in glob.glob('*.py'):
    sc.addPyFile(src)
sc.addPyFile('pyparsing.egg')

meta_data = sc.textFile(argv[1])
meta_handler = MetaFileHandler(meta_data)
rdd = sc.textFile(argv[2],partitions)

# generate key value
rdd_kv = rdd.map(lambda x: meta_handler.meta_kv_mapper(x))

# validate data, keep validate message is not empty
rdd_kv_invalid = rdd_kv.mapPartitions(
    lambda x: meta_handler.meta_validate_fields(x)).filter(lambda x: x[1] > '')

# generate to string and save
rdd_to_str = rdd_kv_invalid.map(
    lambda x: meta_handler.meta_to_str(x))
now = datetime.datetime.now()
rdd_to_str.saveAsTextFile('validate_error_{:%H_%M}.txt'.format(now))
