import sys
import glob
from pyspark import SparkContext
import datetime
from metafile_handler import MetaFileHandler


argv = sys.argv

sc = SparkContext(appName="validate")
# load required files
for src in glob.glob('*.py'):
    sc.addPyFile(src)
sc.addPyFile('pyparsing.egg')

meta_data = sc.textFile(argv[1])
meta_handler = MetaFileHandler(meta_data)
rdd = sc.textFile(argv[2])

# generate key value
rdd_kv = rdd.map(lambda x: meta_handler.meta_kv_mapper(x))

# validate data, keep validate message is not empty
rdd_kv_invalid = rdd_kv.map(
    lambda x: meta_handler.meta_validate_fields(x)).filter(lambda x: x[1] > '')

# generate to string and save
now = datetime.datetime.now()
rdd_kv_invalid.saveAsTextFile('validate_error_{:%H_%M}.txt'.format(now))
