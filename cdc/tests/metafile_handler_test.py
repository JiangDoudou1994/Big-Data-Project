import pytest
from pyspark import SparkContext
from pyspark import SparkConf
from  metafile_handler import MetaFileHandler


@pytest.fixture(scope="session",
        params=[pytest.mark.spark_yarn('yarn'),
            pytest.mark.spark_local('local')])
def spark_context(request):
    conf = (SparkConf()
                .setMaster("local[2]")
                .setAppName("pytest-pyspark-local-testing")
                )
    request.addfinalizer(lambda: sc.stop())
    sc=SparkContext(conf=conf)
    return sc

def test_kv_mapper(spark_context):
    meta=spark_context.textFile('testdata.meta')
    test_class = MetaFileHandler(meta)
    assert test_class.ids==[1,3]
    kv= test_class.meta_kv_mapper("key1,v1,key2,v2,v3")
    assert kv==(['key1','key2'],['key1','v1','key2','v2','v3'])
