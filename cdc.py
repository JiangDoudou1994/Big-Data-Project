from pyspark import SparkContext
sc=SparkContext(appName="cdc")
import unittest

class MetaFileHandler:
    def __init__(self, file_name):
        self.file_name=file_name
    def meta_kv_mapper(self, x):
        return (x[0], x[2]),(x[1],x[4])




meta1 = MetaFileHandler('testdata1.txt')
meta2 = MetaFileHandler('testdata2.txt')
rdd1=sc.textFile(meta1.file_name)
rdd2=sc.textFile(meta2.file_name)

# convert to k/v
rdd1_kv = rdd1.map(lambda x: meta1.meta1_kv_mapper(x))
rdd2_kv = rdd2.map(lambda x: meta2.meta2_kv_mapper(x))
cla
# display result
rdd1_kv.collect()
rdd2_kv.collect()

# full outer join
rdd_combo = rdd1_kv.fullOuterJoin(rdd2_kv)
# display result
rdd_combo.collect()

# get results
rdd_del = rdd_combo.filter(lambda x: x[1][0] == None)
rdd_add = rdd_combo.filter(lambda x: x[1][1] == None)
# the below formula needs to be fixed to use metadata
rdd_chg = rdd_combo.filter(lambda x: x[1][0] != None and x[1][1] != None and x[1][0] != x[1][1])
#display
print rdd_del.collect()
print rdd_add.collect()
print rdd_chg.collect()


class cdc(unittest.TestCase):
    test_data=[]
    f=open(file_name)
    for line in f.readlines():
        line = line.strip().split(' ')
        test_data.append(line)
    
    test_class=MetaFileHandler('testdata.txt')

    def test_cdc(self):
        for data in self.test_data:
            data_before=(data[0],data[2]),(data[1],data[4])
            data_after=self.test_class.meta_kv_mapper(data)
            self.assertTrue(data_after==data_before)



if __name__=='__main__':
    unittest.main()
