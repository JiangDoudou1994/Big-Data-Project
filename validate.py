from pyspark import SparkContext
from utils.rule_parser import rule_parser
import datetime
import validate_rules


def parse(date):
    kwargs = {}
    parsedate = par.parse(date, **kwargs)
    return parsedate


class MetaFileHandler:

    def __init__(self, file_name, rules):
        self.file_name = file_name
        self.rules = rules

    def meta_kv_mapper(self, x):
        x = x.strip().split(',')
        rule_index = [int(r[0]) - 1 for r in self.rules]
# only return id and column with rules
        return x[0], ([x[index] for index in rule_index])

    def validate_on_rules(self, value, rule):
        rp = rule_parser()
        rule = rp.parse(value, rule)
        validate = validate_rules.Validate()
        return validate.validate(value, rule)

    def meta_validate_fields(self, x):
        err = ''
        index = 0
        for rule in self.rules:
            for u in rule[1].split(';'):
                if(not self.validate_on_rules(x[1][index], u.encode('ascii')[1:-1])):
                    err = err + u
            index += 1
        return [x, err]

    def meta_to_str(self, x):
        return x

sc = SparkContext(appName="validata")
meta_data = sc.textFile("testdata.meta")
rules = meta_data.filter(lambda x: len(x.split('||')) > 1).map(
    lambda x: [int(x.split(',')[0]), x.split('||')[1]])
print rules.collect()

meta = MetaFileHandler('testdata.txt', rules.collect())
rdd = sc.textFile(meta.file_name)
rdd_kv = rdd.map(lambda x: meta.meta_kv_mapper(x))
rdd_kv_invalid = rdd_kv.map(lambda x: meta.meta_validate_fields(x)).filter(lambda x:x[1]>'')
rdd_to_str = rdd_kv_invalid.map(
    lambda x: meta.meta_to_str(x))  # .coalesce(1,shuffle=True)
now = datetime.datetime.now()
rdd_to_str.saveAsTextFile('validate_error_{:%H_%M}.txt'.format(now))
