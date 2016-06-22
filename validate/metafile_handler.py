
from rule_parser import rule_parser
import validate_rules

class MetaFileHandler:

    def __init__(self, meta_data):

        self.ids= meta_data.filter(lambda x: x.split(',')[-1]=='Y').map(lambda x:int(x.split(',')[0])).collect()
        self.rules = meta_data.filter(lambda x: len(x.split('||')) > 1).map(
            lambda x: [int(x.split(',')[0]), x.split('||')[1]]).collect()

    def meta_kv_mapper(self, value):
	try:
	    x = value.split(',')
	    rule_index = [int(r[0]) - 1 for r in self.rules]
	# only return id and column with rules
	    ids=[]
	    print self.ids
	    for i in self.ids:
	        ids.append(x[i-1])
		
	    return tuple(ids), ([x[index] for index in rule_index])
        #except BaseException as error:
	    print self.ids
	    print x
            print error

    def validate_on_rules(self, value, rule):
        rp = rule_parser()
        rule = rp.parse(value, rule)
        validate = validate_rules.Validate()
        return validate.validate(value, rule)

    def meta_validate_fields(self, x):
        err = ''
        index = 0
	try:
            for rule in self.rules:
                for u in rule[1].split(';'):
                    if(not self.validate_on_rules(x[1][index], u.encode('ascii')[1:-1])):
                        err = err + " " + x[1][index] + " should " + u
                    index += 1
            return [x, err]
	except BaseException as error:
	    print error

    def meta_to_str(self, x):
        return x
