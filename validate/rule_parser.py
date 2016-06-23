#import pyparsing
import re
import datetime
import linecache
import sys
from placeholder import PlaceHolder
from function import Function
import date_helper


class rule_parser:

    def __init__(self):
        ph = PlaceHolder()
        self.placeholders = map(lambda x: x.upper(), dir(ph))

    def underscore_to_camelcase(self, word):
        return ''.join(x.capitalize() or '_' for x in word.split('_'))

    def camelcase_to_underscore(self, word):
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', word)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    # print exception of source line number
    def PrintException(self):
        exc_type, exc_obj, tb = sys.exc_info()
        f = tb.tb_frame
        lineno = tb.tb_lineno
        filename = f.f_code.co_filename
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        print 'EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj)

    def parse(self, value, rule):

        self.value = date_helper.parse(value)
        rule = self.parse_placeholder(rule)
        rule = self.parse_function(rule)
        return rule
	
    # extract placeholder from rule and covert to date
    def parse_placeholder(self,  rule):
	from pyparsing import Word, alphas, OneOrMore, replaceWith, Dict, oneOf, Regex, delimitedList, Suppress
        result = rule
        try:
            before = Regex('[^#]*')
            after = Regex('.*')
            ps = ' '.join(self.placeholders)
            placeholder = '#' + OneOrMore(oneOf(ps))
            placeholder.setParseAction(self.replace_placeholder)
            final = before + placeholder + after
            result = final.parseString(rule)

            return ''.join(result)
        except Exception as error:
            return result

    # calcute date
    def parse_function(self, rule):
        result = rule
        try:
            compare = Regex("[><=]*")
            function_name = Word(alphas)
            parameter = delimitedList(Regex("[^{,)}]*"))
            final = compare + function_name + \
                Suppress('(') + parameter + Suppress(')')
            result = final.parseString(rule)
            function = Function()

            methodToCall = getattr(
                function, self.camelcase_to_underscore(result[1]))
            if len(result) > 3:
                return result[0] + ' ' + methodToCall(*result[2:])
            else:
                return result[0] + ' ' + methodToCall(self.value)

        except Exception as error:
            return result

    # call placeholder function to get value
    def replace_placeholder(self, string, loc, toks):

        tok = ''.join(toks[1:])
        ph = PlaceHolder()
        try:
            # Will call same name function but lower case
            methodToCall = getattr(ph, tok.lower())
            return methodToCall(self.value)
        except Exception as error:
            print error
            return 'Do not have this placeholder {0}'.format(tok)
