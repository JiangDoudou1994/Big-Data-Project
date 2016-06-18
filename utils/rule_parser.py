import pyparsing
import re
import datetime
from pyparsing import Word, alphas, OneOrMore, replaceWith, Dict, oneOf, Regex, delimitedList,Suppress
from utils import date_helper
from core.placeholder import PlaceHolder 
from core.function import Function 


class rule_parser:

    def __init__(self):
        ph=PlaceHolder()
        self.placeholders =map(lambda x:x.upper() , dir(ph))
       # function=Function()
       # self.functions =map(lambda x:self.underscore_to_camelcase(x), dir(function))

    def underscore_to_camelcase(self,word):
        return ''.join(x.capitalize() or '_' for x in word.split('_'))
    def camelcase_to_underscore(self,word):
        s1=re.sub('(.)([A-Z][a-z]+)',r'\1_\2',word)
        return re.sub('([a-z0-9])([A-Z])',r'\1_\2',s1).lower()

    def parse(self, value, rule):
        self.value = value
        rule = self.parse_placeholder(rule)
        rule = self.parse_function(rule)
        return rule

    def parse_placeholder(self,  rule):
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
            #print error
            return result

    def parse_function(self, rule):
        result = rule
        try:
            function_name = Word(alphas)
            parameter = delimitedList(Regex("[^{,)}]*"))
            final = function_name + Suppress('(') + parameter +Suppress(')')
            result = final.parseString(rule)
            function=Function()

            methodToCall = getattr(function,self.camelcase_to_underscore( result[0]))
            return methodToCall(*result[1:])

        except Exception as error:
            #print error
            return result

    def replace_placeholder(self, string, loc, toks):

        tok = ''.join(toks[1:])
        ph=PlaceHolder()
        try:
            # Will call same name function but lower case
            methodToCall = getattr(ph, tok.lower())
            return methodToCall(self.value)
        except Exception as error:
            print error
            return 'Do not have this placeholder {0}'.format(tok)
