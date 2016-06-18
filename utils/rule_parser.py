import pyparsing
import datetime
from pyparsing import Word, alphas, OneOrMore, replaceWith, Dict, oneOf, Regex
from utils import date_helper


class rule_parser:

    def __init__(self):
        self.placeholders = ['FDM', 'NOW']

    def parse(self, value, rule):
        self.value = value
        before = Regex('[^#]*')
        after = Regex('.*')
        ps = ' '.join(self.placeholders)
        placeholder = '#' + OneOrMore(oneOf(ps))
        placeholder.setParseAction(self.replace_placeholder)
        final = before + placeholder + after
        result = final.parseString(rule)

        return ''.join(result)

    def placeholder_now(self):
        return datetime.datetime.now().strftime('%m/%d/%Y')

    def placeholder_fdm(self):
        time = date_helper.parse(self.value)
        return datetime.date(time.year, time.month, 1).strftime('%m/%d/%Y')

    def replace_placeholder(self, string, loc, toks):

        tok = ''.join(toks[1:])
        if(tok == 'FDM'):
            return self.placeholder_fdm()
        elif(tok == 'NOW'):
            return self.placeholder_now()
        else:
            return tok
