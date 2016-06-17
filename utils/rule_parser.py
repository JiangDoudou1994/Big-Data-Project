import pyparsing
from pyparsing import Word, alphas, OneOrMore,replaceWith,Dict,oneOf


class rule_parser:

    def parse(self, value, rule):
        placeholder="#"+Word(alphas)
        placeholder.setParseAction(replaceWith('01/01/2016')) 
        expression=OneOrMore(oneOf("> = < >= <="))("expression")
        final=expression + placeholder

        result= final.parseString(rule)
        
        return ' '.join(result) 
