import unittest
import datetime
from utils import rule_parser


class rule_parser_test(unittest.TestCase):

    def setUp(self):

        self.test_class = rule_parser.rule_parser()

    def test_parse_placeholder(self):
        pr=self.test_class.parse( '01/03/2016', '>= #FDM')
        self.assertEqual(pr , '>= 01/01/2016',msg=pr)
        pr=self.test_class.parse( '01/03/2016', '<#NOW')
        now=datetime.datetime.now().strftime('%m/%d/%Y')
        self.assertEqual(pr , '<'+now,msg=pr)
        pr=self.test_class.parse( '01/03/2016', '!{e  #FDM) fef')
        self.assertEqual(pr , '!{e  01/01/2016) fef',msg=pr)
        pr=self.test_class.parse( '01/03/2016', '> #FFF')
        self.assertEqual(pr , '> #FFF',msg=pr)

    def test_parse_function(self):
        pr=self.test_class.parse( '01/03/2016', '> DateAdd(D,5,#FDM)')
        self.assertEqual(pr , '> 01/06/2016',msg=pr)
        pr=self.test_class.parse( '06/18/2016', '< DayOfWeek()')
        self.assertEqual(pr , '< 5',msg=pr)

if __name__ == '__main__':
    unittest.main()
