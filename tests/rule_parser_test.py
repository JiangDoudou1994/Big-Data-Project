import unittest
from utils import rule_parser


class rule_parser_test(unittest.TestCase):

    def setUp(self):

        self.test_class = rule_parser.rule_parser()

    def test_parse_placeholder(self):
        pr=self.test_class.parse( '01/03/2016', '>= #FDM')
        self.assertEqual(pr , '>= 01/01/2016',msg=pr)

if __name__ == '__main__':
    unittest.main()
