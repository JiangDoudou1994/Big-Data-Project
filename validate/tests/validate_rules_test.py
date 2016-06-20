import unittest
import validate_rules


class validate_rules_test(unittest.TestCase):

    def setUp(self):
        self.test_class = validate_rules.Validate()
    
    def test_valid(self):
        result=self.test_class.validate( '01/03/2016', '>= 01/02/2016')
        self.assertTrue(result)
        result=self.test_class.validate( '01/03/2016', '< 01/04/2016')
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()
