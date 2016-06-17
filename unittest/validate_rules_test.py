import unittest
import validate_rules


class validate_rules_test(unittest.TestCase):

    def setUp(self):
        self.test_class = validate_rules.MetaFileHandler()

    def test_validate_oldest(self):
        old_1 = '08-09-1994'
        temp_1 = validate_rules.parse(old_1)
        old_2 = '12-09-1900'
        temp_2 = validate_rules.parse(old_2)
        old_3 = '11-30-2015'
        temp_3 = validate_rules.parse(old_3)
        old_4 = '08-31-2001'
        temp_4 = validate_rules.parse(old_4)
        self.assertTrue(self.test_class.validate_oldest(temp_1, old_2) == '')
        self.assertTrue(self.test_class.validate_oldest(temp_2, old_4) != '')
        self.assertTrue(self.test_class.validate_oldest(temp_3, old_1) == '')

    def test_validate_now(self):
        now_1 = '08-09-1994'
        temp_1 = validate_rules.parse(now_1)
        now_2 = '12-09-1900'
        temp_2 = validate_rules.parse(now_2)
        now_3 = '11-30-2015'
        temp_3 = validate_rules.parse(now_3)
        now_4 = '08-31-2001'
        temp_4 = validate_rules.parse(now_4)
        self.assertTrue(self.test_class.validate_now(temp_1, now_2) != '')
        self.assertTrue(self.test_class.validate_now(temp_2, now_4) == '')
        self.assertTrue(self.test_class.validate_now(temp_3, now_1) != '')

    def test_validate_weekdays(self):
        temptime_1 = validate_rules.parse('08-12-2011')
        temptime_2 = validate_rules.parse('12-06-2007')
        temptime_3 = validate_rules.parse('04-10-2006')
        temptime_4 = validate_rules.parse('03-03-2007')
        tempweekdays_1 = [0, 5]
        tempweekdays_2 = [4]
        tempweekdays_3 = [0, 1, 2, 4, 5, 6]
        self.assertTrue(
            self.test_class.validate_weekdays(
                temptime_1,
                tempweekdays_2) != '')
        self.assertTrue(
            self.test_class.validate_weekdays(
                temptime_2,
                tempweekdays_3) == '')
        self.assertTrue(
            self.test_class.validate_weekdays(
                temptime_3,
                tempweekdays_1) != '')
        self.assertTrue(
            self.test_class.validate_weekdays(
                temptime_4,
                tempweekdays_2) == '')

    def test_validate_fdm(self):
        temptime_1 = validate_rules.parse('08-09-1994')
        temptime_2 = validate_rules.parse('12-01-1900')
        tempadd_1 = 5
        tempadd_2 = 15
        tempadd_3 = 6
        self.assertTrue(
            self.test_class.validate_fdm(
                temptime_1, tempadd_2) != '')
        self.assertTrue(
            self.test_class.validate_fdm(
                temptime_1, tempadd_3) == '')
        self.assertTrue(
            self.test_class.validate_fdm(
                temptime_2, tempadd_1) != '')

    def test_validate_fmm(self):
        temp_1 = validate_rules.parse('08-12-2011')
        temp_2 = validate_rules.parse('12-06-2007')
        temp_3 = validate_rules.parse('03-03-2007')
        self.assertTrue(self.test_class.validate_fmm(temp_1) == '')
        self.assertTrue(self.test_class.validate_fmm(temp_2) == '')
        self.assertTrue(self.test_class.validate_fmm(temp_3) != '')


if __name__ == '__main__':
    unittest.main()
