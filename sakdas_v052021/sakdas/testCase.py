import unittest
import json


class testCase(unittest.TestCase):
    def setUp(self):
        # Load test data
        with open('new.json') as f:
            self.data = json.load(f)
         

    def test_sum(self):
        
        actualValue = self.data["number_of_record"]
        expectValue = 10127
        self.assertEqual(actualValue, expectValue, "Should be 6")

    def test_sum_tuple(self):
        actualValue = self.data["number_of_record"]
        expectValue = 10127
        self.assertEqual(actualValue, expectValue, "Should be 6")



if __name__ == '__main__':
    unittest.main()