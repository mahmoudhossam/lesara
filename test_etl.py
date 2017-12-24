import unittest
import pandas as pd
from etl import ETL


class ETLTests(unittest.TestCase):
    """ Tests for the simple ETL task. """

    def setUp(self):
        self.etl = ETL()
        data = {'customer_id': [1, 1], 'order_id': [1, 2],
                'order_item_id': [5, 6], 'num_items': [2, 3],
                'revenue': [90, 50],
                'created_at_date': ['2017-12-24', '2017-10-12']}
        self.test_df = pd.DataFrame.from_dict(data)

    def test_max_items(self):
        """ Tests if max_items returns the order with the most items
            even if they have multiple orders.
        """
        expected = pd.DataFrame.from_dict(
            {0: {'customer_id': 1, 'order_id': 2, 'num_items': 3}},
            orient='index')
        expected = expected.set_index('customer_id')
        actual = self.etl.max_items(self.test_df)
        self.assertTrue(actual.equals(expected))

    def test_max_revenue(self):
        """ Tests if max_revenue returns the order
            with the highest revenue.
        """
        expected = pd.DataFrame.from_dict(
            {0: {'customer_id': 1, 'order_id': 1, 'revenue': 90}},
            orient='index')
        actual = self.etl.max_revenue(self.test_df)
        self.assertTrue(actual.equals(expected))


if __name__ == '__main__':
    unittest.main()
