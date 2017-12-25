import os
from datetime import datetime
import pandas as pd
import numpy
import dill


class ETL:

    def __init__(self):
        current_directory = os.path.dirname(os.path.abspath(__file__))
        self.df = pd.read_csv(current_directory + '/orders.csv',
                              parse_dates=['created_at_date'])

    def max_items(self, df):
        """ Calculates the maximum number of items in one order. """
        order_items = df[['customer_id', 'order_id', 'num_items']]\
            .groupby(['customer_id', 'order_id']).sum()
        maximum_items = order_items.reset_index().groupby(['customer_id']).max()
        return maximum_items.drop('order_id', axis=1).\
            rename(columns={'num_items': 'max items'})

    def max_revenue(self, df):
        """ Calculates the maximum revenue in one order. """
        order_revenue = df[['customer_id', 'order_id', 'revenue']]\
            .groupby(['customer_id', 'order_id']).sum()
        maximum_revenue = order_revenue.reset_index().drop('order_id', axis=1)\
            .groupby('customer_id').max()
        return maximum_revenue.rename(columns={'revenue': 'max revenue'})

    def total_revenue(self, df):
        """ Calculates the total revenue for a customer across all orders. """
        return df[['customer_id', 'revenue']].groupby(['customer_id']).sum()\
            .rename(columns={'revenue': 'total revenue'})

    def total_orders(self, df):
        """ Calculates the total number of orders for a customer. """
        unique_orders = df[['customer_id', 'order_id']].\
            groupby(['customer_id']).order_id.nunique()
        return unique_orders.to_frame().\
            rename(columns={'order_id': 'total orders'})

    def days_since_last_order(self, df):
        """ Calculates the number of days since a customer's last order. """
        last_order = df[['customer_id', 'created_at_date']].\
            groupby('customer_id').max()['created_at_date']
        interval = last_order.subtract(pd.Timestamp(datetime(2017, 10, 17))).abs()
        interval_in_days = (interval / numpy.timedelta64(1, 'D')).astype(int)
        return interval_in_days.to_frame().rename(columns={'created_at_date':
                                                           'days since last order'})

    def calculate_clv(self):
        """ Calculates CLV for all customers """
        customer_data = self.max_items(self.df)
        customer_data = customer_data.join(self.max_revenue(self.df))
        customer_data = customer_data.join(self.total_orders(self.df))
        customer_data = customer_data.join(self.total_revenue(self.df))
        customer_data = customer_data.join(self.days_since_last_order(self.df))
        model = dill.load(open("model.dill", 'rb'))
        result = pd.DataFrame()
        for index, row in customer_data.iterrows():
            # Longest interval is missing, putting in a zero
            args = numpy.array([[row['max items'], row['max revenue'],
                                 row['total revenue'], row['total orders'],
                                 row['days since last order'], 0]])
            result[index] = model.predict(args)
        result.to_csv('result.csv')


if __name__ == '__main__':
    etl = ETL()
    etl.calculate_clv()
