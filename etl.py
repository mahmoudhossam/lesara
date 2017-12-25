from datetime import datetime
import pandas as pd
import numpy
import dill


class ETL:

    def __init__(self):
        self.df = pd.read_csv('./orders.csv', parse_dates=['created_at_date'])

    def max_items(self, df):
        """ Calculates the maximum number of items in one order. """
        return df[['customer_id', 'order_id', 'num_items']]\
            .groupby(['customer_id', 'order_id']).sum().\
            reset_index().groupby(['customer_id']).max().\
            drop('order_id', axis=1).\
            rename(columns={'num_items': 'max items'})

    def max_revenue(self, df):
        """ Calculates the maximum revenue in one order. """
        df = df[['customer_id', 'order_id', 'revenue']]\
            .groupby(['customer_id', 'order_id']).sum()\
            .reset_index().drop('order_id', axis=1)\
            .groupby('customer_id').max().\
            rename(columns={'revenue': 'max revenue'})
        return df

    def total_revenue(self, df):
        """ Calculates the total revenue for a customer across all orders. """
        return df[['customer_id', 'revenue']].groupby(['customer_id']).sum()\
            .rename(columns={'revenue': 'total revenue'})

    def total_orders(self, df):
        """ Calculates the total number of orders for a customer. """
        return df[['customer_id', 'order_id']].groupby(['customer_id']).\
            order_id.nunique().to_frame().\
            rename(columns={'order_id': 'total orders'})

    def days_since_last_order(self, df):
        """ Calculates the number of days since a customer's last order. """
        return df[['customer_id', 'created_at_date']].groupby('customer_id').\
            max()['created_at_date'].\
            subtract(pd.Timestamp(datetime(2017, 10, 17))).abs().to_frame().\
            rename(columns={'created_at_date': 'days since last order'})

    def calculate_clv(self):
        """ Calculates CLV for all customers """
        customer_data = self.max_items(self.df).\
            join(self.max_revenue(self.df)).join(
                self.total_orders(self.df)).join(
                    self.total_revenue(self.df)).join(
                        self.days_since_last_order(self.df))
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
