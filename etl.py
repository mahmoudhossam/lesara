from datetime import datetime
import pandas as pd


class ETL:

    def __init__(self):
        self.df = pd.read_csv('./orders.csv', parse_dates=['created_at_date'])

    def max_items(self, df):
        return df[['customer_id', 'order_id', 'num_items']]\
            .groupby(['customer_id', 'order_id']).sum().\
            reset_index().groupby(['customer_id']).max()

    def max_revenue(self, df):
        df = df[['customer_id', 'order_id', 'revenue']]\
            .groupby(['customer_id', 'order_id']).sum()\
            .reset_index().drop('order_id', axis=1)\
            .groupby('customer_id').max()
        return df

    def total_revenue(self, df):
        return df[['customer_id', 'revenue']].groupby(['customer_id']).sum()

    def total_orders(self, df):
        return df[['customer_id', 'order_id']].groupby(['customer_id']).\
            order_id.nunique().to_frame()

    def days_since_last_order(self, df):
        return df[['customer_id', 'created_at_date']].groupby('customer_id').\
            max()['created_at_date'].\
            subtract(pd.Timestamp(datetime(2017, 10, 17))).abs().to_frame()
