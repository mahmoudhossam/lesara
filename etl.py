import pandas as pd


class ETL:

    def __init__(self):
        self.df = pd.read_csv('./orders.csv', parse_dates=['created_at_date'])

    def max_items(self, df):
        return df.groupby(['customer_id', 'order_id'])['customer_id',
                                                       'order_id',
                                                       'num_items'].sum().\
            groupby(['customer_id']).max()

    def max_revenue(self, df):
        max_revenue = df[['customer_id', 'order_id', 'revenue']].\
            groupby(['customer_id'])['revenue'].idxmax()
        df = df[['customer_id', 'order_id', 'revenue']].iloc[max_revenue]
        return df

    def total_revenue(self, df):
        return df[['customer_id', 'revenue']].groupby(['customer_id']).sum()

    def total_orders(self, df):
        return df[['customer_id', 'order_id']].groupby(['customer_id']).\
            order_id.nunique().to_frame()
