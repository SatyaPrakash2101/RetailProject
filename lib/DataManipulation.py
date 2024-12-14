from pyspark.sql.functions import *

def filter_complete_orders(orders_df):
    return orders_df.filter("order_status = 'COMPLETE'")

def join_orders_customers(orders_df,customers_df):
    return orders_df.join(customers_df,"customer_id")

def count_orders_state_city(joined_df):
    return joined_df.groupBy("customer_state","customer_city").count()

