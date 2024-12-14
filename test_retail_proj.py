import pytest
from lib.DataReader import read_customer,read_orders
from lib.DataManipulation import filter_complete_orders

def test_read_customers_df(spark):
    customers_count = read_customer(spark,"LOCAL").count()
    assert customers_count == 12435

def test_read_orders_df(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68884

def test_filter_ccomplete_orders(spark):
    orders_df = read_orders(spark,"LOCAL")
    filered_count = filter_complete_orders(orders_df).count()
    assert filered_count == 22900

