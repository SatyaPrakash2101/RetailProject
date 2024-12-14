from lib import ConfigReader


#defining Schema

def get_customers_schema():
    schema = "customer_id int,customer_fname string,customer_lname string,customer_username string,customer_password string,customer_address string,customer_city string,customer_state string,customer_pincode string"
    return schema

#creating data frame

def read_customer(spark,env):
    conf = ConfigReader.get_app_config(env)
    customers_file_path = conf["customers.file.path"]
    return spark.read \
        .format("csv") \
        .option("header",True) \
        .schema(get_customers_schema()) \
        .load(customers_file_path)

#defining orders schema

def get_orders_schema():
    schema = "order_id int,order_date string,customer_id int,order_status string"
    return schema

#create data frame

def read_orders(spark,env):
    conf = ConfigReader.get_app_config(env)
    orders_file_path = conf["orders.file.path"]
    return spark.read \
        .format("csv") \
        .option("header",True) \
        .schema(get_orders_schema()) \
        .load(orders_file_path)


