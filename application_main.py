import sys
from lib import DataManipulation,DataReader,Utils

if __name__ == '__main__':
    if len(sys.argv) < 2 :
        print("Please Specify The Enviroment")
        sys.exit(-1)

    job_run_env = sys.argv[1]
    print("Creating Spark Session")
    spark = Utils.get_spark_session(job_run_env)
    print("Ceated Spark Session")


    orders_df = DataReader.read_orders(spark,job_run_env)
    orders_filtered = DataManipulation.filter_complete_orders(orders_df)

    customers_df = DataReader.read_customer(spark,job_run_env)
    joined_df = DataManipulation.join_orders_customers(orders_filtered,customers_df)

    aggregated_results = DataManipulation.count_orders_state_city(joined_df)

    aggregated_results.show()

    print("end of main")
