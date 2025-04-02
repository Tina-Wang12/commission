import pandas as pd
from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import col
import configparser
import logging
from datetime import datetime
import sys
import psycopg2

# Load the main configuration and credentials
config_path = r'/home/spark/belladp/config'
config_file = os.path.join(config_path, 'bell_commission_nh.ini')
config = configparser.ConfigParser()
config.sections()
config.read(config_file)
output = '/var/working/bell/output'

# Retrieve database credentials from ini file
dev_url = config.get("postgres", "dev_url")
dev_username = config.get("postgres", "username")
dev_password = config.get("postgres", "password")
dev_driver = config.get("postgres", "driver")

# Retrieve database schemas from ini file
brs = config.get("postgres", "brs")
adp = config.get("postgres", "adp")
stg = config.get("postgres", "stg")
mdb = config.get("postgres", "mdb")


feeds = {
    "brs": {
        'tables': ['bell_payroll_calendar', 'bell_commission_rate', 'nominal_roll_agent',
                   'bell_osl_sales_feed', 'bell_osl_hierarchy_newratecard'],
        'url': brs,
        'driver': dev_driver,
        'username': dev_username,
        'password': dev_password
    },
    "adp": {
        'tables': ['employee_profile_history'],
        'url': adp,
        'driver': dev_driver,
        'username': dev_username,
        'password': dev_password
    },
    "mdb": {
        'tables': ['clientprogram_mapping'],
        'url': mdb,
        'driver': dev_driver,
        'username': dev_username,
        'password': dev_password
    }
}

# Path configurations
output = '/var/working/bell/output'
log_path = "/home/spark/belladp/logs"

# Initialize Spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("Bell Commission Calculation") \
    .config("spark.sql.caseSensitive", "false") \
    .getOrCreate()
spark.sparkContext.setLogLevel('FATAL')

case_sensitivity = spark.conf.get("spark.sql.caseSensitive")
print(f"Is case sensitivity enabled? {case_sensitivity}")

# Initialize logging
log4jLogger = spark._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.debug("Initializing Bell Commission Calculation spark job..")

# Function definitions (no changes made to logic)
def job_logger(msg, lvl='info'):
    logging.basicConfig(filename=os.path.join(log_path, f'bell_commission_payment_{datetime.now().strftime("%Y%m%d")}.log'),
                        level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        filemode='a')
    logger = logging.getLogger("py4j")
    logger.setLevel(logging.INFO)
    if lvl == 'info':
        logger.info(msg)
    elif lvl == 'debug':
        logger.debug(msg)
    elif lvl == 'error':
        logger.error(msg)
    elif lvl == 'warning':
        logger.warning(msg)
    else:
        logger.info(msg)

def write_txt(df, payperiod, filename_prefix):
    df = df.withColumn("service_tn", col("service_tn").cast("string")) if "service_tn" in df.columns else df
    df = df.toPandas()
    if 'service_tn' in df.columns:
        df['service_tn'] = df['service_tn'].apply(lambda x: x.zfill(10) if pd.notnull(x) else x)
    # Ensure payperiod is extracted and formatted properly
    payperiod_formatted = f'{payperiod}'  
    output_file = os.path.join(output, f'{filename_prefix}_{payperiod_formatted}_{datetime.now().strftime("%Y_%m_%d")}.csv')

    df.to_csv(output_file, index=False)
    os.system(f'chmod 666 {output_file}')
    return output_file

def import_table(url, table_name, driver, username, password):
    job_logger(f"Importing table: {table_name}")
    df = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("user", username) \
        .option("password", password) \
        .option("encoding", "UTF-8") \
        .option("dbtable", table_name) \
        .load()
    df.createOrReplaceTempView(table_name)
    job_logger(f"Table {table_name} imported successfully")
    return df

def save_to_postgresql(df, table_name, url, driver, username, password):
    try:
        df.write \
            .format("jdbc") \
            .mode("overwrite") \
            .option("url", url) \
            .option("driver", driver) \
            .option("dbtable", table_name) \
            .option("user", username) \
            .option("password", password) \
            .save()
    except Exception as e:
        job_logger(f"Error saving table {table_name}: {e}", 'error')
        raise

def main():
    job_logger("Job is Bell Commission Calculation", 'info')

    # Ensure SparkContext is active
    if spark.sparkContext._jsc is None:
        raise RuntimeError("SparkContext is stopped. Restart the Spark job.")

    # Import and create views for all feed tables
    for key in [key for key in feeds.keys() if 'tables' in feeds[key].keys()]:
        for table in feeds[key]['tables']:
            df = import_table(feeds[key]['url'], table, feeds[key]['driver'], feeds[key]['username'], feeds[key]['password'])
            df.createOrReplaceTempView(table)
            job_logger(f"{table} has been loaded and registered as a view", 'info')

    # List registered tables/views for debugging
    print("Registered tables/views:", [table.name for table in spark.catalog.listTables()])

    # Define `inputdate`
    inputdate = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
    inputdate = f"'{inputdate}'"

    # Execute queries
    queries = ["Bell_employee_ADP", "Bell_current_payperiod", "bmt_payperiod", "Bell_newhire_employee", 
               "Bell_nh_current_pp", "Bell_rate_card", "Bell_lastv_nr", "Bell_brs_nr", "Bell_act", "bmt_act", 
               "Bell_bmt_nr", "brs_commission", "bmt_commission","bmt_bonus", "bell_commission_final"]

    payperiod = None

    for query in queries:
        if query in config['queries']:
            temp = config['queries'][query]
            job_logger(f"Executing query: {query}", 'info')
            sql_query = temp.format('%', inputdate)
            print(f"Executing SQL: {sql_query}")
            
            try:
                df = spark.sql(sql_query)
                df.createOrReplaceTempView(query)
                job_logger(f"Query {query} executed successfully", 'info')
                df.show()

                # Debugging: Print available columns
                print(f"Columns in {query}: {df.columns}")

                # Extract payperiod only from bell_brs_commission_final
                if query == "bell_brs_commission_final":
                    print(f"🔍 Debug: Checking columns in {query}: {df.columns}")

                    if "payperiod" in df.columns:
                        row_count = df.count()
                        print(f"🔍 Debug: {query} has {row_count} rows.")

                        if row_count > 0:
                            payperiod_row = df.select("payperiod").first()
                            if payperiod_row and payperiod_row[0] is not None:
                                payperiod = payperiod_row[0]  # Extract payperiod
                                print(f"✅ Payperiod extracted successfully: {payperiod}")
                            else:
                                print("⚠️ Warning: 'payperiod' column exists but first row is None.")
                                payperiod = "unknown"
                        else:
                            print("⚠️ Warning: DataFrame has zero rows, setting payperiod to 'unknown'.")
                            payperiod = "unknown"
                    else:
                        print("⚠️ Warning: 'payperiod' column is missing from DataFrame.")
                        payperiod = "unknown"
                    csv_file = write_txt(df, payperiod, "bell_commission_payment")
                    job_logger(f"bell_commission_final exported to {csv_file}", 'info')
                    # Save to PostgreSQL
                    job_logger("Saving bell_commission_final to PostgreSQL.", 'info')
                    try:
                        save_to_postgresql(df, "bell_brs_commission_temp", brs, dev_driver, dev_username, dev_password)
                        job_logger("Data successfully saved to PostgreSQL table bell_brs_commission_temp.", 'info')
                    except Exception as e:
                        job_logger(f"Failed to save to PostgreSQL: {e}", 'error')
                        raise
                
                # Export specific queries to CSV files
                if query in ["Bell_brs_nr", "Bell_bmt_nr", "Bell_act", "bmt_act", "brs_commission", "bmt_commission","bmt_bonus"]:
                    csv_file = write_txt(df, payperiod, f"{query}_output")
                    job_logger(f"{query} results exported to {csv_file}", 'info')
                    
            
            except Exception as e:
                job_logger(f"Error executing query {query}: {e}", 'error')
                print(f"Error executing query {query}: {e}")
                raise

    job_logger("Bell BRS Commission process completed successfully.", 'info')


if __name__ == "__main__":
    main()
