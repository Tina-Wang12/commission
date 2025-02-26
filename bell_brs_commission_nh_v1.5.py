import pandas as pd
from pyspark.sql import SparkSession
import os
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, col, when
import configparser
import logging
from datetime import datetime, timedelta
import sys
import psycopg2

# Load the main configuration and credentials
config_path = r'/home/spark/belladp/config'
config_file = os.path.join(config_path, 'bell_brs_commission_nh.ini')
config = configparser.ConfigParser()
config.sections()
config.read(config_file)
output = '/var/working/bell/output'

# Load credentials from master config file
creds_file = r'/home/spark/main_config/master_config_file.ini'
creds = configparser.ConfigParser()
creds.sections()
creds.read(creds_file)


# Extracting relevant paths and credentials
config_path = config['path']['config_path']
log_path = config['path']['log_path']
brs = config['postgres']['brs']
adp = config['postgres']['adp']
stg = config['postgres']['stg']
mdb = config['postgres']['mdb']
dev_driver = config['postgres']['driver']
dev_url = creds['postgres']['url']  # Added dev_url

# Extracting credentials from the master config file
dev_username = creds['postgres']['username']
dev_password = creds['postgres']['password']
host = creds['postgres']['hostname']

# Initialize Spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("Bell NMT Commission Calculation") \
    .config("spark.sql.caseSensitive", "false") \
    .getOrCreate()
spark.sparkContext.setLogLevel('FATAL')

case_sensitivity = spark.conf.get("spark.sql.caseSensitive")
print(f"Is case sensitivity enabled? {case_sensitivity}")

# Initialize logging
log4jLogger = spark._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)
LOGGER.debug("initializing Bell NMT Commission Calculation spark job..")

queries = ["Bell_employee_ADP", "Bell_current_payperiod", "Bell_newhire_employee", "Bell_nh_current_pp",
           "Bell_rate_card", "Bell_lastv_nr", "Bell_brs_nr", "Bell_act", "NCST_commission",
           "NTRP_ATL_commisison", "brs_commisison_notncstntrp", "bell_brs_commission_final"]

tables = {"bell_brs_commission_final":"bell_brs_commission_temp","Bell_brs_nr": "bell_brs_nrwithnh_temp"}

feeds = {
    "brs": {'tables': ['bell_payroll_calendar', 'bell_commission_rate', 'nominal_roll_agent',
                       'bell_osl_sales_feed', 'bell_osl_hierarchy_newratecard'],
            'url': brs, 'driver': dev_driver, 'username': dev_username, 'password': dev_password},
    "adp": {'tables': ['wm_adpemployees'], 'url': adp, 'driver': dev_driver, 'username': dev_username, 'password': dev_password},
    "mdb": {'tables': ['clientprogram_mapping'], 'url': mdb, 'driver': dev_driver, 'username': dev_username, 'password': dev_password}
}


def job_logger(msg, lvl='info'):
    logging.basicConfig(filename=os.path.join(log_path, f'bell_brs_commission_payment_{datetime.now().strftime("%Y%m%d")}.log'),
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


def write_txt(df,payperiod,filename_prefix):
    # Check if 'service_tn' exists in the DataFrame before casting
    if 'service_tn' in df.columns:
        df = df.withColumn("service_tn", col("service_tn").cast("string"))
    
    # Convert Spark DataFrame to Pandas DataFrame
    df = df.toPandas()    

    # If 'service_tn' is in the DataFrame, format it to preserve leading zeros
    if 'service_tn' in df.columns:
        df['service_tn'] = df['service_tn'].apply(lambda x: x.zfill(10) if pd.notnull(x) else x)
        
        # Convert 'service_tn' to string with leading zeros preserved
        #def format_service_tn(tn):
        #    if pd.isnull(tn):  # If the value is null, return as is
        #        return tn
        #    tn_str = str(tn)
        #    return tn_str.zfill(10) if len(tn_str) < 10 else tn_str

        #df = df.toPandas()
        #df['service_tn'] = df['service_tn'].apply(format_service_tn)
    #else:
        #df = df.toPandas()
    
    # Ensure payperiod is extracted and formatted properly
    payperiod_formatted = f'{payperiod}'  
    # Format file name with payperiod and current date
    output_file = os.path.join(output, f'{filename_prefix}_{payperiod_formatted}_{datetime.now().strftime("%Y_%m_%d")}.csv')
    # Convert Spark DataFrame to Pandas DataFrame
    #df = df.toPandas()

    # Write to CSV
    df.to_csv(output_file, index=False)
    # Change file permission
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
    df.write \
        .format("jdbc") \
        .mode("overwrite") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password) \
        .save()


def main():
    job_logger("Job is Bell BRS Commission Calculation", 'info')

    for key in [key for key in feeds.keys() if 'tables' in feeds[key].keys()]:
        for table in feeds[key]['tables']:
            dfeed = import_table(feeds[key]['url'], table, feeds[key]['driver'], feeds[key]['username'], feeds[key]['password'])
            dfeed.createOrReplaceTempView(table)
            dfeed.show()
            job_logger(f"{table} has been loaded: {dfeed.count()}", 'info')

    payperiod = None  # Variable to store payperiod from bell_brs_commission_final

    # First process the bell_brs_commission_final query to extract payperiod
    for query in queries:
        inputdate = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y-%m-%d')
        inputdate = f"'{inputdate}'"
        
        temp = config['queries'][query]
        job_logger(f"Running query: {query}")
        print("Executing SQL Query:")
        print(temp.format('%', inputdate))
        df = spark.sql(temp.format('%', inputdate))
        df.createOrReplaceTempView(query)
        job_logger(f"Query {query} executed successfully")
        df.show()

        # Extract payperiod from bell_brs_commission_final
        if query == "bell_brs_commission_final":
            payperiod = df.select("payperiod").first()["payperiod"] if "payperiod" in df.columns else "unknown"
            write_txt(df, payperiod, "bell_brs_commission_payment")
            save_to_postgresql(df, tables[query], stg, dev_driver, dev_username, dev_password)
            job_logger(f'{query} has been written to PostgreSQL table {tables[query]}', 'info')

   # Now process the Bell_brs_nr query and use the same payperiod
    if payperiod:
        query = "Bell_brs_nr"
        temp = config['queries'][query]
        df = spark.sql(temp.format('%', inputdate))
        df.createOrReplaceTempView(query)
        write_txt(df, payperiod, "bell_brs_nr")
        save_to_postgresql(df, tables[query], stg, dev_driver, dev_username, dev_password)
        job_logger(f'{query} has been written to PostgreSQL table {tables[query]}', 'info')

    job_logger("Bell BRS Commission process completed successfully.")

if __name__ == "__main__":
    main()