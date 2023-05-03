from functools import *
import os
import re
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from delta import configure_spark_with_delta_pip


def createSparkSession(appName = "calista-analytics", n_executor_instances = "20", n_executor_cores = "3", executor_memory = "12g", master = "spark://n1:7077"):
    """
    Creates a Delta-ready spark sesssion.

        Args:
            appName (string): The name of the running spark application. 
            n_executor_instances (string): Total number of executors. 
            n_executor_cores (string): Number of cores per executor. 
            executor_memory (string): Amount of memory per executor. 
            master (string): master URL.

        Returns:
            spark: The running sparkSession.
    """   
    builder = SparkSession.builder.appName(appName) \
        .config("spark.executor.instances", n_executor_instances) \
        .config("spark.executor.cores", n_executor_cores) \
        .config("spark.executor.memory", executor_memory) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master(master)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark
   



def find_source_data(start_date, end_date, days, hours, source_base_path = "/mnt/mind-data"):
    """
    Searches the MIND database for files that matches the given query. 
    
        Args:
            start_date (string): Start date of query. Expects a "yyyy-mm-dd" format.
            end_date (string): End date of query. Expects a "yyyy-mm-dd" format.
            days (string): Which specific days of the week to consider. Given as a comma-separated string: "mon, thu, sat" or "all". 
            hours (string): Which hours of the day to consider. Given as a comma-separated string of ranges: "00-06, 12-15". Hours are up to but not included. Thus "12-15" will collect the hours: 12, 13, and 14. 
            source_base_path (string): Path to the MIND mount.   

        Returns:
            file_paths (list[str]): A list of file-paths that matched the query. 
    """  
    file_paths = []

    start_date, end_date, days, hours = _validate_input(start_date, end_date, days, hours)

    if days == "all":
        dates = [date for date in [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]]
    else:
        dates = [date for date in [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)] if date.weekday() in days]

    for date in dates:
        # Source data is grouped into folders based on arrival date, which can vary significantly, and so we have to check a range of potential folders.  
        search_directories = [os.path.join(source_base_path, datetime.strftime(date + timedelta(days=i), "%y%m%d")) for i in range(10)] 

        files = ['DW_{0}_{1}.dat.gz'.format(datetime.strftime(date, "%Y%m%d"), hour) for hour in hours]

        result = _process_directories(search_directories, files)

        file_paths.extend(result)

    return file_paths



# TODO: Calculate estimate completion times and report this to the user before starting the ingestion work. 
# TODO: Put in a check on the existence of any given hdfs path.

def ingest(files, job_name, spark, delta_path = "hdfs://n1:9000/delta/tmp",  batch_size=10):
    """
    Performs batch reads of the given source files into a Spark DataFrame and saves them to Delta.    
    
        Args:
            files (list[str]): A list of source file paths.
            job_name (string): A unique job name. Used as a path on Delta. 
            spark (SparkSession): The current running SparkSession.
            delta_path (string): The base path on which to store the batch saves.
            batch_size (int): The number of files to process each batch.
        
    """
    batches = [files[i:i+batch_size] for i in range(0, len(files), batch_size)]

    schema = _get_schema()
    dfs = []
    batch_num = 0
    for batch in batches:
        df = spark.read.csv(batch, schema=schema, sep='\t') \
        .withColumn("timestamp", to_timestamp("USAGE_DTTM", "ddMMMyyy:HH:mm:ss")) \
        .withColumn("date", date_format("timestamp", "yyyy-MM-dd")) \
        .withColumn("time", date_format("timestamp", "HH:mm")) \
        .select("date","time","SCRAMBLED_IMSI", "Download_Type", "MCC", "MNC", "LAC", "SAC", "TAC",
                    "E_NODE_B_ID", "SECTOR_ID", "RAT", "NEW_CALL_ATTEMPTS", "NEW_ANSWERED_CALLS",
                    "NEW_FAILED_CALLS", "NEW_DROPPED_CALLS", "NEW_PS_ATTEMPTS", "NEW_FAILED_PS", "NEW_DROPPED_PS",
                    "NEW_SMS_ATTEMPTS", "NEW_FAILED_SMS", "NEW_LU_ATTEMPTS", "NEW_FAILED_LU", "NEW_RAUTAU_ATTEMPTS",
                    "NEW_FAILED_RAUTAU", "NEW_ATTACH_ATTEMPTS", "NEW_FAILED_ATTACH", "NEW_DETACH_ATTEMPTS",
                    "NEW_DL_VOLUME", "NEW_DL_MAX_THROUGHPUT", "NEW_UL_VOLUME", "NEW_UL_MAX_THROUGHPUT")

        path = os.path.join(delta_path, job_name, "batch_" + str(batch_num))
        df.write.format("delta").save(path)
        batch_num += 1



def union(dataframes):
    """
    Returns the union of a list of dataframes.

        Args: 
            dataframes (list[DataFrame]): A list of Spark DataFrames with the same schema.

        Returns: union (DataFrame): A Spark DataFrame that is the union of dataframes.

    """
    union_func = lambda df1, df2: df1.union(df2)
    union = functools.reduce(union_func, dataframes)
    return union



def save_as_delta(dataframe, path, partitionColumn = "date", mode = "default"):
    """
    Saves a spark DataFrame as a Delta Table.
    
        Args:
            dataframe (string): The Spark DataFrame to save.
            path (string): A path to save the DataFrame to.
            partitionColumn (string): Which column to partiton by.
            mode (string): Can be either of: default, ignore, append, or overwrite.

    """    
    dataframe.write.format("delta").partitionBy(partitionColumn).mode(mode).save(path)

    

def load_delta_table(path, spark):
    """
    Loads a Delta Table into a Spark DataFrame
    
        Args:
            path (string): Path to a Delta Table
            spark (SparkSession): The current SparkSession.

        Returns:
            df (DataFrame): The Delta Table as a Spark DataFrame
    """    
    df = spark.read.format("delta").load(path)
    return df



def _validate_input(start_date, end_date, days, hours):
    """
    Validates and checks the search query for common mistakes.
    
        Args:
            start_date (string): Start date of query. Expects a "yyyy-mm-dd" format.
            end_date (string): End date of query. Expects a "yyyy-mm-dd" format.
            days (string): Which specific days of the week to consider. Given as a comma-separated string: "mon, thu, sat" or "all". 
            hours (string): Which hours of the day to consider. Given as a comma-separated string of ranges: "00-06, 12-15". Hours are up to but not included. Thus "12-15" will collect the hours: 12, 13, and 14. 

        Returns:
            start_date (datetime.datetime): Validated start_date as a datetime object.
            end_date (datetime.datetime): Validated end_date as a datetime object.
            days_list (list[str]): A list of validated days.
            hours_list (list[str]): A list of validated hours.
    """   
    format = "%Y-%m-%d"

    # Handle input for "start_date" and "end_date" 
    try:
        start_date = datetime.strptime(start_date, format)
    except ValueError as ve:
        raise ValueError('Invalid start date: {0}. Make sure it follows the expected format: yyyy-mm-dd and uses actual calender dates.'.format(start_date)) from ve

    try:
        end_date = datetime.strptime(end_date, format)
    except ValueError as ve:
        raise ValueError('Invalid end date: {0}. Make sure it follows the expected format: yyyy-mm-dd and uses actual calender dates.'.format(end_date)) from ve

    if end_date < start_date:
        raise ValueError('End date {0} is set earlier than start date {1}'.format(end_date.strftime("%Y-%m-%d"), start_date.strftime("%Y-%m-%d")))


    # Handle input for "days"
    days_pattern = re.compile(r'^[a-zA-Z]{3}(, ?[a-zA-Z]{3})*$')
    if not days_pattern.match(days):
        raise ValueError('The input string for days: "{0}" is not in a valid format. Please provide as a string of comma separated days. Example: "mon, tue, wed"'.format(days))

    days_list = [day.strip().lower() for day in days.split(',')]
    valid_days = ["mon", "tue", "wed", "thu", "fri", "sat", "sun", "all"]

    for day in days_list:
        if day not in valid_days:
            raise ValueError('The given day "{0}" is not a valid day. Please use the standard three letter abbreviations: mon, tue, wed, thu, fri, sat, sun, or all.'.format(day, days))

    if "all" in days_list and len(days_list) > 1: 
        raise ValueError('Use of "all" together with other specified days: {0}. Option "all" should be used alone.'.format(days))

    day_map = {day: i for i, day in enumerate(valid_days)}
    
    if "all" in days_list:
        days_list = [day_map[day] for day in valid_days]
    else:
        days_list = [day_map[day] for day in days_list]


    # Handle input for "hours"
    hours_pattern = re.compile(r'^\d{2}-\d{2}(, ?\d{2}-\d{2})*$')
    if not hours_pattern.match(hours):
        raise ValueError('The input string for hours: "{0}" is not in a valid format. Please provide as a string of comma separated ranges. Example: "06-09, 13-15"'.format(hours))

    hours_list = []
    valid_hours = [str(i).zfill(2) for i in range(0,25)]

    for h in [h.strip() for h in hours.split(',')]:
        start, end = h.split('-')
        if end <= start: 
            raise ValueError('Wrong order of hours: "{0}" The first hour in any range should be strictly smaller than the second'.format(h))
        if start not in valid_hours or end not in valid_hours:
            raise ValueError('Make sure both start "{0}" and end "{1}" are valid hours of the day.'.format(start, end))
        hours_list += [str(day).zfill(2) for day in range(int(start), int(end))]
    
    hours_list.sort()

    if len(hours_list) != len(set(hours_list)):
        raise ValueError("Make sure that the ranges in hours: {0} do not overlap".format(hours))

    
    return start_date, end_date, days_list, hours_list




def _process_directories(search_directories, files):
    """
    Performs a multi-threaded search of potential data source directories. 
    
        Args:
            search_directories (list[str]): List of potential data source directory paths.
            files (list[str]): List of file names to search for

        Returns:
            results (list[str]): List of full paths to the queried files.
    """   
    search_directories = [path for path in search_directories if os.path.isdir(path)]

    def process_directory(dir):
        output = []
        for file in os.listdir(dir):
            if file in files:
                output.append(os.path.join(dir, file))
        return output

    result = []

    with ThreadPoolExecutor() as executor:
        for rv in executor.map(process_directory, search_directories):
            result.extend(rv)

    return result




def _get_schema():
    """
    Returns the CSV schema of the source data files.

        Returns:
            schema (pyspark.sql.types.StructType): The CSV schema of the source data files.
    """   
    headers = ["USAGE_DTTM", "SCRAMBLED_IMSI", "Download_Type", "MCC", "MNC", "LAC", "SAC", "TAC",
                "E_NODE_B_ID", "SECTOR_ID", "RAT", "NEW_CALL_ATTEMPTS", "NEW_ANSWERED_CALLS",
                "NEW_FAILED_CALLS", "NEW_DROPPED_CALLS", "NEW_PS_ATTEMPTS", "NEW_FAILED_PS", "NEW_DROPPED_PS",
                "NEW_SMS_ATTEMPTS", "NEW_FAILED_SMS", "NEW_LU_ATTEMPTS", "NEW_FAILED_LU", "NEW_RAUTAU_ATTEMPTS",
                "NEW_FAILED_RAUTAU", "NEW_ATTACH_ATTEMPTS", "NEW_FAILED_ATTACH", "NEW_DETACH_ATTEMPTS",
                "NEW_DL_VOLUME", "NEW_DL_MAX_THROUGHPUT", "NEW_UL_VOLUME", "NEW_UL_MAX_THROUGHPUT"]

    fields = [StructField(field_name, StringType(), True) 
                if field_name in ["USAGE_DTTM", "SCRAMBLED_IMSI", "Download_Type"] 
                else StructField(field_name, IntegerType(), True) 
                for field_name in headers]

    schema = StructType(fields)

    return schema





