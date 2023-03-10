################################################################################################
# Notebook_Name -  Common_Function_ABC.py
# Purpose - To create entry for Batch, Job and file run and insert them in the respective Azure sql tables.
# Version - 2.0
# History - 
#################################################################################################

# ----------------------------------------------------------------------------------- #
# ----------------------------- Importing Packages ---------------------------------- #
# ----------------------------------------------------------------------------------- #

import pyspark
from pyspark import SparkConf, SQLContext, SparkContext, StorageLevel
from pyspark.sql import SparkSession, SQLContext, DataFrame, Window, Row
from pyspark.context import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from functools import reduce
import uuid
import os
import sys
from datetime import datetime, timedelta
from uuid import uuid1, uuid4
import pyodbc as pyodbc
#from pyspark.dbutils import DBUtils
import traceback

#------------------------------------------------------------------------------------ #
    
# ----------------------------Declaring Global Variables----------------------------- #

pyodbc_connection_str1 = "Driver={ODBC Driver 17 for SQL Server};Server="
pyodbc_connection_str2 = ".database.windows.net;Database="
scope="databrickscope"
UID = ";UID=" 
PWD = ";PWD="
insert_into = "Insert into "
update = "UPDATE "
SET = " SET Status ='"

#----------------------------------------------------------------------------------- #

jdbc = "jdbc"
url = "url"
query_str = "query" 
user = "user"
password = "password"
driver = "driver"
sqldriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

#----------------------------------------------------------------------------------- #

column_batch = " ([ExecutionRunID],[BatchID],[SourceID],[BatchStartTime],[BatchEndTime],[Status]) Values('"
batch = "', BatchEndTime ='"
where_condition_batch = "' WHERE (BatchID = '"
batch_id_and_status = " AND Status ='Started"

column_job = " ([JobRunID],[ExecutionRunID],[JobID],[JobName],[SourceSystem],[SourcePath],[TargetSystem],[TargetPath],[SourceCount],[TargetCount],[RejectCount],[JobStartTime],[JobEndTime],[DurationInSeconds],[Status]) Values('"
job_time = "', JobEndTime ='"
source_count_job = "', SourceCount ="
target_count_job = ", TargetCount ="
reject_count_job = ", RejectCount ="
duration_in_seconds_job = ", DurationInSeconds ="
where_condition_job = " WHERE (JobStartTime = '"
and_condition_job = "' AND SourcePath ='"

column_file = " ([FileRunId],[ExecutionRunID],[JobRunID],[FileId],[LoadStartTime],[SourceCount],[TargetCount],[LoadEndTime],[DurationInSeconds],[Status]) Values('"
load_time_file = "', LoadEndTime ='"
source_count_file = "', SourceCount ='"
target_count_file = "', TargetCount ='"
duration_in_seconds_file = "', DurationInSeconds ='"
where_condition_file = "' WHERE (LoadStartTime = '"
and_condition_file = "' AND FileRunId ="


batch_run_stat_table_name = "[abc].[Batch_Run_Stats]"



column2 = " ([ExecutionRunID],[ErrorCode],[ErrorID],[ErrorMessage],[ErrorType],[ObjectName],[ObjectRunID],[CreatedDate]) Values("
column3 = " ([FileRunId],[ExecutionRunID],[JobRunID],[FileId],[LoadStartTime],[SourceCount],[TargetCount],[LoadEndTime],[DurationInSeconds],[Status]) Values("
column4 = " ([ExecutionRunID],[ErrorCode],[ErrorID],[ErrorMessage],[ErrorType],[ObjectName],[ObjectRunID],[CreatedDate]) Values("
column5 = " ([JobRunID],[ExecutionRunID],[JobID],[JobName],[SourceSystem],[SourcePath],[TargetSystem],[TargetPath],[SourceCount],[TargetCount],[RejectCount],[JobStartTime],[JobEndTime],[DurationInSeconds],[Status]) Values("
column6 = " ([ExecutionRunID],[ErrorCode],[ErrorID],[ErrorMessage],[ErrorType],[ObjectName],[ObjectRunID],[CreatedDate]) Values("


# ----------------------------------------------------------------------------------- #

#---Code to read param file from ADLS location---#
spark = SparkSession.builder.getOrCreate()
#dbutils = DBUtils(spark)

storage_account_name = "adlsrawtoadlscurated"
#tenant_id = dbutils.secrets.get(scope,"tenantid")
#client_id = dbutils.secrets.get(scope, "clientid")
#client_secret = dbutils.secrets.get(scope, "clientsecret")

tenant_id = '36da45f1-dd2c-4d1f-af13-5abe46b99921'
client_id = 'f2be9926-39c9-4fa1-930b-abfdf97a8178'
client_secret = 'T1i8Q~NcCuq6o90Q_S643znPSKOb-gKWj-KwbcDy'

spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", f"{client_id}")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", f"{client_secret}")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

param_file_location = "abfss://abc@adlsrawtoadlscurated.dfs.core.windows.net/abc_param_file.csv"

param_df = spark.read.format("text").option("inferSchema", "true").option("header", "false").load(param_file_location)
param_list=param_df.collect()

# -------------------------------------------------------------------------------------------------------------------------------------- #
# FunctionName - Parse_Job_Param
# Purpose - To get and set the paramater values
# Arguments - No. of arguments expected 1
# Arg1( extParam )  - parameter name who's value needs to get & set
# One Sample function calling statement - Parse_Job_Param(extParam)
# One Function calling statement with Example value - server_name = Parse_Job_Param('server_name') 
# -------------------------------------------------------------------------------------------------------------------------------------- #

def Parse_Job_Param(extParam):
    rsltVal=""
    for row in param_list:
        valList=row['value'].split(',',1)
        if len(valList) > 1:
            if (extParam == valList[0]):
                rsltVal=valList[1]
                break
        else:
            if (extParam == valList[0]):
                rsltVal=""
                break
    return(rsltVal)


# -------------------------------------------------------------------------------------------------------------------------------------- #

# -------------------------------------------------------------------------------------------------------------------------------------- #
# FunctionName - Batch_Run_Stats_Entry 
# Purpose - To make an entry to the [abc].[Batch_Run_Stats] table when the batch starts Initial Batch Run Stats Entry made with start time & update the same entry based on condition to match batchid and started status and update end time and status completed in the Final Batch Run Stats.  
# Function Return – This function returns either 1 or 0. A return value of 0 indicates that the function executed without error, while a return value of 1 indicates that an error occurred during execution. 
# Arguments - No. of arguments expected 6
# Arg1( batch_id )  - Unique Id of a Batch 
# Arg2( source_id )  - Id for a particular source like ADLS = 1, NAS = 2, SQL = 3 ... 
# Arg3( batch_start_time )  - Time when batch start running 
# Arg4( batch_end_time )  - Time when batch completed 
# Arg5( status )  - Running status of batch like it is Started, Completed 
# Arg6( query_type )  - Type of sql query like insert or update. 
# One Sample function calling statement - Batch_Run_Stats_Entry(batch_id, source_id, batch_start_time, batch_end_time, status, query_type)
# ------------------------------------------------------------------------------------------------------------------------------------- #


def Batch_Run_Stats_Entry(batch_id, source_id, batch_start_time, batch_end_time, status, query_type):
    
    try:
        
        server_name = Parse_Job_Param('server_name')
        database_name = Parse_Job_Param('database_name')
        sql_user_key = Parse_Job_Param('sql_user_key')
        sql_pass_key = Parse_Job_Param('sql_pass_key')
        
        #jdbc_username= dbutils.secrets.get(scope, sql_user_key)
        #jdbc_password = dbutils.secrets.get(scope, sql_pass_key)

        jdbc_username= "testmiuser1"
        jdbc_password = "testmisqlpoc1@123"
        
        batch_start_time_str = str(batch_start_time)
        batch_start_time_new = batch_start_time_str[0:23]
        batch_id_str = str(batch_id)
        source_id_str = str(source_id)
        execution_run_id = datetime.now().strftime('%Y%m%d%H%M%S-') + str(uuid1())
        
        
        #### Checking the queryType and based on it performing the insert or update query on the SQL table ####
        if query_type == "Insert":
            
            cnxn_str = (pyodbc_connection_str1 + server_name + pyodbc_connection_str2 + database_name + UID + jdbc_username + PWD + jdbc_password + ";")

            #### Connecting with pyodbc with the above created connection to perform the insert and update query in SQL table ####
            cnxn = pyodbc.connect(cnxn_str)

            #### Creating a pyodbc connection object ####
            cursor = cnxn.cursor()
            
            query = insert_into + batch_run_stat_table_name + column_batch + execution_run_id + "','" + batch_id_str + "','" + source_id_str + "','" + batch_start_time_new + "','" + batch_end_time + "','" + status + "')"
            
            #### Executing the Insert query to the sql table with the help of pyodbc object ####
            cursor.execute(query)

            #### Committed the changes so it will reflect in the original sql table ####
            cnxn.commit()
            
      
        elif query_type == "Update":
            
            batch_end_time_str = str(batch_end_time)
            batch_end_time_new = batch_end_time_str[0:23]
        
            #### Creating the connection string to connect to pyodbc with the help of Servername and database name ####
            cnxn_str = (pyodbc_connection_str1 + server_name + pyodbc_connection_str2 + database_name + UID + jdbc_username + PWD + jdbc_password + ";")

            #### Connecting with pyodbc with the above created connection to perform the insert and update query in SQL table ####
            cnxn = pyodbc.connect(cnxn_str)

            #### Creating a pyodbc connection object ####
            cursor = cnxn.cursor()

            query = update + batch_run_stat_table_name + SET + status + batch + batch_end_time_new + where_condition_batch + batch_id_str + "'" + batch_id_and_status + "')"
            
            #### Executing the Insert query to the sql table with the help of pyodbc object ####
            cursor.execute(query)

            #### Committed the changes so it will reflect in the original sql table ####
            cnxn.commit()
    
        return 0
            
    except Exception as ex:
        
        message = str(ex)
        print(message)
        return 1
        
#------------------------------------------------#      

# -------------------------------------------------------------------------------------------------------------------------------------- #
# FunctionName - Job_Run_Stats_Entry 
# Purpose - To make an entry to the [abc].[Job_Run_Stats] table when the Job starts Initial Job Run Stats Entry Made with Start Time source path & update the same entry based on condition to match start time and sourcepath and update query with end time, duration, reject count and status completed in the Final Job Run Stats. 
# Function Return – This function returns either 1 or 0. A return value of 0 indicates that the function executed without error, while a return value of 1 indicates that an error occurred during execution. 
# Arguments - No. of arguments expected 12 
# Arg1( job_id )  -  Unique Id for a databricks job 
# Arg2( job_name )  - Unique Name of a databricks job 
# Arg3( source_system )  - Source from we are copying the data like ADLS, NAS, SQL server 
# Arg4( source_path )  - file path from where we need to copy the file data, it can be NAS drive path ADLS raw layer path. 
# Arg5( target_system )  -  Target where we are copying the data like ADLS raw, curated layers 
# Arg6( target_path )  - file path where we will copy the file data, it can be ADLS curated, raw layer path. 
# Arg7( source_count )  - Record count value from source object like ADLS file, NAS drive file, SQL Table Integer value 
# Arg8( target_count )  - Record count value after the transformation such deduplication performed on source file and we moved the file from source  to ADLS. Integer value 
# Arg9( job_start_time )  - Time when a databricks Initial  job start running. 
# Arg10( job_end_time )  - Time when a databricks Final job completed or failed. 
# Arg11( status )  - status of file like it is copied successfully or failed 
# Arg12( query_type )  - Type of sql query like insert or update. 
# One Sample function calling statement - Job_Run_Stats_Entry(job_id, job_name, source_system, source_path, target_system, target_path, source_count, target_count, job_start_time, job_end_time, status, query_type)
# ------------------------------------------------------------------------------------------------------------------------------------- # 
        
def Job_Run_Stats_Entry(job_id, job_name, source_system, source_path, target_system, target_path, source_count, target_count, job_start_time, job_end_time, status, query_type):
    
    try:
        
        server_name = Parse_Job_Param('server_name')
        database_name = Parse_Job_Param('database_name')
        sql_user_key = Parse_Job_Param('sql_user_key')
        sql_pass_key = Parse_Job_Param('sql_pass_key')
        job_run_stat_table_name = Parse_Job_Param('job_run_stat_table_name')
        
        #jdbc_username= dbutils.secrets.get(scope, sql_user_key)
        #jdbc_password = dbutils.secrets.get(scope, sql_pass_key)

        jdbc_username= "testmiuser1"
        jdbc_password = "testmisqlpoc1@123"
        
        job_start_time_str = str(job_start_time)
        job_start_time_new = job_start_time_str[0:23] 
        
        job_start_time_dt = datetime.strptime(job_start_time_str, '%Y-%m-%d %H:%M:%S.%f')
               

        if query_type == "Insert":
            
            job_id_str = str(job_id)
        
            url_link = f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}"

            querydata = "select ExecutionRunID from [abc].[Batch_Run_Stats] where BatchID = (select BatchID from [metaabc].[Job_Metadata] where JobID =" + job_id_str + ") and Status ='Started'"

            execution_run_id_df = (spark.read 
                  .format(jdbc)\
                  .option(url, url_link)\
                  .option(query_str, querydata)\
                  .option(user, jdbc_username)\
                  .option(password, jdbc_password)\
                  .option(driver, sqldriver)\
                  .load()
                )


            execution_run_id=execution_run_id_df.collect()[0][0]
            
            job_run_id = str(uuid4())
            reject_count = "0"
            duration_in_seconds = "0"
            cnxn_str = (pyodbc_connection_str1 + server_name + pyodbc_connection_str2 + database_name + UID + jdbc_username + PWD + jdbc_password + ";")
            cnxn = pyodbc.connect(cnxn_str)
            cursor = cnxn.cursor()
            
            query = insert_into + job_run_stat_table_name + column_job + str(job_run_id) + "','" + str(execution_run_id) + "'," + job_id_str + ",'" + job_name + "','" + source_system + "','" + source_path + "','" + target_system + "','" + target_path +"'," + source_count + "," + target_count + "," + reject_count + ",'"+ job_start_time_new + "','"  + job_end_time + "'," + duration_in_seconds + ",'" + status + "')"
            
            #### Executing the Insert query to the sql table with the help of pyodbc object ####
            cursor.execute(query)

            #### Committed the changes so it will reflect in the original sql table ####
            cnxn.commit()

        elif query_type == "Update":
            
            
            job_end_time_str = str(job_end_time)
            job_end_time_new = job_end_time_str[0:23]
            
            job_end_time_dt = datetime.strptime(job_end_time_str, '%Y-%m-%d %H:%M:%S.%f')

            duration = job_end_time_dt - job_start_time_dt
            
            duration_in_seconds = int(duration.total_seconds())
            
            duration_in_seconds = str(duration_in_seconds)
            
            
            if (source_count == None):
                
                try:
                   
                    source_count_query = Parse_Job_Param('source_count_query')
 
                    source_count_df = (spark.read 
                          .format(jdbc)\
                          .option(url, url_link)\
                          .option(query_str, source_count_query)\
                          .option(user, jdbc_username)\
                          .option(password, jdbc_password)\
                          .option(driver, sqldriver)\
                          .load()
                        )


                    source_count = source_count_df.collect()[0][0]
                    
                    
                except Exception as ex:
                    
                    source_count = '0'
                    
            if (target_count == None):
                
                try:
                    
                    target_count_query = Parse_Job_Param('target_count_query')
 
                    target_count_df = (spark.read 
                          .format(jdbc)\
                          .option(url, url_link)\
                          .option(query_str, target_count_query)\
                          .option(user, jdbc_username)\
                          .option(password, jdbc_password)\
                          .option(driver, sqldriver)\
                          .load()
                        )


                    target_count = target_count_df.collect()[0][0]
                    
                    
                except Exception as ex:
                    
                    target_count = '0'                    
        
            reject_count = int(source_count) - int(target_count)
            
            cnxn_str = (pyodbc_connection_str1 + server_name + pyodbc_connection_str2 + database_name + UID + jdbc_username + PWD + jdbc_password + ";")
            cnxn = pyodbc.connect(cnxn_str)
            cursor = cnxn.cursor()
            
            query = update + job_run_stat_table_name + SET + status + job_time + job_end_time_new + source_count_job + str(source_count) + target_count_job + str(target_count) + reject_count_job + str(reject_count) + duration_in_seconds_job + duration_in_seconds + where_condition_job + job_start_time_new + and_condition_job + source_path + "')"
            
            
            cursor.execute(query)
            cnxn.commit()   
            
        return 0
    
    except Exception as ex:
        stack_trace = traceback.format_exc()
        print(stack_trace)
        message = str(ex)
        print(message)
        return 1

# -------------------------------------------------------------------------------------------------------------------------------------- #
# FunctionName - File_Run_Stats_Entry  
# Purpose - To make an Entry to the [abc].[File_Run_Stats] table when the Job Started Initial File Run starts Entry Made & update the same entry based on condition to match start time and FileRunId, and update query with end time and status completed in Final File Run Stats.  
# Function Return – This function returns either 1 or 0. A return value of 0 indicates that the function executed without error, while a return value of 1 indicates that an error occurred during execution.  
# Arguments No of Argumenets expected 8  
# Arg1 ( file_id ) - Unique Id for a File  
# Arg2 ( source_count ) - Record count value from source object like ADLS file, NAS drive file, SQL Table 
# Arg3 ( target_count ) - Record count value after we moved the file from source to ADLS. 
# Arg4 ( load_start_time ) - Time when Initial File start running 
# Arg5 ( load_end_time ) - Time when Final File transferred completed or failed  
# Arg6 ( status ) - Status of file like it is copied successfully or failed 
# Arg7 ( query_type ) - Type of sql query like insert or update. 
# One Sample function calling statement - File_Run_Stats_Entry(file_id, source_count, target_count, load_start_time, load_end_time, status, query_type)  
# -------------------------------------------------------------------------------------------------------------------------------------- #
        
def File_Run_Stats_Entry(file_id, source_count, target_count, load_start_time, load_end_time, status, query_type):
    
    try:
        
        server_name = Parse_Job_Param('server_name')
        database_name = Parse_Job_Param('database_name')
        sql_user_key = Parse_Job_Param('sql_user_key')
        sql_pass_key = Parse_Job_Param('sql_pass_key')
        file_run_stat_table_name = Parse_Job_Param('file_run_stat_table_name')
        
        
        #jdbc_username= dbutils.secrets.get(scope, sql_user_key)
        #jdbc_password = dbutils.secrets.get(scope, sql_pass_key)

        jdbc_username= "testmiuser1"
        jdbc_password = "testmisqlpoc1@123"
        
        load_start_time_str = str(load_start_time)
        load_start_time_new = load_start_time_str[0:23]
        
        load_start_time_dt = datetime.strptime(load_start_time_str, '%Y-%m-%d %H:%M:%S.%f')
        
        

        if query_type == "Insert":
            
            file_id_str = str(file_id)

            url_link = f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}"

            querydata = "select ExecutionRunID,JobRunID from [abc].[Job_Run_Stats] where JobId = (select JobID from [metaabc].[File_Metadata] where FileID =" + file_id_str + ") and Status ='Started'"

            id_df = (spark.read 
                  .format(jdbc)\
                  .option(url, url_link)\
                  .option(query_str, querydata)\
                  .option(user, jdbc_username)\
                  .option(password, jdbc_password)\
                  .option(driver, sqldriver)\
                  .load()
                )


            execution_run_id=id_df.collect()[0][0]
            job_run_id=id_df.collect()[0][1]
            
            file_run_id = str(uuid4())
            duration_in_seconds = "0"
            cnxn_str = (pyodbc_connection_str1 + server_name + pyodbc_connection_str2 + database_name + UID + jdbc_username + PWD + jdbc_password + ";")
            cnxn = pyodbc.connect(cnxn_str)
            cursor = cnxn.cursor()
            
            query = insert_into + file_run_stat_table_name + column_file + file_run_id + "','" + str(execution_run_id) + "','" + str(job_run_id) + "'," + file_id + ",'" + load_start_time_new + "'," + source_count + ",'" + target_count + "','" + load_end_time + "','" + duration_in_seconds + "','" + status + "')"
            
            
            #### Executing the Insert query to the sql table with the help of pyodbc object ####
            cursor.execute(query)

            #### Committed the changes so it will reflect in the original sql table ####
            cnxn.commit()

        elif query_type == "Update":
            
            
            
            load_end_time_str = str(load_end_time)
            load_end_time_new = load_end_time_str[0:23]
            
            load_end_time_new_dt = datetime.strptime(load_end_time_str, '%Y-%m-%d %H:%M:%S.%f')

            duration = load_end_time_new_dt - load_start_time_dt
            
            duration_in_seconds = int(duration.total_seconds())
            
            duration_in_seconds = str(duration_in_seconds)
            
            
            
            if (source_count == None):
                
                try:
                   
                    source_count_query = Parse_Job_Param('source_count_query')
 
                    source_count_df = (spark.read 
                          .format(jdbc)\
                          .option(url, url_link)\
                          .option(query_str, source_count_query)\
                          .option(user, jdbc_username)\
                          .option(password, jdbc_password)\
                          .option(driver, sqldriver)\
                          .load()
                        )


                    source_count = source_count_df.collect()[0][0]
                    
                    
                except Exception as ex:
                    
                    source_count = '0'
                    
            if (target_count == None):
                
                try:
                    
                    target_count_query = Parse_Job_Param('target_count_query')
 
                    target_count_df = (spark.read 
                          .format(jdbc)\
                          .option(url, url_link)\
                          .option(query_str, target_count_query)\
                          .option(user, jdbc_username)\
                          .option(password, jdbc_password)\
                          .option(driver, sqldriver)\
                          .load()
                        )


                    target_count = target_count_df.collect()[0][0]
                    
                    
                except Exception as ex:
                    
                    target_count = '0'                    
            
            cnxn_str = (pyodbc_connection_str1 + server_name + pyodbc_connection_str2 + database_name + UID + jdbc_username + PWD + jdbc_password + ";")
            cnxn = pyodbc.connect(cnxn_str)
            cursor = cnxn.cursor()
            
            query = update + file_run_stat_table_name + SET + status + load_time_file + load_end_time_new + source_count_file + source_count + target_count_file + target_count + duration_in_seconds_file + duration_in_seconds + where_condition_file + load_start_time_new + "')"
            
            cursor.execute(query)
            cnxn.commit()   
            
        return 0
    
    except Exception as ex:
        stack_trace = traceback.format_exc()
        print(stack_trace)
        message = str(ex)
        print(message)
        return 1
                   
            
