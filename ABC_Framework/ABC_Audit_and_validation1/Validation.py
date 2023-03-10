#######################################################################################
# Notebook_Name -  Validations_abc.py
# Purpose - To run the Validation checks on a particular object before and after of it processed and creating the validation checks result and storing it into Azure SQL Tables. 
# Version - 1.0
# History - 
#######################################################################################

# ----------------------------------------------------------------------------------- #
# ----------------------------- Importing Packages ---------------------------------- #
# ----------------------------------------------------------------------------------- #

from pyspark.sql import SparkSession
#from pyspark.dbutils import DBUtils
from datetime import datetime, timedelta
from pyspark.sql.functions import *
import pyodbc as pyodbc
import pandas as pd
import asyncio
import traceback
import os, uuid, sys
from pyspark.sql import SparkSession
import hashlib
import pandas as pd



# ------------------------------------------------------------------------------------ #
# ----------------------------- Building Spark Session ------------------------------- #
# ------------------------------------------------------------------------------------ #

try:
    spark = SparkSession.builder.getOrCreate()
#    dbutils = DBUtils(spark)
except Exception as ex:
    Error_Message = "Error Occured While Creating the Spark Session - "+ str(ex)
    print(Error_Message)

# ----------------------------------------------------------------------------------- #
# ---------------------- Declared Global Variable and connection -------------------- #
# ----------------------------------------------------------------------------------- #

try:
    #tenant_id = dbutils.secrets.get("databrickscope","tenantid")
    #client_id = dbutils.secrets.get("databrickscope", "clientid")
    #client_secret = dbutils.secrets.get("databrickscope", "clientsecret")
    tenant_id = '36da45f1-dd2c-4d1f-af13-5abe46b99921'
    client_id = 'f2be9926-39c9-4fa1-930b-abfdf97a8178'
    client_secret = 'T1i8Q~NcCuq6o90Q_S643znPSKOb-gKWj-KwbcDy'
    storage_account_name = "adlsrawtoadlscurated"
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
    
    Querydata_1 = " Select * From abc.Audit_Log where Product_Name not like 'Post%' "
    Querydata_2 = " Select FileName,BalanceAmountColumn from metaabc.File_MetaData "
    Querydata_3 = " Select * from metaabc.File_MetaData "
    Querydata_4 = " Select * from abc.Audit_Log  where Product_Name like 'Post%' "
    Query1 = "Select TOP 50 * From abc.Job_Rule_Assignment Where Job_Id = "
    Query2 = " and ValidationType = 'Pre'"
    Query3 = " and ValidationType = 'Post'"
    Query4 = "SELECT * From abc.File_Checksum_details"
    Order_rule_id = " ORDER BY Rule_Id"
    TableName = "abc.Job_Rule_Execution_Log"
    Username = "Ingestion-Pattern"
    logs_info = []
    Logs_column_name = ["Logs"]
except Exception as ex:
    print("Error Occured declaring Global Variables - " + str(ex))

# -------------------------------------------------------------------------------------------------------------------------------------- #
# FunctionName - Pre_Validation_Wrapper 
# Purpose - To Fetch Argument Values from Configure File URL which is located in ADLS and used those arguments to trigger all Pre validation rules after object processed, fetching all the rule ids assigned for a particular jobid and based on the rule id triggering those validation rules for a particular object and returning the output of the validation rule. Stored the output in [abc].[Job_Rule_Execution_Log] Table and create’s pre-validation log file.  
# Function Return – This Function return set of validation function results either passed or failed Based on Rule id Passed to the Function.  
# Arguments No of Argumenets expected 1 
# Arg1 ( Config_File_URL ) 
# Pre_Validation_Wrapper(Config_File_URL) 
# ------------------------------------------------------------------------------------------------------------------------------------- #

def Pre_Validation_Wrapper(Config_File_URL):
					
    try:
        Argument_list_df = spark.read.option("header", "true").csv(Config_File_URL)
        Argument_list_pdf = Argument_list_df.toPandas()

        JobId = int(Argument_list_pdf.iloc[0]['JobId'])
        ObjectPath = str(Argument_list_pdf.iloc[0]['ObjectPath'])
        ObjectName = str(Argument_list_pdf.iloc[0]['ObjectName'])
        BalanceAmountColumn = str(Argument_list_pdf.iloc[0]['BalanceAmountColumn'])
        ObjectType = str(Argument_list_pdf.iloc[0]['ObjectType'])
        ObjectPath_PostProcessing = str(Argument_list_pdf.iloc[0]['ObjectPath_PostProcessing'])
        server_name = str(Argument_list_pdf.iloc[0]['server_name'])
        database_name = str(Argument_list_pdf.iloc[0]['database_name'])
        FileType = str(Argument_list_pdf.iloc[0]['FileType'])
        storage_account_name = str(Argument_list_pdf.iloc[0]['storage_account_name'])
        scope = str(Argument_list_pdf.iloc[0]['scope'])
        sqlusername = str(Argument_list_pdf.iloc[0]['sqlusername'])
        sqlpassword = str(Argument_list_pdf.iloc[0]['sqlpassword'])
        log_path = str(Argument_list_pdf.iloc[0]['log_path'])
        tenant_id = str(Argument_list_pdf.iloc[0]['tenant_id'])
        client_id = str(Argument_list_pdf.iloc[0]['client_id'])
        client_secret = str(Argument_list_pdf.iloc[0]['client_secret'])
        Config_file_url = str(Argument_list_pdf.iloc[0]['Config_file_url'])
        Metadata_place = str(Argument_list_pdf.iloc[0]['Metadata_place'])
        Metadata_column = int(Argument_list_pdf.iloc[0]['Metadata_column'])
        Manifest_file_url = str(Argument_list_pdf.iloc[0]['Manifest_file_url'])
        HeadCount_value_start_index = int(Argument_list_pdf.iloc[0]['HeadCount_value_start_index'])
        HeadCount_value_end_index = int(Argument_list_pdf.iloc[0]['HeadCount_value_end_index'])
        BalanceAmount_value_start_index = int(Argument_list_pdf.iloc[0]['BalanceAmount_value_start_index'])
        BalanceAmount_value_end_index = int(Argument_list_pdf.iloc[0]['BalanceAmount_value_end_index'])
        service_principle_credentials = {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret}
        Thrhd_alrt_type = str(Argument_list_pdf.iloc[0]['Thrhd_alrt_type'])
        
        
        current_time = datetime.now()
        ###  Configuring storage account 
        
        # Configuring Storage Account with OAuth
        #spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
        log = f"{current_time} Configuring Storage Account with OAuth"
        logs_info.append(log)
        
        # Configuring Storage Account with ClientCredsTokenProvider
        #spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        log = f"{current_time} Configuring Storage Account with ClientCredsTokenProvider"
        logs_info.append(log)
        
        # Configuring Storage Account using client_id
        #spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
        log = f"{current_time} Configuring Storage Account using client_id"
        logs_info.append(log)
        
        # Configuring Storage Account using client_secret
        #spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
        log = f"{current_time} Configuring Storage Account using client_secret"
        logs_info.append(log)
        
        # Configuring Storage Account using tenant_id
        #spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
        log = f"{current_time} Configuring Storage Account using tenant_id"
        logs_info.append(log)
        
        log = f"{current_time} Created Connection with Storage Account with Service Principles"
        logs_info.append(log)
        
        # Created Variable service_principle_credentials to store tenant_id, client_id, client_secret
        #service_principle_credentials = {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret}
        
        log = f"{current_time} Created Variable service_principle_credentials to store tenant_id, client_id, client_secret"
        logs_info.append(log)
        
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log = f"{current_time} Consolidating Custom logs in Pandas Dataframe"
        logs_info.append(log)
        # The
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        log = f"{current_time} Custom logs are Stored in ADLS"
        logs_info.append(log)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message
    
    try:
        #### Creating SQL Query to fetch rule assigned to a particular Job ID ####
        Querydata = Query1 + str(JobId) + Query2 + Order_rule_id
        current_time = datetime.now()
        log = f"{current_time} wrapperFunction Querydata variable is initialized"
        logs_info.append(log)
        
        #### Fetching the rule assigned to a particular Job ID for pre object processing ####
        JobRuleDetails_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata,service_principle_credentials,log_path)
        log = f"{current_time} JobRuleDetails_df Dataframe is created"
        logs_info.append(log)
        
        #### Fetching all the pre processed Object data from Audit Table(Source) ####
        AuditTable_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata_1,service_principle_credentials,log_path)

        #### Converting Spark Dataframe to Pandas dataframe ####
        JobRuleDetails_pdf = JobRuleDetails_df.toPandas()
        log = f"{current_time} JobRuleDetails_pdf Pandas Dataframe is created"
        logs_info.append(log)
        Output = str('')
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log = f"{current_time} Consolidating Custom logs in Pandas Dataframe"
        logs_info.append(log)
        
        log_path = log_path.replace("abfss", "abfs") 
        
        # Custom logs are Stored in ADLS
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        log = f"{current_time} Custom logs are Stored in ADLS"
        logs_info.append(log)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message

    # Checking ObjectName or Object Path is Blank and ObjectType Not in SQL Table  
    if (( None == ObjectName or ObjectName == "") or ((None == ObjectPath or ObjectPath == "") and (ObjectType != 
                                                                                                    "SQLTable"))) : 
        error_message = "Error Occured : ObjectName or Path is blank"

        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)

        # Consolidating Custom logs in Pandas Dataframe 
        custom_logs = pd.DataFrame(logs_info, columns = None)

        log_path = log_path.replace("abfss", "abfs")
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)

        return "Error Message : ObjectName or Path is blank in Pre Validation Wrapper Function"

    #### Iterating RuleId to trigger a particular rule assigned with the rule id ####
    try:
        for ind in JobRuleDetails_pdf.index:
            if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 0):
                #### Triggering Count Validation pre Object processing rule ####
                Out0 = CheckJobRule_ZeroFileSizeValidation(JobId, ObjectPath, ObjectName, ObjectType, server_name, database_name, FileType, sqlusername, sqlpassword, service_principle_credentials, log_path)
                # Checking if CountValidation return Error
                if (Out0.__contains__("Error")):
                    current_time = datetime.now()
                    log = f"{current_time} Error Occured : {Out0} in ZeroFileSizeValidation"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe 
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    return Out0
                Output = Output + Out0 + " | "
                
            if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 1):
                #### Triggering Count Validation pre Object processing rule ####
                Out1 = CheckJobRule_CountValidation(JobId, ObjectPath, ObjectName, ObjectType,server_name,database_name,FileType, sqlusername, 
                                                    sqlpassword, service_principle_credentials, log_path, AuditTable_df, Metadata_place, 
                                                    Manifest_file_url, HeadCount_value_start_index, HeadCount_value_end_index)
                # Checking if CountValidation return Error
                if (Out1.__contains__("Error")):
                    current_time = datetime.now()
                    log = f"{current_time} Error Occured : {Out1} in CountValidation"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe 
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    return Out1
                Output = Output + Out1 + " | " 

            if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 2):
                #### Triggering Balance Amount Validation pre Object processing rule ####
                Out2 = CheckJobRule_BalanceAmountValidation(JobId, ObjectPath, ObjectName, BalanceAmountColumn, 
                                         ObjectType, server_name, database_name, FileType, sqlusername, sqlpassword, service_principle_credentials, 
                                         log_path, AuditTable_df, Metadata_place, Manifest_file_url, BalanceAmount_value_start_index, 
                                         BalanceAmount_value_end_index, Metadata_column)
                # Checking if BalanceAmountValidation return Error
                if (Out2.__contains__("Error")):
                    current_time = datetime.now()
                    log = f"{current_time} Error Occured : {Out2} in BalanceAmountValidation"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe 
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    return Out2
                Output = Output + Out2 + " | "


            if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 3):
                #### Triggering Threshold Validation pre Object processing rule ####
                Out3 = CheckJobRule_ThresholdValidation(JobId, ObjectPath, ObjectName, ObjectType, server_name,database_name,FileType, 
                                                        sqlusername, sqlpassword, service_principle_credentials, log_path)
                # Checking if ThresholdValidation return Error comment Error while reading file 
                if (Out3.__contains__("Error")):
                    current_time = datetime.now()
                    log = f"{current_time} Error Occured : {Out3} in ThresholdValidation"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe 
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    return Out3
                Output = Output + Out3 + " | "

            if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 4):
                #### Triggering Object Name Validation pre Object processing rule ####
                Out4 = CheckJobRule_FileNameValidation(JobId, ObjectPath, ObjectName, server_name, database_name, ObjectType, sqlusername, 
                                                       sqlpassword, service_principle_credentials, log_path, AuditTable_df)
                # Checking if FileNameValidation return Error comment Error while reading file
                if (Out4.__contains__("Error")):
                    current_time = datetime.now()
                    log = f"{current_time} Error Occured : {Out4} in FileNameValidation"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe 
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    return Out4
                Output = Output + Out4 + " | "

            if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 5):
                #### Triggering Object Size Validation pre Object processing rule ####
                Out5 = CheckJobRule_FileSizeValidation(JobId, ObjectPath, ObjectName, server_name,database_name, ObjectType, sqlusername, 
                                                       sqlpassword, service_principle_credentials, log_path, AuditTable_df)
                # Checking if FileSizeValidation return Error comment Error while reading file
                if (Out5.__contains__("Error")):
                    current_time = datetime.now()
                    log = f"{current_time} Error Occured : {Out5} in FileSizeValidation"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    return Out5
                Output = Output + Out5 + " | "

            if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 6):
                #### Triggering Object Arrival Time Validation pre Object processing rule ####
                Out6 = CheckJobRule_FileArrival_Validation(JobId, ObjectPath, ObjectName, server_name,database_name,ObjectType, sqlusername, 
                                                           sqlpassword, service_principle_credentials, log_path, AuditTable_df)
                # Checking if FileArrival_Validation return Error comment Error while reading file
                if (Out6.__contains__("Error")):
                    current_time = datetime.now()
                    log = f"{current_time} Error Occured : {Out6} in FileArrival_Validation"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    return Out6
                Output = Output + Out6 + " | "

            if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 7):
                #### Triggering Missing Object Validation pre Object processing rule ####
                Out7 = CheckJobRule_Missing_FileCheck_Validation(JobId,ObjectPath,ObjectName,server_name,database_name, 
                                                                 ObjectType, sqlusername, sqlpassword, service_principle_credentials, log_path, 
                                                                 AuditTable_df)
                # Checking if Missing_FileCheck_Validation return Error comment Error while reading file
                if (Out7.__contains__("Error")):
                    current_time = datetime.now()
                    log = f"{current_time} Error Occured : {Out7} in Missing_FileCheck_Validation"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    return Out7
                Output = Output + Out7 + " | "

            if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 10):
                #### Triggering Missing Object Validation pre Object processing rule ####
                Out10 = CheckJobRule_CountValidation_Egress(JobId, ObjectPath, ObjectName, server_name, database_name, FileType, sqlusername, 
                                                            sqlpassword, service_principle_credentials, log_path, AuditTable_df)
                # Checking if CheckJobRule_CountValidation_Egress return Error comment Error while reading file
                if (Out10.__contains__("Error")):
                    current_time = datetime.now()
                    log = f"{current_time} Error Occured : {Out10} in Count Validation in Egress Pattern"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    return Out10
                
            if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 11):
                #### Triggering  Duplicate File check pre Object processing rule ####
                Out11 = CheckJobRule_DuplicateFileCheck(JobId, ObjectPath, ObjectName, ObjectType, server_name, database_name, FileType, sqlusername, sqlpassword, service_principle_credentials, log_path)
                # Checking if CheckJobRule_Duplicate_File_Check return Error comment Error while reading file
                if (Out11.__contains__("Error")):
                    current_time = datetime.now()
                    log = f"{current_time} Error Occured : {Out11} in Duplicate File Check"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    return Out11
                
                Output = Output + Out11 + " | "

        current_time = datetime.now()    
        log = f"{current_time} {Output}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        return Output
    
    except Exception as ex:
        error_message = error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log = f"{current_time} Consolidating Custom logs in Pandas Dataframe"
        logs_info.append(log)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        log = f"{current_time} Custom logs are Stored in ADLS"
        logs_info.append(log)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message


# -------------------------------------------------------------------------------------------------------------------------------------- #
# FunctionName - Post_Validation_Wrapper 
# Purpose - To Fetch Argument Values from Configure File URL which is located in ADLS and used those arguments to trigger all Post validation rules after object processed, fetching all the rule ids assigned for a particular jobid and based on the rule id’s triggering those validation rules for a particular object and returning the output of the validation rule which we are storing in [abc].[Job_Rule_Execution_Log] Table and creates post-validation log file.  
# Function Return – This Function return set of validation function results either passed or failed Based on Rule id Passed to the Function.  
# Arguments No of Argumenets expected 1 
# Arg1 ( Config_File_URL ) 
# Post_Validation_Wrapper(Config_File_URL) 
# -------------------------------------------------------------------------------------------------------------------------------------- #


def Post_Validation_Wrapper(Config_File_URL):
    
    try:
        
        #tenant_id = dbutils.secrets.get("databrickscope","tenantid")
        #client_id = dbutils.secrets.get("databrickscope", "clientid")
        #client_secret = dbutils.secrets.get("databrickscope", "clientsecret")
        tenant_id = '36da45f1-dd2c-4d1f-af13-5abe46b99921'
        client_id = 'f2be9926-39c9-4fa1-930b-abfdf97a8178'
        client_secret = 'T1i8Q~NcCuq6o90Q_S643znPSKOb-gKWj-KwbcDy'
        storage_account_name = "adlsrawtoadlscurated"
        
        current_time = datetime.now()

        # Configuring Storage Account with OAuth
        spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
        log = f"{current_time} Configuring Storage Account with OAuth"
        logs_info.append(log)
        
        # Configuring Storage Account with ClientCredsTokenProvider
        spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        log = f"{current_time} Configuring Storage Account with ClientCredsTokenProvider"
        logs_info.append(log)
        
        # Configuring Storage Account using client_id
        spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
        log = f"{current_time} Configuring Storage Account using client_id"
        logs_info.append(log)
        
        # Configuring Storage Account using client_secret
        spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
        log = f"{current_time} Configuring Storage Account using client_secret"
        logs_info.append(log)
        
        # Configuring Storage Account using tenant_id
        spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
        log = f"{current_time} Configuring Storage Account using tenant_id"
        logs_info.append(log)
        
        log = f"{current_time} Created Connection with Storage Account with Service Principles"
        logs_info.append(log)
        
        # Created Variable service_principle_credentials to store tenant_id, client_id, client_secret
        service_principle_credentials = {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret}
        log = f"{current_time} Created Variable service_principle_credentials to store tenant_id, client_id, client_secret"
        logs_info.append(log)
        
        Argument_list_df = spark.read.option("header", "true").csv(Config_File_URL)
        Argument_list_pdf = Argument_list_df.toPandas()

        JobId = int(Argument_list_pdf.iloc[0]['JobId'])
        ObjectPath = str(Argument_list_pdf.iloc[0]['ObjectPath'])
        ObjectName = str(Argument_list_pdf.iloc[0]['ObjectName'])
        BalanceAmountColumn = str(Argument_list_pdf.iloc[0]['BalanceAmountColumn'])
        ObjectType = str(Argument_list_pdf.iloc[0]['ObjectType'])
        ObjectPath_PostProcessing = str(Argument_list_pdf.iloc[0]['ObjectPath_PostProcessing'])
        server_name = str(Argument_list_pdf.iloc[0]['server_name'])
        database_name = str(Argument_list_pdf.iloc[0]['database_name'])
        FileType = str(Argument_list_pdf.iloc[0]['FileType'])
        storage_account_name = str(Argument_list_pdf.iloc[0]['storage_account_name'])
        scope = str(Argument_list_pdf.iloc[0]['scope'])
        sqlusername = str(Argument_list_pdf.iloc[0]['sqlusername'])
        sqlpassword = str(Argument_list_pdf.iloc[0]['sqlpassword'])
        log_path = str(Argument_list_pdf.iloc[0]['log_path'])
        Metadata_column = int(Argument_list_pdf.iloc[0]['Metadata_column'])
        #client_id = str(Argument_list_pdf.iloc[0]['client_id'])
        #client_secret = str(Argument_list_pdf.iloc[0]['client_secret'])
        Config_file_url = str(Argument_list_pdf.iloc[0]['Config_file_url'])
        Metadata_place = str(Argument_list_pdf.iloc[0]['Metadata_place'])
        Manifest_file_url = str(Argument_list_pdf.iloc[0]['Manifest_file_url'])
        HeadCount_value_start_index = int(Argument_list_pdf.iloc[0]['HeadCount_value_start_index'])
        HeadCount_value_end_index = int(Argument_list_pdf.iloc[0]['HeadCount_value_end_index'])
        BalanceAmount_value_start_index = int(Argument_list_pdf.iloc[0]['BalanceAmount_value_start_index'])
        BalanceAmount_value_end_index = int(Argument_list_pdf.iloc[0]['BalanceAmount_value_end_index'])
        service_principle_credentials = {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret}
        Sql_query_balanceamount = str(Argument_list_pdf.iloc[0]['Sql_query_balanceamount'])
        Sql_query_recordcount = str(Argument_list_pdf.iloc[0]['Sql_query_recordcount'])
        Post_Metadata_place = str(Argument_list_pdf.iloc[0]['Post_Metadata_place'])
        Post_FileType = str(Argument_list_pdf.iloc[0]['Post_FileType'])
        
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log = f"{current_time} Consolidating Custom logs in Pandas Dataframe"
        logs_info.append(log)
        
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        log = f"{current_time} Custom logs are Stored in ADLS"
        logs_info.append(log)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message
    
    try:
        current_time = datetime.now()
        #### Creating SQL Query to fetch rule assigned to a particular Job ID ####
        #Querydata = 'Select * From abc.Job_Rule_Assignment Where Job_Id = ' + str(JobId) + " and ValidationType = 'Post'"
        Querydata = Query1 + str(JobId) + Query3

        log = f"{current_time} wrapperFunction_postFile Querydata variable is initialized"
        logs_info.append(log)

        #### Fetching the rule assigned to a particular Job ID for post Object Processing ####

        JobRuleDetails_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata,service_principle_credentials,log_path)

        log = f"{current_time} JobRuleDetails_df Dataframe is created in post file processing"
        logs_info.append(log)

        #### Converting Spark Dataframe to Pandas dataframe ####

        JobRuleDetails_pdf = JobRuleDetails_df.toPandas()
        Output = str('')

        log = f"{current_time} JobRuleDetails_pdf Pandas Dataframe is created in post file processing"
        logs_info.append(log)
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message
    
    # Checking ObjectName or Object Path is Blank and ObjectType Not in SQL Table 
    if (( None == ObjectName or ObjectName == "") or ((None == ObjectPath_PostProcessing or ObjectPath_PostProcessing == "") and (ObjectType != "SQLTable"))) : 
        error_message = "ObjectName or Path is blank"
        
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        
        return "\n Error Message : ObjectName or Path is blank in Post Wrapper Function"
    
    #### Iterating RuleId to trigger a particular rule assigned with the rule id ####
    try:
        for ind in JobRuleDetails_pdf.index:
            if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 8):
                #### Triggering count Validation rule post Object processing ####
                Out8 = CheckJobRule_CountValidation_PostFileProcessing(JobId, ObjectPath_PostProcessing, 
                                                                       ObjectName,server_name,database_name,Post_FileType, sqlusername, 
                                                                       sqlpassword, service_principle_credentials, log_path, Post_Metadata_place, 
                                                                       Manifest_file_url, HeadCount_value_start_index, HeadCount_value_end_index, 
                                                                       Metadata_column, Sql_query_recordcount)
                # Checking if CountValidation_PostFileProcessing return Error comment
                if (Out8.__contains__("Error")):
                    current_time = datetime.now()
                    log = f"{current_time} Error Occured : {Out8} in CountValidation_PostFileProcessing"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    return Out8
                Output = Output + Out8 + " | "

            if ((JobRuleDetails_pdf['Rule_Id'][ind]) == 9):
                #### Triggering Balance Amount Validation rule post Object processing ####
                Out9 = CheckJobRule_BalanceAmountValidation_PostFileProcessing(JobId, ObjectPath_PostProcessing, ObjectName,
                                                                               BalanceAmountColumn, server_name, database_name, 
                                                                               Post_FileType, sqlusername, sqlpassword, service_principle_credentials, 
                                                                               log_path, Post_Metadata_place, Manifest_file_url, 
                                                                               BalanceAmount_value_start_index, BalanceAmount_value_end_index, 
                                                                               Metadata_column,Sql_query_balanceamount)
                # Checking if BalanceAmountValidation_PostFileProcessing return Error
                if (Out9.__contains__("Error")):
                    current_time = datetime.now()
                    log = f"{current_time} Error Occured : {Out9} in BalanceAmountValidation_PostFileProcessing"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    return Out9
                Output = Output + Out9 + " "

        current_time = datetime.now()    
        log = f"{current_time} {Output}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        return Output
    
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message

#------------------------------------ #   
# FunctionName – CheckJobRule_ZeroFileSizeValidation 
# Purpose – To Calculate Source File Size in bytes. validating whether it is zero bytes or not.  If it is zero bytes it will stop the execution, if not it will continue the further validations.  
# Function Return - This Function return Zero File Size Validation Stop or Continue, and it return to main wrapper function 
# Arguments No of Arguments expected 11 
# Arg1  ( JobId )  - Unique Databricks JobId for the Ingestion Pattern 
# Arg2 ( ObjectPath ) - Path from ADLS, NAS Drive 
# Arg3  ( ObjectName ) - File Name 
# Arg4 ( ObjectType ) – File or  NASFile  or SQLTable 
# Arg5 ( server_name ) - Server name, you can Find in Azure SQL   
# Example testsqldb-ss (testsql-ss/testsqldb-ss) is the Database name testsql-ss is a servername 
# Arg6 ( database_name )  – Initial database name, you can Find in Azure SQL  
# Example testsqldb-ss (testsql-ss/testsqldb-ss) is the Database name in that testsqldb-ss is a database name. 
# Arg7 ( FileType ) –  FileType is Format of the File such as csv or json or parquet 
# Arg8 ( sqlusername ) – keyvaults stored sql username value 
# Arg9 ( sqlpassword ) – keyvaults stored  sql password value 
# Arg10 ( service_principle_credentials ) – It consists of {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function. 
# Arg11 (  log_path ) – Path for Logs to be Stored Example ADLS path. 
# Function calling Statement. 
# CheckJobRule_ZeroFileSizeValidation(JobId, ObjectPath, ObjectName, ObjectType, server_name, database_name,  FileType, sqlusername, sqlpassword, service_principle_credentials, log_path) 
#------------------------------------ #
    
def CheckJobRule_ZeroFileSizeValidation(JobId, ObjectPath, ObjectName, ObjectType, server_name, database_name, FileType, sqlusername, sqlpassword, service_principle_credentials, log_path):
    
    try:
        #### Creating RuleID Value ####
        RuleId = int(0)
        HeaderValue = True
        # Declaring function level variables 
        Source_Value_Type = "Source File"
        Target_Value_Type = "Target File"
        if(ObjectType == 'File'):
            Source_Name = "ADLS RAW Layer"
            Target_Name = "ADLS Curated Layer"
            
        if(ObjectType == 'NASFile'):
            Source_Name = "Fileshare File"
            Target_Name = "ADLS RAW Layer"
            
        if(ObjectType == 'SQLTable'):
            Source_Name = "SQL Table"
            Target_Name = "ADL RAW layer"
    
        #### Checking Object Type wheather is it ADLS File, SQL Table or NAS Drive file ####
        if (ObjectType == 'File'):
            try:
                #### Fetching data from Given Object path(Target) for a particular Object ####
                RawLogData_df = spark.read.load(path=ObjectPath, format=f"{FileType}", header = True)
            except Exception as ex:
                current_time = datetime.now()
                error_message = "\n Error Message : " + str(ex)
                log = f"{current_time} Error Occured {error_message}"
                logs_info.append(log)
                # Consolidating Custom logs in Pandas Dataframe
                custom_logs = pd.DataFrame(logs_info, columns = None)
                log_path = log_path.replace("abfss", "abfs") 
                # Custom logs are Stored in ADLS 
                custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                stack_trace = traceback.format_exc()
                print(stack_trace)
                return error_message

        if(ObjectType == 'SQLTable'):
            Querydata = Query1[0:14] + str(ObjectName)
            try:
                RawLogData_df = commonSQLFunction(server_name, database_name, sqlusername, sqlpassword, Querydata, service_principle_credentials, 
                                                  log_path)
            except Exception as ex:
                current_time = datetime.now()
                error_message = "\n Error Message : " + str(ex)
                log = f"{current_time} Error Occured {error_message}"
                logs_info.append(log)
                # Consolidating Custom logs in Pandas Dataframe
                custom_logs = pd.DataFrame(logs_info, columns = None)
                log_path = log_path.replace("abfss", "abfs") 
                # Custom logs are Stored in ADLS 
                custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                stack_trace = traceback.format_exc()
                print(stack_trace)
                return error_message

        if(ObjectType == 'NASFile'):
            if(FileType == 'json'):
                try:
                    #### Fetching data from Given Object path(Target) for a particular Object ####
                    file = pd.read_json(ObjectPath, lines=True)
                    RawLogData_df = spark.createDataFrame(file)
                except Exception as ex:
                    current_time = datetime.now()
                    error_message = "\n Error Message : " + str(ex)
                    log = f"{current_time} Error Occured {error_message}"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS 
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    stack_trace = traceback.format_exc()
                    print(stack_trace)
                    return error_message
                

            if(FileType == 'csv'):
                try:
                    #### Fetching data from Given Object path(Target) for a particular Object ####
                    file = pd.read_csv(ObjectPath)
                    RawLogData_df = spark.createDataFrame(file)
                except Exception as ex:
                    current_time = datetime.now()
                    error_message = "\n Error Message : " + str(ex)
                    log = f"{current_time} Error Occured {error_message}"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS 
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    stack_trace = traceback.format_exc()
                    print(stack_trace)
                    return error_message
                
        
        num_partitions = RawLogData_df.rdd.getNumPartitions()

        # Get the default parallelism level of the Spark cluster
        default_parallelism = spark.sparkContext.defaultParallelism

        # Calculate an estimate of the DataFrame's size in memory
        RawLogData_df_size = RawLogData_df.rdd.map(lambda x: len(str(x))).sum() * num_partitions / default_parallelism

        df_size = int(RawLogData_df_size)
        
        SourceValue = str(df_size)
        TargetValue = str(df_size)
        #### Checking the condition and storing the result ####
        if df_size == 0:
            RuleStatus = 'Failed'
            dbutils.notebook.exit()
        else:
            RuleStatus = 'Passed'
        
        Created_Time = str(datetime.now())
        New_date = Created_Time[0:23]
        
        # Calling commonInsertFunction to insert validation rule result into SQL ABC table
        commonInsertFunction(server_name, database_name, sqlusername, sqlpassword, TableName, JobId, RuleId, 
                             str(SourceValue), str(TargetValue), Source_Value_Type, Target_Value_Type, Source_Name, 
                             Target_Name, RuleStatus, New_date, Username, service_principle_credentials, log_path)

        #### Checking the Status of rule result and storing the function return value ####
        if (RuleStatus == 'Passed'):
            out = 'Zero FileSize Validation Passed'
        else:
            out = 'Zero FileSize Validation Failed'

        #### Returning the validation rule result to the child notebook  ####
        
        log = f"{Created_Time} {out}"
        logs_info.append(log)
        del RawLogData_df
        return out
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message
    
    
# ------------------------------------------------------------------------------------------------------------------------------------ #
# FunctionName – CheckJobRule_CountValidation 
# Purpose - Fetching the record count from Audit Table by stabilizing the connection with Azure SQL server for a particular object as Source Count and then fetch the record count value for the same object from ADLS File which we copied from different sources like NAS , ADLS, SQL Server and then then comparing the Source value and target value and storing the comparison result in string and returning the string as function output to the main wrapper function.  
# Function Return - This Function return record count validation Passed or Failed, and it return to main wrapper function  
# Arguments No of Arguments expected 16 
# Arg1  ( JobId )   -  Unique Databricks JobId for the Ingestion Pattern 
# Arg2 ( ObjectPath )  - Path from ADLS,  NAS Drive   
# Arg3  ( ObjectName ) – File Name  
# Arg4 ( ObjectType ) – File or  NASFile  or SQLTable  
# Arg5 ( server_name )  - Server name, You can Find in  Azure SQL   
# Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name testsql-ss is a servername  
# Arg6 ( database_name ) – Initial database name, You can Find in  Azure SQL  
# Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name in that testsqldb-ss is a database name.  
# Arg7 ( FileType ) –  FileType is Format of the File such as csv or json or parquet 
# Arg8 ( sqlusername ) – keyvaults stored sql username value 
# Arg9 (  sqlpassword ) – keyvaults stored  sql password value 
# Arg10 ( service_principle_credentials ) – It consist {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function.  
# Arg11 ( log_path ) – Path For Logs to be Stored Example ADLS path.  
# Arg12 ( AuditTable_df ) – It is Audit Table Dataframe which is calculated and  fetched inside the wrapper function.  
# Arg13 ( Metadata_place ) – Header  or Trailer  Which indicates the Metadata information.  
# Arg14 ( Manifest_file_url ) - Manifest file location  Which is a path of the ADLS.  
# Arg15 ( HeadCount_value_start_index ) – Head Count Start Index Value. Integer value is passed. 
# Arg16 ( HeadCount ) – Headcount Integer value is Passed. 
# Function calling Statement. 
# CheckJobRule_CountValidation(JobId, ObjectPath, ObjectName,ObjectType,server_name,database_name,FileType, sqlusername,sqlpassword, service_principle_credentials, log_path, AuditTable_df, Metadata_place, Manifest_file_url, HeadCount_value_start_index, HeadCount_value_end_index) 
# ------------------------------------------------------------------------------------------------------------------------------------ #

def CheckJobRule_CountValidation(JobId, ObjectPath, ObjectName, ObjectType,server_name,database_name,FileType, sqlusername, sqlpassword, 
                                 service_principle_credentials, log_path, AuditTable_df, Metadata_place, Manifest_file_url, 
                                 HeadCount_value_start_index, HeadCount_value_end_index):
    
    try:

        #### Creating RuleID Value ####
        RuleId = int(1)
        SourceCount = "Source Count"
        HeaderValue = True
        # Declaring function level variables 
        Source_Value_Type = "Source Count"
        Target_Value_Type = "Target Count"
        if(ObjectType == 'File'):
            Source_Name = "Audit Table"
            Target_Name = "ADLS Raw Layer"
            
        if(ObjectType == 'NASFile'):
            Source_Name = "Audit Table"
            Target_Name = "NAS Drive"
            
        if(ObjectType == 'SQLTable'):
            Source_Name = "Audit Table"
            Target_Name = "Azure SQL Server"
            
        if(Metadata_place == 'File'):
            HeaderValue = True
            #### Reading Manifest file from ADLS ####
            Manifestfile_df = spark.read.text(Manifest_file_url)
            #### Fetching Manifestfile_MetaString from ManifestFile ####
            Manifestfile_MetaString = Manifestfile_df.collect()[0][0]
            #### Fetching Source Count Value from Manifestfile_MetaString ####
            SourceValue = int(Manifestfile_MetaString[HeadCount_value_start_index:HeadCount_value_end_index])
        if(Metadata_place == 'Header'):
            HeaderValue = False
            Data_df = spark.read.load(path=ObjectPath, format=f"{FileType}")
            #BalanceColName = "_c"+str(Metadata_column)
            Data_pdf = Data_df.toPandas()
            MetadataString = Data_pdf.iloc[0,-1]
            SourceValue = int(MetadataString[HeadCount_value_start_index:HeadCount_value_end_index])
            
        if(Metadata_place == 'Footer'):
            HeaderValue = True
            Data_df = spark.read.load(path=ObjectPath, format=f"{FileType}", header = True)
            Data_pdf = Data_df.toPandas()
            MetadataString = Data_pdf.iloc[-1,-1]
            SourceValue = int(MetadataString[HeadCount_value_start_index:HeadCount_value_end_index])

        #### Fetching data from Audit Table(Source) for a particular Object ####
        #AuditTableFileCount_df = AuditTable_df.filter(AuditTable_df.File_Name == ObjectName)

        #### Converting Spark Dataframe to Pandas dataframe ####
        #AuditTableFileCount_pdf = AuditTableFileCount_df.toPandas()

        #### Fetching Source Count for a particular Object ####
        #SourceValue = int(AuditTableFileCount_pdf['File_Count'])

        #### Checking Object Type wheather is it ADLS File, SQL Table or NAS Drive file ####
        if (ObjectType == 'File'):
            try:
                ## Reading file from filepath and name variable
                RawLogData_df = spark.read.load(path=ObjectPath, format=f"{FileType}", header = HeaderValue)
                
            except Exception as ex:
                current_time = datetime.now()
                error_message = "\n Error Message : " + str(ex)
                log = f"{current_time} Error Occured {error_message}"
                logs_info.append(log)
                # Consolidating Custom logs in Pandas Dataframe
                custom_logs = pd.DataFrame(logs_info, columns = None)
                log_path = log_path.replace("abfss", "abfs") 
                # Custom logs are Stored in ADLS 
                custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                stack_trace = traceback.format_exc()
                print(stack_trace)
                return error_message

        if (ObjectType == 'SQLTable'):
            #### Creating SQL Query to fetch data for a particular Object ####
            Querydata = Query1[0:14] + str(ObjectName)
            try:       
                RawLogData_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata,service_principle_credentials, 
                                                  log_path)
            except Exception as ex:
                current_time = datetime.now()
                error_message = "\n Error Message : " + str(ex)
                log = f"{current_time} Error Occured {error_message}"
                logs_info.append(log)
                # Consolidating Custom logs in Pandas Dataframe
                custom_logs = pd.DataFrame(logs_info, columns = None)
                log_path = log_path.replace("abfss", "abfs") 
                # Custom logs are Stored in ADLS 
                custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                stack_trace = traceback.format_exc()
                print(stack_trace)
                return error_message

        if(ObjectType == 'NASFile'):
            if(FileType == 'json'):
                try:
                #### Fetching data from Given Object path(Target) for a particular Object ####
                    file = pd.read_json(ObjectPath, lines=True)
                except Exception as ex:
                    current_time = datetime.now()
                    error_message = "\n Error Message : " + str(ex)
                    log = f"{current_time} Error Occured {error_message}"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS 
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    stack_trace = traceback.format_exc()
                    print(stack_trace)
                    return error_message
                    
                #### Creating Spark dataframe ####
                RawLogData_df = spark.createDataFrame(file)
            if(FileType == 'csv'):
                try:
                    #### Fetching data from Given Object path(Target) for a particular Object ####
                    file = pd.read_csv(ObjectPath)
                except Exception as ex:
                    current_time = datetime.now()
                    error_message = "\n Error Message : " + str(ex)
                    log = f"{current_time} Error Occured {error_message}"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS 
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    stack_trace = traceback.format_exc()
                    print(stack_trace)
                    return error_message
                    
                #### Creating Spark dataframe ####
                RawLogData_df = spark.createDataFrame(file)
        #### Fetching Target Count for a particular Object ####
        TargetValue = int(RawLogData_df.count())


        #### Checking the condition and storing the Validation rule result ####
        if (SourceValue == TargetValue):
            RuleStatus = 'Passed'
        else:
            RuleStatus = 'Failed'

        Created_Time = str(datetime.now())
        New_date = Created_Time[0:23]
        
        # Calling commonInsertFunction to insert validation rule result into SQL ABC table
        commonInsertFunction(server_name, database_name, sqlusername, sqlpassword, TableName, JobId, RuleId, 
                             str(SourceValue), str(TargetValue), Source_Value_Type, Target_Value_Type, Source_Name, 
                             Target_Name, RuleStatus, New_date, Username, service_principle_credentials, log_path)

        #### Checking the Status of rule result and storing the function return value ####
        if (RuleStatus == 'Passed'):
            out = str('CountValidation passed')
        else:
            out = str('CountValidation Failed')

        #### Returning the validation rule result to the child notebook  ####
        
        log = f"{Created_Time} {out}"
        logs_info.append(log)
        del RawLogData_df
        return out
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message
    
    ################################
    
# ------------------------------------------------------------------------------------------------------------------------------------ #
# Function - CheckJobRule_BalanceAmountValidation 
# Purpose - Fetching the Balance amount column value from Audit Table by stablizing the connection with Azure SQL server for a particular object as Source Value and then fetch the whole data for the same object from ADLS File or NAS drive File or SQL Table based on Object type and then storing it in spark data frame and calculating the balance amount for particular column as target value then comparing the Source value and target value and storing the comparison result in string and returning the string as function output to the main wrapper function. 
# Function Return - This Function return Balance Amount validation Passed or Failed and it return to main wrapper function  
# Arguments No of Arguments expected 18 
# Arg1  ( JobId ) -  Unique Databricks JobId for the Ingestion Pattern 
# Arg2 (  ObjectPath ) - Path from ADLS,  NAS Drive 
# Arg3  (  ObjectName ) - File Name 
# Arg4 (  BalanceAmountColumn ) – It indicate column name of Balance Amount  
# Arg5 (  ObjectType ) - File or  NASFile  or SQLTable 
# Arg6 (  server_name ) - Server name, You can Find in  Azure SQL   
# Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name testsql-ss is a servername 
# Arg7 (  database_name ) – Initial database name, You can Find in  Azure SQL  
# Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name in that testsqldb-ss is a database name. 
# Arg8 (  FileType )  –  FileType is Format of the File such as csv or json or parquet 
# Arg9 (  sqlusername ) – keyvaults stored sql username value 
# Arg10 (  sqlpassword ) – keyvaults stored  sql password value 
# Arg11 (  service_principle_credentials ) – It consist {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function. 
# Arg12 (  log_path ) – Path For Logs to be Stored Example ADLS path. 
# Arg13 (  AuditTable_df ) – It is Audit Table Dataframe which is calculated and  fetched inside the wrapper function. 
# Arg14 (  Metadata_place ) – Header  or Trailer  Which indicates the Metadata information. 
# Arg15 (  Manifest_file_url ) -  Manifest file location  Which is a path of the ADLS. 
# Arg16 (  BalanceAmount_value_start_index ) – Balance Amount Start Index value. Integer value is passed 
# Arg17 (  BalanceAmount_value_end_index ) – Balance Amount End index value. Integer value is passed 
# Arg18 (  Metadata_column ) – Metadata Column Integer value is passed  
# Function calling Statement. 
# CheckJobRule_BalanceAmountValidation(JobId, ObjectPath, ObjectName, BalanceAmountColumn, ObjectType, server_name, database_name, FileType, sqlusername, sqlpassword, service_principle_credentials, log_path, AuditTable_df, Metadata_place, Manifest_file_url, BalanceAmount_value_start_index, BalanceAmount_value_end_index, Metadata_column) 
# ----------------------------------------------------------------------------------------------------------------------------------- #

def CheckJobRule_BalanceAmountValidation(JobId, ObjectPath, ObjectName, BalanceAmountColumn, 
                                         ObjectType, server_name, database_name, FileType, sqlusername, sqlpassword, service_principle_credentials, 
                                         log_path, AuditTable_df, Metadata_place, Manifest_file_url, BalanceAmount_value_start_index, 
                                         BalanceAmount_value_end_index, Metadata_column):

    try:

        #### Creating RuleID Value ####
        RuleId = int(2)
        HeaderValue = True
        
        # Declaring function level variables
        Source_Value_Type = "Source Balance Amount"
        Target_Value_Type = "Target Balance Amount"
        if(ObjectType == 'File'):
            Source_Name = "Audit Table"
            Target_Name = "ADLS Raw Layer"
            
        if(ObjectType == 'NASFile'):
            Source_Name = "Audit Table"
            Target_Name = "NAS Drive"
            
        if(ObjectType == 'SQLTable'):
            Source_Name = "Audit Table"
            Target_Name = "Azure SQL Server"
            
            
        
        if(Metadata_place == 'File'):
            HeaderValue = True
            #### Reading Manifest file from ADLS ####
            Manifestfile_df = spark.read.text(Manifest_file_url)
            #### Fetching Manifestfile_MetaString from ManifestFile ####
            Manifestfile_MetaString = Manifestfile_df.collect()[0][0]
            #### Fetching Source Balanceamount Value from Manifestfile_MetaString ####
            SourceValue = int(Manifestfile_MetaString[BalanceAmount_value_start_index:BalanceAmount_value_end_index])
        if(Metadata_place == 'Header'):
            HeaderValue = False
            Data_df = spark.read.load(path=ObjectPath, format=f"{FileType}", header = False)
            Metadata_column = Metadata_column - 1
            BalanceColName = "_c"+str(Metadata_column)
            Data_df = Data_df.withColumnRenamed(BalanceColName, BalanceAmountColumn)
            Data_pdf = Data_df.toPandas()
            MetadataString = Data_pdf.iloc[0,-1]
            SourceValue = int(MetadataString[BalanceAmount_value_start_index:BalanceAmount_value_end_index])
            #SourceValue = 358016
        if(Metadata_place == 'Footer'):
            HeaderValue = True
            Data_df = spark.read.load(path=ObjectPath, format=f"{FileType}", header = True)
            Data_pdf = Data_df.toPandas()
            MetadataString = Data_pdf.iloc[-1,-1]
            SourceValue = int(MetadataString[BalanceAmount_value_start_index:BalanceAmount_value_end_index])
            #SourceValue = 358016
        
            
        #Querydata = "Select * from abc.Audit_Log where Product_Name not like 'Post%' "

        #### Fetching all the pre processed Object data from Audit Table(Source) ####
        #AuditTable_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata_1,service_principle_credentials,log_path)

        #### Fetching data from Audit Table(Source) for a particular Object ####
        #AuditTableFileCount_df = AuditTable_df.filter(AuditTable_df.File_Name == ObjectName)

        #### Converting Spark Dataframe to Pandas dataframe ####
        #AuditTableFileCount_pdf = AuditTableFileCount_df.toPandas()

        #### Fetching Source Balance Amount for a particular Object ####
        #SourceValue = int(AuditTableFileCount_pdf['File_Balance_Amount'])
        
        #Querydata = "Select FileName,BalanceAmountColumn from metaabc.File_MetaData"
        
        FileMetaData_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata_2,service_principle_credentials,log_path)

        FileMetaDataFilter_df = FileMetaData_df.filter(FileMetaData_df.FileName == ObjectName)
        BalanceAmountColumn1 = FileMetaDataFilter_df.collect()[0][1]
        #### Checking Object Type whether is it ADLS File, SQL Table or NAS Drive file ####
        if (ObjectType == 'File'):
            #TargetData_df = spark.read.csv(path=ObjectPath + ObjectName, header=True)
            try:
            #### Fetching data from Given Object path(Target) for a particular Object ####
                TargetData_df = spark.read.load(path=ObjectPath, format=f"{FileType}", header = HeaderValue)
                Metadata_column = 8 - 1
                BalanceColName = "_c"+str(Metadata_column)
                TargetData_df = TargetData_df.withColumnRenamed(BalanceColName, BalanceAmountColumn)
            except Exception as ex:
                current_time = datetime.now()
                error_message = "\n Error Message : " + str(ex)
                log = f"{current_time} Error Occured {error_message}"
                logs_info.append(log)
                # Consolidating Custom logs in Pandas Dataframe
                custom_logs = pd.DataFrame(logs_info, columns = None)
                log_path = log_path.replace("abfss", "abfs") 
                # Custom logs are Stored in ADLS 
                custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                return error_message

        if (ObjectType == 'SQLTable'):
            #### Creating SQL Query to fetch data for a particular Object ####
            Querydata = Query1[0:14] + str(ObjectName)
            #### Creating Connection with Azure Sql Server ####
            try:
                TargetData_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata,service_principle_credentials,log_path)
            except Exception as ex:
                current_time = datetime.now()
                error_message = "\n Error Message : " + str(ex)
                log = f"{current_time} Error Occured {error_message}"
                logs_info.append(log)
                # Consolidating Custom logs in Pandas Dataframe
                custom_logs = pd.DataFrame(logs_info, columns = None)
                log_path = log_path.replace("abfss", "abfs") 
                # Custom logs are Stored in ADLS 
                custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                stack_trace = traceback.format_exc()
                print(stack_trace)
                return error_message

        if(ObjectType == 'NASFile'):
            if(FileType == 'json'):
                #### Fetching data from Given Object path(Target) for a particular Object ####
                try:
                    file = pd.read_json(ObjectPath, lines=True)
                except Exception as ex:
                    current_time = datetime.now()
                    error_message = "\n Error Message : " + str(ex)
                    log = f"{current_time} Error Occured {error_message}"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS 
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    stack_trace = traceback.format_exc()
                    print(stack_trace)
                    return error_message
                #### Creating Spark dataframe ####
                TargetData_df = spark.createDataFrame(file)

            if(FileType == 'csv'):
                #### Fetching data from Given Object path(Target) for a particular Object ####
                try:
                    file = pd.read_csv(ObjectPath)
                except Exception as ex:
                    current_time = datetime.now()
                    error_message = "\n Error Message : " + str(ex)
                    log = f"{current_time} Error Occured {error_message}"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS 
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    stack_trace = traceback.format_exc()
                    print(stack_trace)
                    return error_message
                #### Creating Spark dataframe ####
                TargetData_df = spark.createDataFrame(file)
                    
        #### Fetching Target Balance Amount for a particular Object ####
        BalanceAmount = TargetData_df.agg({BalanceAmountColumn: 'sum'})
        for col in BalanceAmount.dtypes:
            TargetValue = int(BalanceAmount.first()[col[0]])

        #### Checking the condition and storing the result ####
        if (SourceValue == TargetValue):
            RuleStatus = 'Passed'
        else:
            RuleStatus = 'Failed'
            
        Created_Time = str(datetime.now())
        New_date = Created_Time[0:23]
        
        # Calling commonInsertFunction to insert validation rule result into SQL ABC table
        commonInsertFunction(server_name, database_name, sqlusername, sqlpassword, TableName, JobId, RuleId, 
                             str(SourceValue), str(TargetValue), Source_Value_Type, Target_Value_Type, Source_Name, 
                             Target_Name, RuleStatus, New_date, Username, service_principle_credentials, log_path)
        
        #### Checking the Status of rule result and storing the function return value ####
        if (RuleStatus == 'Passed'):
            out = str('BalanceAmountValidation passed')
        else:
            out = str('BalanceAmountValidation Failed')

        #### Returning the validation rule result to the child notebook  ####
        
        log = f"{Created_Time} {out}"
        logs_info.append(log)
        del FileMetaData_df
        del TargetData_df
        return out
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message

# ------------------------------------------------------------------------------------------------------------------------------------- #
# Function - CheckJobRule_ThresholdValidation 
# Purpose - Fetching the record count value from Audit Table by stablizing the connecting with Azure SQL server for a particular object as Source Value and then fetch the minimum and maximum Threshold record count value for the same object from File metadata azure sql table as target range value and then then comparing the Source value and target value and storing the comparison result in string and returning the string as function output to the main wrapper function. 
# Function Return - This Functin return Threshold  validation Passed or Failed and it return to main wrapper function  
# Arguments No of Arguments expected 11 
# Arg1  ( JobId )  - Unique Databricks JobId for the Ingestion Pattern 
# Arg2 ( ObjectPath ) - Path from ADLS,  NAS Drive 
# Arg3  ( ObjectName ) - File Name 
# Arg4 ( ObjectType ) –  File or  NASFile  or SQLTable 
# Arg5 ( server_name ) - Server name, You can Find in  Azure SQL   
# Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name testsql-ss is a servername 
# Arg6 ( database_name )  – Initial database name, You can Find in  Azure SQL  
# Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name in that testsqldb-ss is a database name. 
# Arg7 ( FileType )  –  FileType is Format of the File such as csv or json or parquet 
# Arg8 ( sqlusername )  – keyvaults stored sql username value 
# Arg9 ( sqlpassword )  – keyvaults stored  sql password value 
# Arg10 ( service_principle_credentials ) – It consist {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function. 
# Arg11 (  log_path ) – Path For Logs to be Stored Example ADLS path. 
# Function calling Statement. 
# CheckJobRule_ThresholdValidation(JobId, ObjectPath, ObjectName, ObjectType, server_name,database_name,FileType, sqlusername, sqlpassword, service_principle_credentials, log_path) 
# ------------------------------------------------------------------------------------------------------------------------------------- #


def CheckJobRule_ThresholdValidation(JobId, ObjectPath, ObjectName, ObjectType,server_name,database_name,FileType,  sqlusername, sqlpassword, service_principle_credentials, log_path):
    
    try:
        #### Creating RuleID Value ####
        RuleId = int(3)
        
        # Declaring function level variables
        Source_Value_Type = "Source Threshold Value"
        Target_Value_Type = "Target Object Count"
        if(ObjectType == 'File'):
            Source_Name = "Audit Table"
            Target_Name = "ADLS Raw Layer"
            
        if(ObjectType == 'NASFile'):
            Source_Name = "Audit Table"
            Target_Name = "NAS Drive"
            
        if(ObjectType == 'SQLTable'):
            Source_Name = "Audit Table"
            Target_Name = "Azure SQL Server"

        #Querydata = "Select * from metaabc.File_MetaData"
        #### Fetching all the pre processed Object data from FileMetadata Table(Source) ####
        FileMetadata_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata_3,service_principle_credentials,log_path)

        #### Fetching data from Audit Table(Source) for a particular Object ####
        FileMetadata_df = FileMetadata_df.filter(FileMetadata_df.FileName == ObjectName)

        #### Converting Spark Dataframe to Pandas dataframe ####
        FileMetadata_pdf = FileMetadata_df.toPandas()

        #### Fetching Minimum and maximum threshold values ####
        SourceValue_min = FileMetadata_pdf.loc[0]['MinValue']
        SourceValue_max = FileMetadata_pdf.loc[0]['MaxValue']

        #### Creating the Source Value by using minimum and maximum value ####
        SourceValue = 'MinValue = ' + str(SourceValue_min) + ' and MaxValue = ' + str(SourceValue_max)

        #### Checking Object Type wheather is it ADLS File, SQL Table or NAS Drive file ####
        if (ObjectType == 'File'):
            try:
                #### Fetching data from Given Object path(Target) for a particular Object ####
                RawLogData_df = spark.read.load(path=ObjectPath, format=f"{FileType}", header = True)
            except Exception as ex:
                current_time = datetime.now()
                error_message = "\n Error Message : " + str(ex)
                log = f"{current_time} Error Occured {error_message}"
                logs_info.append(log)
                # Consolidating Custom logs in Pandas Dataframe
                custom_logs = pd.DataFrame(logs_info, columns = None)
                log_path = log_path.replace("abfss", "abfs") 
                # Custom logs are Stored in ADLS 
                custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                stack_trace = traceback.format_exc()
                print(stack_trace)
                return error_message

        if(ObjectType == 'SQLTable'):
            Querydata = Query1[0:14] + str(ObjectName)
            try:
                RawLogData_df = commonSQLFunction(server_name, database_name, sqlusername, sqlpassword, Querydata, service_principle_credentials, 
                                                  log_path)
            except Exception as ex:
                current_time = datetime.now()
                error_message = "\n Error Message : " + str(ex)
                log = f"{current_time} Error Occured {error_message}"
                logs_info.append(log)
                # Consolidating Custom logs in Pandas Dataframe
                custom_logs = pd.DataFrame(logs_info, columns = None)
                log_path = log_path.replace("abfss", "abfs") 
                # Custom logs are Stored in ADLS 
                custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                stack_trace = traceback.format_exc()
                print(stack_trace)
                return error_message

        if(ObjectType == 'NASFile'):
            if(FileType == 'json'):
                try:
                    #### Fetching data from Given Object path(Target) for a particular Object ####
                    file = pd.read_json(ObjectPath, lines=True)
                except Exception as ex:
                    current_time = datetime.now()
                    error_message = "\n Error Message : " + str(ex)
                    log = f"{current_time} Error Occured {error_message}"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS 
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    stack_trace = traceback.format_exc()
                    print(stack_trace)
                    return error_message
                RawLogData_df = spark.createDataFrame(file)

            if(FileType == 'csv'):
                try:
                    #### Fetching data from Given Object path(Target) for a particular Object ####
                    file = pd.read_csv(ObjectPath)
                except Exception as ex:
                    current_time = datetime.now()
                    error_message = "\n Error Message : " + str(ex)
                    log = f"{current_time} Error Occured {error_message}"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS 
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    stack_trace = traceback.format_exc()
                    print(stack_trace)
                    return error_message
                RawLogData_df = spark.createDataFrame(file)
        
        #### Fetching Target Value for a particular Object ####
        TargetValue = int(RawLogData_df.count())

        #### Checking the condition and storing the result ####
        if (TargetValue >= SourceValue_min and TargetValue <= SourceValue_max):
            RuleStatus = 'Passed'
        else:
            RuleStatus = 'Failed'
        
        Created_Time = str(datetime.now())
        New_date = Created_Time[0:23]
        
        # Calling commonInsertFunction to insert validation rule result into SQL ABC table
        commonInsertFunction(server_name, database_name, sqlusername, sqlpassword, TableName, JobId, RuleId, 
                             str(SourceValue), str(TargetValue), Source_Value_Type, Target_Value_Type, Source_Name, 
                             Target_Name, RuleStatus, New_date, Username, service_principle_credentials, log_path)

        #### Checking the Status of rule result and storing the function return value ####
        if (RuleStatus == 'Passed'):
            out = 'ThresholdValidation passed'
        else:
            out = 'ThresholdValidation Failed'

        #### Returning the validation rule result to the child notebook  ####
        
        log = f"{Created_Time} {out}"
        logs_info.append(log)
        del RawLogData_df
        return out
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message

# ------------------------------------------------------------------------------------------------------------------------------------- #
# FunctionName - CheckJobRule_FileNameValidation 
# Purpose - Fetching the Object Name value from Audit Table by stabilizing the connecting with Azure SQL server for a particular object as Source Value and then checking is there any object with the same name existing in audit table and storing the result in string and returning the string as function output to the main wrapper function. 
# Function Return - This Functin return FileName validation Passed or Failed and it return to main wrapper function  
# Arguments - No of Argumenets expected 11 
# Arg1  ( JobId )  - Unique Databricks JobId for the Ingestion Pattern 
# Arg2 ( ObjectPath ) - Path from ADLS,  NAS Drive 
# Arg3  ( ObjectName ) - File Name 
# Arg4 ( server_name ) - Server name, You can Find in  Azure SQL   
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name testsql-ss is a servername 
# Arg5 ( database_name )  – Initial database name, You can Find in  Azure SQL  
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name in that testsqldb-ss is a database name. 
# Arg6 ( ObjectType ) –  File or  NASFile  or SQLTable 
# Arg7 ( sqlusername )  – keyvaults stored sql username value 
# Arg8 ( sqlpassword )  – keyvaults stored  sql password value 
# Arg9 ( service_principle_credentials ) – It consist {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function. 
# Arg10 (  log_path ) – Path For Logs to be Stored Example ADLS path. 
# Arg11 ( AuditTable_df ) – It is Audit Table Dataframe which is calculated and  fetched inside the wrapper function. 
# Function calling Statement. 
# CheckJobRule_FileNameValidation(JobId, ObjectPath, ObjectName, server_name, database_name, ObjectType, sqlusername, sqlpassword, service_principle_credentials, log_path, AuditTable_df) 
# ------------------------------------------------------------------------------------------------------------------------------------- #


def CheckJobRule_FileNameValidation(JobId, ObjectPath, ObjectName,server_name,database_name,ObjectType, sqlusername, sqlpassword, 
                                    service_principle_credentials, log_path, AuditTable_df):
    
    try:
        RuleId = int(4)
        
        # Declaring function level variables
        Source_Value_Type = "Object Name"
        Target_Value_Type = "Object Name"
        if(ObjectType == 'File'):
            Source_Name = "Audit Table"
            Target_Name = "ADLS Raw Layer"
            
        if(ObjectType == 'NASFile'):
            Source_Name = "Audit Table"
            Target_Name = "NAS Drive"
            
        if(ObjectType == 'SQLTable'):
            Source_Name = "Audit Table"
            Target_Name = "Azure SQL Server"

        #### Fetching Object Data ####
        #Querydata = "Select * from abc.Audit_Log  where Product_Name not like 'Post%' "
        #AuditTable_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata_1,service_principle_credentials,log_path)
        #### Checking Validation Condition ####
        AuditTable_df = int(AuditTable_df.filter(AuditTable_df.File_Name == ObjectName).count())

        #### Checking the condition and storing the result ####
        if (AuditTable_df > 0):
            RuleStatus = 'Passed'
        else:
            RuleStatus = 'Failed'

        Created_Time = str(datetime.now())
        New_date = Created_Time[0:23]
        
        # Calling commonInsertFunction to insert validation rule result into SQL ABC table
        commonInsertFunction(server_name, database_name, sqlusername, sqlpassword, TableName, JobId, RuleId, 
                             str(ObjectName), str(ObjectName), Source_Value_Type, Target_Value_Type, Source_Name, 
                             Target_Name, RuleStatus, New_date, Username, service_principle_credentials, log_path)

        if (RuleStatus == 'Passed'):
            out = 'FileNameValidation passed'
        else:
            out = 'FileNameValidation Failed'

        #### Returning the validation rule result to the child notebook  ####
        
        log = f"{Created_Time} {out}"
        logs_info.append(log)
        del AuditTable_df
        return out
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message

#-----------------------------------------------------------------------------------------------------------------------------------------#
# FunctionName  - CheckJobRule_DuplicateFileCheck 
# Purpose - Duplicate File check Validate the freshly arrived files against the previous files for that file pattern. It Ensures that the Check sum of new file doesn't match for already present file in the checksum. if matches it consider as Duplicate file and failed the validation, else new record entry will be inserted to the check sum details table. 
# Function Return - This Function return Duplicate File validation Passed or Failed and it return to main wrapper function 
# Arguments - No of Arguments expected 11
# Arg1  ( JobId )  - Unique Databricks JobId for the Ingestion Pattern 
# Arg2 ( ObjectPath ) - Path from ADLS,  NAS Drive 
# Arg3  ( ObjectName ) - File Name 
# Arg4 ( ObjectType ) –  File or  NASFile  or SQLTable 
# Arg5 ( server_name ) - Server name, You can Find in  Azure SQL   
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name testsql-ss is a servername 
# Arg6 ( database_name )  – Initial database name, You can Find in  Azure SQL  
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name in that testsqldb-ss is a database name. 
# Arg7 (  FileType ) – Filetype is Format of the File such as CSV or JSON or PARQUET 
# Arg8 ( sqlusername )  – keyvaults stored sql username value 
# Arg9 ( sqlpassword )  – keyvaults stored  sql password value 
# Arg10 ( service_principle_credentials ) – It consist {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function. 
# Arg11 (  log_path ) – Path For Logs to be Stored Example ADLS path. 
# Function calling Statement 
# CheckJobRule_DuplicateFileCheck(JobId, ObjectPath, ObjectName, ObjectType, server_name, database_name, FileType, sqlusername, sqlpassword, service_principle_credentials, log_path) 
#------------------------------------------------------------------------------------------------------------------------------------------#


def CheckJobRule_DuplicateFileCheck(JobId, ObjectPath, ObjectName, ObjectType, server_name, database_name, FileType, sqlusername, sqlpassword, service_principle_credentials, log_path):
    
    try:

        #### Creating RuleID Value ####
        RuleId = int(11)
        SourceCount = "Source Check Sum"
        HeaderValue = True
        # Declaring function level variables 
        Source_Value_Type = "Source Check Sum"
        Target_Value_Type = "Target Check Sum"
        if(ObjectType == 'File'):
            Source_Name = "File CheckSum Table"
            Target_Name = "ADLS Raw Layer"
            
        if(ObjectType == 'NASFile'):
            Source_Name = "File CheckSum Table"
            Target_Name = "NAS Drive"
            
        if(ObjectType == 'SQLTable'):
            Source_Name = "File CheckSum Table"
            Target_Name = "Azure SQL Server"
        
        #### Fetching data from checksum Table (Source) for a particular Object ####
        Check_sum_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Query4,service_principle_credentials,log_path)
        
        ### Converting into Pandas DataFrame
        Check_sum_df = Check_sum_df.toPandas()
        
        ### Fetching Checksum Column  
        Source_Checksum_Column = Check_sum_df["Checksum_Value"]
        
        #### Checking Object Type wheather is it ADLS File, SQL Table or NAS Drive file ####
        if (ObjectType == 'File'):
            ## Reading file from filepath and name variable
            try:
                RawLogData_df = spark.read.load(path=ObjectPath, format=f"{FileType}", header = True)

                RawLogData_df = RawLogData_df.toPandas()

                RawLogData_df_string = ','.join(RawLogData_df.apply(lambda x: ','.join(x.astype(str)), axis=1))

                RawLogData_df_check_sum = hashlib.sha512(RawLogData_df_string.encode()).hexdigest()
				
            except Exception as ex:
                current_time = datetime.now()
                error_message = "\n Error Message : " + str(ex)
                log = f"{current_time} Error Occured {error_message}"
                logs_info.append(log)
                # Consolidating Custom logs in Pandas Dataframe
                custom_logs = pd.DataFrame(logs_info, columns = None)
                log_path = log_path.replace("abfss", "abfs") 
                # Custom logs are Stored in ADLS 
                custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                stack_trace = traceback.format_exc()
                print(stack_trace)
                return error_message

        if (ObjectType == 'SQLTable'):
            
            #### Creating SQL Query to fetch data for a particular Object ####
            
            try:
                Querydata = Query1[0:14] + str(ObjectName)
                RawLogData_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata,service_principle_credentials,log_path)
                RawLogData_df = RawLogData_df.toPandas()
                RawLogData_df_string = ','.join(RawLogData_df.apply(lambda x: ','.join(x.astype(str)), axis=1))
                RawLogData_df_check_sum = hashlib.sha512(RawLogData_df_string.encode()).hexdigest()
				
            except Exception as ex:
                current_time = datetime.now()
                error_message = "\n Error Message : " + str(ex)
                log = f"{current_time} Error Occured {error_message}"
                logs_info.append(log)
                # Consolidating Custom logs in Pandas Dataframe
                custom_logs = pd.DataFrame(logs_info, columns = None)
                log_path = log_path.replace("abfss", "abfs") 
                # Custom logs are Stored in ADLS 
                custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                stack_trace = traceback.format_exc()
                print(stack_trace)
                return error_message

        if(ObjectType == 'NASFile'):
            if(FileType == 'json'):
                try:
                #### Fetching data from Given Object path(Target) for a particular Object ####
                    file = pd.read_json(ObjectPath, lines=True)
                    
                    RawLogData_df = spark.createDataFrame(file)
                    RawLogData_df = RawLogData_df.toPandas()
                    RawLogData_df_string = ','.join(RawLogData_df.apply(lambda x: ','.join(x.astype(str)), axis=1))
                    RawLogData_df_check_sum = hashlib.sha512(RawLogData_df_string.encode()).hexdigest()
                except Exception as ex:
                    current_time = datetime.now()
                    error_message = "\n Error Message : " + str(ex)
                    log = f"{current_time} Error Occured {error_message}"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS 
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    stack_trace = traceback.format_exc()
                    print(stack_trace)
                    return error_message
                    
                
            if(FileType == 'csv'):
                try:
                    #### Fetching data from Given Object path(Target) for a particular Object ####
                    file = pd.read_csv(ObjectPath)
                    
                    RawLogData_df = spark.createDataFrame(file)
                    
                    RawLogData_df = RawLogData_df.toPandas()
                    RawLogData_df_string = ','.join(RawLogData_df.apply(lambda x: ','.join(x.astype(str)), axis=1))
                    RawLogData_df_check_sum = hashlib.sha512(RawLogData_df_string.encode()).hexdigest()
                except Exception as ex:
                    current_time = datetime.now()
                    error_message = "\n Error Message : " + str(ex)
                    log = f"{current_time} Error Occured {error_message}"
                    logs_info.append(log)
                    # Consolidating Custom logs in Pandas Dataframe
                    custom_logs = pd.DataFrame(logs_info, columns = None)
                    log_path = log_path.replace("abfss", "abfs") 
                    # Custom logs are Stored in ADLS 
                    custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
                    stack_trace = traceback.format_exc()
                    print(stack_trace)
                    return error_message
                    
               
        #### Fetching Target Count for a particular Object ####
        
        Target_check_sum = RawLogData_df_check_sum
        
        TargetValue = f"Check_sum = {RawLogData_df_check_sum}" 
        
        #print(Target_check_sum)
        
        for each_check_sum_value in Source_Checksum_Column:
            
            #### Checking the condition and storing the Validation rule result ####
            if (each_check_sum_value == Target_check_sum):
                RuleStatus = 'Passed'
                SourceValue = f"Check_sum = {each_check_sum_value}" 
                break
            else:
                RuleStatus = 'Failed'
                SourceValue = f"Check_sum = {each_check_sum_value}" 
        
        #print(RuleStatus)
        
        Created_Time = str(datetime.now())
        New_date = Created_Time[0:23]
        
        # Calling commonInsertFunction to insert validation rule result into SQL ABC table
        commonInsertFunction(server_name, database_name, sqlusername, sqlpassword, TableName, JobId, RuleId, 
                             str(SourceValue), str(TargetValue), Source_Value_Type, Target_Value_Type, Source_Name, 
                             Target_Name, RuleStatus, New_date, Username, service_principle_credentials, log_path)

        #### Checking the Status of rule result and storing the function return value ####
        
        
        if (RuleStatus == 'Passed'):
            out = str("Duplicate_File_Check_Validation Passed")
        else:
            Check_sum_Table = "abc.File_Checksum_details" 
        
            ChecksumInsertFunction(server_name, database_name, sqlusername, sqlpassword, Check_sum_Table, ObjectName, RawLogData_df_check_sum, New_date, Username, service_principle_credentials, log_path)
            
            out = str("New Unique CheckSumid is Created and Inserted to abc.File_Checksum_details Table")

        #### Returning the validation rule result to the child notebook  ####
        
        log = f"{Created_Time} {out}"
        logs_info.append(log)
        #print(out)
        del RawLogData_df
        return out
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message

# ------------------------------------------------------------------------------------------------------------------------------------- #
# FunctionName -  CheckJobRule_FileSizeValidation 
# Purpose - Fetching the Object Size value from Audit Table by stabilizing the connection with Azure SQL server for a particular object as Source Value and then fetch the object size value for the same object from File metadata azure sql table as target value and then then comparing the Source value and target value and storing the comparison result in string and returning the string as function output to the main wrapper function. 
# Function Return - This Function return File Size validation Passed or Failed, and it return to main wrapper function 
# Arguments - No of Arguments expected 11 
# Arg1  ( JobId )  - Unique Databricks JobId for the Ingestion Pattern 
# Arg2 ( ObjectPath ) - Path from ADLS,  NAS Drive 
# Arg3  ( ObjectName ) - File Name 
# Arg4 ( server_name ) - Server name, You can Find in  Azure SQL   
# Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name testsql-ss is a servername 
# Arg5 ( database_name )  – Initial database name, You can Find in  Azure SQL  
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name in that testsqldb-ss is a database name. 
# Arg6 ( ObjectType ) –  File or  NASFile  or SQLTable 
# Arg7 ( sqlusername )  – keyvaults stored sql username value 
# Arg8 ( sqlpassword )  – keyvaults stored  sql password value 
# Arg9 ( service_principle_credentials ) – It consist {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function. 
# Arg10 (  log_path ) – Path For Logs to be Stored Example ADLS path. 
# Arg11 ( AuditTable_df ) – It is Audit Table Dataframe which is calculated and  fetched inside the wrapper function. 
# Function calling Statement. 
# CheckJobRule_FileSizeValidation(JobId, ObjectPath, ObjectName, server_name,database_name, ObjectType, sqlusername, sqlpassword, service_principle_credentials, log_path, AuditTable_df) 
# ------------------------------------------------------------------------------------------------------------------------------------ #

def CheckJobRule_FileSizeValidation(JobId, ObjectPath, ObjectName,server_name,database_name,ObjectType, sqlusername, sqlpassword, 
                                    service_principle_credentials, log_path, AuditTable_df):
    try:
        RuleId = int(5)
        
        # Declaring function level variables
        Source_Value_Type = "Source Object Size"
        Target_Value_Type = "Target Object Size"
        if(ObjectType == 'File'):
            Source_Name = "Audit Table"
            Target_Name = "ADLS Raw Layer"
            
        if(ObjectType == 'NASFile'):
            Source_Name = "Audit Table"
            Target_Name = "NAS Drive"
            
        if(ObjectType == 'SQLTable'):
            Source_Name = "Audit Table"
            Target_Name = "Azure SQL Server"

        #### Fetching all the pre processed Object data from Audit Table(Source) ####
        #Querydata = "Select * from abc.Audit_Log  where Product_Name not like 'Post%' "
        #AuditTable_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata_1,service_principle_credentials,log_path)
        AuditTableFileCount_df = AuditTable_df.filter(AuditTable_df.File_Name == ObjectName)
        AuditTableFileCount_pdf = AuditTableFileCount_df.toPandas()

        #### Fetching Source Value(File Size) for a particular Object ####
        SourceValue = AuditTableFileCount_pdf.loc[0]['File_Size']

        #### Fetching all the pre processed Object data from FileMetadata Table(Target) ####
        #Querydata = "Select * from metaabc.File_MetaData"
        FileMetadata_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata_3,service_principle_credentials,log_path)
        FileMetadata_df = FileMetadata_df.filter(FileMetadata_df.FileName == ObjectName)
        FileMetadata_pdf = FileMetadata_df.toPandas()

        #### Fetching Target Value(File Size) for a particular Object ####
        TargetValue = FileMetadata_pdf.loc[0]['FileSize']

        #### Checking the condition and storing the result ####
        if (SourceValue == TargetValue):
            RuleStatus = 'Passed'
        else:
            RuleStatus = 'Failed'
        
        Created_Time = str(datetime.now())
        New_date = Created_Time[0:23]
        
        # Calling commonInsertFunction to insert validation rule result into SQL ABC table
        commonInsertFunction(server_name, database_name, sqlusername, sqlpassword, TableName, JobId, RuleId, 
                             str(SourceValue), str(TargetValue), Source_Value_Type, Target_Value_Type, Source_Name, 
                             Target_Name, RuleStatus, New_date, Username, service_principle_credentials, log_path)

        if (RuleStatus == 'Passed'):
            out5 = str('File Size Validation passed')
        else:
            out5 = str('File Size Validation Failed')

        #### Returning the validation rule result to the child notebook  ####
        
        log = f"{Created_Time} {out5}"
        logs_info.append(log)
        del AuditTableFileCount_df
        return out5
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        return error_message

# -------------------------------------------------------------------------------------------------------------------------------------- #
# FunctionName -  CheckJobRule_Missing_FileCheck_Validation 
# Purpose - Fetching the Object File Arrival Time from Audit Table by stabilizing the connection with Azure SQL server for a particular object as Source Value and then fetch the Buffer time for the same object from File metadata azure sql table and then calculate the positive and negative buffer value for the object and check is the object arriving in that time range or not as target value and storing the result in string and returning the string as function output to the main wrapper function. 
# Function Return - This Function return Missing File check validation Passed or Failed and it return to main wrapper function 
# Arguments - No of Arguments expected 11 
# Arg1  ( JobId )  - Unique Databricks JobId for the Ingestion Pattern 
# Arg2 ( ObjectPath ) - Path from ADLS,  NAS Drive 
# Arg3  ( ObjectName ) - File Name 
# Arg4 ( server_name ) - Server name, You can Find in  Azure SQL   
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name testsql-ss is a servername 
# Arg5 ( database_name )  – Initial database name, You can Find in  Azure SQL  
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name in that testsqldb-ss is a database name. 
# Arg6 ( ObjectType ) –  File or  NASFile  or SQLTable 
# Arg7 ( sqlusername )  – keyvaults stored sql username value 
# Arg8 ( sqlpassword )  – keyvaults stored  sql password value 
# Arg9 ( service_principle_credentials ) – It consist {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function. 
# Arg10 (  log_path ) – Path For Logs to be Stored Example ADLS path. 
# Arg11 ( AuditTable_df ) – It is Audit Table Dataframe which is calculated and  fetched inside the wrapper function. 
# Function calling Statement 
# CheckJobRule_Missing_FileCheck_Validation(JobId,ObjectPath,ObjectName,server_name,database_name, ObjectType, sqlusername, sqlpassword, service_principle_credentials, log_path, AuditTable_df) 
# -------------------------------------------------------------------------------------------------------------------------------------- #

def CheckJobRule_Missing_FileCheck_Validation(JobId, ObjectPath, ObjectName,server_name,database_name,ObjectType, sqlusername, 
                                              sqlpassword, service_principle_credentials, log_path, AuditTable_df):
    
    try:
        RuleId = int(7)
        
        # Declaring function level variables
        Source_Value_Type = "Source Object Arrival Time"
        Target_Value_Type = "Target Expected Time"
        if(ObjectType == 'File'):
            Source_Name = "Audit Table"
            Target_Name = "ADLS Raw Layer"
            
        if(ObjectType == 'NASFile'):
            Source_Name = "Audit Table"
            Target_Name = "NAS Drive"
            
        if(ObjectType == 'SQLTable'):
            Source_Name = "Audit Table"
            Target_Name = "Azure SQL Server"

        #### Fetching data from Audit Table(Source) ####
        #Querydata = "Select * from abc.Audit_Log  where Product_Name not like 'Post%' "
        #AuditTable_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata_1,service_principle_credentials,log_path)
        AuditTableFileCount_df = AuditTable_df.filter(AuditTable_df.File_Name == ObjectName)
        AuditTableFileCount_pdf = AuditTableFileCount_df.toPandas()

        #### Fetching File Arrival Time for a particular Object ####
        Source_FileNameValue = AuditTableFileCount_pdf.loc[0]['File_Name']
        FileArrival_Time = AuditTableFileCount_pdf.loc[0]['File_Arrival_Time']

        #### Fetching Source Value (Object Arrival Time) for a particular Object ####
        File_Arrival_Time = FileArrival_Time.strftime("%H:%M:%S")

        #### Fetching data from Metadata Table(Target) ####
        #Querydata = "Select * from metaabc.File_MetaData"
        FileMetadata_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata_3,service_principle_credentials,log_path)
        FileMetadata_df = FileMetadata_df.filter(FileMetadata_df.FileName == ObjectName)
        FileMetadata_pdf = FileMetadata_df.toPandas()

        #### Fetching File Name Time for a particular Object ####
        Target_FileNameValue = FileMetadata_pdf.loc[0]['FileName']

        #### Fetching File Buffer Time for a particular Object ####
        File_Expected_Time = FileMetadata_pdf.loc[0]['File_Expected_Time']

        #### Fetching File Arrival Time for a particular Object ####
        File_Buffer_Time = FileMetadata_pdf.loc[0]['File_Buffer_Time']

        #### Calculating Positive and Negative Buffer time ####
        File_Buffer_Time = File_Buffer_Time / 60
        Postive_Buffer_Time = File_Expected_Time + timedelta(hours=File_Buffer_Time)
        Postive_Buffer_Time = Postive_Buffer_Time.strftime("%H:%M:%S")
        Negative_Buffer_Time = File_Expected_Time - timedelta(hours=File_Buffer_Time)
        Negative_Buffer_Time = Negative_Buffer_Time.strftime("%H:%M:%S")

        #### Checking the condition and storing the result ####
        if (Source_FileNameValue == Target_FileNameValue) and (Negative_Buffer_Time < File_Arrival_Time < 
                                                               Postive_Buffer_Time):
            TargetValue = 'File has arrived in the time interval from ' + str(Negative_Buffer_Time) + ' to ' + str(Postive_Buffer_Time)
            RuleStatus = "Passed"
        else:
            TargetValue = 'File has not arrived in the time interval from ' + str(Negative_Buffer_Time) + ' to ' + str(Postive_Buffer_Time)
            RulesStatus = "Failed"

        Created_Time = str(datetime.now())
        New_date = Created_Time[0:23]
        
        # Calling commonInsertFunction to insert validation rule result into SQL ABC table
        commonInsertFunction(server_name, database_name, sqlusername, sqlpassword, TableName, JobId, RuleId, 
                             str(File_Arrival_Time), str(TargetValue), Source_Value_Type, Target_Value_Type, Source_Name, 
                             Target_Name, RuleStatus, New_date, Username, service_principle_credentials,log_path)

        if (RuleStatus == 'Passed'):
            out = 'Missing_FileCheck_Validation passed'
        else:
            out = 'Missing_FileCheck_Validation Failed'

        #### Returning the validation rule result to the child notebook  ####
        
        log = f"{Created_Time} {out}"
        logs_info.append(log)
        del AuditTableFileCount_df
        return out
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message

# -------------------------------------------------------------------------------------------------------------------------------------- #
# FunctionName -  CheckJobRule_FileArrival_Validation 
# Purpose - Fetching the Object File Arrival Time from Audit Table by stabilizing the connection with Azure SQL server for a particular object as Source Value and then fetch the Buffer time for the same object from File metadata azure sql table and then calculate the positive and negative buffer value for the object and check is the object arriving in that time range or not and based on that assigning the value as on time, late as target value and storing the result in string and returning the string as function output to the main wrapper function 
# Function Return - This Function return File Arrival  validation Ontime  or Delayed and it return to main wrapper function 
# Arguments - No of Arguments expected 11 
# Arg1  ( JobId )  - Unique Databricks JobId for the Ingestion Pattern 
# Arg2 ( ObjectPath ) - Path from ADLS,  NAS Drive 
# Arg3  ( ObjectName ) - File Name 
# Arg4 ( server_name ) - Server name, You can Find in  Azure SQL   
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name testsql-ss is a servername 
# Arg5 ( database_name )  – Initial database name, You can Find in  Azure SQL  
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name in that testsqldb-ss is a database name. 
# Arg6 ( ObjectType ) –  File or  NASFile  or SQLTable 
# Arg7 ( sqlusername )  – keyvaults stored sql username value 
# Arg8 ( sqlpassword )  – keyvaults stored  sql password value 
# Arg9 ( service_principle_credentials ) – It consist {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function. 
# Arg10 (  log_path ) – Path For Logs to be Stored Example ADLS path. 
# Arg11 ( AuditTable_df ) – It is Audit Table Dataframe which is calculated and  fetched inside the wrapper function. 
# Function calling Statement 
# CheckJobRule_FileArrival_Validation(JobId, ObjectPath, ObjectName, server_name,database_name,ObjectType, sqlusername, sqlpassword, service_principle_credentials, log_path, AuditTable_df) 
# ------------------------------------------------------------------------------------------------------------------------------------- #

def CheckJobRule_FileArrival_Validation(JobId, ObjectPath, ObjectName,server_name,database_name,ObjectType, sqlusername, sqlpassword, 
                                        service_principle_credentials, log_path, AuditTable_df):
    
    try:
        RuleId = int(6)
        
        # Declaring function level variables
        Source_Value_Type = "Source Object Arrival Time"
        Target_Value_Type = "Target Object Arrival Time"
        if(ObjectType == 'File'):
            Source_Name = "Audit Table"
            Target_Name = "ADLS Raw Layer"
            
        if(ObjectType == 'NASFile'):
            Source_Name = "Audit Table"
            Target_Name = "NAS Drive"
            
        if(ObjectType == 'SQLTable'):
            Source_Name = "Audit Table"
            Target_Name = "Azure SQL Server"

        #### Fetching data from Audit Table(Source) ####
        #Querydata = "Select * from abc.Audit_Log  where Product_Name not like 'Post%' "
        #AuditTable_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata_1,service_principle_credentials,log_path)

        AuditTableFileCount_df = AuditTable_df.filter(AuditTable_df.File_Name == ObjectName)
        AuditTableFileCount_pdf = AuditTableFileCount_df.toPandas()

        #### Fetching File Name for a particular Object ####
        Source_FileNameValue = AuditTableFileCount_pdf.loc[0]['File_Name']

        #### Fetching File Arrival Time for a particular Object ####
        FileArrival_Time = AuditTableFileCount_pdf.loc[0]['File_Arrival_Time']

        #### Formating the Time value ####
        File_Arrival_Time = FileArrival_Time.strftime("%H:%M:%S")

        #### Fetching data from FileMetadata Table(Target) ####
        #Querydata = "Select * from metaabc.File_MetaData"
        FileMetadata_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata_3,service_principle_credentials,log_path)
        FileMetadata_df = FileMetadata_df.filter(FileMetadata_df.FileName == ObjectName)
        FileMetadata_pdf = FileMetadata_df.toPandas()

        #### Fetching File Name for a particular Object ####
        Target_FileNameValue = FileMetadata_pdf.loc[0]['FileName']

         #### Fetching File Expected Time for a particular Object ####
        File_Expected_Time = FileMetadata_pdf.loc[0]['File_Expected_Time']

         #### Fetching File Buffer Time for a particular Object ####
        File_Buffer_Time = FileMetadata_pdf.loc[0]['File_Buffer_Time']

        #### Formating and calculating File Buffer Time value ####
        File_Expected_Time_tt = File_Expected_Time.strftime("%H:%M:%S")
        File_Buffer_Time = File_Buffer_Time / 60

        Postive_Buffer_Time = File_Expected_Time + timedelta(hours=File_Buffer_Time)
        Postive_Buffer_Time = Postive_Buffer_Time.strftime("%H:%M:%S")
        Negative_Buffer_Time = File_Expected_Time - timedelta(hours=File_Buffer_Time)
        Negative_Buffer_Time = Negative_Buffer_Time.strftime("%H:%M:%S")
        
        TargetValue = 'Postive_Buffer_Time = ' + str(Postive_Buffer_Time) + ' and Negative_Buffer_Time = ' + str(Negative_Buffer_Time)


        #### Checking the condition and storing the result ####
        if (Source_FileNameValue == Target_FileNameValue) and Negative_Buffer_Time <= File_Arrival_Time <= File_Expected_Time_tt:
            RuleStatus = "On Time"
        else:
            RuleStatus = "Delay"
        
        Created_Time = str(datetime.now())
        New_date = Created_Time[0:23]
        
        # Calling commonInsertFunction to insert validation rule result into SQL ABC table
        commonInsertFunction(server_name, database_name, sqlusername, sqlpassword, TableName, JobId, RuleId, 
                             str(File_Arrival_Time), str(TargetValue), Source_Value_Type, Target_Value_Type, Source_Name, 
                             Target_Name, RuleStatus, New_date, Username, service_principle_credentials,log_path)

        if (RuleStatus == 'On Time' or RuleStatus == 'Delay'):
            out = 'FileArrival_Validation passed'
        else:
            out = 'FileArrival_Validation Failed'

        #### Returning the validation rule result to the child notebook  ####
        
        log = f"{Created_Time} {out}"
        logs_info.append(log)
        del AuditTableFileCount_df
        return out
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message

# -------------------------------------------------------------------------------------------------------------------------------------- #
# FunctionName - CheckJobRule_BalanceAmountValidation_PostFileProcessing
# Purpose - Fetching the Balance amount column value from Audit Table by stablizing the connection with Azure SQL server for a particular object as Source Value and then fetch the whole data for the same object from ADLS File which we copied from different sources like NAS , ADLS, SQL Server and then storing it in spark dataframe and calculating the balance amount for particular column as target value then comparing the Source value and target value and storing the comparison result in string and returning the string as function output to the main wrapper function.
# Arguments - No. of arguments expected 7 
# Arg1( JobId )  - Databricks Job Id 
# Arg2( ObjectPath_PostProcessing )  - ADLS file path of object after we processed and saved in adls.
# Arg3( ObjectName )  - Name of the file or azure sql table for which we are running validation checks.
# Arg4( BalanceAmountColumn )  - Name of a column from table or file for which we will calculate balance amount.
# Arg5( server_name )  - Azure SQL Server name which we are using to read tables data from it
# Arg6( database_name )  - Azure SQL Database name which we are using to read tables data from it
# Arg7( FileType )  - type of file like .csv, .json, .parquet
# One Sample function calling statement - CheckJobRule_BalanceAmountValidation_PostFileProcessing(JobId, ObjectPath_PostProcessing, ObjectName, BalanceAmountColumn,server_name,database_name,FileType)
# One Function calling statement with Example value -
#CheckJobRule_BalanceAmountValidation_PostFileProcessing(461636582315684,'abfss://validationrule@adlstoadls.dfs.core.windows.net/','employees_20221215T180510.csv','SALARY','testsql-ss','testsqldb-ss','csv')# Function - CheckJobRule_BalanceAmountValidation_PostFileProcessing 
# Purpose - Fetching the Balance amount column value from Audit Table by stabilizing the connection with Azure SQL server for a particular object as Source Value and then fetch the whole data for the same object from ADLS File which we copied from different sources like NAS , ADLS, SQL Server and then storing it in spark data frame and calculating the balance amount for particular column as target value then comparing the Source value and target value and storing the comparison result in string and returning the string as function output to the main wrapper function. 
# Function Return - This Function returns Balance Amount Validation Post File Passed or Failed, and it returns to main wrapper function. 
# Arguments - No. of arguments expected 16 
# Arg1  ( JobId )  -  Unique Databricks JobId for the Ingestion Pattern 
# Arg2 (  ObjectPath_PostProcessing )  - Path from ADLS,  NAS Drive   
# Arg3  (  ObjectName ) - File or  NASFile  or SQLTable 
# Arg4 ( BalanceAmountColumn )  -  – It indicate column name of Balance Amount  
# Arg5 ( server_name ) -  Server name, You can Find in  Azure SQL 
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name testsql-ss is a servername 
# Arg6 ( database_name ) – Initial database name, You can Find in  Azure SQL  
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name in that testsqldb-ss is a database name. 
# Arg7 ( FileType ) –  FileType is Format of the File such as CSV or JSON or PARQUET 
# Arg8 ( sqlusername ) – keyvaults stored sql username value 
# Arg9 ( sqlpassword )  – keyvaults stored  sql password value 
# Arg10 (  service_principle_credentials ) – It consist {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function. 
# Arg11 (  log_path ) – Path For Logs to be Stored Example ADLS path. 
# Arg12 (  Metadata_place ) – Header  or Trailer or SQLQUERY Which indicates the Metadata information. 
# Arg13 (  Manifest_file_url )  - Manifest file location  Which is a path of the ADLS. 
# Arg14 (  BalanceAmount_value_start_index ) – Balance Amount Start Index value. Integer value is passed 
# Arg15 (  BalanceAmount_value_end_index ) – Balance Amount End index value. Integer value is passed 
# Arg16 (  Metadata_column ) – Metadata Column Integer value is passed  
# Arg17 ( Sql_query_balanceamount ) - Sql query to fetch Aggregate BalanceAmount  
# Function calling Statement 
# CheckJobRule_BalanceAmountValidation_PostFileProcessing(JobId, ObjectPath_PostProcessing, ObjectName, BalanceAmountColumn, server_name, database_name, FileType, sqlusername, sqlpassword, service_principle_credentials, log_path, Metadata_place, Manifest_file_url, BalanceAmount_value_start_index, BalanceAmount_value_end_index, Metadata_column, Sql_query_balanceamount) 
# ------------------------------------------------------------------------------------------------------------------------------------- #

def CheckJobRule_BalanceAmountValidation_PostFileProcessing(JobId, ObjectPath_PostProcessing, ObjectName,
                                                            BalanceAmountColumn,server_name,database_name,FileType, sqlusername, 
                                                            sqlpassword, service_principle_credentials, log_path, Metadata_place, 
                                                            Manifest_file_url, BalanceAmount_value_start_index, BalanceAmount_value_end_index, 
                                                            Metadata_column, Sql_query_balanceamount):
    try:
        RuleId = int(9)
        HeaderValue = True
        # Declaring function level variables
        Source_Value_Type = "Source Balance Amount Post Object Processing"
        Target_Value_Type = "Target Balance Amount Post Object Processing"
        Source_Name = "Audit Table"
        Target_Name = "ADLS"
        
        if(Metadata_place == 'File'):
            HeaderValue = True
            #### Reading Manifest file from ADLS ####
            Manifestfile_df = spark.read.text(Manifest_file_url)
            #### Fetching Manifestfile_MetaString from ManifestFile ####
            Manifestfile_MetaString = Manifestfile_df.collect()[0][0]
            #### Fetching Source Balanceamount Value from Manifestfile_MetaString ####
            SourceValue = int(Manifestfile_MetaString[BalanceAmount_value_start_index:BalanceAmount_value_end_index])
        if(Metadata_place == 'Header'):
            HeaderValue = False
            Data_df = spark.read.load(path=ObjectPath_PostProcessing, format=f"{FileType}", header = False)
            Metadata_column = Metadata_column - 1
            BalanceColName = "_c"+str(Metadata_column)
            Data_df = Data_df.withColumnRenamed(BalanceColName, BalanceAmountColumn)
            Data_pdf = Data_df.toPandas()
            MetadataString = Data_pdf.iloc[0,-1]
            SourceValue = int(MetadataString[BalanceAmount_value_start_index:BalanceAmount_value_end_index])
        if(Metadata_place == 'Footer'):
            HeaderValue = True
            Data_df = spark.read.load(path=ObjectPath_PostProcessing, format=f"{FileType}", header = True)
            Data_pdf = Data_df.toPandas()
            MetadataString = Data_pdf.iloc[-1,-1]
            SourceValue = int(MetadataString[BalanceAmount_value_start_index:BalanceAmount_value_end_index])
        if(Metadata_place == 'SQLQUERY'):
            HeaderValue = True
            Data_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Sql_query_balanceamount,service_principle_credentials,log_path)
            SourceValue = Data_df.collect()[0][0]
            
            
        #Querydata = "Select FileName,BalanceAmountColumn from metaabc.File_MetaData"
        FileMetaData_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata_2,service_principle_credentials,log_path)
        FileMetaDataFilter_df = FileMetaData_df.filter(FileMetaData_df.FileName == ObjectName)
        BalanceAmountColumn1 = FileMetaDataFilter_df.collect()[0][1]

        #### Reading File Based on FileType and stored in dataFrame ####
        
        try:
            RawLogData_df = spark.read.load(path=ObjectPath_PostProcessing, format=f"{FileType}", header = HeaderValue)
            Metadata_column = 8 - 1
            BalanceColName = "_c"+str(Metadata_column)
            RawLogData_df = RawLogData_df.withColumnRenamed(BalanceColName, BalanceAmountColumn)
        except Exception as ex:
            current_time = datetime.now()
            error_message = "\n Error Message : " + str(ex)
            log = f"{current_time} Error Occured {error_message}"
            logs_info.append(log)
            # Consolidating Custom logs in Pandas Dataframe
            custom_logs = pd.DataFrame(logs_info, columns = None)
            log_path = log_path.replace("abfss", "abfs") 
            # Custom logs are Stored in ADLS 
            custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
            stack_trace = traceback.format_exc()
            print(stack_trace)
            return error_message

        #### Calculating Balance amount value post file processing ####
        BalanceAmount = RawLogData_df.agg({BalanceAmountColumn: 'sum'})
        for col in BalanceAmount.dtypes:
            TargetValue = int(BalanceAmount.first()[col[0]])

        #### Checking the condition and storing the result ####
        if (SourceValue == TargetValue):
            RuleStatus = 'Passed'
        else:
            RuleStatus = 'Failed'
        
        Created_Time = str(datetime.now())
        New_date = Created_Time[0:23]
        
        # Calling commonInsertFunction to insert validation rule result into SQL ABC table
        commonInsertFunction(server_name, database_name, sqlusername, sqlpassword, TableName, JobId, RuleId, 
                             str(SourceValue), str(TargetValue), Source_Value_Type, Target_Value_Type, Source_Name, 
                             Target_Name, RuleStatus, New_date, Username, service_principle_credentials, log_path)

        if (RuleStatus == 'Passed'):
            out = 'BalanceAmountValidation Post File Processing passed'
            
        else:
            out = 'BalanceAmountValidation Post File Processing Failed'
            

        #### Returning the validation rule result to the child notebook  ####
        
        log = f"{Created_Time} {out}"
        logs_info.append(log)
        del FileMetaData_df
        del RawLogData_df
        return out
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message

# -------------------------------------------------------------------------------------------------------------------------------------- #
# Function - CheckJobRule_CountValidation_PostFileProcessing 
# Purpose - Fetching the record count from Audit Table by stabilizing the connection with Azure SQL server for a particular object as Source Count and then fetch the record count value for the same object from ADLS File which we copied from different sources like NAS , ADLS, SQL Server and then then comparing the Source value and target value and storing the comparison result in string and returning the string as function output to the main wrapper function. 
# Function Return - This Function returns Count Validation Post File Passed or Failed, and it returns to main wrapper function. 
# Arguments - No. of arguments expected 16 
# Arg1  ( JobId )  -  Unique Databricks JobId for the Ingestion Pattern 
# Arg2 (  ObjectPath_PostProcessing )  - Path from ADLS,  NAS Drive   
# Arg3  (  ObjectName ) - File or  NASFile  or SQLTable 
# Arg4 ( server_name ) -  Server name, You can Find in  Azure SQL 
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name testsql-ss is a servername 
# Arg5 ( database_name ) – Initial database name, You can Find in  Azure SQL  
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name in that testsqldb-ss is a database name. 
# Arg6 ( FileType ) –  FileType is Format of the File such as CSV or JSON or PARQUET 
# Arg7 (  sqlusername ) – keyvaults stored sql username value 
# Arg8 (  sqlpassword )  – keyvaults stored  sql password value 
# Arg9 (  service_principle_credentials ) – It consist {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function. 
# Arg10 (  log_path ) – Path For Logs to be Stored Example ADLS path. 
# Arg11 (  Metadata_place ) – Header  or Trailer  Which indicates the Metadata information. 
# Arg12 (  Manifest_file_url )  - Manifest file location  Which is a path of the ADLS. 
# Arg13 (  HeadCount_value_start_index)  – Head Count Start Index Value. Integer value is passed. 
# Arg14 (  HeadCount_value_end_index ) – Head Count End Index Value. Integer value is passed. 
# Arg15 (  Metadata_column ) – Metadata column name Integer 
# Arg16 ( Sql_query_recordcount ) - Sql_query_recordcount to fetch the record count  
# Function calling Statement 
# CheckJobRule_CountValidation_PostFileProcessing(JobId, ObjectPath_PostProcessing, ObjectName,server_name,database_name,FileType, sqlusername, sqlpassword, service_principle_credentials, log_path, Metadata_place, Manifest_file_url, HeadCount_value_start_index, HeadCount_value_end_index, Metadata_column, Sql_query_recordcount) 
# ------------------------------------------------------------------------------------------------------------------------------------- #

def CheckJobRule_CountValidation_PostFileProcessing(JobId, ObjectPath_PostProcessing, ObjectName, server_name, database_name, 
                                                    FileType, sqlusername, sqlpassword, service_principle_credentials, log_path, Metadata_place, 
                                                    Manifest_file_url, HeadCount_value_start_index, HeadCount_value_end_index, Metadata_column, Sql_query_recordcount):
    
    try:
        RuleId = int(8)
        HeaderValue = True
        # Declaring function level variables
        Source_Value_Type = "Source Count Post Object Processing"
        Target_Value_Type = "Target Count Post Object Processing"
        Source_Name = "Audit Table"
        Target_Name = "ADLS"

        if(Metadata_place == 'File'):
            HeaderValue = True
            #### Reading Manifest file from ADLS ####
            Manifestfile_df = spark.read.text(Manifest_file_url)
            #### Fetching Manifestfile_MetaString from ManifestFile ####
            Manifestfile_MetaString = Manifestfile_df.collect()[0][0]
            #### Fetching Source Count Value from Manifestfile_MetaString ####
            SourceValue = int(Manifestfile_MetaString[HeadCount_value_start_index:HeadCount_value_end_index])
        if(Metadata_place == 'Header'):
            HeaderValue = False
            Data_df = spark.read.load(path=ObjectPath_PostProcessing, format=f"{FileType}")
            #BalanceColName = "_c"+str(Metadata_column)
            Data_pdf = Data_df.toPandas()
            MetadataString = Data_pdf.iloc[0,-1]
            SourceValue = int(MetadataString[HeadCount_value_start_index:HeadCount_value_end_index])
            
        if(Metadata_place == 'Footer'):
            HeaderValue = True
            Data_df = spark.read.load(path=ObjectPath_PostProcessing, format=f"{FileType}", header = True)
            Data_pdf = Data_df.toPandas()
            MetadataString = Data_pdf.iloc[-1,-1]
            SourceValue = int(MetadataString[HeadCount_value_start_index:HeadCount_value_end_index])
        
        if(Metadata_place == 'SQLQUERY'):
            HeaderValue = True
            Data_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Sql_query_recordcount,service_principle_credentials,log_path)
            SourceValue = Data_df.collect()[0][0]
            
        #### Checking Object Type wheather is it Parquet or not and based on that further processing it ####
        try:
            RawLogData_df = spark.read.load(path=ObjectPath_PostProcessing, format=f"{FileType}", header = HeaderValue)
        except Exception as ex:
            current_time = datetime.now()
            error_message = "\n Error Message : " + str(ex)
            log = f"{current_time} Error Occured {error_message}"
            logs_info.append(log)
            # Consolidating Custom logs in Pandas Dataframe
            custom_logs = pd.DataFrame(logs_info, columns = None)
            log_path = log_path.replace("abfss", "abfs") 
            # Custom logs are Stored in ADLS 
            custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
            return error_message

        #### Fetching Target Count for a particular Object ####
        TargetValue = int(RawLogData_df.count())

        #### Checking the condition and storing the result ####
        if (SourceValue == TargetValue):
            RuleStatus = 'Passed'
        else:
            RuleStatus = 'Failed'
        
        Created_Time = str(datetime.now())
        New_date = Created_Time[0:23]
        
        # Calling commonInsertFunction to insert validation rule result into SQL ABC table
        commonInsertFunction(server_name, database_name, sqlusername, sqlpassword, TableName, JobId, RuleId, 
                             str(SourceValue), str(TargetValue), Source_Value_Type, Target_Value_Type, Source_Name, 
                             Target_Name, RuleStatus, New_date, Username, service_principle_credentials, log_path)

        if (RuleStatus == 'Passed'):
            out = 'CountValidation Post File Processing passed'
           
        else:
            out = 'CountValidation Post File Processingn Failed'
            
        log = f"{Created_Time} {out}"
        logs_info.append(log)
        #### Returning the validation rule result to the child notebook  ####
        del RawLogData_df
        return out
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message

# ------------------------------------------------------------------------------------------------------------------------------------- #
# FunctionName - CheckJobRule_CountValidation_Egress 
# Purpose - To Validate the recount count of file. source is ADLS and Target will be SQL table. If it Match it consider as passed else Failed. Result will be updated into [abc].[Job_Rule_Execution_Log] 
# Function Return - This Function return record count validation Passed or Failed, and it return to main wrapper function 
# Arguments - No of Arguments expected 10 
# Arg1  ( JobId )  - Unique Databricks JobId for the Ingestion Pattern 
# Arg2 ( ObjectPath ) - Path from ADLS,  NAS Drive 
# Arg3  ( ObjectName ) - File Name 
# Arg4 ( server_name ) - Server name, You can Find in  Azure SQL   
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name testsql-ss is a servername 
# Arg5 ( database_name )  – Initial database name, You can Find in  Azure SQL  
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name in that testsqldb-ss is a database name. 
# Arg6 ( FileType )  –  FileType is Format of the File such as csv or json or parquet 
# Arg7 ( sqlusername )  – keyvaults stored sql username value 
# Arg8 ( sqlpassword )  – keyvaults stored  sql password value 
# Arg9 ( service_principle_credentials ) – It consist {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function. 
# Arg10 (  log_path ) – Path For Logs to be Stored Example ADLS path. 
# Function calling Statement 
# CheckJobRule_CountValidation_Egress(JobId, ObjectPath, ObjectName, server_name, database_name, FileType, sqlusername, sqlpassword, service_principle_credentials, log_path)     
# ------------------------------------------------------------------------------------------------------------------------------------- #   
    
    
def CheckJobRule_CountValidation_Egress(JobId, ObjectPath, ObjectName, server_name, database_name, FileType, sqlusername, sqlpassword, service_principle_credentials, log_path):
    try:
        RuleId = int(10)

        # Declaring function level variables
        Source_Value_Type = "Source Count for Egress Object"
        Target_Value_Type = "Target Count for Egress Object"
        Source_Name = "ADLS"
        Target_Name = "SQL Server"

        #### Checking Object Type wheather is it Parquet or not and based on that further processing it ####
        try:
            RawLogData_df = spark.read.parquet(ObjectPath)
        except Exception as ex:
            current_time = datetime.now()
            error_message = "\n Error Message : " + str(ex)
            log = f"{current_time} Error Occured {error_message}"
            logs_info.append(log)
            # Consolidating Custom logs in Pandas Dataframe
            custom_logs = pd.DataFrame(logs_info, columns = None)
            log_path = log_path.replace("abfss", "abfs") 
            # Custom logs are Stored in ADLS 
            custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
            stack_trace = traceback.format_exc()
            print(stack_trace)
            return error_message

        #### Fetching Source Count for a particular Object ####
        SourceValue = int(RawLogData_df.count())

        #### Fetching all the Post processed Object data from Audit Table(Source) ####
        Querydata_5 = "Select count(*) as Target_Value from SalesLT.formula1dl_results"
        TargetValue_df = commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,Querydata_5,service_principle_credentials,log_path)
        TargetValue_pdf = TargetValue_df.toPandas()
        #### Fetching Target Count for a particular Object ####
        TargetValue = int(TargetValue_pdf['Target_Value'])

        #### Checking the condition and storing the result ####
        if (SourceValue == TargetValue):
            RuleStatus = 'Passed'
        else:
            RuleStatus = 'Failed'

        Created_Time = str(datetime.now())
        New_date = Created_Time[0:23]

        # Calling commonInsertFunction to insert validation rule result into SQL ABC table
        commonInsertFunction(server_name, database_name, sqlusername, sqlpassword, TableName, JobId, RuleId, 
                             str(SourceValue), str(TargetValue), Source_Value_Type, Target_Value_Type, Source_Name, 
                             Target_Name, RuleStatus, New_date, Username, service_principle_credentials, log_path)

        if (RuleStatus == 'Passed'):
            out = 'CountValidation for Egress passed'

        else:
            out = 'CountValidation for Egress Failed'

        #### Returning the validation rule result to the child notebook  ####
        del RawLogData_df
        return out
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message
        

# ------------------------------------------------------------------------------------------------------------------------------------- #
# FunctionName - commonInsertFunction 
# Purpose - To Insert output status of each Rule id of the execution into abc.Job_Rule_Execution_Log Table.  
# Function Return – This function returns either 1 or 0. A return value of 0 indicates that the function executed without error, while a return value of 1 indicates that an error occurred during execution.
# Arguments - No of Arguments expected 18
# Arg1  ( ServerName ) - Server name, You can Find in  Azure SQL 
# Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name testsql-ss is a servername  
# Arg2 ( DatabaseName ) - Initial database name, You can Find in  Azure SQL  
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name in that testsqldb-ss is a database name. 
# Arg3  ( jdbcUsername ) - keyvaults stored sql username value 
# Arg4 ( jdbcPassword ) - keyvaults stored  sql password value 
# Arg5 ( TableName ) - Job abc.Rule Execution_Log
# Arg6 ( Job_Id ) - Unique Databricks JobId for the Ingestion Pattern 
# Arg7 ( Rule_Id ) - Rule Id's
# Arg8 ( Source_Value ) - ADls or NASDrive or SQLTable 
# Arg9 ( Target_Value ) -  ADls or NASDrive or SQLTable 
# Arg10 ( Source_Value_Type ) - ADls or NASDrive or SQLTable
# Arg11 ( Target_Value_Type ) - ADls or NASDrive or SQLTable
# Arg12 ( Source_Name ) - ADls or NASDrive or SQLTable
# Arg13 ( Target_Name ) - ADls or NASDrive or SQLTable
# Arg14 ( Rule_Run_Status ) - ADls or NASDrive or SQLTable
# Arg15 ( Created_Time ) - Time 
# Arg16 ( Created_By ) - User 
# Arg17 ( service_principle_credentials ) -  It consist {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function. 
# Arg18 ( log_path ) - Path For Logs to be Stored Example ADLS path.
# Function calling Statement 
#commonInsertFunction(ServerName, DatabaseName, jdbcUsername, jdbcPassword, TableName, Job_Id, Rule_Id, Source_Value, Target_Value, Source_Value_Type, Target_Value_Type, Source_Name, Target_Name, Rule_Run_Status, Created_Time, Created_By, service_principle_credentials, log_path)
#Function calling Statement 
#commonInsertFunction(ServerName, DatabaseName, jdbcUsername, jdbcPassword, TableName, Job_Id, Rule_Id, Source_Value, Target_Value, Source_Value_Type, Target_Value_Type, Source_Name, Target_Name, Rule_Run_Status, Created_Time, Created_By, service_principle_credentials, log_path)
# ------------------------------------------------------------------------------------------------------------------------------------- #
    
def commonInsertFunction(ServerName, DatabaseName, jdbcUsername, jdbcPassword, TableName, Job_Id, Rule_Id, Source_Value, 
                         Target_Value, Source_Value_Type, Target_Value_Type, Source_Name, Target_Name, Rule_Run_Status, 
                         Created_Time, Created_By, service_principle_credentials, log_path):
    try:
        # Declaring function level variables
        pyodbc_conn = "Driver={ODBC Driver 17 for SQL Server};Server="
        pyodbc_server = ".database.windows.net;Database="
        UID = ";UID="
        PWD = ";PWD="
        insert_statement = "Insert into "
        tables_name = " (Job_Id, Rule_Id, Source_Value, Target_Value, Source_Value_Type, Target_Value_Type, Source_Name, Target_Name, Rule_Run_Status, Created_Time, Created_By) Values(" 
        
    
#        cnxn_str = ("Driver={ODBC Driver 17 for SQL Server};Server=" + ServerName + ".database.windows.net;Database=" + DatabaseName + ";UID=" + jdbcUsername + ";PWD=" + jdbcPassword + ";")
        
        cnxn_str = (pyodbc_conn + ServerName + pyodbc_server + DatabaseName + UID + jdbcUsername + PWD + jdbcPassword + ";")

        #### Connecting with pyodbc with the above created connection to perform the insert and update query in SQL table 
        cnxn = pyodbc.connect(cnxn_str)

        #### Creating a pyodbc connection object ####
        cursor = cnxn.cursor()
            
        #### Creating Insert SQL query  ####
        
        #query = "Insert into "+ TableName + " (Job_Id, Rule_Id, Source_Value, Target_Value, Source_Value_Type, Target_Value_Type, Source_Name, Target_Name, Rule_Run_Status, Created_Time, Created_By) Values(" + str(Job_Id) +  "," + str(Rule_Id) + ",'" + Source_Value + "','" + Target_Value + "','" + Source_Value_Type + "','" + Target_Value_Type + "','" + Source_Name + "','" + Target_Name + "','" + Rule_Run_Status + "','" + Created_Time + "','" + Created_By + "')"
        
        query = insert_statement + TableName + tables_name + str(Job_Id) +  "," + str(Rule_Id) + ",'" + Source_Value + "','" + Target_Value + "','" + Source_Value_Type + "','" + Target_Value_Type + "','" + Source_Name + "','" + Target_Name + "','" + Rule_Run_Status + "','" + Created_Time + "','" + Created_By + "')"
        
        
        #### Executing the Insert query to the sql table with the help of pyodbc object ####
        cursor.execute(query)

        #### Committed the changes so it will reflect in the original sql table ####
        cnxn.commit()
        return 0
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        
        return 1

# ------------------------------------------------------------------------------------------------------------------------------------- #           
# FunctionName - commonSQLFunction 
# Purpose - To Create Dataframe from Sql Query 
# Function Return - This Function return dataframe of the sql query
# Arguments - No of Arguments expected 7
# Arg1  ( server_name ) - Server name, You can Find in  Azure SQL   
# Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name testsql-ss is a servername 
# Arg2 ( database_name ) - Initial database name, You can Find in  Azure SQL  
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name in that testsqldb-ss is a database name. 
# Arg3  ( sqlusername ) - keyvaults stored sql username value
# Arg4 ( sqlpassword ) - keyvaults stored  sql password value 
# Arg5 ( sqlquery ) - Sql Query  
# Arg6 ( service_principle_credentials ) -  It consist {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function.
# Arg7 ( log_path ) - Path For Logs to be Stored Example ADLS path. 
# Function calling Statement 
# commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,sqlquery,service_principle_credentials,log_path)
# ------------------------------------------------------------------------------------------------------------------------------------- #          
        
        
def commonSQLFunction(server_name,database_name,sqlusername,sqlpassword,sqlquery,service_principle_credentials,log_path):
    
    try:
        ### Inserting validation rule result into SQL ABC table
        Common_df = spark.read.format("jdbc") \
            .option("url", f"jdbc:sqlserver://{server_name}.database.windows.net:1433;database={database_name}") \
            .option("query", sqlquery) \
            .option("user", sqlusername) \
            .option("password", sqlpassword) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .load()
        return Common_df
    
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        return error_message

# ------------------------------------------------------------------------------------------------------------------------------------- # 
# FunctionName - ChecksumInsertFunction 
# Purpose - To Insert Checksum into abc.File_Checksum_details Table 
# Function Return – This function returns either 1 or 0. A return value of 0 indicates that the function was executed without error, while a return value of 1 indicates that an error occurred during execution.
# Arguments - No of Arguments expected 11
# Arg1 ( ServerName ) - Server name, You can Find in  Azure SQL   
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name testsql-ss is a servername 
# Arg2 ( DatabaseName ) - Initial database name, You can Find in  Azure SQL  
#  Example testsqldb-ss (testsql-ss/testsqldb-ss)  is the Database name in that testsqldb-ss is a database name. 
# Arg3  ( jdbcUsername ) - keyvaults stored sql username value 
# Arg4 ( jdbcPassword ) -  keyvaults stored  sql password value 
# Arg5 ( TableName ) - Table Name abc.File_Checksum_details
# Arg6 ( FileName ) - FileName 
# Arg7 ( Checksum_Value ) - Unique Checksum value 
# Arg8 ( Created_Time ) - Datetime 
# Arg9 ( Created_By ) - Username
# Arg10 ( service_principle_credentials ) -It consist {'tenant_id': tenant_id, 'client_id' : client_id, 'client_secret': client_secret} which is fetched inside the wrapper function. 
# Arg11 ( log_path ) - Path For Logs to be Stored Example ADLS path.
# Function calling Statement 
#ChecksumInsertFunction(ServerName, DatabaseName, jdbcUsername, jdbcPassword, TableName, FileName, Checksum_Value, Created_Time, Created_By, service_principle_credentials, log_path)
# ------------------------------------------------------------------------------------------------------------------------------------- # 
    
def ChecksumInsertFunction(ServerName, DatabaseName, jdbcUsername, jdbcPassword, TableName, FileName, Checksum_Value, Created_Time, Created_By, service_principle_credentials, log_path):
    try:
        # Declaring function level variables
        pyodbc_conn = "Driver={ODBC Driver 17 for SQL Server};Server="
        pyodbc_server = ".database.windows.net;Database="
        UID = ";UID="
        PWD = ";PWD="
        insert_statement = "Insert into "
        Insert_column_names = " (FileName, Checksum_Value, Created_Time, Created_By) Values('" 
        
#        cnxn_str = ("Driver={ODBC Driver 17 for SQL Server};Server=" + ServerName + ".database.windows.net;Database=" + DatabaseName + ";UID=" + jdbcUsername + ";PWD=" + jdbcPassword + ";")
        
        cnxn_str = (pyodbc_conn + ServerName + pyodbc_server + DatabaseName + UID + jdbcUsername + PWD + jdbcPassword + ";")

        #### Connecting with pyodbc with the above created connection to perform the insert and update query in SQL table 
        cnxn = pyodbc.connect(cnxn_str)

        #### Creating a pyodbc connection object ####
        cursor = cnxn.cursor()
            
        #### Creating Insert SQL query  ####
        
        #query = "Insert into "+ TableName + " (Job_Id, Rule_Id, Source_Value, Target_Value, Source_Value_Type, Target_Value_Type, Source_Name, Target_Name, Rule_Run_Status, Created_Time, Created_By) Values(" + str(Job_Id) +  "," + str(Rule_Id) + ",'" + Source_Value + "','" + Target_Value + "','" + Source_Value_Type + "','" + Target_Value_Type + "','" + Source_Name + "','" + Target_Name + "','" + Rule_Run_Status + "','" + Created_Time + "','" + Created_By + "')"
              
        query = insert_statement + TableName + Insert_column_names + FileName +  "','" + str(Checksum_Value) + "','"  + Created_Time + "','" + Created_By + "')"
        
        
        #### Executing the Insert query to the sql table with the help of pyodbc object ####
        cursor.execute(query)

        #### Committed the changes so it will reflect in the original sql table ####
        cnxn.commit()
        
        return 0
        
    except Exception as ex:
        error_message = "\n Error Message : " + str(ex)
        current_time = datetime.now()
        log = f"{current_time} Error Occured {error_message}"
        logs_info.append(log)
        # Consolidating Custom logs in Pandas Dataframe
        custom_logs = pd.DataFrame(logs_info, columns = None)
        log_path = log_path.replace("abfss", "abfs") 
        # Custom logs are Stored in ADLS 
        custom_logs.to_csv(log_path, storage_options = service_principle_credentials, index = False, header = False)
        stack_trace = traceback.format_exc()
        print(stack_trace)
        
        return 1

