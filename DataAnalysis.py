# Databricks notebook source
# #month of October
# import os
# from datetime import datetime
# path = '/dbfs/mnt/datalake/raw/data/DailyCostMgmtData/20221001-20221031'
# fdpaths = [path+"/"+file_names for file_names in os.listdir(path)]
# print(" file_path " + " create_date " + " modified_date ")
# for path in fdpaths:
#   statinfo = os.stat(path)
#   create_date = datetime.fromtimestamp(statinfo.st_ctime)
#   modified_date = datetime.fromtimestamp(statinfo.st_mtime)
#   print(path, create_date, modified_date)

# COMMAND ----------

# #Month of November
# import os
# from datetime import datetime
# file_list = []
# path = '/dbfs/mnt/datalake/raw/data/DailyCostMgmtData/20221101-20221130'
# fdpaths = [path+"/"+file_names for file_names in os.listdir(path)]
# #print(" file_path " + " create_date " + " modified_date ")
# for path in fdpaths:
#     statinfo = os.stat(path)
#     create_date = datetime.fromtimestamp(statinfo.st_ctime)
#     modified_date = datetime.fromtimestamp(statinfo.st_mtime)
#     monthly_period = path.rsplit('/',1)[0].rsplit('/',1)[1] #this is wonky, clean it up later
#     file_list.append([path, monthly_period, create_date, modified_date])
# #   print(path, create_date, modified_date)

# COMMAND ----------

#each month, cost data gets exported automatically to a new folder with the syntax YYYYMM01-YYYYMM[EOM]. We need to combine all the subfolder file paths in order to do some analysis on them
#analysis i.e. figure out EOM files, current date, give users ability to choose, etc. 
import os 
from datetime import datetime
base_path = '/dbfs/mnt/datalake/raw/data/DailyCostMgmtData/'
months_to_analyze = os.listdir(base_path)
files_to_analyze = []
for month in months_to_analyze:
    file_path = base_path + month
    for file in os.listdir(file_path):
        path = file_path + '/' + file
        statinfo = os.stat(path)
        create_date = datetime.fromtimestamp(statinfo.st_ctime)
        modified_date = datetime.fromtimestamp(statinfo.st_mtime)
        monthly_period = path.rsplit('/',1)[0].rsplit('/',1)[1] #this is wonky, clean it up later
        files_to_analyze.append([path, monthly_period, create_date, modified_date])


# COMMAND ----------

files_to_analyze

# COMMAND ----------

#create a dataframe from the list of files that we were working with earlier 
column_list = ['FilePath', 'MonthlyPeriod', 'CreateDate', 'ModifiedDate']

file_list_df = spark.createDataFrame(data=files_to_analyze, schema = column_list)
file_list_df.createOrReplaceTempView('file_list')

# COMMAND ----------

#file_list_df.createOrReplaceTempView()

# COMMAND ----------

cleansed_data_df = spark.sql(""" 
with my_data as (
select *, 
rank() over (partition by monthlyperiod order by modifieddate desc) basic_rank --window function, window over my results 

from file_list) 

select 
*,
case when basic_rank = 1 then 1 else 0 end eom_file_indicator,
case when modifieddate= (select max(modifieddate) from file_list) then 1 else 0 end as current_date_indicator
from my_data """ ) 

display(cleansed_data_df)


# COMMAND ----------

#keep only files that have an eom indicator of 1
files_to_keep_df = cleansed_data_df.where(cleansed_data_df.eom_file_indicator == 1)

display(files_to_keep_df )

# COMMAND ----------

# #create separate dataframes and add them together individually
# df_list = []
# for rows in files_to_keep_df.select("FilePath").collect():
#     file_path = rows[0]
#     file_path = file_path.replace('/dbfs', '')
#     df_list.append(file_path)

# #first dataframe 
# df1 = spark.read.csv(path=df_list[0], header=True)
# from pyspark.sql.functions import col,lit
# columns = files_to_keep_df.columns
# for i in columns:
#     row = files_to_keep_df.select(i).collect()
#     correct = row[0][0]
#     df1 = df1.withColumn(i, lit(correct))

# #second dataframe
# df2 = spark.read.csv(path=df_list[1], header=True)
# columns = files_to_keep_df.columns
# for i in columns:
#     row = files_to_keep_df.select(i).collect()
#     correct = row[1][0]
#     df2 = df2.withColumn(i, lit(correct))
   
# #third dataframe
# df3 = spark.read.csv(path=df_list[2], header=True)
# columns = files_to_keep_df.columns
# for i in columns:
#     row = files_to_keep_df.select(i).collect()
#     correct = row[2][0]
#     df3 = df3.withColumn(i, lit(correct))
# #combine dataframes
# df_result = df1.union(df2).union(df3)

# #display(df_result)
# print(df1.count())
# print(df2.count())
# print(df3.count())
# print(df_result.count())

# COMMAND ----------

from pyspark.sql.functions import col,lit
    
#create list of files to grab data from
df_list = []
for rows in files_to_keep_df.select("FilePath").collect():
    file_path = rows[0]
    file_path = file_path.replace('/dbfs', '')
    df_list.append(file_path)   

#iterate through df_list and create dataframes for each item in list
results = []
for file in range(len(df_list)):
    df = spark.read.csv(path=df_list[file], header=True)
    columns = files_to_keep_df.columns
    for i in columns:
        row = files_to_keep_df.select(i).collect()
        correct = row[file][0]
        df = df.withColumn(i, lit(correct))  
    results.append(df)
    
#combine dataframes into one dataframe 
df_final = results[0]
for item in range(len(results) - 1):
    df_final = df_final.union(results[item + 1])

    
#display(df_final)
df_final.count()

# COMMAND ----------

#display(df_final)

# COMMAND ----------

#saving dataframe as csv file and saving to refined folder with current date 
from datetime import date
today = date.today()

df_final.toPandas().to_csv("/dbfs/mnt/datalake/refined/refineddata" + str(today) + ".csv")

# COMMAND ----------

#figure out what files are in our directory 

#where azure export is pointed to 
#base_path = dbutils.fs.ls('/mnt/datalake/raw/data/DailyCostMgmtData')

#empty list to fill with file paths 
file_names = []

#iterate over each folder and subfolder in the directory, appending base filename to list
#for folder in base_path:
     #print(folder.path)
#     folder_contents = dbutils.fs.ls(folder.path)
#     for subfolder in folder_contents:
#         file_names.append(subfolder.path)

# COMMAND ----------

#dbutils.fs.ls('/mnt/datalake')

# COMMAND ----------

# MAGIC %md Anything below this is exploratory at this point 

# COMMAND ----------

#load in all the files that we found in our directory
#df = spark.read.csv(path=file_names, header=True)
#load one file by explicit path 
#df = spark.read.csv('dbfs:/mnt/datalake/raw/data/DailyCostMgmtData/20221001-20221031/DailyCostMgmtData_1d9ea3d8-03a1-459e-aeba-e51b182aeb6b.csv', header=True)

#how many records do i have
#df.count()

# COMMAND ----------

#display(df)

# COMMAND ----------

#dbutils.data.summarize(df)

# COMMAND ----------

#dbutils.fs.ls('/mnt/datalake/raw/samplecbbdata.csv')

# COMMAND ----------

#df = spark.read.csv('/mnt/datalake/raw/samplecbbdata.csv', header=True)

# COMMAND ----------

#display(df)

# COMMAND ----------

#df.write.format('delta').save('/mnt/datalake/raw/samplecbbdata.delta')

# COMMAND ----------

#df.write.saveAsTable('foo.bar')

# COMMAND ----------


