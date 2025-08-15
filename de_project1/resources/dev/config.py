import os

key = "youtube_project"
iv = "youtube_encyptyo"
salt = "youtube_AesEncryption"

#AWS Access And Secret key
aws_access_key = ""
aws_secret_key = ""
bucket_name = "de-rinkal-project-1"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"


#Database credential
# MySQL database connection properties
mysql_host="localhost"
mysql_user="root"
database_name = "de_project"
project_stag_table="product_staging_table"
mysql_password = ""
url = f"jdbc:mysql://localhost:3306/{database_name}"
properties = {
    "user": "root",
    "password": "",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

#Extra as m. told to do dynamic
db_name = "de_project"
table_name = "product_staging_table"


#Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id","store_id","product_name","sales_date","sales_person_id","price","quantity","total_cost"]


# File Download location
local_directory = "C:\\Users\\Rinkal Patidar\\Pycharm_DE_Project-Folders\\file_from_s3\\"
customer_data_mart_local_file = "C:\\Users\\Rinkal Patidar\\Pycharm_DE_Project-Folders\\customer_data_mart\\"
sales_team_data_mart_local_file = "C:\\Users\\Rinkal Patidar\\Pycharm_DE_Project-Folders\\sales_team_data_mart\\"
sales_team_data_mart_partitioned_local_file = "C:\\Users\\Rinkal Patidar\\Pycharm_DE_Project-Folders\\sales_partition_data\\"
error_folder_path_local = "C:\\Users\\Rinkal Patidar\\Pycharm_DE_Project-Folders\\error_files\\"

# customer_data_mart_local_file = r"C:\Users\Rinkal Patidar\customer_data_mart"
# sales_team_data_mart_local_file = r"C:\Users\Rinkal Patidar\sales_team_data_mart"

#   Required for my_sql_session.py file
host = "localhost"
user = "root"
password = ""
database = "de_project"



