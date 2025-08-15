from pyspark.sql.functions import *
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_write import DatabaseWriter

# calculation for customer mart
# find out the customer total purchase every month
# write the data into MySQL table


# def give_final_sales_team_mart_df(s3_customer_store_sales_df_join):
#     cols = [
#         "store_id", "sales_person_id", "sales_person_first_name", "sales_person_last_name"
#         , "store_manager_name", "manager_id", "is_manager"
#         , "sales_person_address", "sales_person_pincode", "sales_date", "total_cost",
#         expr("SUBSTRING(sales_date,1,7) as sales_month")
#         ]
#     final_sales_team_mart_df=s3_customer_store_sales_df_join \
#                                         .select(cols)
#     return final_sales_team_mart_df


def sales_mart_calculation_table_write(final_sales_team_data_mart_df):
    window = Window.partitionBy("store_id", "sales_person_id", "sales_month")
    final_sales_team_data_mart = final_sales_team_data_mart_df \
        .withColumn("sales_month",
                    substring(col("sales_date"),1,7)) \
        .withColumn("total_sales_every_month",
                    sum(col("total_cost")).over(window)) \
        .select("store_id", "sales_person_id",
                concat(col("sales_person_first_name"), lit(" "), col("sales_person_last_name")).alias(
                    "full_name"),
                    "sales_month",
                    "total_sales_every_month").distinct()

    rank_window = Window.partitionBy("store_id", "sales_month").orderBy(
        col("total_sales_every_month").desc())
    final_sales_team_data_mart_ranked_table = final_sales_team_data_mart \
        .withColumn("rnk", rank().over(rank_window)) \
        .withColumn("incentive", when(col("rnk") == 1,
                                      col("total_sales_every_month") * 0.01).otherwise(lit(0))) \
        .withColumn("incentive", round(col("incentive"), 2)) \
        .withColumn("total_sales", col("total_sales_every_month")) \
        .select("store_id", "sales_person_id", "full_name", "sales_month", "total_sales", "incentive")

    #WRITING DATA INTO SALES_TEAM_DATA_MART IN MYSQL
    print("writing the data into sales_team_data_mart")
    db_writer = DatabaseWriter(url=config.url, properties=config.properties)
    db_writer.write_dataframe(final_sales_team_data_mart_ranked_table, config.sales_team_data_mart_table)

# def sales_mart_calculation_table_write(final_sales_team_data_mart_df):
#     # Step 1: Compute total sales per salesperson per store per month
#     monthly_sales_window = Window.partitionBy("store_id", "sales_person_id", "sales_month")
#
#     sales_with_total = final_sales_team_data_mart_df \
#         .withColumn("total_sales", sum("total_cost").over(monthly_sales_window)) \
#         .drop("total_cost") \
#         .dropDuplicates(["store_id", "sales_person_id", "sales_month", "total_sales"])
#
#     # Step 2: Rank them within each store and month
#     rank_window = Window.partitionBy("store_id", "sales_month").orderBy(col("total_sales").desc())
#
#     ranked_sales = sales_with_total \
#         .withColumn("rank", rank().over(rank_window)) \
#         .filter(col("rank") == 1)  # Keep only top 1 salesperson per store-month
#
#     # Step 3: Calculate incentive and format final DataFrame
#     top_salespersons = ranked_sales \
#         .withColumn("incentive", round(col("total_sales") * 0.01, 2)) \
#         .withColumn("full_name", concat_ws(" ", col("sales_person_first_name"), col("sales_person_last_name")))
#
#     final_output = top_salespersons.select(
#         "store_id",
#         "sales_person_id",
#         "full_name",
#         "sales_month",
#         "total_sales",
#         "incentive"
#     ).orderBy("store_id", "sales_month", "sales_person_id")
#     print("*************** Displaying top performers *************")
#     final_output.show()
#     # Step 4: Write to MySQL
#     print("Writing top salespersons per store per month to sales_team_data_mart...")
#     db_writer = DatabaseWriter(url=config.url, properties=config.properties)
#     db_writer.write_dataframe(final_output, config.sales_team_data_mart_table)