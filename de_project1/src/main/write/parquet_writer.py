import traceback
from src.main.utility.logging_config import *
class DataFormatWriter:
    def __init__(self,mode,data_format):
        self.mode = mode
        self.data_format = data_format

#     def dataframe_writer(self,df, file_path):
#         try:
#             df.write.format(self.data_format) \
#                 .option("header", "true") \
#                 .mode(self.mode) \
#                 .option("path", file_path) \
#                 .save()
#         except Exception as e:
#             logger.error(f"Error writing the data : {str(e)}")
#             traceback_message = traceback.format_exc()
#             print(traceback_message)
#             raise e

    def dataframe_writer(self, df, file_path):
        try:
            df.write.format(self.data_format) \
                .option("header", "true") \
                .mode(self.mode) \
                .option("path", file_path) \
                .save()
        except Exception as e:
            logger.error(f"Error writing the data : {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e

# import os
# import traceback
# from urllib.parse import quote
# from src.main.utility.logging_config import *
#
# class DataFormatWriter:
#     def __init__(self, mode, data_format):
#         self.mode = mode
#         self.data_format = data_format
#
#     def dataframe_writer(self, df, file_path):
#         try:
#             # ✅ 1. Local path normalize
#             file_path = os.path.abspath(file_path)
#
#             # ✅ 2. Agar path file hai to uska folder bana do
#             if file_path.endswith(".parquet") or file_path.endswith(".csv"):
#                 file_path = os.path.splitext(file_path)[0]
#
#             # ✅ 3. Folder create karo (local case)
#             os.makedirs(file_path, exist_ok=True)
#
#             # ✅ 4. Windows space handle karo for Spark (file:/// + %20 encoding)
#             spark_path = f"file:///{quote(file_path.replace(os.sep, '/'))}"
#
#             # ✅ 5. Write DataFrame
#             df.write.format(self.data_format) \
#                 .option("header", "true") \
#                 .mode(self.mode) \
#                 .save(spark_path)
#
#             logger.info(f"✅ Data written successfully to {spark_path}")
#
#         except Exception as e:
#             logger.error(f"Error writing the data : {str(e)}")
#             traceback_message = traceback.format_exc()
#             print(traceback_message)
#             raise e
