import random

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json

def transform(input_path, output_path, schema_path):

        df = spark.read.format("parquet")\
                .option("header",True)\
                .load(input_path)

        f = open(schema_path)
        schema = json.load(f)
        fields = schema['field']
        for x in fields:
            df = df.withColumnRenamed(x['name'], x['output_field_name'])
            if 'harmonization' in x['transformations'].keys():
                if x['transformations']['harmonization'] == 'CHANGE_DATA_TYPE':
                        df=df.withColumn(x['name'], df[x['name']].cast(x['transformations']['nativeDataType']))

        df.printSchema()
        df.write.parquet(output_path + "/"+ schema['name'])

if __name__ == "__main__":
        spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

        f = open("config.json")
        input = json.load(f)

        input_path = input['input_path']
        output_path = input['output_path']
        schema_path = input['input_avro_schema_path']

        transform(input_path, output_path, schema_path)



# schema = StructType([
#         StructField("id", IntegerType(), True, metadata= {"cmt":"id for data"}),
#         StructField("first_name", StringType(), True, metadata= {"cmt":"first_name for data"}),
#         StructField("last_name", StringType(), True, metadata= {"cmt":"last_name for data"}),
#         StructField("email", StringType(), True, metadata= {"cmt":"email for data"}),
#         StructField("gender", StringType(), True, metadata= {"cmt":"gender for data"}),
#         StructField("ip_address", StringType(), True, metadata= {"cmt":"id for data"}),
#         StructField("cc", StringType(), True, metadata= {"cmt":"id for data"}),
#         StructField("country", StringType(), True, metadata= {"cmt":"id for data"}),
#         StructField("birthdate", StringType(), True, metadata= {"cmt":"id for data"}),
#         StructField("salary", DoubleType(), True, metadata= {"cmt":"id for data"}),
#         StructField("title", StringType(), True, metadata= {"cmt":"id for data"}),
#         StructField("comments", StringType(), True, metadata= {"cmt":"id for data"}),
# ])


# print(df.info())
# #
# df.write.parquet("new_file.parquet")

# df = spark.read.parquet("./new_file.parquet/part-00000-3b06e785-cc7e-48d5-8525-8a4294975a38-c000.snappy.parquet")
# df.printSchema()
# print(df.schema.jsonValue())

# df.show(truncate=False)
# print(df.count())
# print(df.distinct().count())

# new_df = df.withColumn("first_name", expr("explode(array_repeat(first_name,2))"))
# new_df = new_df.withColumnRenamed("gender","sex")
# # print(new_df.count())
# new_df.show()

