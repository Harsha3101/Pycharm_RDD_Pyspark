from pyspark.sql import *


def session():
    spark = SparkSession.builder.master('local[3]').appName('coder1').getOrCreate()
    return spark


def data_creator(spark, data, column_names=False):
    data = spark.read.options(header=column_names).csv(data)
    return data


# start the session
spark = session()

# data file location
data_loc = r"C:\Users\peddi\PycharmProjects\pyspark_demo\Company_data.csv"
# header parameter
column_names = True
# function
create_data_creator = data_creator(spark, data_loc, column_names)

create_data_creator.show()
create_data_creator.printSchema()
create_data_creator.na.drop(how='all').show()