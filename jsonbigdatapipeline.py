import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

## arraytype and structtype represents json
## So keep transforming de json until there is no arraytye or structype
## array is transformed by using explode function
## structure is transformed by using "."
def readjson(df):
    column_list = []
    for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name,explode(column_name))
            column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)
    df = df.select(column_list)
    return df
    
## transformation functions that takes json and converts to csv
def transformation_json(df):
  readjson_flag = True
  while readjson_flag:
    df = readjson(df);
    readjson_flag = False
    for column_name in df.schema.names:
      if isinstance(df.schema[column_name].dataType, ArrayType):
        readjson_flag = True
      elif isinstance(df.schema[column_name].dataType, StructType):
        readjson_flag = True;
  return df;

#Main function that is going to be called when the glue job is triggered by lambda
def main():
    ## read arguments received from aws lambda and save them in separated variables
    args = getResolvedOptions(sys.argv, ["FILENAME","BUCKETNAME"])
    file_name=args['FILENAME']
    bucket_name=args['BUCKETNAME']
    print("Bucket Name" , bucket_name)
    print("File Name" , file_name)

    ## compute input file path which is sourcebigdata
    input_file_path="s3a://{}/{}".format(bucket_name,file_name)
    print("Input File Path : ",input_file_path);
    df = spark.read.option("multiline", True).option("inferSchema", False).json(input_file_path)

    ## calling the transformation function that takes json and transform to csv
    df1=transformation_json(df)
    df1.coalesce(1).write.format("csv").option("header", "true").save("s3a://destinationbigdata/{}".format(file_name.split('.')[0]))

main()
