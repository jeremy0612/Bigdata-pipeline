from pyspark.sql import SparkSession
from pyspark.sql.functions import when, to_date, sum
from pyspark.sql.functions import  col 
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    # Initialize the spark job
    spark = SparkSession \
        .builder \
        .master('local') \
        .appName('MyApp') \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .config('spark.mongodb.input.uri', 'mongodb://admin:password@mongo/stackoverflow?authSource=admin') \
        .config('spark.mongodb.output.uri', 'mongodb://admin:password@mongo/stackoverflow?authSource=admin') \
        .getOrCreate()

    answer_df = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option("collection","answers")\
        .load()

    #  Convert the datatype
    answer_df = answer_df.withColumn("CreationDate", to_date(answer_df.CreationDate, "yyyy-MM-dd'T'HH:mm:ss'Z'"))\
        .withColumn("OwnerUserId", answer_df.OwnerUserId.cast(IntegerType()))
    # Filter outliers
    answer_df = answer_df.withColumn("OwnerUserId", when(answer_df.OwnerUserId == "NA", None).otherwise(answer_df.OwnerUserId)).alias("answer")
    
    result = answer_df.groupBy("ParentId")\
        .count()\
        .select(col("ParentId").alias("Id"), col("count").alias("NUmber of Answers"))\
        .orderBy("Id", ascending=True)
    
    writer = result.write.format('csv')\
        .save(
            path="/usr/local/data/number_of_answers", 
            mode="overwrite",
            header="true"
        )


