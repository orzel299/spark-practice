import sys
from operator import add
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, TimestampType

from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("LastFmAnalysis")\
        .getOrCreate()

    schema = StructType() \
	.add("userid", StringType(), False) \
	.add("timestamp", TimestampType(), False) \
	.add("artist_id", StringType(), True) \
	.add("artist_name", StringType(), False) \
	.add("track_id", StringType(), True) \
	.add("track_name", StringType(), False)   

    df_usage = spark.read.format("csv") \
	.option("delimiter", "\t") \
	.schema(schema) \
	.load("/data/userid-timestamp-artid-artname-traid-traname.tsv")

    
    wind_usr = Window.orderBy("timestamp").partitionBy("userid")
    wind_sess  = Window.partitionBy("userid", "sessionid")

    df_with_sess_dur = df_usage \
	.withColumn("delta", (F.col("timestamp").cast("long")-F.lag("timestamp", 1).over(wind_usr).cast("long"))/60)\
	.withColumn("tempid", F.when(F.col("delta") > 20, 1).otherwise(0)) \
	.withColumn("sessionid", F.sum("tempid").over(wind_usr)) \
	.drop("tempid") \
	.withColumn("sessionDuration", (F.max("timestamp").over(wind_sess).cast("long") - F.min("timestamp").over(wind_sess).cast("long"))/60)
	
    df_longest_sess = df_with_sess_dur \
	.select("userid", "sessionid", "sessionDuration") \
	.distinct() \
	.withColumn("rank", F.rank().over(Window.orderBy(F.col("sessionDuration").desc()))) \
	.filter(F.col("rank") <= 50) \
	.select("userid", "sessionid")

    df_result = df_with_sess_dur \
	.join(df_longest_sess, ["userid", "sessionid"], "inner") \
	.groupBy("artist_name", "track_name") \
	.count().alias("count") \
	.withColumn("rank", F.dense_rank().over(Window.orderBy(F.col("count").desc()))) \
	.filter(F.col("rank")<=10) \
	.orderBy("rank")

    df_result.write.option("sep","\t").option("header", "true").csv("/result/result.csv", "overwrite")

    spark.stop()
