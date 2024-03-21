# import types
import pyspark
from pyspark.sql import SparkSession, types
from pyspark.sql import functions as F

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()
spark.sparkContext.setLogLevel("OFF")
    
green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .load()
    

def peek(mini_batch, batch_id):
    first_row = mini_batch.take(1)

    if first_row:
        print(first_row[0])


schema = types.StructType([
    types.StructField("lpep_pickup_datetime", types.StringType(), True),
    types.StructField("lpep_dropoff_datetime", types.StringType(), True),
    types.StructField("PULocationID", types.IntegerType(), True),
    types.StructField("DOLocationID", types.IntegerType(), True),
    types.StructField("trip_distance", types.FloatType(), True),    
    types.StructField("tip_amount", types.FloatType(), True),
])
    
# query = green_stream.writeStream.foreachBatch(peek).start()


green_stream = green_stream \
  .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
  .select("data.*")


# add a column of timestamp 5 mis
query = green_stream.withColumn("timestamp", F.current_timestamp()) \
    .groupBy(F.window("timestamp", "5 minutes"), "DOLocationID").count().sort("count", ascending=False)
# write a query to print one row of the stream
# query = green_stream.writeStream.foreachBatch(peek).start()

query = query.writeStream.outputMode("complete").format("console").start()
# query = green_stream.WriteStream.foreachBatch(peek).start()
query.awaitTermination()
# green_stream.printSchema()