from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("WordCountDockerTest") \
    .getOrCreate()

text = spark.read.text("input.txt")

words = (
    text
    .selectExpr("explode(split(value, ' ')) as word")
    .groupBy("word")
    .count()
)

words.show()

spark.stop()
