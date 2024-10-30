from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PageRankProject") \
    .getOrCreate()

context = spark.sparkContext
data = spark.sparkContext.textFile("public_lddm_data/small_page_links.nt")

# Transform the data into a more readable format
tdd_transformed = data.map(lambda x: (x.split()[0], x.split()[2]))

rows = data.take(5)
for row in rows:
    print("Row: " + str(row))
