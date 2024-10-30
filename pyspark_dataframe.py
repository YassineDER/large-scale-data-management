from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder \
    .appName("PageRankProject") \
    .getOrCreate()

data = spark.read.text("public_lddm_data/small_page_links.nt")
data_transformed = data.select(
    regexp_extract('value', r'<(http://dbpedia.org/resource/[^>]+)>', 1).alias('source'),
    regexp_extract('value', r'<http://dbpedia.org/property/wikilink> <(http://dbpedia.org/resource/[^>]+)>', 1).alias('destination')
)

rows = data_transformed.take(5)
for row in rows:
    print("Row: " + str(row))
