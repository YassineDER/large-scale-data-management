import findspark
from pyspark.sql import SparkSession

findspark.init()

# Créer une session Spark
spark = SparkSession.builder \
    .master("local") \
    .appName("PageRankProject") \
    .config("spark.ui.port", "4050") \
    .getOrCreate()

# Lire un fichier .nt
data = spark.read.text("public_lddm_data/small_page_links.nt")
rows = data.take(5)
for row in rows:
    print("Row: %s" % row)
