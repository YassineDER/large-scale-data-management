import re, time, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

if len(sys.argv) != 5:
    print("Usage: python3 pagerank_dataframe.py <data_input_file> <iterations> <with_partition: \"true\" || \"false\"> <output_path>", file=sys.stderr)
    sys.exit(-1)

input_file = sys.argv[1]
num_iterations = int(sys.argv[2])
with_partition = sys.argv[3].lower() == "true"
output_path = sys.argv[4]

def computeContribs(urls, rank):
    """Calcule les contributions de rang pour les voisins."""
    num_urls = len(urls)
    for url in urls:
        yield url, rank / num_urls


def parseNeighbors(urls):
    """Analyse une paire d'URLs et retourne (source, destination)."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[2]

# Initialiser Spark
spark = SparkSession.builder.appName("PythonPageRank").getOrCreate()
df = spark.read.text(input_file)

# Créer un DataFrame de liens (source, destination) et les regrouper par source
links = df.rdd.map(lambda urls: parseNeighbors(urls[0])).distinct().groupByKey().mapValues(list)
links_df = links.toDF(["url", "neighbors"])

ranks_df = links_df.select("url").withColumn("rank", lit(1.0))

if with_partition:
    links_df = links_df.repartition("url")

start_time = time.time()

# Algorithme PageRank
for i in range(num_iterations):
    contribs = links_df.join(ranks_df, "url") \
        .select("url", "neighbors", "rank") \
        .rdd.flatMap(lambda url_neighbors_rank: [(neighbor, url_neighbors_rank[2] / len(url_neighbors_rank[1])) for neighbor in url_neighbors_rank[1]]) \
        .reduceByKey(lambda a, b: a + b)

    contribs_df = contribs.toDF(["url", "contrib"])
    ranks_df = contribs_df.withColumn("rank", (col("contrib") * 0.85 + 0.15)).select("url", "rank")

    # Saving the ranks to a text file
ranks_df.orderBy("rank", ascending=False) \
    .rdd.map(lambda row: f"{row.url} has rank: {row.rank}") \
    .coalesce(1) \
    .saveAsTextFile(output_path)

# L'entitée avec le plus haut rang
highest_rank = ranks_df.orderBy("rank", ascending=False).first()

# Calcul du temps d'exécution
end_time = time.time()

# Arrêter la session Spark
spark.stop()
execution_time = end_time - start_time
print(f"Temps d'exécution : {execution_time} secondes")
print(f"Entité avec le plus haut rang : {highest_rank.url} avec un rang de {highest_rank.rank}")