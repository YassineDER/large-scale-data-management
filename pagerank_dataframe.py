import re, sys, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def computeContribs(urls, rank):
    """Calcule les contributions de rang pour les voisins."""
    num_urls = len(urls)
    for url in urls:
        yield url, rank / num_urls


def parseNeighbors(urls):
    """Analyse une paire d'URLs et retourne (source, destination)."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[2]


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: pagerank <file> <iterations> <with_partition: \"true\" || \"false\"> <output_path>", file=sys.stderr)
        sys.exit(-1)

    input_file = sys.argv[1]
    num_iterations = int(sys.argv[2])
    with_partition = sys.argv[3].lower() == "true"
    output_path = sys.argv[4]

    # Initialiser Spark
    spark = SparkSession.builder.appName("PythonPageRank").getOrCreate()

    df = spark.read.text(input_file)
    # Créer un DataFrame de liens (source, destination) et les regrouper par source
    links_df = df.select(
        regexp_extract('value', r'<([^>]+)>', 1).alias('source'),
        regexp_extract('value', r'<[^>]+> <([^>]+)>', 1).alias('destination')
    ).distinct()

    # Initialiser les rangs
    ranks_df = links_df.select("source").distinct().withColumn("rank", lit(1.0))

    if with_partition:
        numPartitions = 4
        links_df = links_df.repartition(numPartitions)
        ranks_df = ranks_df.repartition(numPartitions)

    # Effectuer les itérations de PageRank
    start_time = time.time()
    for iteration in range(num_iterations):
        # Calculer les contributions des URL pour chaque voisin
        contribs_df = links_df.join(ranks_df, links_df.source == ranks_df.source) \
            .select("destination", (col("rank") / count("destination")).alias("contrib")) \
            .groupBy("destination").agg(sum("contrib").alias("rank"))

        # Recalculer les rangs avec une pondération de 0.85
        ranks_df = contribs_df.withColumn("rank", col("rank") * 0.85 + 0.15)

    ranks_df.write.format("text").save(output_path)

    # L'entité de rank la plus élevée
    result = "Max PageRank entity is : %s (%s)" % ranks_df.orderBy(col("rank").desc()).first()

    # Calcul du temps d'exécution
    end_time = time.time()
    execution_time = end_time - start_time

    # Sauvegarde du résultat dans un txt, dans output_path
    with open(output_path + "/result.txt", "w") as f:
        f.write(result + "\n")
        f.write("Execution Time : " + str(execution_time) + " seconds\n")

    spark.stop()

