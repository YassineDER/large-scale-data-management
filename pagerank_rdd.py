import re, sys, time
from operator import add
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel


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
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    input_file = sys.argv[1]
    num_iterations = int(sys.argv[2])
    with_partition = sys.argv[3].lower() == "true"

    # Initialiser Spark
    spark = SparkSession.builder.appName("PythonPageRank").getOrCreate()

    # Charger le fichier d'entrée sous forme de RDD brut.
    lines = spark.sparkContext.textFile(input_file)
    # Créer un RDD de liens (source, destination) et les regrouper par source
    links = lines.map(parseNeighbors).distinct().groupByKey().cache()

    # Initialiser les rangs des URLs à 1.0 ; les URLs sans lien entrant seront exclus
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    if with_partition:
        numPartitions = 4
        lines_partitioned = lines.partitionBy(numPartitions).glom()
        links_partitioned = links.partitionBy(numPartitions).glom()
        ranks_partitioned = ranks.partitionBy(numPartitions).glom()

    # Effectuer les itérations de PageRank
    for iteration in range(num_iterations):
        # Calculer les contributions des URL pour chaque voisin
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])
        )

        # Recalculer les rangs avec une pondération de 0.85
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Récupérer et afficher les résultats
    for (link, rank) in ranks.collect():
        print(f"{link} a un PageRank de : {rank:.4f}")


    start_time = spark.sparkContext.startTime
    end_time = int(time.time_ns() / 1000000)
    print(f"Temps d'exécution : {(end_time - start_time) / 1000:.2f} secondes")
    spark.stop()

