import re, sys, time
from operator import add
from pyspark.sql import SparkSession


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

    # Charger le fichier d'entrée sous forme de RDD brut.
    lines = spark.sparkContext.textFile(input_file)
    # Créer un RDD de liens (source, destination) et les regrouper par source
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    # Initialiser les rangs des URLs à 1.0 ; les URLs sans lien entrant seront exclus
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    if with_partition:
        numPartitions = 4
        lines = lines.partitionBy(numPartitions).glom()
        links = links.partitionBy(numPartitions).glom()
        ranks = ranks.partitionBy(numPartitions).glom()

    # Effectuer les itérations de PageRank
    start_time = time.time()
    for iteration in range(num_iterations):
        # Calculer les contributions des URL pour chaque voisin
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])
        )

        # Recalculer les rangs avec une pondération de 0.85
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: (rank * 0.85) + 0.15)

    # ranks.saveAsTextFile(output_path)

    # L'entité de rank la plus élevée
    max_rank = ranks.max(key=lambda rank_tuple: rank_tuple[1])
    result = "Max PageRank entity is : " + str(max_rank)

    # Calcul du temps d'exécution
    end_time = time.time()
    execution_time = str(end_time - start_time)

    # Sauvegarde du résultat dans un txt, dans output_path
    with open(output_path + "result.txt", "r") as f:
        f.write(result + "\n")
        f.write("Execution Time : " + execution_time + " seconds\n")

    spark.stop()

