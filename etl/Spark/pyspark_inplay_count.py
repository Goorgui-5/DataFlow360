from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StringType, IntegerType

# Création session Spark
spark = SparkSession.builder \
    .appName("KafkaLiveMatchesCountINPLAY") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma des messages live_matches
schema = StructType() \
    .add("match_id", IntegerType()) \
    .add("competition", StringType()) \
    .add("date_heure", StringType()) \
    .add("equipe_dom", StringType()) \
    .add("equipe_ext", StringType()) \
    .add("score_dom", IntegerType()) \
    .add("score_ext", IntegerType()) \
    .add("statut", StringType())

# Lecture du topic Kafka live_matches
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "live_matches") \
    .option("startingOffsets", "latest") \
    .load()

# Transformation JSON → colonnes
matches = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 🔹 Filtrer uniquement les matchs en cours (IN_PLAY)
matches_inplay = matches.filter(col("statut") == "IN_PLAY")

# 🔹 Compter le nombre de matchs en cours par compétition
matches_count = matches_inplay.groupBy("competition").agg(count("*").alias("nb_matchs_en_cours"))

# Affichage en temps réel
query = matches_count.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
