# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 1 - Ingestão de Dados
# MAGIC Neste primeiro notebook introdutório faremos a ingestão dos dados como Dataframes PySpark, e os salvaremos no Databricks como Delta tables para que possamos trabalhar posteriormente em cima dos dados.

# COMMAND ----------

# DBTITLE 1,Raw paths
# Definição das variáveis de caminho dos arquivos raw.

crabpotandothercatchables = "/Volumes/stardew_project/raw/raw_csvs/crabpotandothercatchables.csv"

fish_detail = "/Volumes/stardew_project/raw/raw_csvs/fish_detail.csv"

fish_price_breakdown = "/Volumes/stardew_project/raw/raw_csvs/fish_price_breakdown.csv"

legendary_fish_detail = "/Volumes/stardew_project/raw/raw_csvs/legendary_fish_detail.csv"

legendary_fish_price_breakdown = "/Volumes/stardew_project/raw/raw_csvs/legendary_fish_price_breakdown.csv"

legendaryfishII = "/Volumes/stardew_project/raw/raw_csvs/legendaryfishII.csv"

nightmarketfish = "/Volumes/stardew_project/raw/raw_csvs/nightmarketfish.csv"

villagers = "/Volumes/stardew_project/raw/raw_csvs/villagers.csv"

behavior = "/Volumes/stardew_project/raw/raw_csvs/behavior.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções auxiliares
# MAGIC Import das funções auxiliares que irão realizar o processo de salvamento na camada Bronze dos DataFrames.

# COMMAND ----------

# DBTITLE 1,Importação das Funções Auxiliares
# MAGIC %run /Workspace/Users/sjoao5498@gmail.com/stardew_valley_fishing_etl/utils/Functions

# COMMAND ----------

# DBTITLE 1,Importação dos Dados
# Leitura dos arquivos CSV raw em Spark Dataframes.

df_crabpotandothercatchables_raw = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(crabpotandothercatchables) \
      .withColumn("ingestion_timestamp", current_timestamp())

df_fish_detail_raw = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(fish_detail) \
      .withColumn("ingestion_timestamp", current_timestamp())

df_fish_price_breakdown_raw = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(fish_price_breakdown) \
      .withColumn("ingestion_timestamp", current_timestamp())

df_legendary_fish_detail_raw = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(legendary_fish_detail) \
      .withColumn("ingestion_timestamp", current_timestamp())

df_legendary_fish_price_breakdown_raw = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(legendary_fish_price_breakdown) \
      .withColumn("ingestion_timestamp", current_timestamp())

df_legendaryfishII_raw = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(legendaryfishII) \
      .withColumn("ingestion_timestamp", current_timestamp())

df_nightmarketfish_raw = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(nightmarketfish) \
      .withColumn("ingestion_timestamp", current_timestamp())

df_villagers_raw = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(villagers) \
      .withColumn("ingestion_timestamp", current_timestamp())

df_behavior_raw = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(behavior) \
      .withColumn("ingestion_timestamp", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC # Salvando os dados na camada Bronze
# MAGIC Depois de fazermos a ingestão inicial dos dados, podemos salvar nossos DataFrames na camada Bronze para que prossigamos fazendo refinamentos posteriormente na camada Silver, garantindo o acesso aos dados de forma segura.

# COMMAND ----------

# DBTITLE 1,crabpotandothercatchables
try:
    save_df(df_crabpotandothercatchables_raw, "bronze", "crabpotandothercatchables", "name")
except Exception as e:
    print("Erro ao salvar a tabela crabpotandothercatchables: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,fish_detail
try:
    save_df(df_fish_detail_raw, "bronze", "fish_detail", "name")
except Exception as e:
    print("Erro ao salvar a tabela fish_detail: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,fish_price_breakdown
try:
    save_df(df_fish_price_breakdown_raw, "bronze", "fish_price_breakdown", "name")
except Exception as e:
    print("Erro ao salvar a tabela fish_price_breakdown: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,legendary_fish_detail
try:
    save_df(df_legendary_fish_detail_raw, "bronze", "legendary_fish_detail", "name")
except Exception as e:
    print("Erro ao salvar a tabela legendary_fish_detail: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,legendary_fish_price_breakdown
try:
    save_df(df_legendary_fish_price_breakdown_raw, "bronze", "legendary_fish_price_breakdown", "name")
except Exception as e:
    print("Erro ao salvar a tabela legendary_fish_price_breakdown: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,legendaryfishii
try:
    save_df(df_legendaryfishII_raw, "bronze", "legendaryfishii", "name")
except Exception as e:
    print("Erro ao salvar a tabela legendary_fishII_detail: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,nightmarketfish
try:
    save_df(df_nightmarketfish_raw, "bronze", "nightmarketfish", "name")
except Exception as e:
    print("Erro ao salvar a tabela nightmarketfish: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,villagers
try:
    save_df(df_villagers_raw, "bronze", "villagers", "name")
except Exception as e:
    print("Erro ao salvar a tabela villagers: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,behavior
try:
    save_df(df_behavior_raw, "bronze", "behavior", "behavior")
except Exception as e:
    print("Erro ao salvar a tabela behavior: ", e)
    raise
