# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 1 - Ingestão de Dados
# MAGIC Neste primeiro notebook introdutório faremos a ingestão dos dados como Dataframes PySpark, e os salvaremos no Databricks como Delta tables para que possamos trabalhar em cima deles.

# COMMAND ----------

# Definição das variáveis de caminho dos arquivos raw.

crabpotandothercatchables = "/Volumes/stardew_project/raw/raw_csvs/crabpotandothercatchables.csv"
fish_detail = "/Volumes/stardew_project/raw/raw_csvs/fish_detail.csv"
fish_price_breakdown = "/Volumes/stardew_project/raw/raw_csvs/fish_price_breakdown.csv"
legendary_fish_detail = "/Volumes/stardew_project/raw/raw_csvs/legendary_fish_detail.csv"
legendary_fish_price_breakdown = "/Volumes/stardew_project/raw/raw_csvs/legendary_fish_price_breakdown.csv"
legendaryfishII = "/Volumes/stardew_project/raw/raw_csvs/legendaryfishII.csv"
nightmarketfish = "/Volumes/stardew_project/raw/raw_csvs/nightmarketfish.csv"
villagers = "/Volumes/stardew_project/raw/raw_csvs/villagers.csv"

# COMMAND ----------

# Leitura dos arquivos CSV raw em Spark Dataframes.

df_crabpotandothercatchables = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(crabpotandothercatchables)

df_fish_detail = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(fish_detail)

df_fish_price_breakdown = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(fish_price_breakdown)

df_legendary_fish_detail = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(legendary_fish_detail)

df_legendaryfishII = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(legendaryfishII)

df_nightmarketfish = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(nightmarketfish)

df_villagers = spark.read.option("header", "true") \
  .option("inferSchema", "true") \
    .csv(villagers)

# COMMAND ----------

# Salvar os arquivos raw como Delta Tables para que possamos manipulá-los posteriormente.

try:
    df_crabpotandothercatchables.write.mode("overwrite").option("delta.columnMapping.mode", "name").saveAsTable("stardew_project.bronze.crabpotandothercatchables")

    df_fish_detail.write.mode("overwrite").option("delta.columnMapping.mode", "name").saveAsTable("stardew_project.bronze.fish_detail")

    df_fish_price_breakdown.write.mode("overwrite").option("delta.columnMapping.mode", "name").saveAsTable("stardew_project.bronze.fish_price_breakdown")

    df_legendary_fish_detail.write.mode("overwrite").option("delta.columnMapping.mode", "name").saveAsTable("stardew_project.bronze.legendary_fish_detail")

    df_legendaryfishII.write.mode("overwrite").option("delta.columnMapping.mode", "name").saveAsTable("stardew_project.bronze.legendaryfishII")

    df_nightmarketfish.write.mode("overwrite").option("delta.columnMapping.mode", "name").saveAsTable("stardew_project.bronze.nightmarketfish")

    df_villagers.write.mode("overwrite").option("delta.columnMapping.mode", "name").saveAsTable("stardew_project.bronze.villagers")
except Exception as e:
    print("Erro ao salvar tabela.", e)

# COMMAND ----------

