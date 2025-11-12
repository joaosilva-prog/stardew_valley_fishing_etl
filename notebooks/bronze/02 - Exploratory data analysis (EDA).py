# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 1.1 - Análise exploratória dos dados
# MAGIC Com os nossos dados devidamente importados para dentro do Databricks, vamos agora fazer uma análise em cima destes dados brutos para conhecermos sua estrutura e nos preperarmos para a fase de tratamento posterior.

# COMMAND ----------

# Importando nossos dados como DataFrames diretamente do nosso ambiente Databricks.

df_crabpotandothercatchables_bronze = spark.read.table("stardew_project.bronze.crabpotandothercatchables")

df_fish_detail_bronze = spark.read.table("stardew_project.bronze.fish_detail")

df_fish_price_breakdown_bronze = spark.read.table("stardew_project.bronze.fish_price_breakdown")

df_legendary_fish_detail_bronze = spark.read.table("stardew_project.bronze.legendary_fish_detail")

df_legendaryfishII_bronze = spark.read.table("stardew_project.bronze.legendaryfishii")

df_nightmarketfish_bronze = spark.read.table("stardew_project.bronze.nightmarketfish")

df_villagers_bronze = spark.read.table("stardew_project.bronze.villagers")

df_behavior_bronze = spark.read.table("stardew_project.bronze.behavior")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploração visual dos dados
# MAGIC Vamos visualizar o conteúdo resumido de cada um dos DataFrames.

# COMMAND ----------

df_crabpotandothercatchables_bronze.display()

# COMMAND ----------

df_fish_detail_bronze.limit(5).display()

# COMMAND ----------

df_fish_price_breakdown_bronze.limit(5).display()

# COMMAND ----------

df_legendary_fish_detail_bronze.limit(5).display()

# COMMAND ----------

df_legendaryfishII_bronze.limit(5).display()

# COMMAND ----------

df_nightmarketfish_bronze.limit(5).display()

# COMMAND ----------

df_villagers_bronze.limit(5).display()

# COMMAND ----------

df_behavior_bronze.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploração de tipos dos dados
# MAGIC Vamos começar a identificar quais são os tipos das colunas dos dados presentes na nossa fonte de dados.

# COMMAND ----------

df_crabpotandothercatchables_bronze.printSchema()

df_fish_detail_bronze.printSchema()

df_fish_price_breakdown_bronze.printSchema()

df_legendary_fish_detail_bronze.printSchema()

df_legendaryfishII_bronze.printSchema()

df_nightmarketfish_bronze.printSchema()

df_villagers_bronze.printSchema()

df_behavior_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploração de estatísticas dos dados
# MAGIC Vamos visualizar algumas estatísticas rápidas acerca dos nossos dados.

# COMMAND ----------

df_crabpotandothercatchables_bronze.describe().show()

# COMMAND ----------

df_fish_detail_bronze.describe().show()

# COMMAND ----------

df_fish_price_breakdown_bronze.describe().show()

# COMMAND ----------

df_legendary_fish_detail_bronze.describe().show()

# COMMAND ----------

df_legendaryfishII_bronze.describe().show()

# COMMAND ----------

df_nightmarketfish_bronze.describe().show()

# COMMAND ----------

df_villagers_bronze.describe().show()

# COMMAND ----------

df_behavior_bronze.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Qualidade dos dados
# MAGIC Agora, vamos explorar um pouco mais a fundo a respeito da qualidade dos dados que possuímos para trabalhar, identificar possíveis colunas com valores nulos, duplicatas e/ou tipos de dados inconsistentes.

# COMMAND ----------

# Contando a quantidade de valores nulos por coluna em cada um dos DataFrames.
from pyspark.sql.functions import col
import pyspark.sql.functions as f

df_crabpotandothercatchables_nulls = df_crabpotandothercatchables_bronze.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_crabpotandothercatchables_bronze.columns]
)

df_fish_detail_bronze_nulls = df_fish_detail_bronze.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_fish_detail_bronze.columns]
)

df_fish_price_breakdown_nulls = df_fish_price_breakdown_bronze.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_fish_price_breakdown_bronze.columns]
)

df_legendary_fish_detail_nulls = df_legendary_fish_detail_bronze.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_legendary_fish_detail_bronze.columns]
)

df_legendaryfishII_nulls = df_legendaryfishII_bronze.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_legendaryfishII_bronze.columns]
)

df_nightmarketfish_nulls = df_nightmarketfish_bronze.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_nightmarketfish_bronze.columns]
)

df_villagers_nulls = df_villagers_bronze.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_villagers_bronze.columns]
)

df_behavior_nulls = df_behavior_bronze.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_behavior_bronze.columns]
)

display(df_crabpotandothercatchables_nulls)
display(df_fish_detail_bronze_nulls)
display(df_fish_price_breakdown_nulls)
display(df_legendary_fish_detail_nulls)
display(df_legendaryfishII_nulls)
display(df_nightmarketfish_nulls)
display(df_villagers_nulls)
display(df_behavior_nulls)

# COMMAND ----------

# Vamos contabilizar se temos duplicatas nos dados e se sim, quantas são.

df_crabpotandothercatchables_duplicates = df_crabpotandothercatchables_bronze.groupBy(*[c for c in df_crabpotandothercatchables_bronze.columns]).count().where("count > 1").withColumnRenamed("count", "Duplicates")

df_fish_detail_duplicates = df_fish_detail_bronze.groupBy(*[c for c in df_fish_detail_bronze.columns]).count().where("count > 1").withColumnRenamed("count", "Duplicates")

df_fish_price_breakdown_duplicates = df_fish_price_breakdown_bronze.groupBy(*[c for c in df_fish_price_breakdown_bronze.columns]).count().where("count > 1").withColumnRenamed("count", "Duplicates")

df_legendary_fish_detail_duplicates = df_legendary_fish_detail_bronze.groupBy(*[c for c in df_legendary_fish_detail_bronze.columns]).count().where("count > 1").withColumnRenamed("count", "Duplicates")

df_legendaryfishII_duplicates = df_legendaryfishII_bronze.groupBy(*[c for c in df_legendaryfishII_bronze.columns]).count().where("count > 1").withColumnRenamed("count", "Duplicates")

df_nightmarketfish_duplicates = df_nightmarketfish_bronze.groupBy(*[c for c in df_nightmarketfish_bronze.columns]).count().where("count > 1").withColumnRenamed("count", "Duplicates")

df_villagers_duplicates = df_villagers_bronze.groupBy(*[c for c in df_villagers_bronze.columns]).count().where("count > 1").withColumnRenamed("count", "Duplicates")

df_behavior_duplicates = df_behavior_bronze.groupBy(*[c for c in df_behavior_bronze.columns]).count().where("count > 1").withColumnRenamed("count", "Duplicates")

display(df_crabpotandothercatchables_duplicates)
display(df_fish_detail_duplicates)
display(df_fish_price_breakdown_duplicates)
display(df_legendary_fish_detail_duplicates)
display(df_legendaryfishII_duplicates)
display(df_nightmarketfish_duplicates)
display(df_villagers_duplicates)
display(df_behavior_duplicates)