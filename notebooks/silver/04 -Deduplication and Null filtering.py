# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 3 - Limpeza dos Dados
# MAGIC Depois dos nomes e tipos dos dados das colunas ajustados, vamos começar a realizar processos de limpezas nos dados, lidando com valores nulos e com dados duplicados.

# COMMAND ----------

# DBTITLE 1,Importação dos Dados
# Importando nossos dados como DataFrames para trabalharmos com os dados ausentes e duplicados.

df_crabpotandothercatchables_silver = spark.read.table("stardew_project.silver.crabpotandothercatchables")

df_fish_detail_silver = spark.read.table("stardew_project.silver.fish_detail")

df_fish_price_breakdown_silver = spark.read.table("stardew_project.silver.fish_price_breakdown")

df_legendary_fish_detail_silver = spark.read.table("stardew_project.silver.legendary_fish_detail")

df_legendary_fish_price_breakdown_silver = spark.read.table("stardew_project.silver.legendary_fish_price_breakdown")

df_legendary_fishII_detail_silver = spark.read.table("stardew_project.silver.legendary_fishii_detail")

df_nightmarketfish_silver = spark.read.table("stardew_project.silver.nightmarketfish")

df_villagers_silver = spark.read.table("stardew_project.silver.villagers")

df_behavior_silver = spark.read.table("stardew_project.silver.behavior")

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções auxiliares
# MAGIC Import das funções auxiliares que irão realizar o processo de padronização posterior aplicado aos DataFrames.

# COMMAND ----------

# DBTITLE 1,Import das Funções Auxiliares
# MAGIC %run /Workspace/Users/sjoao5498@gmail.com/stardew_valley_fishing_etl/utils/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Limpando dados: df_crabpotandothercatchables_silver

# COMMAND ----------

# DBTITLE 1,df_crabpotandothercatchables_silver
# Tratamento de nulos e deduplicação dos dados.

df_crabpotandothercatchables_silver = clean_nulls(df_crabpotandothercatchables_silver)
df_crabpotandothercatchables_silver = df_crabpotandothercatchables_silver.withColumn("used_in", f.regexp_replace(col("used_in"), '"', ""))
df_crabpotandothercatchables_silver = df_crabpotandothercatchables_silver.dropDuplicates(["name"])

df_crabpotandothercatchables_silver.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_crabpotandothercatchables_silver.columns]
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Limpando dados: df_fish_detail_silver

# COMMAND ----------

# DBTITLE 1,df_fish_detail_silver
# Tratamento de nulos e deduplicação dos dados.

df_fish_detail_silver = clean_nulls(df_fish_detail_silver)
df_fish_detail_silver = df_fish_detail_silver.withColumn("used_in", f.regexp_replace(col("used_in"), '"', "")) \
    .withColumn("description", f.regexp_replace(col("description"), '"', ""))
df_fish_detail_silver = df_fish_detail_silver.dropDuplicates(["name"])

df_fish_detail_silver.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_fish_detail_silver.columns]
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Limpando dados: df_fish_price_breakdown_silver

# COMMAND ----------

# DBTITLE 1,df_fish_price_breakdown_silver
# Tratamento de nulos e deduplicação dos dados.

df_fish_price_breakdown_silver = clean_nulls(df_fish_price_breakdown_silver)
df_fish_price_breakdown_silver = df_fish_price_breakdown_silver.dropDuplicates(["name"])

df_fish_price_breakdown_silver.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_fish_price_breakdown_silver.columns]
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Limpando dados: df_legendary_fish_detail_silver

# COMMAND ----------

# DBTITLE 1,df_legendary_fish_detail_silver
# Tratamento de nulos e deduplicação dos dados.

df_legendary_fish_detail_silver = clean_nulls(df_legendary_fish_detail_silver)
df_legendary_fish_detail_silver = df_legendary_fish_detail_silver.dropDuplicates(["name"])

df_legendary_fish_detail_silver.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_legendary_fish_detail_silver.columns]
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Limpando dados: df_legendary_fish_price_breakdown_silver

# COMMAND ----------

# DBTITLE 1,df_legendary_fish_price_breakdown_silver
# Tratamento de nulos e deduplicação dos dados.

df_legendary_fish_price_breakdown_silver = clean_nulls(df_legendary_fish_price_breakdown_silver)
df_legendary_fish_price_breakdown_silver = df_legendary_fish_price_breakdown_silver.dropDuplicates(["name"])

df_legendary_fish_price_breakdown_silver.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_legendary_fish_price_breakdown_silver.columns]
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Limpando dados: df_legendary_fishII_detail_silver

# COMMAND ----------

# DBTITLE 1,df_legendary_fishII_detail_silver
# Tratamento de nulos e deduplicação dos dados.

df_legendary_fishII_detail_silver = clean_nulls(df_legendary_fishII_detail_silver)
df_legendary_fishII_detail_silver = df_legendary_fishII_detail_silver.dropDuplicates(["name"])

df_legendary_fishII_detail_silver.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_legendary_fishII_detail_silver.columns]
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Limpando dados: df_nightmarketfish_silver

# COMMAND ----------

# DBTITLE 1,df_nightmarketfish_silver
# Tratamento de nulos e deduplicação dos dados.

df_nightmarketfish_silver = clean_nulls(df_nightmarketfish_silver)
df_nightmarketfish_silver = df_nightmarketfish_silver.dropDuplicates(["name"])

df_nightmarketfish_silver.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_nightmarketfish_silver.columns]
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Limpando dados: df_villagers_silver

# COMMAND ----------

# DBTITLE 1,df_villagers_silver
# Tratamento de nulos e deduplicação dos dados.

df_villagers_silver = clean_nulls(df_villagers_silver)
df_villagers_silver = df_villagers_silver.dropDuplicates(["name"])

df_villagers_silver.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_villagers_silver.columns]
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Limpando dados: df_behavior_silver

# COMMAND ----------

# DBTITLE 1,df_behavior_silver
# Tratamento de nulos e deduplicação dos dados.

df_behavior_silver = clean_nulls(df_behavior_silver)
df_behavior_silver = df_behavior_silver.dropDuplicates(["behavior"])

df_behavior_silver.agg(*
    [f.sum(f.when(col(c).isNull(), 1).otherwise(0)).alias(c) 
        for c in df_behavior_silver.columns]
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Salvando os dados na camada Silver
# MAGIC Depois das colunas deduplicadas e com nulos padronizados, podemos salvar nossos DataFrames na camada Silver para que prossigamos fazendo refinamentos posteriormente, garantindo o acesso aos novos dados modificados de forma segura.

# COMMAND ----------

# DBTITLE 1,crabpotandothercatchables
try:
    save_df(df_crabpotandothercatchables_silver, "silver", "crabpotandothercatchables", "name")
except Exception as e:
    print("Erro ao salvar a tabela crabpotandothercatchables: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,fish_detail
try:
    save_df(df_fish_detail_silver, "silver", "fish_detail", "name")
except Exception as e:
    print("Erro ao salvar a tabela fish_detail: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,fish_price_breakdown
try:
    save_df(df_fish_price_breakdown_silver, "silver", "fish_price_breakdown", "name")
except Exception as e:
    print("Erro ao salvar a tabela fish_price_breakdown: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,legendary_fish_detail
try:
    save_df(df_legendary_fish_detail_silver, "silver", "legendary_fish_detail", "name")
except Exception as e:
    print("Erro ao salvar a tabela legendary_fish_detail: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,legendary_fish_price_breakdown
try:
    save_df(df_legendary_fish_price_breakdown_silver, "silver", "legendary_fish_price_breakdown", "name")
except Exception as e:
    print("Erro ao salvar a tabela legendary_fish_price_breakdown: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,legendary_fishII_detail
try:
    save_df(df_legendary_fishII_detail_silver, "silver", "legendary_fishII_detail", "name")
except Exception as e:
    print("Erro ao salvar a tabela legendary_fishII_detail: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,nightmarketfish
try:
    save_df(df_nightmarketfish_silver, "silver", "nightmarketfish", "name")
except Exception as e:
    print("Erro ao salvar a tabela nightmarketfish: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,villagers
try:
    save_df(df_villagers_silver, "silver", "villagers", "name")
except Exception as e:
    print("Erro ao salvar a tabela villagers: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,behavior
try:
    save_df(df_behavior_silver, "silver", "behavior", "behavior")
except Exception as e:
    print("Erro ao salvar a tabela behavior: ", e)
    raise
