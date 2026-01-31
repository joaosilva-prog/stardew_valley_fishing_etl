# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 2 - Padronização de colunas e tipos (schema)
# MAGIC Agora que já possuímos os dados armazenados como Delta tables internamente e já realizamos uma etapa prévia de EDA (Análise Exploratória), iremos iniciar o tratamento da camada Silver padronizando tipos, nomes e dados das colunas.

# COMMAND ----------

# DBTITLE 1,Importação dos Dados
# Importando nossos dados como DataFrames para trabalharmos com as colunas e seus dados.

df_crabpotandothercatchables_silver = spark.read.table("stardew_project.bronze.crabpotandothercatchables")

df_fish_detail_silver = spark.read.table("stardew_project.bronze.fish_detail")

df_fish_price_breakdown_silver = spark.read.table("stardew_project.bronze.fish_price_breakdown")

df_legendary_fish_detail_silver = spark.read.table("stardew_project.bronze.legendary_fish_detail")

df_legendary_fish_price_breakdown_silver = spark.read.table("stardew_project.bronze.legendary_fish_price_breakdown")

df_legendaryfishII_silver = spark.read.table("stardew_project.bronze.legendaryfishii")

df_nightmarketfish_silver = spark.read.table("stardew_project.bronze.nightmarketfish")

df_villagers_silver = spark.read.table("stardew_project.bronze.villagers")

df_behavior_silver = spark.read.table("stardew_project.bronze.behavior")

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções auxiliares
# MAGIC Import das funções auxiliares que irão realizar o processo de padronização posterior aplicado aos DataFrames.

# COMMAND ----------

# DBTITLE 1,Import das Funções Auxiliares
# MAGIC %run /Workspace/Users/sjoao5498@gmail.com/stardew_valley_fishing_etl/utils/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_crabpotandothercatchables_silver

# COMMAND ----------

# DBTITLE 1,df_crabpotandothercatchables_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_crabpotandothercatchables_silver = rename_cols(df_crabpotandothercatchables_silver)
crab_origin_cols = df_crabpotandothercatchables_silver.columns

# COMMAND ----------

# DBTITLE 1,df_crabpotandothercatchables_silver
# Padronização de tipos e desempacotamento (adição) de colunas.

crabpot_split = {
    "trap_chance_non_mariner": "trap_chance_non_mariner",
    "trap_chance_mariner": "trap_chance_mariner"
}

df_crabpotandothercatchables_silver = split_col(df_crabpotandothercatchables_silver, crabpot_split, 0, "%", "integer")
df_crabpotandothercatchables_silver = min_and_max(df_crabpotandothercatchables_silver, "size_inches", "-", "integer", True)
df_crabpotandothercatchables_silver = reordering_df(df_crabpotandothercatchables_silver, crab_origin_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_fish_detail_silver

# COMMAND ----------

# DBTITLE 1,df_fish_detail_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_fish_detail_silver = rename_cols(df_fish_detail_silver)
df_fish_detail_silver = normalize_column(df_fish_detail_silver, "time")
fishdetail_origin_cols = df_fish_detail_silver.columns

# COMMAND ----------

# DBTITLE 1,df_fish_detail_silver
# Padronização de tipos e desempacotamento (adição) de colunas.

df_fish_detail_silver = split_col(df_fish_detail_silver, {"difficulty_behavior": "difficulty"}, 0, " ", "integer")
df_fish_detail_silver = split_col(df_fish_detail_silver, {"difficulty_behavior": "behavior"}, 1, " ", "string").drop("difficulty_behavior")
df_fish_detail_silver = min_and_max(df_fish_detail_silver, "size_inches", "-", "integer", True)
df_fish_detail_silver = split_time(df_fish_detail_silver, "time", "-", True)
df_fish_detail_silver = reordering_df(df_fish_detail_silver, fishdetail_origin_cols)
df_fish_detail_silver = df_fish_detail_silver.withColumn("time_min", 
    f.when(f.col("time_min") == "Any", "Anytime").otherwise(f.col("time_min"))
)
df_fish_detail_silver = df_fish_detail_silver.withColumn("time_max", 
    f.when(f.col("time_max") == "Any", "Anytime").otherwise(f.col("time_max"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_fish_price_breakdown_silver

# COMMAND ----------

# DBTITLE 1,df_fish_price_breakdown_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_fish_price_breakdown_silver = rename_cols(df_fish_price_breakdown_silver)
fish_price_breakdown_origin_cols = df_fish_price_breakdown_silver.columns

# COMMAND ----------

# DBTITLE 1,df_fish_price_breakdown_silver
# Padronização de tipos e desempacotamento (adição) de colunas.

df_fish_price_breakdown_silver = normalize_prices(df_fish_price_breakdown_silver, ["name", "ingestion_timestamp"])
df_fish_price_breakdown_silver = reordering_df(df_fish_price_breakdown_silver, fish_price_breakdown_origin_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_legendary_fish_detail_silver

# COMMAND ----------

# DBTITLE 1,df_legendary_fish_detail_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_legendary_fish_detail_silver = rename_cols(df_legendary_fish_detail_silver)
df_legendary_fish_detail_silver = normalize_column(df_legendary_fish_detail_silver, "time")
legendary_fish_detail_origin_cols = df_legendary_fish_detail_silver.columns

# COMMAND ----------

# DBTITLE 1,df_legendary_fish_detail_silver
# Padronização de tipos e desempacotamento (adição) de colunas.

df_legendary_fish_detail_silver = split_col(df_legendary_fish_detail_silver, {"difficulty_behavior": "difficulty"}, 0, " ", "integer")
df_legendary_fish_detail_silver = split_col(df_legendary_fish_detail_silver, {"difficulty_behavior": "behavior"}, 1, " ", "string").drop("difficulty_behavior")
df_legendary_fish_detail_silver = min_and_max(df_legendary_fish_detail_silver, "size_inches", "-", "integer", True)
df_legendary_fish_detail_silver = split_time(df_legendary_fish_detail_silver, "time", "-", True)
df_legendary_fish_detail_silver = reordering_df(df_legendary_fish_detail_silver, legendary_fish_detail_origin_cols)
df_legendary_fish_detail_silver = df_legendary_fish_detail_silver.withColumn("time_min", 
    f.when(f.col("time_min") == "Any", "Anytime").otherwise(f.col("time_min"))
)
df_legendary_fish_detail_silver = df_legendary_fish_detail_silver.withColumn("time_max", 
    f.when(f.col("time_max") == "Any", "Anytime").otherwise(f.col("time_max"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_legendary_fish_price_breakdown_silver

# COMMAND ----------

# DBTITLE 1,df_legendary_fish_price_breakdown_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_legendary_fish_price_breakdown_silver = rename_cols(df_legendary_fish_price_breakdown_silver)
df_legendary_fish_price_breakdown_origin_cols = df_legendary_fish_price_breakdown_silver.columns

# COMMAND ----------

# DBTITLE 1,df_legendary_fish_price_breakdown_silver
# Padronização de tipos e desempacotamento (adição) de colunas.

df_legendary_fish_price_breakdown_silver = normalize_prices(df_legendary_fish_price_breakdown_silver, ["name", "ingestion_timestamp"])
df_legendary_fish_price_breakdown_silver = reordering_df(df_legendary_fish_price_breakdown_silver, df_legendary_fish_price_breakdown_origin_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_legendaryfishII_silver

# COMMAND ----------

# DBTITLE 1,df_legendaryfishII_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_legendary_fishII_detail_silver = rename_cols(df_legendaryfishII_silver)
df_legendary_fishII_detail_silver = normalize_column(df_legendary_fishII_detail_silver, "time")
df_legendary_fishII_detail_origin_cols = df_legendary_fishII_detail_silver.columns

# COMMAND ----------

# DBTITLE 1,df_legendaryfishII_silver
# Padronização de tipos e desempacotamento (adição) de colunas.

df_legendary_fishII_detail_silver = split_col(df_legendary_fishII_detail_silver, {"difficulty_behavior": "difficulty"}, 0, " ", "integer")
df_legendary_fishII_detail_silver = split_col(df_legendary_fishII_detail_silver, {"difficulty_behavior": "behavior"}, 1, " ", "string").drop("difficulty_behavior")
df_legendary_fishII_detail_silver = min_and_max(df_legendary_fishII_detail_silver, "size_inches", "-", "integer", True)
df_legendary_fishII_detail_silver = split_time(df_legendary_fishII_detail_silver, "time", "-", True)
df_legendary_fishII_detail_silver = normalize_prices(df_legendary_fishII_detail_silver, ["name", "description", "location", "time_min", "time_max", "season", "weather", "size_inches_min", "size_inches_max", "difficulty", "behavior", "base_xp", "ingestion_timestamp"])
df_legendary_fishII_detail_silver = reordering_df(df_legendary_fishII_detail_silver, df_legendary_fishII_detail_origin_cols)
df_legendary_fishII_detail_silver = df_legendary_fishII_detail_silver.withColumn("time_min", 
    f.when(f.col("time_min") == "Any", "Anytime").otherwise(f.col("time_min"))
)
df_legendary_fishII_detail_silver = df_legendary_fishII_detail_silver.withColumn("time_max", 
    f.when(f.col("time_max") == "Any", "Anytime").otherwise(f.col("time_max"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_nightmarketfish_silver

# COMMAND ----------

# DBTITLE 1,df_nightmarketfish_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_nightmarketfish_silver = rename_cols(df_nightmarketfish_silver)
df_nightmarketfish_origin_cols = df_nightmarketfish_silver.columns

# COMMAND ----------

# DBTITLE 1,df_nightmarketfish_silver
# Padronização de tipos e desempacotamento (adição) de colunas.

df_nightmarketfish_silver = split_col(df_nightmarketfish_silver, {"difficulty_behavior": "difficulty"}, 0, " ", "integer")
df_nightmarketfish_silver = split_col(df_nightmarketfish_silver, {"difficulty_behavior": "behavior"}, 1, " ", "string").drop("difficulty_behavior")
df_nightmarketfish_silver = min_and_max(df_nightmarketfish_silver, "size", "-", "integer", True)
df_nightmarketfish_silver = normalize_prices(df_nightmarketfish_silver, ["name", "description", "location", "size_min", "size_max", "difficulty", "behavior", "base_xp", "used_in", "ingestion_timestamp"])
df_nightmarketfish_silver = reordering_df(df_nightmarketfish_silver, df_nightmarketfish_origin_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_behavior_silver

# COMMAND ----------

# DBTITLE 1,df_behavior_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_behavior_silver = rename_cols(df_behavior_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_villagers_silver

# COMMAND ----------

# DBTITLE 1,df_villagers_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_villagers_silver = rename_cols(df_villagers_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC # Salvando os dados na camada Silver
# MAGIC Depois de padronizados e de algumas colunas serem "desempacotadas", podemos salvar nossos DataFrames na camada Silver para que prossigamos fazendo refinamentos posteriormente, garantindo o acesso aos novos dados modificados de forma segura.

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
