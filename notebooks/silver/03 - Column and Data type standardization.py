# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 2 - Padronização de colunas e tipos (schema)
# MAGIC Agora que já possuímos os dados armazenados como Delta tables internamente e já realizamos uma etapa prévia de EDA (Análise Exploratória), iremos iniciar o tratamento da camada Silver padronizando tipos, nomes e dados das colunas.

# COMMAND ----------

# DBTITLE 1,Importação de Dados
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

# DBTITLE 1,Imports de funções úteis
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.sql.functions import col, split
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import instr

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções auxiliares
# MAGIC Criação de funções auxiliares para facilitar o processo de padronização posterior aplicado a diversos DataFrames.

# COMMAND ----------

# DBTITLE 1,rename_cols
# Função auxiliar rename_cols, que renomeia e padroniza caracteres especiais para lower_snake_case as colunas dos DataFrames, além de adicionar uma coluna "ingestion_timestamp" para fins de auditoria posterior.

import re

def rename_cols(df):

  origin_names = list(df.columns)

  names_to_replace = {}

  for column in origin_names:
    names_to_replace[column] = re.sub(r"[^A-Za-z0-9]+", "_", column.lower())

  for key, value in names_to_replace.items():
    names_to_replace[key] = re.sub(r"^_+|_+$", "", value)
  
  df = df.withColumnsRenamed(names_to_replace)
  return df.withColumn("ingestion_timestamp", current_timestamp())

# COMMAND ----------

# DBTITLE 1,split_col
# Função split_col, que extrai a primeira ou segunda posição de uma coluna e a retorna como ela mesma.

def split_col(df, column: str, position: int, sep: str, cast: str, alias: str | None = None):
    if alias is None:
        df = df.withColumn(column, split(col(column), sep)[position].cast(cast))
    else:
        df = df.withColumn(alias, split(col(column), sep)[position].cast(cast))
    return df

# COMMAND ----------

# DBTITLE 1,normalize_column

# Função normalize_column, que normaliza unicodes dos valores das linhas da coluna passada como argumento para "-".

def normalize_column(df, column: str):
    if df.schema[column].dataType == StringType():
        df = df.withColumn(column, f.regexp_replace(col(column), r"\p{Pd}", "-"))
        df = df.withColumn(column, f.regexp_replace(col(column), r"\s*-\s*", "-"))
    else:
        pass

    return df

# COMMAND ----------

# DBTITLE 1,min_and_max
# Função min_and_max, que extrai valores mínimos e máximos das colunas e as transforma em colunas distintas.

def min_and_max(df, column: str, sep: str, cast: str, drop: bool):
    col_min = column + "_min"
    col_max = column + "_max"

    df = df.withColumns({
        col_min: f.when(col(column).contains(sep), split(col(column), sep)[0].cast(cast)).otherwise(col(column).cast(cast)),
        col_max: f.when(col(column).contains(sep), split(col(column), sep)[1].cast(cast)).otherwise(col(column).cast(cast))
    })

    if drop == True:
        df = df.drop(column)
    else:
        pass
    return df

# COMMAND ----------

# DBTITLE 1,split_time
# Função split_time, que extrai valores mínimos e máximos da coluna time e as transforma em colunas distintas, mantendo como string.

def split_time(df, column: str, sep: str, drop: bool):
    col_min = column + "_min"
    col_max = column + "_max"

    df = df.withColumns({
        col_min: f.when(col(column).contains(sep), split(col(column), sep)[0]).otherwise(col(column)),
        col_max: f.when(col(column).contains(sep), split(col(column), sep)[1]).otherwise(col(column))
    })

    if drop == True:
        df = df.drop(column)
    else:
        pass
    return df

# COMMAND ----------

# DBTITLE 1,normalize_prices
# Função normalize_prices, que serve para normalizar as colunas de preço dos DataFrames e fazer o casting corretamente de cada um dos casos.

def normalize_prices(df, cols_to_skip: list):
    all_cols = df.columns
    cols_to_str = ["price", "fish_profession_25", "fish_profession_50", "fisher_profession_25", "angler_profession_50"]

    for column in all_cols:
      if column in cols_to_skip:
        continue
      else:
        df = df.withColumn(column, f.regexp_replace(col(column), r"(?<=\d),(?=\d)|(?<=\d)g(?=[,\s]|$)", ""))

    for column in all_cols:
      if column not in cols_to_str and column not in cols_to_skip:
        df = df.withColumn(column, col(column).cast("integer"))
      else:
         df = df.withColumn(column, col(column).cast("string"))

    return df


# COMMAND ----------

# DBTITLE 1,reordering_df
# Função reordering_df, que serve para reordenarmos as colunas dos DataFrames de volta para suas colocações originais (retirando as adições de colunas do final).

def reordering_df(df, old_columns):
    new_columns = df.columns
    final_order = []

    replaces = {
        "difficulty_behavior": ["difficulty", "behavior"],
        "size_inches": ["size_inches_min", "size_inches_max"],
        "time": ["time_min", "time_max"]
    }

    for old_col in old_columns:
        if old_col in new_columns:
            final_order.append(old_col)
        elif old_col in replaces.keys():
            final_order.append(replaces[old_col][0])
            final_order.append(replaces[old_col][1])
        elif old_col + "_min" in new_columns:
            final_order.append(old_col + "_min")
            final_order.append(old_col + "_max")
        else:
            pass

    df = df.select(final_order)
    return df

# COMMAND ----------

# DBTITLE 1,save_df
# Função save_df, que serve para salvar os DataFrames finais como tabelas Delta fazendo schema enforcement e com schema evolution ativado para toda a sessão.

def save_df(df, table: str, key: str):
    view_name = (table + "df")
    df.createOrReplaceTempView(view_name)

    df_schema = df.schema

    map_types = {}
    for tipo in df_schema:
        map_types[tipo.name] = tipo.dataType.simpleString()

    final_schema = ""
    for name, tipo in map_types.items():
        final_schema += name + " " + tipo + ", "

    final_schema = final_schema.rstrip(", ")
    final_schema = "(" + final_schema + ")"

    if spark.catalog.tableExists(f"stardew_project.silver.{table}"):
        print(f"tabela {table} existe")
        spark.sql(f"""
            MERGE INTO stardew_project.silver.{table} AS tgt
            USING {view_name} AS src
            ON tgt.{key} = src.{key}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        print(f"tabela {table} não existe")
        spark.sql(f"""
                  CREATE TABLE stardew_project.silver.{table}
                  {final_schema}
                  USING DELTA 
                  AS SELECT * FROM {view_name}""")
        
    spark.sql(f"DROP VIEW {view_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_crabpotandothercatchables_silver

# COMMAND ----------

# DBTITLE 1,df_crabpotandothercatchables_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_crabpotandothercatchables_silver = rename_cols(df_crabpotandothercatchables_silver)
crab_origin_cols = df_crabpotandothercatchables_silver.columns

df_crabpotandothercatchables_silver.limit(5).display()

# COMMAND ----------

# DBTITLE 1,df_crabpotandothercatchables_silver
# Padronização de tipos e desempacotamento (adição) de colunas.

df_crabpotandothercatchables_silver = split_col(df_crabpotandothercatchables_silver, "trap_chance_non_mariner", 0, "%", "int")
df_crabpotandothercatchables_silver = split_col(df_crabpotandothercatchables_silver, "trap_chance_mariner", 0, "%", "int")
df_crabpotandothercatchables_silver = min_and_max(df_crabpotandothercatchables_silver, "size_inches", "-", "int", True)
df_crabpotandothercatchables_silver = reordering_df(df_crabpotandothercatchables_silver, crab_origin_cols)

df_crabpotandothercatchables_silver.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_fish_detail_silver

# COMMAND ----------

# DBTITLE 1,df_fish_detail_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_fish_detail_silver = rename_cols(df_fish_detail_silver)
df_fish_detail_silver = normalize_column(df_fish_detail_silver, "time")
fishdetail_origin_cols = df_fish_detail_silver.columns

df_fish_detail_silver.limit(5).display()

# COMMAND ----------

# DBTITLE 1,df_fish_detail_silver
# Padronização de tipos e desempacotamento (adição) de colunas.

df_fish_detail_silver = split_col(df_fish_detail_silver, "difficulty_behavior", 0, " ", "int", "difficulty")
df_fish_detail_silver = split_col(df_fish_detail_silver, "difficulty_behavior", 1, " ", "string", "behavior").drop("difficulty_behavior")
df_fish_detail_silver = min_and_max(df_fish_detail_silver, "size_inches", "-", "int", True)
df_fish_detail_silver = split_time(df_fish_detail_silver, "time", "-", True)
df_fish_detail_silver = reordering_df(df_fish_detail_silver, fishdetail_origin_cols)

df_fish_detail_silver.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_fish_price_breakdown_silver

# COMMAND ----------

# DBTITLE 1,df_fish_price_breakdown_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_fish_price_breakdown_silver = rename_cols(df_fish_price_breakdown_silver)
fish_price_breakdown_origin_cols = df_fish_price_breakdown_silver.columns

df_fish_price_breakdown_silver.limit(5).display()

# COMMAND ----------

# DBTITLE 1,df_fish_price_breakdown_silver
# Padronização de tipos e desempacotamento (adição) de colunas.

df_fish_price_breakdown_silver = normalize_prices(df_fish_price_breakdown_silver, ["name", "ingestion_timestamp"])
df_fish_price_breakdown_silver = reordering_df(df_fish_price_breakdown_silver, fish_price_breakdown_origin_cols)

df_fish_price_breakdown_silver.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_legendary_fish_detail_silver

# COMMAND ----------

# DBTITLE 1,df_legendary_fish_detail_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_legendary_fish_detail_silver = rename_cols(df_legendary_fish_detail_silver)
df_legendary_fish_detail_silver = normalize_column(df_legendary_fish_detail_silver, "time")
legendary_fish_detail_origin_cols = df_legendary_fish_detail_silver.columns

df_legendary_fish_detail_silver.limit(5).display()

# COMMAND ----------

# DBTITLE 1,df_legendary_fish_detail_silver
# Padronização de tipos e desempacotamento (adição) de colunas.

df_legendary_fish_detail_silver = split_col(df_legendary_fish_detail_silver, "difficulty_behavior", 0, " ", "int", "difficulty")
df_legendary_fish_detail_silver = split_col(df_legendary_fish_detail_silver, "difficulty_behavior", 1, " ", "string", "behavior").drop("difficulty_behavior")
df_legendary_fish_detail_silver = min_and_max(df_legendary_fish_detail_silver, "size_inches", "-", "int", True)
df_legendary_fish_detail_silver = split_time(df_legendary_fish_detail_silver, "time", "-", True)
df_legendary_fish_detail_silver = reordering_df(df_legendary_fish_detail_silver, legendary_fish_detail_origin_cols)

df_legendary_fish_detail_silver.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_legendary_fish_price_breakdown_silver

# COMMAND ----------

# DBTITLE 1,df_legendary_fish_price_breakdown_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_legendary_fish_price_breakdown_silver = rename_cols(df_legendary_fish_price_breakdown_silver)
df_legendary_fish_price_breakdown_origin_cols = df_legendary_fish_price_breakdown_silver.columns

df_legendary_fish_price_breakdown_silver.limit(5).display()

# COMMAND ----------

# DBTITLE 1,df_legendary_fish_price_breakdown_silver
# Padronização de tipos e desempacotamento (adição) de colunas.

df_legendary_fish_price_breakdown_silver = normalize_prices(df_legendary_fish_price_breakdown_silver, ["name", "ingestion_timestamp"])
df_legendary_fish_price_breakdown_silver = reordering_df(df_legendary_fish_price_breakdown_silver, df_legendary_fish_price_breakdown_origin_cols)

df_legendary_fish_price_breakdown_silver.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_legendaryfishII_silver

# COMMAND ----------

# DBTITLE 1,df_legendaryfishII_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_legendary_fishII_detail_silver = rename_cols(df_legendaryfishII_silver)
df_legendary_fishII_detail_silver = normalize_column(df_legendary_fishII_detail_silver, "time")
df_legendary_fishII_detail_origin_cols = df_legendary_fishII_detail_silver.columns

df_legendary_fishII_detail_silver.limit(5).display()

# COMMAND ----------

# DBTITLE 1,df_legendaryfishII_silver
# Padronização de tipos e desempacotamento (adição) de colunas.

df_legendary_fishII_detail_silver = split_col(df_legendary_fishII_detail_silver, "difficulty_behavior", 0, " ", "int", "difficulty")
df_legendary_fishII_detail_silver = split_col(df_legendary_fishII_detail_silver, "difficulty_behavior", 1, " ", "string", "behavior").drop("difficulty_behavior")
df_legendary_fishII_detail_silver = min_and_max(df_legendary_fishII_detail_silver, "size_inches", "-", "int", True)
df_legendary_fishII_detail_silver = split_time(df_legendary_fishII_detail_silver, "time", "-", True)
df_legendary_fishII_detail_silver = normalize_prices(df_legendary_fishII_detail_silver, ["name", "description", "location", "time_min", "time_max", "season", "weather", "size_inches_min", "size_inches_max", "difficulty", "behavior", "base_xp", "ingestion_timestamp"])
df_legendary_fishII_detail_silver = reordering_df(df_legendary_fishII_detail_silver, df_legendary_fishII_detail_origin_cols)

df_legendary_fishII_detail_silver.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_nightmarketfish_silver

# COMMAND ----------

# DBTITLE 1,df_nightmarketfish_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_nightmarketfish_silver = rename_cols(df_nightmarketfish_silver)
df_nightmarketfish_origin_cols = df_nightmarketfish_silver.columns

df_nightmarketfish_silver.limit(5).display()

# COMMAND ----------

# DBTITLE 1,df_nightmarketfish_silver
# Padronização de tipos e desempacotamento (adição) de colunas.

df_nightmarketfish_silver = split_col(df_nightmarketfish_silver, "difficulty_behavior", 0, " ", "int", "difficulty")
df_nightmarketfish_silver = split_col(df_nightmarketfish_silver, "difficulty_behavior", 1, " ", "string", "behavior").drop("difficulty_behavior")
df_nightmarketfish_silver = min_and_max(df_nightmarketfish_silver, "size", "-", "int", True)
df_nightmarketfish_silver = normalize_prices(df_nightmarketfish_silver, ["name", "description", "location", "size_min", "size_max", "difficulty", "behavior", "base_xp", "used_in", "ingestion_timestamp"])
df_nightmarketfish_silver = reordering_df(df_nightmarketfish_silver, df_nightmarketfish_origin_cols)

df_nightmarketfish_silver.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_behavior_silver

# COMMAND ----------

# DBTITLE 1,df_behavior_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_behavior_silver = rename_cols(df_behavior_silver)

df_behavior_silver.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Padronizando colunas: df_villagers_silver

# COMMAND ----------

# DBTITLE 1,df_villagers_silver
# Padronização dos nomes das colunas para lower_snake_case.

df_villagers_silver = rename_cols(df_villagers_silver)

df_villagers_silver.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Salvando os dados na camada Silver
# MAGIC Depois de padronizados e de algumas colunas serem "desempactodas", podemos salvar nossos DataFrames na camada Silver para que prossigamos fazendo refinamentos posteriormente e para garantirmos o acesso aos novos dados modificados de forma segura.

# COMMAND ----------

# DBTITLE 1,crabpotandothercatchables
try:
    save_df(df_crabpotandothercatchables_silver, "crabpotandothercatchables", "name")
except Exception as e:
    print("Erro ao salvar a tabela crabpotandothercatchables: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,fish_detail
try:
    save_df(df_fish_detail_silver, "fish_detail", "name")
except Exception as e:
    print("Erro ao salvar a tabela fish_detail: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,fish_price_breakdown
try:
    save_df(df_fish_price_breakdown_silver, "fish_price_breakdown", "name")
except Exception as e:
    print("Erro ao salvar a tabela fish_price_breakdown: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,legendary_fish_detail
try:
    save_df(df_legendary_fish_detail_silver, "legendary_fish_detail", "name")
except Exception as e:
    print("Erro ao salvar a tabela legendary_fish_detail: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,legendary_fish_price_breakdown
try:
    save_df(df_legendary_fish_price_breakdown_silver, "legendary_fish_price_breakdown", "name")
except Exception as e:
    print("Erro ao salvar a tabela legendary_fish_price_breakdown: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,legendary_fishII_detail
try:
    save_df(df_legendary_fishII_detail_silver, "legendary_fishII_detail", "name")
except Exception as e:
    print("Erro ao salvar a tabela legendary_fishII_detail: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,nightmarketfish
try:
    save_df(df_nightmarketfish_silver, "nightmarketfish", "name")
except Exception as e:
    print("Erro ao salvar a tabela nightmarketfish: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,villagers
try:
    save_df(df_villagers_silver, "villagers", "name")
except Exception as e:
    print("Erro ao salvar a tabela villagers: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,behavior
try:
    save_df(df_behavior_silver, "behavior", "behavior")
except Exception as e:
    print("Erro ao salvar a tabela behavior: ", e)
    raise