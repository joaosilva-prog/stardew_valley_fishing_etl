# Databricks notebook source
# MAGIC %md
# MAGIC # Definição de Funções Auxiliares
# MAGIC Este Notebook serve como local centralizado de todas as funções auxiliares criadas e utilizadas em todos os processos de limpeza e padronização dos dados realizados no projeto.

# COMMAND ----------

# DBTITLE 1,Spark Imports
import pyspark.sql.functions as f
from pyspark.sql.types import StringType, IntegerType, FloatType
from pyspark.sql.functions import col, split
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import instr

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

def split_col(df, columnsAndAliases: dict, position: int, sep: str, cast: str):

    for column, alias in columnsAndAliases.items():
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
        df = df.withColumn(column, f.regexp_replace(col(column), r"(?<=\d),(?=\d)|(?<=\d)g(?=[,\s]|$)|(?<=,)\s+", ""))

    all_cols = df.columns

    for column in all_cols:
      if column in cols_to_skip:
        continue
      elif column in cols_to_str:
        df = df.withColumn(column, col(column).cast("string"))
      else:
        df = df.withColumn(column, col(column).cast("integer"))

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

# DBTITLE 1,clean_nulls
# Função clean_nulls, que utiliza do dict null_map que mapeia os valores usados para substituir nulos das colunas dos DataFrames para fazer replace nos valores nulos.

null_map = {
    "used_in": "No uses",
    "location": "Unknown",
    "weather": "Any",
    "season": "Unknown",
    "size_inches_min": 9999,
    "size_inches_max": 9999,
    "trap_chance_non_mariner": 9999,
    "trap_chance_mariner": 9999,
    "description": "Unknown",
    "behavior": "Unknown",
    "base_xp": 9999,
    "time_min": "Unknown",
    "time_max": "Unknown",
    "difficulty": 9999,
    "fisher_profession_25": "Unknown",
    "angler_profession_50": "Unknown",
}

def clean_nulls(df):

    df_schema = df.schema

    map_types = {}
    for tipo in df_schema:
        map_types[tipo.name] = tipo.dataType.simpleString()

    for column, org_type in map_types.items():
        if column in null_map.keys():
            df = df.fillna(null_map[column], [column])
        else:
            if org_type == "string":
                df = df.fillna("Unknown", [column])
            elif org_type == "int":
                df = df.fillna(9999, [column])
            elif org_type == "float":
                df = df.fillna(999.9, [column])
            elif org_type == "timestamp":
                df = df.fillna("1970-01-01 00:00:00", [column])
            else:
                pass
    
    return df

# COMMAND ----------

# DBTITLE 1,save_df
# Função save_df, que serve para salvar os DataFrames finais como tabelas Delta fazendo schema enforcement e com schema evolution ativado para toda a sessão.

def save_df(df, schema: str, table: str, key: str):
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

    if spark.catalog.tableExists(f"stardew_project.{schema}.{table}"):
        print(f"tabela {table} existe")
        spark.sql(f"""
            MERGE INTO stardew_project.{schema}.{table} AS tgt
            USING {view_name} AS src
            ON tgt.{key} = src.{key}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    else:
        print(f"tabela {table} não existe")
        spark.sql(f"""
                  CREATE TABLE stardew_project.{schema}.{table}
                  {final_schema}
                  USING DELTA 
                  """)
        spark.sql(f"""
                  INSERT INTO stardew_project.{schema}.{table}
                  SELECT * FROM {view_name}
                  """)
        
    spark.sql(f"DROP VIEW {view_name}")
