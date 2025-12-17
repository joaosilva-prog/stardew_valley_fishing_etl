# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 4 - Divisão dos Dados
# MAGIC Com nulos e duplicatas devidamente tratados, podemos realizar uma fase de separação dos dados em novos DataFrames e também a realocação de colunas de alguns DataFrames, preparando os dados para uma etapa de enriquecimento posterior.

# COMMAND ----------

# DBTITLE 1,Importação dos Dados
# Importando nossos dados como DataFrames para fazermos a divisão dos dados.

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

# DBTITLE 1,Import das Funções Auxiliares
# MAGIC %run /Workspace/Users/sjoao5498@gmail.com/stardew_valley_fishing_etl/utils/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Separação df_crabpotandothercatchables_silver
# MAGIC Vamos separar o DataFrame df_crabpotandothercatchables_silver em dois, já que atualmente ele possui dados conceitualmente diferentes armazenados juntos.

# COMMAND ----------

# DBTITLE 1,df_crabpotandothercatchables_silver
# Separação e criação de novos DataFrames.

df_crab_pot_fish_silver = df_crabpotandothercatchables_silver.where("size_inches_min <> 9999 and size_inches_max <> 9999")
df_other_catchables_silver = df_crabpotandothercatchables_silver.where("size_inches_min = 9999 and size_inches_max = 9999") \
    .select(["name", "description", "location", "used_in", "ingestion_timestamp"])

# COMMAND ----------

# MAGIC %md
# MAGIC # Transposição de colunas df_fish_price_breakdown_silver
# MAGIC Vamos transpor as colunas do DataFrame df_fish_price_breakdown_silver, já que em sua forma atual os dados se encontram em "long format", dificultando operações com os dados.

# COMMAND ----------

# DBTITLE 1,df_fish_price_breakdown_silver
# Renomeação e transposição horizontal de colunas e dados.

df_fish_price_breakdown_silver = df_fish_price_breakdown_silver.withColumn(
    "name", f.when(col("name") == "Fisher Profession (+25%)", "fisher_profession_base")
    .when(col("name") == "Base Price", "base_price")
    .when(col("name") == "BP Silver", "base_price_silver")
    .when(col("name") == "FP Irridium", "fisher_profession_iridium")
    .when(col("name") == "AP Irridium", "angler_profession_iridium")
    .when(col("name") == "AP Gold", "angler_profession_gold")
    .when(col("name") == "FP Silver", "fisher_profession_silver")
    .when(col("name") == "BP Irridium", "base_price_iridium")
    .when(col("name") == "BP Gold", "base_price_gold")
    .when(col("name") == "Angler Profession (+50%)", "angler_profession_base")
    .when(col("name") == "AP Silver", "angler_profession_silver")
    .when(col("name") == "FP Gold", "fisher_profession_gold")
)

df_fish_price_breakdown_silver = df_fish_price_breakdown_silver.unpivot(["name", "ingestion_timestamp"], ['pufferfish', 'anchovy', 'tuna', 'sardine', 'bream', 'largemouth_bass', 'smallmouth_bass', 'rainbow_trout', 'salmon', 'walleye', 'perch', 'carp', 'catfish', 'pike', 'sunfish', 'red_mullet', 'herring', 'eel', 'octopus', 'red_snapper', 'squid', 'sea_cucumber', 'super_cucumber', 'ghostfish', 'stonefish', 'ice_pip', 'lava_eel', 'sandfish', 'scorpion_carp', 'flounder', 'midnight_carp', 'sturgeon', 'tiger_trout', 'bullhead', 'tilapia', 'chub', 'dorado', 'albacore', 'shad', 'lingcod', 'halibut', 'woodskip', 'void_salmon', 'slimejack', 'stingray', 'lionfish', 'blue_discus', 'goby'], "fish", "price")

df_fish_price_breakdown_silver = df_fish_price_breakdown_silver.groupBy("fish", "ingestion_timestamp").pivot("name").agg(f.first("price"))

df_fish_price_breakdown_silver = df_fish_price_breakdown_silver.withColumn("fish", f.initcap(f.regexp_replace(col("fish"), "_", " ")))

df_fish_price_breakdown_silver = df_fish_price_breakdown_silver.select("fish", "base_price", "base_price_silver", "base_price_gold", "base_price_iridium", "fisher_profession_base", "fisher_profession_silver", "fisher_profession_gold", "fisher_profession_iridium", "angler_profession_base", "angler_profession_silver", "angler_profession_gold", "angler_profession_iridium", "ingestion_timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC # Transposição de colunas df_legendary_fish_price_breakdown_silver
# MAGIC Vamos transpor as colunas do DataFrame df_legendary_fish_price_breakdown_silver, já que em sua forma atual os dados se encontram em "long format", dificultando operações com os dados.

# COMMAND ----------

# DBTITLE 1,df_legendary_fish_price_breakdown_silver
# Renomeação e transposição horizontal de colunas e dados.

df_legendary_fish_price_breakdown_silver = df_legendary_fish_price_breakdown_silver.withColumn(
    "name", f.when(col("name") == "Fisher Profession (+25%)", "fisher_profession_base")
    .when(col("name") == "Base Price", "base_price")
    .when(col("name") == "BP Silver", "base_price_silver")
    .when(col("name") == "FP Irridium", "fisher_profession_iridium")
    .when(col("name") == "AP Irridium", "angler_profession_iridium")
    .when(col("name") == "AP Gold", "angler_profession_gold")
    .when(col("name") == "FP Silver", "fisher_profession_silver")
    .when(col("name") == "BP Irridium", "base_price_iridium")
    .when(col("name") == "BP Gold", "base_price_gold")
    .when(col("name") == "Angler Profession (+50%)", "angler_profession_base")
    .when(col("name") == "AP Silver", "angler_profession_silver")
    .when(col("name") == "FP Gold", "fisher_profession_gold")
)

df_legendary_fish_price_breakdown_silver = df_legendary_fish_price_breakdown_silver.unpivot(["name", "ingestion_timestamp"], ['crimsonfish', 'angler', 'legend', 'glacierfish', 'mutant_carp'], "fish", "price")

df_legendary_fish_price_breakdown_silver = df_legendary_fish_price_breakdown_silver.groupBy("fish", "ingestion_timestamp")\
    .pivot("name").agg(f.first("price"))

df_legendary_fish_price_breakdown_silver = df_legendary_fish_price_breakdown_silver.withColumn("fish", f.initcap(f.regexp_replace(col("fish"), "_", " ")))
    
df_legendary_fish_price_breakdown_silver = df_legendary_fish_price_breakdown_silver.select("fish", "base_price", "base_price_silver", "base_price_gold", "base_price_iridium", "fisher_profession_base", "fisher_profession_silver", "fisher_profession_gold", "fisher_profession_iridium", "angler_profession_base", "angler_profession_silver", "angler_profession_gold", "angler_profession_iridium", "ingestion_timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC # Split de colunas de Preço df_legendary_fishII_detail_silver
# MAGIC Vamos extrair corretamente os valores de preços contidos no DataFrame df_legendary_fishII_detail_silver e movê-los para o DataFrame df_legendary_fish_price_breakdown_silver que contêm as informações de preços correspondentes.

# COMMAND ----------

# DBTITLE 1,df_legendary_fishII_detail_silver
# Extração de colunas e adição a outro DataFrame.

values_base = {
    "price": "base_price",
    "fisher_profession_25": "fisher_profession_base",
    "angler_profession_50": "angler_profession_base"
}

values_silver = {
    "price": "base_price_silver",
    "fisher_profession_25": "fisher_profession_silver",
    "angler_profession_50": "angler_profession_silver"
}

values_gold = {
    "price": "base_price_gold",
    "fisher_profession_25": "fisher_profession_gold",
    "angler_profession_50": "angler_profession_gold"
}

values_iridium = {
    "price": "base_price_iridium",
    "fisher_profession_25": "fisher_profession_iridium",
    "angler_profession_50": "angler_profession_iridium"
}

df_legendary_fishII_detail_silver = split_col(df_legendary_fishII_detail_silver, values_base, 0, ",", "integer")
df_legendary_fishII_detail_silver = split_col(df_legendary_fishII_detail_silver, values_silver, 1, ",", "integer")
df_legendary_fishII_detail_silver = split_col(df_legendary_fishII_detail_silver, values_gold, 2, ",", "integer")
df_legendary_fishII_detail_silver = split_col(df_legendary_fishII_detail_silver, values_iridium, 3, ",", "integer").drop("price", "fisher_profession_25", "angler_profession_50")

# COMMAND ----------

# DBTITLE 1,df_legendary_fishII_detail_silver
# Extração de colunas e adição a outro DataFrame.

df_temp = df_legendary_fishII_detail_silver.select(f.col("name").alias("fish"), "base_price", "base_price_silver", "base_price_gold", "base_price_iridium", "fisher_profession_base", "fisher_profession_silver", "fisher_profession_gold", "fisher_profession_iridium", "angler_profession_base", "angler_profession_silver", "angler_profession_gold", "angler_profession_iridium", "ingestion_timestamp")

df_legendary_fish_price_breakdown_silver = df_legendary_fish_price_breakdown_silver.unionByName(df_temp)

df_legendary_fishII_detail_silver = df_legendary_fishII_detail_silver.drop("base_price", "base_price_silver", "base_price_gold", "base_price_iridium", "fisher_profession_base", "fisher_profession_silver", "fisher_profession_gold", "fisher_profession_iridium", "angler_profession_base", "angler_profession_silver", "angler_profession_gold", "angler_profession_iridium")

# COMMAND ----------

# MAGIC %md
# MAGIC # Split de colunas de Preço df_nightmarketfish_silver
# MAGIC Vamos extrair corretamente os valores de preços contidos no DataFrame df_nightmarketfish_silver e criarmos o DataFrame df_nightmarketfish_price_breakdown_silver que conterá as informações de preços correspondentes.

# COMMAND ----------

# DBTITLE 1,df_nightmarketfish_silver
# Extração de colunas e adição a outro DataFrame.

values_base = {
    "price": "base_price",
    "fish_profession_25": "fisher_profession_base",
    "angler_profession_50": "angler_profession_base"
}

values_silver = {
    "price": "base_price_silver",
    "fish_profession_25": "fisher_profession_silver",
    "angler_profession_50": "angler_profession_silver"
}

values_gold = {
    "price": "base_price_gold",
    "fish_profession_25": "fisher_profession_gold",
    "angler_profession_50": "angler_profession_gold"
}

values_iridium = {
    "price": "base_price_iridium",
    "fish_profession_25": "fisher_profession_iridium",
    "angler_profession_50": "angler_profession_iridium"
}

df_nightmarketfish_silver = split_col(df_nightmarketfish_silver, values_base, 0, ",", "integer")
df_nightmarketfish_silver = split_col(df_nightmarketfish_silver, values_silver, 1, ",", "integer")
df_nightmarketfish_silver = split_col(df_nightmarketfish_silver, values_gold, 2, ",", "integer")
df_nightmarketfish_silver = split_col(df_nightmarketfish_silver, values_iridium, 3, ",", "integer").drop("price", "fish_profession_25", "angler_profession_50")

# COMMAND ----------

# DBTITLE 1,df_nightmarketfish_silver
# Extração de colunas e adição a outro DataFrame.

df_nightmarketfish_price_breakdown_silver = df_nightmarketfish_silver.select(f.col("name").alias("fish"), "base_price", "base_price_silver", "base_price_gold", "base_price_iridium", "fisher_profession_base", "fisher_profession_silver", "fisher_profession_gold", "fisher_profession_iridium", "angler_profession_base", "angler_profession_silver", "angler_profession_gold", "angler_profession_iridium", "ingestion_timestamp")

df_nightmarketfish_silver = df_nightmarketfish_silver.drop("base_price", "base_price_silver", "base_price_gold", "base_price_iridium", "fisher_profession_base", "fisher_profession_silver", "fisher_profession_gold", "fisher_profession_iridium", "angler_profession_base", "angler_profession_silver", "angler_profession_gold", "angler_profession_iridium")

# COMMAND ----------

# MAGIC %md
# MAGIC # Salvando os dados na camada Silver
# MAGIC Depois de algumas colunas devidamente realocadas e dados divididos corretamente, podemos salvar nossos DataFrames na camada Silver como versões atualizadas apóes mudarem de Schema, para que prossigamos fazendo refinamentos posteriormente, garantindo o acesso aos novos dados modificados de forma segura.

# COMMAND ----------

# DBTITLE 1,crab_pot_fish
try:
    save_df(df_crab_pot_fish_silver, "silver", "crab_pot_fish", "name")
except Exception as e:
    print("Erro ao salvar a tabela crab_pot_fish: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,other_catchables
try:
    save_df(df_other_catchables_silver, "silver", "other_catchables", "name")
except Exception as e:
    print("Erro ao salvar a tabela other_catchables: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,fish_price_breakdown_v2
try:
    save_df(df_fish_price_breakdown_silver, "silver", "fish_price_breakdown_v2", "fish")
except Exception as e:
    print("Erro ao salvar a tabela fish_price_breakdown_v2: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,legendary_fish_price_breakdown_v2
try:
    save_df(df_legendary_fish_price_breakdown_silver, "silver", "legendary_fish_price_breakdown_v2", "fish")
except Exception as e:
    print("Erro ao salvar a tabela legendary_fish_price_breakdown_v2: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,legendary_fishII_detail_v2
try:
    save_df(df_legendary_fishII_detail_silver, "silver", "legendary_fishII_detail_v2", "name")
except Exception as e:
    print("Erro ao salvar a tabela legendary_fishII_detail_v2: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,nightmarketfish_v2
try:
    save_df(df_nightmarketfish_silver, "silver", "nightmarketfish_v2", "name")
except Exception as e:
    print("Erro ao salvar a tabela nightmarketfish_v2: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,nightmarketfish_price_breakdown
try:
    save_df(df_nightmarketfish_price_breakdown_silver, "silver", "nightmarketfish_price_breakdown", "fish")
except Exception as e:
    print("Erro ao salvar a tabela nightmarketfish_price_breakdown: ", e)
    raise
