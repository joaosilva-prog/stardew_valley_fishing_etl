# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 4 - Realocação dos Dados
# MAGIC Com nulos e duplicatas devidamente tratados, podemos realizar uma fase de separação de alguns dados e também a realocação de colunas de alguns DataFrames, preparando os dados para uma etapa de enriquecimento posterior.

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

# MAGIC %md
# MAGIC # Funções auxiliares
# MAGIC Import das funções auxiliares que irão realizar o processo de padronização posterior aplicado aos DataFrames.

# COMMAND ----------

# DBTITLE 1,Import das Funções Auxiliares
# MAGIC %run /Workspace/Users/sjoao5498@gmail.com/stardew_valley_fishing_etl/utils/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Criação do DataFrame df_fish_location
# MAGIC Antes de iniciarmos as operações com os DataFrames vamos criar o df_fish_location, que conterá as informações dos locais e regiões de todos os peixes através dos DataFrames.

# COMMAND ----------

# DBTITLE 1,Declaração de variáveis
# Definição de variáveis importantes que contém os dados a serem extraídos para a criação do DataFrame.

dfs = [df_crabpotandothercatchables_silver, df_fish_detail_silver, df_legendary_fish_detail_silver, df_legendary_fishII_detail_silver]

location_types = ["Ocean", "River", "Pond", "Lake", "Waterfall", "Mines", "Desert", "Sewer", "Swamp", "Freshwater", "Saltwater", "Beach"]

regions = ["Ginger Island", "Town", "Forest", "Mountain", "Secret Woods", "North of JojaMart", "Arrowhead Island", "East Pier", "Volcano Caldera", "Cindersap Forest"]

special_area = ["Witch's Swamp", "Wooden plank bridge", "Mutant Bug Lair", "Pirate Cove", "The Mountain Lake near the log", "Forest Waterfalls", "Forest Farm"]

rules_source = ["Requires level 6 fishing", "Requires level 10 fishing", "Requires level 3 fishing", "Requires level 5 fishing", "Requires fishing level 4", "Mines (20, 60), Ghost drops", "Mines 20F", "Mines 60F", "Mines 100F", "Levels 20, 60, and 100 of the Mines", "Everywhere but the Farm Pond of the Standard Farm", "Ocean via Fishing, Beach via Foraging"]

# COMMAND ----------

# DBTITLE 1,Criação do df_fish_location
# Criação do DataFrame fish_location com a extração de dados de diversos DataFrames.

for i, df in enumerate(dfs):
    for location in location_types:
        df = df.withColumn(f"has_{location.lower()}", f.when(col("location").contains(location), True).otherwise(False))
        dfs[i] = df

df_crabpotandothercatchables_silver, df_fish_detail_silver, df_legendary_fish_detail_silver, df_legendary_fishII_detail_silver = dfs

df_fish_location = df_crabpotandothercatchables_silver.select("name", "location", "has_ocean", "has_river", "has_pond", "has_lake", "has_waterfall", "has_mines", "has_desert", "has_sewer", "has_swamp", "has_freshwater", "has_saltwater", "has_beach")

df_fish_location = df_fish_detail_silver.select("name", "location", "has_ocean", "has_river", "has_pond", "has_lake", "has_waterfall", "has_mines", "has_desert", "has_sewer", "has_swamp", "has_freshwater", "has_saltwater", "has_beach").union(df_fish_location)

df_fish_location = df_legendary_fish_detail_silver.select("name", "location", "has_ocean", "has_river", "has_pond", "has_lake", "has_waterfall", "has_mines", "has_desert", "has_sewer", "has_swamp", "has_freshwater", "has_saltwater", "has_beach").union(df_fish_location)

df_fish_location = df_legendary_fishII_detail_silver.select("name", "location", "has_ocean", "has_river", "has_pond", "has_lake", "has_waterfall", "has_mines", "has_desert", "has_sewer", "has_swamp", "has_freshwater", "has_saltwater", "has_beach").union(df_fish_location)

for i, df in enumerate(dfs):
    df = df.drop("has_ocean", "has_river", "has_pond", "has_lake", "has_waterfall", "has_mines", "has_desert", "has_sewer", "has_swamp", "has_freshwater", "has_saltwater", "has_beach")
    dfs[i] = df

df_crabpotandothercatchables_silver, df_fish_detail_silver, df_legendary_fish_detail_silver, df_legendary_fishII_detail_silver = dfs

df_fish_location = split_location(df_fish_location, "location_type", "location", location_types, ["name", "location", "location_type"], "Special")
df_fish_location = split_location(df_fish_location, "region", "location", regions, ["name", "location", "location_type", "region"], "Everywhere")
df_fish_location = split_location(df_fish_location, "special_area", "location", special_area, ["name", "location", "location_type", "region", "special_area"])
df_fish_location = split_location(df_fish_location, "rules", "location", rules_source, ["name", "location", "location_type", "region", "special_area", "rules"])

# COMMAND ----------

# MAGIC %md
# MAGIC # Separação df_crabpotandothercatchables_silver
# MAGIC Vamos separar o DataFrame df_crabpotandothercatchables_silver em dois, já que atualmente ele possui dados conceitualmente diferentes armazenados juntos e popular o df_fish_location com seus respectivos dados.

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

price_new_values = {
    "Fisher Profession (+25%)": "fisher_profession_base",
    "Base Price": "base_price",
    "BP Silver": "base_price_silver",
    "FP Irridium": "fisher_profession_iridium",
    "AP Irridium": "angler_profession_iridium",
    "AP Gold": "angler_profession_gold",
    "FP Silver": "fisher_profession_silver",
    "BP Irridium": "base_price_iridium",
    "BP Gold": "base_price_gold",
    "Angler Profession (+50%)": "angler_profession_base",
    "AP Silver": "angler_profession_silver",
    "FP Gold": "fisher_profession_gold"
}


df_fish_price_breakdown_silver = replace_col_values(df_fish_price_breakdown_silver, "name", price_new_values)

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

price_new_values = {
    "Fisher Profession (+25%)": "fisher_profession_base",
    "Base Price": "base_price",
    "BP Silver": "base_price_silver",
    "FP Irridium": "fisher_profession_iridium",
    "AP Irridium": "angler_profession_iridium",
    "AP Gold": "angler_profession_gold",
    "FP Silver": "fisher_profession_silver",
    "BP Irridium": "base_price_iridium",
    "BP Gold": "base_price_gold",
    "Angler Profession (+50%)": "angler_profession_base",
    "AP Silver": "angler_profession_silver",
    "FP Gold": "fisher_profession_gold"
}

df_legendary_fish_price_breakdown_silver = replace_col_values(df_legendary_fish_price_breakdown_silver, "name", price_new_values)

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
# MAGIC # Junção dos DFs df_legendary_fish_detail e df_legendaru_fishII_detail
# MAGIC Depois de normalizado o DataFrame df_legendary_fishII_detail_silver, vamos uni-lo ao DataFrame df_legendary_fish_detail_silver em um único DF final com informações de peixes lendários.

# COMMAND ----------

# DBTITLE 1,df_legendary_fish_detail_final_silver
df_legendary_fish_detail_final_silver = df_legendary_fish_detail_silver.unionByName(df_legendary_fishII_detail_silver)

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
# MAGIC # Adição de colunas df_behavior
# MAGIC Vamos adicionar colunas de mapeamento estrutural importantes para o df_behavior.

# COMMAND ----------

# DBTITLE 1,df_behavior
df_behavior_silver = df_behavior_silver.withColumn("behavior_complexity", f.when(col("behavior") == "Dart", "Difícil").when(col("behavior") == "Smooth", "Muito Fácil").otherwise("Médio")) \
  .withColumn("complexity_score", f.when(col("behavior") == "Dart", lit(5)).when(col("behavior") == "Smooth", lit(1)).otherwise(3)) \
    .withColumn("behavior", f.lower(col("behavior"))) \
      .select("behavior", "description", "behavior_complexity", "complexity_score", "ingestion_timestamp")

# COMMAND ----------

# MAGIC %md
# MAGIC # Salvando os dados na camada Silver
# MAGIC Depois de algumas colunas devidamente realocadas e dados divididos corretamente, podemos salvar nossos DataFrames na camada Silver como versões atualizadas apóes mudarem de Schema, para que prossigamos fazendo refinamentos posteriormente, garantindo o acesso aos novos dados modificados de forma segura.

# COMMAND ----------

# DBTITLE 1,fish_location
try:
    df_fish_location.write.mode("overwrite").saveAsTable("stardew_project.silver.fish_location")
except Exception as e:
    print("Erro ao salvar a tabela fish_location: ", e)
    raise

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

# DBTITLE 1,legendary_fish_detail_v2
try:
    save_df(df_legendary_fish_detail_final_silver, "silver", "legendary_fish_detail_v2", "name")
except Exception as e:
    print("Erro ao salvar a tabela legendary_fish_detail_v2: ", e)
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

# COMMAND ----------

# DBTITLE 1,behavior_v2
try:
    save_df(df_behavior_silver, "silver", "behavior_v2", "behavior")
except Exception as e:
    print("Erro ao salvar a tabela behavior_v2: ", e)
    raise
