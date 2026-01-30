# Databricks notebook source
# MAGIC %md
# MAGIC # Fase 5 - Enriquecimento dos Dados
# MAGIC Depois dos dados devidamente tratados durante toda a camada Silver, vamos iniciar a camada Gold enriquecendo os dados, adicionando novas colunas relevantes.

# COMMAND ----------

# DBTITLE 1,Importação dos Dados
# Importando nossos dados como DataFrames para fazermos o enriquecimento.

df_crab_pot_fish_gold = spark.read.table("stardew_project.silver.crab_pot_fish")

df_other_catchables_gold = spark.read.table("stardew_project.silver.other_catchables")

df_fish_detail_gold = spark.read.table("stardew_project.silver.fish_detail")

df_fish_location_gold = spark.read.table("stardew_project.silver.fish_location")

df_fish_price_breakdown_gold = spark.read.table("stardew_project.silver.fish_price_breakdown_v2")

df_legendary_fish_detail_final_gold = spark.read.table("stardew_project.silver.legendary_fish_detail_v2")

df_legendary_fish_price_breakdown_gold = spark.read.table("stardew_project.silver.legendary_fish_price_breakdown_v2")

df_nightmarketfish_gold = spark.read.table("stardew_project.silver.nightmarketfish_v2")

df_nightmarketfish_price_breakdown_gold = spark.read.table("stardew_project.silver.nightmarketfish_price_breakdown")

df_behavior_gold = spark.read.table("stardew_project.silver.behavior_v2")

# COMMAND ----------

# MAGIC %md
# MAGIC # Funções auxiliares
# MAGIC Import da função auxiliar que vai realizar o processo de salvamento posterior aos DataFrames.

# COMMAND ----------

# DBTITLE 1,Import das Funções Auxiliares
# MAGIC %run /Workspace/Users/sjoao5498@gmail.com/stardew_valley_fishing_etl/utils/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Adicionando as colunas effort_score e effort
# MAGIC Vamos agora adicionar aos DataFrames que possuem informações sobre os peixes colunas sobre esforço para serem pegos, o que vai nos ajudar futuramente com a geração de consultas importantes para Dashboards.

# COMMAND ----------

# DBTITLE 1,effort_score e effort
df_fish_detail_join = df_fish_detail_gold.join(df_fish_location_gold, "name", "inner").join(df_behavior_gold, "behavior", "inner")

df_fish_detail_location_effort = df_fish_detail_join.groupBy("name").agg(f.when(f.count(col("location_type")) == 1, lit(1)).when(f.count(col("location_type")) <= 3, lit(2)).otherwise(lit(3)).alias("location_effort"))

df_fish_detail_join = df_fish_detail_join.join(df_fish_detail_location_effort, "name", "inner")

df_fish_detail_join = df_fish_detail_join.withColumn("effort_score", col("location_effort") + col("complexity_score")).withColumn("effort", f.when(col("effort_score").isin(1, 2, 3), "Baixo") \
    .when((col("effort_score").isin(4, 5, 6)) & (col("rules").isNull()), "Médio") \
        .otherwise("Alto"))

df_fish_detail_gold = df_fish_detail_join.select("name", "effort_score", "effort").join(df_fish_detail_gold, "name", "right_outer").distinct()

# COMMAND ----------

# DBTITLE 1,effort_score e effort
df_legendary_fish_detail_join = df_legendary_fish_detail_final_gold.join(df_fish_location_gold, "name", "inner").join(df_behavior_gold, "behavior", "inner")

df_legendary_fish_detail_location_effort = df_legendary_fish_detail_join.groupBy("name").agg(f.when(f.count(col("location_type")) == 1, lit(1)).when(f.count(col("location_type")) <= 3, lit(2)).otherwise(lit(3)).alias("location_effort"))

df_legendary_fish_detail_join = df_legendary_fish_detail_join.join(df_legendary_fish_detail_location_effort, "name", "inner")

df_legendary_fish_detail_join = df_legendary_fish_detail_join.withColumn("effort_score", col("location_effort") + col("complexity_score")).withColumn("effort", f.when(col("effort_score").isin(1, 2, 3), "Baixo") \
    .when((col("effort_score").isin(4, 5, 6)) & (col("rules").isNull()), "Médio") \
        .otherwise("Alto"))

df_legendary_fish_detail_final_gold = df_legendary_fish_detail_join.select("name", "effort_score", "effort").join(df_legendary_fish_detail_final_gold, "name", "right_outer").distinct()

# COMMAND ----------

# DBTITLE 1,effort_score e effort
df_nightmarketfish_gold = df_nightmarketfish_gold.withColumn("behavior", f.lower(col("behavior")))

df_nightmarketfish_join = df_nightmarketfish_gold.join(df_behavior_gold, "behavior", "inner")

df_nightmarketfish_join = df_nightmarketfish_join.withColumn("location_effort", lit(3))

df_nightmarketfish_join = df_nightmarketfish_join.withColumn("effort_score", col("location_effort") + col("complexity_score")).withColumn("effort", f.when(col("effort_score").isin(1, 2, 3), "Baixo") \
    .when((col("effort_score").isin(4, 5, 6)), "Médio") \
        .otherwise("Alto"))

df_nightmarketfish_gold = df_nightmarketfish_join.select("name", "effort_score", "effort").join(df_nightmarketfish_gold, "name", "right_outer")

# COMMAND ----------

# DBTITLE 1,effort_score e effort
df_crab_pot_fish_join = df_crab_pot_fish_gold.join(df_fish_location_gold, "name", "inner")

df_crab_pot_fish_location_effort = df_crab_pot_fish_join.groupBy("name").agg(f.when(f.count(col("location_type")) == 1, lit(1)).when(f.count(col("location_type")) <= 3, lit(2)).otherwise(lit(3)).alias("location_effort"))

df_crab_pot_fish_join = df_crab_pot_fish_join.join(df_crab_pot_fish_location_effort, "name", "inner")

df_crab_pot_fish_join = df_crab_pot_fish_join.withColumn("effort_score", col("location_effort") + lit(1)).withColumn("effort", f.when(col("effort_score").isin(1, 2, 3), "Baixo") \
    .when((col("effort_score").isin(4, 5, 6)) & (col("rules").isNull()), "Médio") \
        .otherwise("Alto"))

df_crab_pot_fish_gold = df_crab_pot_fish_join.select("name", "effort_score", "effort").join(df_crab_pot_fish_gold, "name", "right_outer").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC # Adicionando a coluna is_beginner_friendly
# MAGIC Vamos agora adicionar novamente aos DataFrames que possuem informações sobre os peixes a coluna is_beginner_friendly, que representará a facilidade para novos jogadores em obter o peixe em questão.

# COMMAND ----------

# DBTITLE 1,is_beginner_friendly
df_fish_detail_join = df_fish_detail_join.withColumn("is_beginner_friendly", f.when((col("difficulty").between(0, 50)) & ((col("complexity_score") == 1) | (col("complexity_score") == 2)) & (col("rules").isNull()), True).otherwise(False))

df_fish_detail_gold = df_fish_detail_join.select("name", "is_beginner_friendly").join(df_fish_detail_gold, "name", "right_outer").distinct()

# COMMAND ----------

# DBTITLE 1,is_beginner_friendly
df_legendary_fish_detail_join = df_legendary_fish_detail_join.withColumn("is_beginner_friendly", f.when((col("difficulty").between(0, 50)) & ((col("complexity_score") == 1) | (col("complexity_score") == 2)) & (col("rules").isNull()), True).otherwise(False))

df_legendary_fish_detail_final_gold = df_legendary_fish_detail_join.select("name", "is_beginner_friendly").join(df_legendary_fish_detail_final_gold, "name", "right_outer").distinct()

# COMMAND ----------

# DBTITLE 1,is_beginner_friendly
df_nightmarketfish_join = df_nightmarketfish_join.withColumn("is_beginner_friendly", f.when((col("difficulty").between(0, 50)) & ((col("complexity_score") == 1) | (col("complexity_score") == 2)), True).otherwise(False))

df_nightmarketfish_gold = df_nightmarketfish_join.select("name", "is_beginner_friendly").join(df_nightmarketfish_gold, "name", "right_outer").distinct()

# COMMAND ----------

# DBTITLE 1,is_beginner_friendly
df_crab_pot_fish_join = df_crab_pot_fish_join.withColumn("is_beginner_friendly", f.when((col("trap_chance_non_mariner") >= 10) & ((col("effort_score") == 1) | (col("effort_score") == 2)) & (col("rules").isNull()), True).otherwise(False))

df_crab_pot_fish_gold = df_crab_pot_fish_join.select("name", "is_beginner_friendly").join(df_crab_pot_fish_gold, "name", "right_outer").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC # Adicionando a coluna is_best_early_game_fish
# MAGIC Vamos agora adicionar novamente aos DataFrames que possuem informações sobre os peixes a coluna is_best_early_game_fish, que representará quais peixes são melhores para o começo de jogo, considerando fatores como dificuldade, retornos e mecânica.

# COMMAND ----------

# DBTITLE 1,is_best_early_game_fish
df_fish_detail_join = df_fish_detail_join.withColumn("is_best_early_game_fish", f.when((col("is_beginner_friendly") == True) & ((col("effort") == "Baixo") | (col("effort") == "Médio")) & (col("rules").isNull()) & (col("difficulty").between(0, 50)), True).otherwise(False))

df_fish_detail_gold = df_fish_detail_join.select("name", "is_best_early_game_fish").join(df_fish_detail_gold, "name", "right_outer").distinct()

# COMMAND ----------

# DBTITLE 1,is_best_early_game_fish
df_legendary_fish_detail_join = df_legendary_fish_detail_join.withColumn("is_best_early_game_fish", f.when((col("is_beginner_friendly") == True) & ((col("effort") == "Baixo") | (col("effort") == "Médio")) & (col("rules").isNull()) & (col("difficulty").between(0, 50)), True).otherwise(False))

df_legendary_fish_detail_final_gold = df_legendary_fish_detail_join.select("name", "is_best_early_game_fish").join(df_legendary_fish_detail_final_gold, "name", "right_outer").distinct()

# COMMAND ----------

# DBTITLE 1,is_best_early_game_fish
df_nightmarketfish_join = df_nightmarketfish_join.withColumn("is_best_early_game_fish", f.when((col("is_beginner_friendly") == True) & ((col("effort") == "Baixo") | (col("effort") == "Médio")) & (col("difficulty").between(0, 50)), True).otherwise(False))

df_nightmarketfish_gold = df_nightmarketfish_join.select("name", "is_best_early_game_fish").join(df_nightmarketfish_gold, "name", "right_outer").distinct()

# COMMAND ----------

# DBTITLE 1,is_best_early_game_fish
df_crab_pot_fish_join = df_crab_pot_fish_join.withColumn("is_best_early_game_fish", f.when((col("is_beginner_friendly") == True) & ((col("effort") == "Baixo") | (col("effort") == "Médio")) & (col("rules").isNull()), True).otherwise(False))

df_crab_pot_fish_gold = df_crab_pot_fish_join.select("name", "is_best_early_game_fish").join(df_crab_pot_fish_gold, "name", "right_outer").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC # Adicionando a coluna progression_phase
# MAGIC Agora vamos criar a coluna progression_phase, que classifica quais peixes podem ser encaixados em qual fase de jogo (Early, Mid, Late e Any).

# COMMAND ----------

# DBTITLE 1,progression_phase
df_fish_detail_join = df_fish_detail_join.withColumn("progression_phase", f.when(((col("difficulty").between(0, 60)) | (col("effort_score").between(1, 3))) & ((col("season") == "All Seasons")), "Early") \
    .when(((col("difficulty").between(61, 70)) | (col("effort_score").between(4, 5))) & (col("season") != "All Seasons"), "Mid") \
        .when((col("difficulty") > 70) | (col("effort_score") > 5), "Late").otherwise("Any")
)

df_fish_detail_gold = df_fish_detail_join.select("name", "progression_phase").join(df_fish_detail_gold, "name", "right_outer").distinct()

# COMMAND ----------

# DBTITLE 1,progression_phase
df_legendary_fish_detail_join = df_legendary_fish_detail_join.withColumn("progression_phase", f.when(((col("difficulty").between(0, 60)) | (col("effort_score").between(1, 3))) & ((col("season") == "All Seasons")), "Early") \
    .when(((col("difficulty").between(61, 70)) | (col("effort_score").between(4, 5))) & (col("season") != "All Seasons"), "Mid") \
        .when((col("difficulty") > 70) | (col("effort_score") > 5), "Late").otherwise("Any")
)

df_legendary_fish_detail_final_gold = df_legendary_fish_detail_join.select("name", "progression_phase").join(df_legendary_fish_detail_final_gold, "name", "right_outer").distinct()

# COMMAND ----------

# DBTITLE 1,progression_phase
df_nightmarketfish_join = df_nightmarketfish_join.withColumn("progression_phase", f.when((col("difficulty").between(0, 60)) | (col("effort_score").between(1, 3)), "Early") \
    .when((col("difficulty").between(61, 70)) | (col("effort_score").between(4, 5)), "Mid") \
        .when((col("difficulty") > 70) | (col("effort_score") > 5), "Late").otherwise("Any")
)

df_nightmarketfish_gold = df_nightmarketfish_join.select("name", "progression_phase").join(df_nightmarketfish_gold, "name", "right_outer").distinct()

# COMMAND ----------

# DBTITLE 1,progression_phase
df_crab_pot_fish_join = df_crab_pot_fish_join.withColumn("progression_phase", f.when(col("effort_score").between(1, 3), "Early") \
    .when(col("effort_score").between(4, 5), "Mid") \
        .when(col("effort_score") > 5, "Late").otherwise("Any")
)

df_crab_pot_fish_gold = df_crab_pot_fish_join.select("name", "progression_phase").join(df_crab_pot_fish_gold, "name", "right_outer").distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC # Salvando os dados na camada Gold
# MAGIC Depois de termos colunas derivadas criadas com base nos dados que já possuíamos, podemos salvar nossos DataFrames na camada Gold para que consigamos seguir transformando estes dados em KPIs para Dashboards.

# COMMAND ----------

# DBTITLE 1,fish_detail
try:
    save_df(df_fish_detail_gold, "gold", "fish_detail", "name")
except Exception as e:
    print("Erro ao salvar a tabela fish_detail: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,legendary_fish_detail
try:
    save_df(df_legendary_fish_detail_final_gold, "gold", "legendary_fish_detail", "name")
except Exception as e:
    print("Erro ao salvar a tabela legendary_fish_detail: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,nightmarketfish
try:
    save_df(df_nightmarketfish_gold, "gold", "nightmarketfish", "name")
except Exception as e:
    print("Erro ao salvar a tabela nightmarketfish: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,crab_pot_fish
try:
    save_df(df_crab_pot_fish_gold, "gold", "crab_pot_fish", "name")
except Exception as e:
    print("Erro ao salvar a tabela crab_pot_fish: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,fish_location
try:
    df_fish_location_gold.write.mode("overwrite").saveAsTable("stardew_project.gold.fish_location")
except Exception as e:
    print("Erro ao salvar a tabela fish_location: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,other_catchables
try:
    save_df(df_other_catchables_gold, "gold", "other_catchables", "name")
except Exception as e:
    print("Erro ao salvar a tabela other_catchables: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,fish_price_breakdown
try:
    save_df(df_fish_price_breakdown_gold, "gold", "fish_price_breakdown", "fish")
except Exception as e:
    print("Erro ao salvar a tabela fish_price_breakdown: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,legendary_fish_price_breakdown
try:
    save_df(df_legendary_fish_price_breakdown_gold, "gold", "legendary_fish_price_breakdown", "fish")
except Exception as e:
    print("Erro ao salvar a tabela legendary_fish_price_breakdown: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,nightmarketfish_price_breakdown
try:
    save_df(df_nightmarketfish_price_breakdown_gold, "gold", "nightmarketfish_price_breakdown", "fish")
except Exception as e:
    print("Erro ao salvar a tabela nightmarketfish_price_breakdown: ", e)
    raise

# COMMAND ----------

# DBTITLE 1,behavior
try:
    save_df(df_behavior_gold, "gold", "behavior", "behavior")
except Exception as e:
    print("Erro ao salvar a tabela behavior: ", e)
    raise
