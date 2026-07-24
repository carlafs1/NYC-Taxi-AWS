"""
## Silver
### Consolidação, Padronização e Qualidade dos Dados

*Case Técnico — Engenharia de Dados · iFood*

---

**Objetivo**

Implementar a camada Silver do Lakehouse, responsável pela consolidação dos dados
provenientes da camada Bronze, padronização dos schemas, validação da qualidade dos
dados e tratamento das principais inconsistências identificadas, preparando o
conjunto de dados para consumo analítico na camada Gold.

**Fonte dos dados**

| | |
|---|---|
| Dataset  | NYC TLC Trip Record Data |
| Período  | Janeiro a Maio de 2023 |
| Serviços | Yellow Cab, Green Cab |
| Formato  | Parquet (camada Bronze) |

**Atividades realizadas**

- Leitura consolidada dos dados da camada Bronze;
- Padronização dos schemas entre Yellow e Green Taxi;
- Validação dos tipos de dados;
- Auditoria de consistência dos atributos;
- Tratamento de inconsistências identificadas;
- Validação de registros duplicados;
- Preparação do conjunto de dados para a camada Gold.

---

Adaptação do notebook 02_Silver.ipynb (Databricks) para script PySpark em EMR,
considerando:
- no notebook os arquivos Bronze ficavam num Volume (BRONZE_PATH fixo); aqui são
  lidos direto do S3, na estrutura Hive-style gerada pelo build_bronze.py
  (yellow|green/year=AAAA/month=MM/...).
- `dbutils.fs.ls` e `display()` não existem fora do notebook Databricks — vira
  listagem via boto3 e `.show()`.
- Saída registrada como tabela Iceberg no Glue Data Catalog (glue_catalog.<db>.trips),
  no lugar de Delta + saveAsTable (Unity Catalog).
- Particionamento por year/month (Hive-style, duas partições — mesmo padrão adotado
  na Bronze). year/month são colunas inteiras derivadas do timestamp de pickup,
  usadas tanto como chave de partição física quanto nas análises abaixo — não existe
  mais uma coluna ano_mes (string "yyyy-MM") intermediária.
- OPTIMIZE + ZORDER (sintaxe Delta) vira CALL ...rewrite_data_files (procedimento
  Iceberg equivalente).
- Período parametrizado via --anos-meses (mesmo padrão da Bronze). Leitura da
  Bronze escopada só às partições pedidas; escrita usa overwrite dinâmico por
  partição (year/month) no Iceberg — reprocessar um mês existente substitui só
  aquela partição, mês novo só é adicionado; o resto da tabela Silver não é
  tocado.

**Validações**

As validações desta etapa têm caráter informativo e visam apoiar a análise da 
qualidade dos dados e do processamento. Neste projeto, o acompanhamento será 
realizado visualmente por meio dos relatórios e estatísticas gerados durante 
a execução. Caso sejam identificadas recorrências ou padrões de inconsistência, 
essas validações poderão evoluir para mecanismos automatizados de alerta ou 
interrupção do processamento.  

Uso:
    spark-submit build_silver.py \
        --bronze-bucket <bucket-bronze> \
        --silver-bucket <bucket-silver> \
        --glue-database silver \
        --catalog catalog \
        --anos-meses 2023-01,2023-02,2023-03,2023-04,2023-05
"""

####-------------------####
####----  Imports  ----####
####-------------------####
import argparse
import re

import boto3
from botocore.config import Config
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


####---------------------####
####----  Arguments  ----####
####---------------------####

PADRAO_ANO_MES = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


####---- Recebe os buckets de Bronze/Silver, o database e catalog do Glue, e
####---- o período (--anos-meses) a processar.
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-bucket", required=True)
    parser.add_argument("--silver-bucket", required=True)
    parser.add_argument("--glue-database", default="silver")
    parser.add_argument(
        "--catalog",
        default="catalog",
        help="Nome do catalog Spark, único e compartilhado por bronze/silver/gold via --glue-database; aponta para o Glue Data Catalog via Iceberg",
    )
    parser.add_argument(
        "--anos-meses",
        required=True,
        help="Lista separada por vírgula, ex: 2023-01,2023-02,2023-03,2023-04,2023-05. Mesmo formato usado na Bronze.",
    )

    ####---- Utiliza parse_known_args para ignorar argumentos adicionais que
    ####---- possam ser passados pelo ambiente de execução.
    args, _ = parser.parse_known_args()
    return args


####---- Valida o formato AAAA-MM de cada item de --anos-meses (mesma regra
####---- da Bronze) e remove duplicatas preservando a ordem original.
def validar_anos_meses(anos_meses: list) -> list:
    invalidos = [item for item in anos_meses if not PADRAO_ANO_MES.match(item)]
    if invalidos:
        raise ValueError(
            f"anos-meses inválido(s): {invalidos}. Formato esperado: AAAA-MM (mês entre 01 e 12)."
        )

    anos_meses_unicos = list(dict.fromkeys(anos_meses))
    duplicados = len(anos_meses) - len(anos_meses_unicos)
    if duplicados:
        print(f"Aviso: {duplicados} ano_mes duplicado(s) removido(s) da lista.")

    return anos_meses_unicos


####---- Calcula o intervalo [data_inicio, data_fim_exclusivo) a partir de
####---- --anos-meses, para o filtro de "fora do período" — substitui os
####---- limites fixos (2023-01-01/2023-05-31) do case original, agora que
####---- o período é parametrizado. data_fim_exclusivo é o dia 1 do mês
####---- seguinte ao mais recente, evitando calcular quantos dias tem o mês.
def calcular_intervalo_datas(anos_meses: list) -> tuple:
    ano_min, mes_min = (int(x) for x in min(anos_meses).split("-"))
    ano_max, mes_max = (int(x) for x in max(anos_meses).split("-"))

    data_inicio = f"{ano_min:04d}-{mes_min:02d}-01"

    if mes_max == 12:
        data_fim_exclusivo = f"{ano_max + 1}-01-01"
    else:
        data_fim_exclusivo = f"{ano_max:04d}-{mes_max + 1:02d}-01"

    return data_inicio, data_fim_exclusivo


####-------------------------####
####----  1. Bronze I/O  ----####
####-------------------------####

####---- Lista os arquivos Parquet de um taxi_type na Bronze, escopado às
####---- partições year=/month= de anos_meses (Hive-style, gerada pelo
####---- build_bronze.py) — não lista o bucket inteiro.
def listar_arquivos_bronze(s3_client, bucket: str, taxi_type: str, anos_meses: list) -> list:
    paginator = s3_client.get_paginator("list_objects_v2")
    arquivos = []

    for ano_mes in anos_meses:
        ano, mes = ano_mes.split("-")
        prefixo = f"{taxi_type}/year={ano}/month={mes}/"

        for pagina in paginator.paginate(Bucket=bucket, Prefix=prefixo):
            for obj in pagina.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    arquivos.append(f"s3://{bucket}/{obj['Key']}")

    return sorted(arquivos)


####-------------------------------------------####
####----  2. Leitura, Cast e Padronização  ----####
####-------------------------------------------####

####---- Leitura dos arquivos, cast dos atributos com datatype divergente
####---- (identificado na Bronze; mantido o tipo de origem após análise de
####---- min/max) e padronização dos nomes de coluna.

####---- Yellow

def ler_yellow_silver(spark, path):
    print(f"[Yellow] Lendo: {path}")
    df = spark.read.parquet(path)

    if "Airport_fee" in df.columns:
        df = df.withColumnRenamed("Airport_fee", "airport_fee")

    return (
        df
        .withColumn("VendorID", F.col("VendorID").cast("int"))
        .withColumn("passenger_count", F.col("passenger_count").cast("long"))
        .withColumn("RatecodeID", F.col("RatecodeID").cast("long"))
        .withColumn("payment_type", F.col("payment_type").cast("long"))
        .withColumn("airport_fee", F.col("airport_fee").cast("double"))
        .withColumn("ehail_fee", F.lit(0.0).cast("double"))
        .withColumn("trip_type", F.lit(0).cast("long"))
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
        .withColumn("tipo", F.lit("yellow"))
    )


####---- Green

def ler_green_silver(spark, path):
    print(f"[Green] Lendo: {path}")
    return (
        spark.read.parquet(path)
        .withColumn("VendorID", F.col("VendorID").cast("int"))
        .withColumn("passenger_count", F.col("passenger_count").cast("long"))
        .withColumn("RatecodeID", F.col("RatecodeID").cast("long"))
        .withColumn("payment_type", F.col("payment_type").cast("long"))
        .withColumn("trip_type", F.col("trip_type").cast("long"))
        .withColumn("airport_fee", F.lit(0.0).cast("double"))
        .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
        .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")
        .withColumn("tipo", F.lit("green"))
    )


####---------------------------------------####
####----  3. Schema Final e Validação  ----####
####---------------------------------------####

SCHEMA_SILVER = {
    "VendorID":              "int",
    "pickup_datetime":       "timestamp_ntz",
    "dropoff_datetime":      "timestamp_ntz",
    "store_and_fwd_flag":    "string",
    "RatecodeID":            "long",
    "PULocationID":          "long",
    "DOLocationID":          "long",
    "passenger_count":       "long",
    "trip_distance":         "double",
    "fare_amount":           "double",
    "extra":                 "double",
    "mta_tax":               "double",
    "tip_amount":            "double",
    "tolls_amount":          "double",
    "ehail_fee":             "double",
    "improvement_surcharge": "double",
    "total_amount":          "double",
    "payment_type":          "long",
    "trip_type":             "long",
    "congestion_surcharge":  "double",
    "airport_fee":           "double",
    "tipo":                  "string",
}


####---- Aplica o schema final via cast, coluna a coluna, validando
####---- se todas as colunas esperadas estão presentes.
def aplicar_schema(df, schema_dict):
    colunas_faltantes = []

    for coluna in schema_dict:
        if coluna not in df.columns:
            colunas_faltantes.append(coluna)

    if colunas_faltantes:
        raise ValueError(
            f"Colunas ausentes no DataFrame de origem: {colunas_faltantes}"
        )

    colunas_tratadas = []

    for coluna, tipo in schema_dict.items():
        coluna_tratada = F.col(coluna).cast(tipo).alias(coluna)
        colunas_tratadas.append(coluna_tratada)

    return df.select(*colunas_tratadas)


####---- Compara contagem de não-nulos antes/depois do cast, coluna a
####---- coluna, pra identificar perda de dado causada pelo cast. Só
####---- calcula e retorna os dados — a impressão do resumo (com
####---- percentual) fica a cargo de quem chama, no main().
def validar_casts(df_raw, df_casted, schema_dict):
    expressoes_contagem = []

    for coluna in schema_dict:
        expressoes_contagem.append(F.count(coluna).alias(coluna))

    contagem_raw = df_raw.select(*expressoes_contagem).collect()[0].asDict()
    contagem_cast = df_casted.select(*expressoes_contagem).collect()[0].asDict()

    resultado = []

    for coluna in schema_dict:
        antes = contagem_raw[coluna]
        depois = contagem_cast[coluna]
        perdidos = antes - depois

        resultado.append((coluna, antes, depois, perdidos))

    return resultado


####--------------------------------------####
####----  4. Tratamento de Atributos  ----####
####--------------------------------------####

####---- Trata passenger_count: investiga distribuição, nulos e valor
####---- zero, checa relação com outras variáveis, mede o antes/depois e
####---- substitui nulos e zeros pela mediana. Retorna o df tratado.
####----
####---- A) Distribuição dos valores — frequência por passenger_count.
####---- B) Nulos — quantidade e relação com VendorID, trip_type,
####---- store_and_fwd_flag e total_amount.
####---- C) Zero — as mesmas quatro checagens de B.
####---- D) Estatísticas antes do tratamento (média, mediana).
####---- E) Tratamento — nulos e zeros substituídos pela mediana.
####---- F) Validação — nulos e zeros remanescentes.
####---- G) Estatísticas após o tratamento (média, mediana).
def tratar_passenger_count(df_silver):

    ####---- A) Distribuição dos valores.
    print("\n[passenger_count] A) Distribuição de valores (incl. nulos e zeros):")
    (
        df_silver
        .groupBy("passenger_count")
        .count()
        .orderBy("passenger_count")
    ).show(truncate=False)

    ####---- B) Nulos — relação com VendorID, trip_type, store_and_fwd_flag
    ####---- e total_amount. Achado: concentrados nas linhas onde
    ####---- store_and_fwd_flag também é nulo — sugere metadado não
    ####---- registrado na origem, mas não é confirmável só com os dados
    ####---- disponíveis.
    print("\n[passenger_count] B) % de nulos por VendorID:")
    (
        df_silver
        .groupBy("VendorID")
        .agg(
            F.count("*").alias("total"),
            F.sum(F.col("passenger_count").isNull().cast("int")).alias("passenger_count_nulo"),
        )
        .withColumn(
            "pct_passenger_count_nulo",
            F.round(F.col("passenger_count_nulo") / F.col("total") * 100, 2),
        )
        .orderBy("VendorID")
    ).show(truncate=False)

    print("\n[passenger_count] B) % de nulos por trip_type (só Green):")
    (
        df_silver
        .filter(F.col("tipo") == "green")
        .groupBy("trip_type")
        .agg(
            F.count("*").alias("total"),
            F.sum(F.col("passenger_count").isNull().cast("int")).alias("passenger_count_nulo"),
        )
        .withColumn(
            "pct_passenger_count_nulo",
            F.round(F.col("passenger_count_nulo") / F.col("total") * 100, 2),
        )
        .orderBy("trip_type")
    ).show(truncate=False)

    print("\n[passenger_count] B) % de nulos por store_and_fwd_flag "
          "(concentração encontrada aqui):")
    (
        df_silver
        .groupBy("store_and_fwd_flag")
        .agg(
            F.count("*").alias("total"),
            F.sum(F.col("passenger_count").isNull().cast("int")).alias("passenger_count_nulo"),
        )
        .withColumn(
            "pct_passenger_count_nulo",
            F.round(F.col("passenger_count_nulo") / F.col("total") * 100, 2),
        )
    ).show(truncate=False)

    print("\n[passenger_count] B) total_amount médio/mediano: nulo vs. preenchido:")
    (
        df_silver
        .withColumn("passenger_count_nulo", F.col("passenger_count").isNull())
        .groupBy("passenger_count_nulo")
        .agg(
            F.count("*").alias("total"),
            F.avg("total_amount").alias("total_amount_medio"),
            F.expr("percentile_approx(total_amount, 0.5)").alias("total_amount_mediano"),
        )
    ).show(truncate=False)

    ####---- C) Zero — relação com VendorID, trip_type, store_and_fwd_flag
    ####---- e total_amount. Achado: concentrado no VendorID = 1, não em
    ####---- trip_type/store_and_fwd_flag/total_amount.
    print("\n[passenger_count] C) % de zeros por VendorID "
          "(concentração encontrada aqui):")
    (
        df_silver
        .groupBy("VendorID")
        .agg(
            F.count("*").alias("total"),
            F.sum((F.col("passenger_count") == 0).cast("int")).alias("passenger_count_zero"),
        )
        .withColumn(
            "pct_passenger_count_zero",
            F.round(F.col("passenger_count_zero") / F.col("total") * 100, 2),
        )
        .orderBy("VendorID")
    ).show(truncate=False)

    print("\n[passenger_count] C) % de zeros por trip_type (só Green):")
    (
        df_silver
        .filter(F.col("tipo") == "green")
        .groupBy("trip_type")
        .agg(
            F.count("*").alias("total"),
            F.sum((F.col("passenger_count") == 0).cast("int")).alias("passenger_count_zero"),
        )
        .withColumn(
            "pct_passenger_count_zero",
            F.round(F.col("passenger_count_zero") / F.col("total") * 100, 2),
        )
        .orderBy("trip_type")
    ).show(truncate=False)

    print("\n[passenger_count] C) % de zeros por store_and_fwd_flag:")
    (
        df_silver
        .groupBy("store_and_fwd_flag")
        .agg(
            F.count("*").alias("total"),
            F.sum((F.col("passenger_count") == 0).cast("int")).alias("passenger_count_zero"),
        )
        .withColumn(
            "pct_passenger_count_zero",
            F.round(F.col("passenger_count_zero") / F.col("total") * 100, 2),
        )
    ).show(truncate=False)

    print("\n[passenger_count] C) total_amount médio/mediano: zero vs. preenchido:")
    (
        df_silver
        .withColumn("passenger_count_zero", F.col("passenger_count") == 0)
        .groupBy("passenger_count_zero")
        .agg(
            F.count("*").alias("total"),
            F.avg("total_amount").alias("total_amount_medio"),
            F.expr("percentile_approx(total_amount, 0.5)").alias("total_amount_mediano"),
        )
    ).show(truncate=False)

    ####---- D) Estatísticas antes do tratamento.
    print("\n[passenger_count] D) Média/mediana ANTES do tratamento:")
    (
        df_silver.agg(
            F.avg("passenger_count").alias("media"),
            F.expr("percentile_approx(passenger_count, 0.5)").alias("mediana"),
        )
    ).show(truncate=False)

    ####---- E) Tratamento conjunto — nulos e zeros substituídos pela
    ####---- mediana (menos sensível a extremos que a média). Nenhuma das
    ####---- análises B/C associou os zeros a uma regra de negócio
    ####---- específica — tratados aqui como o mesmo problema de dado
    ####---- ausente que os nulos. A mediana é calculada só sobre valores
    ####---- válidos (> 0) — do contrário os próprios zeros que serão
    ####---- substituídos entrariam no cálculo do valor de substituição.
    mediana = int(
        df_silver
        .filter(F.col("passenger_count") > 0)
        .approxQuantile("passenger_count", [0.5], 0.01)[0]
    )

    df_silver = df_silver.withColumn(
        "passenger_count",
        F.when(
            F.col("passenger_count").isNull() | (F.col("passenger_count") == 0),
            F.lit(mediana),
        ).otherwise(F.col("passenger_count")),
    )

    ####---- F) Validação — nulos e zeros remanescentes.
    print("\n[passenger_count] F) Validação pós-tratamento — devem ser 0:")
    (
        df_silver.agg(
            F.sum(F.col("passenger_count").isNull().cast("int")).alias("nulos_remanescentes"),
            F.sum((F.col("passenger_count") == 0).cast("int")).alias("zeros_remanescentes"),
        )
    ).show(truncate=False)

    ####---- G) Estatísticas após o tratamento.
    print("\n[passenger_count] G) Média/mediana APÓS o tratamento:")
    (
        df_silver.agg(
            F.avg("passenger_count").alias("media"),
            F.expr("percentile_approx(passenger_count, 0.5)").alias("mediana"),
        )
    ).show(truncate=False)

    return df_silver


####---- Trata pickup_datetime/dropoff_datetime: investiga inconsistências
####---- (nulo, dropoff < pickup), corrige as invertidas e recalcula
####---- data_corrida/year/month a partir das colunas tratadas. Retorna o
####---- df com pickup_datetime_tratado/dropoff_datetime_tratado e
####---- data_corrida/year/month já atualizados.
def tratar_timestamps(df_silver):

    ####---- Não foram identificados nulos após a validação dos casts.
    ####---- Encontrados registros com dropoff_datetime anterior a
    ####---- pickup_datetime.
    df_timestamp_validacao = (
        df_silver
        .withColumn(
            "problema_timestamp",
            F.when(F.col("pickup_datetime").isNull(), "pickup_nulo")
             .when(F.col("dropoff_datetime").isNull(), "dropoff_nulo")
             .when(
                 F.col("dropoff_datetime") < F.col("pickup_datetime"),
                 "dropoff_menor_que_pickup",
             )
             .otherwise("valido")
        )
    )

    print("\n[timestamps] Registros com problema (nulo ou dropoff < pickup), "
          "por mês e tipo de problema:")
    (
        df_timestamp_validacao
        .filter(F.col("problema_timestamp") != "valido")
        .groupBy("year", "month", "problema_timestamp")
        .agg(F.count("*").alias("total_registros"))
        .orderBy("year", "month", "problema_timestamp")
    ).show(truncate=False)

    ####---- 93,96% (747/795) desses registros têm store_and_fwd_flag nulo —
    ####---- forte associação, mas não suficiente pra concluir mesma causa.
    print("\n[timestamps] Dos registros com dropoff < pickup, quantos têm "
          "store_and_fwd_flag preenchido:")
    (
        df_silver
        .filter(F.col("dropoff_datetime") < F.col("pickup_datetime"))
        .agg(
            F.count("*").alias("total_inconsistentes"),
            F.count("store_and_fwd_flag").alias("store_and_fwd_flag_preenchido"),
        )
    ).show(truncate=False)

    ####---- 763 registros (96,0%) com diferença de até 1 min; só 2 acima de
    ####---- 1h. Percentual irrelevante da base — tratamento: inverter os
    ####---- timestamps.
    print("\n[timestamps] Diferença (pickup - dropoff) dos registros invertidos, "
          "por faixa de tempo:")
    (
        df_silver
        .filter(F.col("dropoff_datetime") < F.col("pickup_datetime"))
        .withColumn(
            "dif_min",
            (
                F.unix_timestamp("pickup_datetime") -
                F.unix_timestamp("dropoff_datetime")
            ) / 60
        )
        .select(
            F.sum((F.col("dif_min") <= 1).cast("int")).alias("até_1_min"),
            F.sum(((F.col("dif_min") > 1) & (F.col("dif_min") <= 5)).cast("int")).alias("1_a_5_min"),
            F.sum(((F.col("dif_min") > 5) & (F.col("dif_min") <= 60)).cast("int")).alias("5_a_60_min"),
            F.sum((F.col("dif_min") > 60).cast("int")).alias("acima_60_min"),
        )
    ).show(truncate=False)

    ####---- Criadas pickup/dropoff_datetime_tratado com os valores
    ####---- corrigidos. Colunas originais mantidas só para auditoria futura,
    ####---- não são mais usadas daqui em diante. data_corrida, year e month
    ####---- são recalculadas a partir das colunas tratadas — senão uma
    ####---- corrida invertida que atravessa meia-noite ficaria com
    ####---- data/partição baseada no timestamp errado.
    df_silver = (
        df_silver
        .withColumn(
            "pickup_datetime_tratado",
            F.least("pickup_datetime", "dropoff_datetime"),
        )
        .withColumn(
            "dropoff_datetime_tratado",
            F.greatest("pickup_datetime", "dropoff_datetime"),
        )
        .withColumn("data_corrida", F.to_date("pickup_datetime_tratado"))
        .withColumn("year", F.year("pickup_datetime_tratado"))
        .withColumn("month", F.month("pickup_datetime_tratado"))
    )

    ####---- Validação dos registros remanescentes.
    registros_ainda_invertidos = (
        df_silver
        .filter(F.col("dropoff_datetime_tratado") < F.col("pickup_datetime_tratado"))
        .count()
    )
    print(f"\n[timestamps] Registros ainda invertidos após correção "
          f"(deve ser 0): {registros_ainda_invertidos}")

    ####---- Quantidade de registros ajustados.
    print("\n[timestamps] Quantos registros tiveram pickup/dropoff alterados:")
    (
        df_silver.agg(
            F.sum(
                (
                    F.col("pickup_datetime") !=
                    F.col("pickup_datetime_tratado")
                ).cast("int")
            ).alias("pickup_alterado"),

            F.sum(
                (
                    F.col("dropoff_datetime") !=
                    F.col("dropoff_datetime_tratado")
                ).cast("int")
            ).alias("dropoff_alterado"),
        )
    ).show(truncate=False)

    return df_silver


####---- Filtra df_silver ao período efetivamente processado
####---- (data_inicio/data_fim_exclusivo, calculados a partir de
####---- --anos-meses). Investiga o volume fora do período antes de
####---- remover, persiste o resultado (base grande, reaproveitada em
####---- várias análises/ações daqui em diante) e materializa o cache num
####---- único count(). Retorna o df já filtrado e persistido.
def filtrar_periodo_processado(df_silver, data_inicio, data_fim_exclusivo):

    ####---- No case original (jan-mai/2023), 113 registros (0,0007%) tinham
    ####---- data_corrida fora do período esperado. O limite abaixo agora é
    ####---- dinâmico — calculado a partir de --anos-meses, não fixo.
    print(f"\n[período] Registros dentro vs. fora do período solicitado "
          f"({data_inicio} até {data_fim_exclusivo}):")
    (
        df_silver
        .withColumn(
            "fora_do_periodo",
            (F.col("data_corrida") < data_inicio) | (F.col("data_corrida") >= data_fim_exclusivo),
        )
        .groupBy("fora_do_periodo")
        .agg(F.count("*").alias("total"))
    ).show(truncate=False)

    print("\n[período] Registros fora do período, detalhado por tipo/ano/mês:")
    (
        df_silver
        .filter((F.col("data_corrida") < data_inicio) | (F.col("data_corrida") >= data_fim_exclusivo))
        .groupBy("tipo", "year", "month")
        .agg(F.count("*").alias("total"))
        .orderBy("year", "month")
    ).show(truncate=False)

    ####---- Tratamento: sem explicação de negócio plausível, volume
    ####---- irrelevante e fora do escopo do case — registros removidos.
    ####---- MEMORY_AND_DISK por segurança, já que a base é grande. filter +
    ####---- persist na mesma cadeia, e um único count() materializa o
    ####---- cache — evita rodar a linhagem duas vezes.
    df_silver = (
        df_silver
        .filter(
            (F.col("data_corrida") >= data_inicio) & (F.col("data_corrida") < data_fim_exclusivo)
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    total_pos_filtro = df_silver.count()  # conta e materializa o cache na mesma ação
    print(f"\n[período] Total de registros após remover os fora do período: "
          f"{total_pos_filtro:,}")

    return df_silver



####---- Analisa total_amount:
####---------------------------
####---- Investiga outliers pelo critério IQR e sua relação com a duração
####---- das corridas. Analisa também os valores negativos por período,
####---- VendorID e payment_type, recompondo seus componentes financeiros
####---- para validar a consistência de total_amount.
def analisar_total_amount(df_silver):

    ####---- Estatísticas descritivas de total_amount.
    print("\n[total_amount] Estatísticas descritivas (min/max/quartis/média):")
    df_silver.select("total_amount").summary().show(truncate=False)

    ####---- Calcula os limites inferior e superior pelo critério IQR.
    q = (
        df_silver
        .selectExpr(
            "percentile_approx(total_amount, 0.25) as q1",
            "percentile_approx(total_amount, 0.75) as q3",
        )
        .first()
    )

    q1 = q["q1"]
    q3 = q["q3"]
    iqr = q3 - q1

    limite_inferior = q1 - 1.5 * iqr
    limite_superior = q3 + 1.5 * iqr

    print(f"\n[total_amount] Limites de outlier pelo critério IQR "
          f"(Q1 - 1.5*IQR / Q3 + 1.5*IQR):")
    print(f"limite_inferior: {limite_inferior}")
    print(f"limite_superior: {limite_superior}")

    ####---- Quantidade de registros classificados como outliers pelo IQR.
    print("\n[total_amount] Quantidade de registros dentro vs. fora dos limites IQR:")
    (
        df_silver
        .withColumn(
            "outlier",
            (F.col("total_amount") < limite_inferior) |
            (F.col("total_amount") > limite_superior),
        )
        .groupBy("outlier")
        .count()
    ).show(truncate=False)

    ####---- Inspeção dos registros classificados como outliers e de alguns
    ####---- atributos financeiros e operacionais associados.
    print("\n[total_amount] Amostra dos outliers (maiores total_amount primeiro):")
    (
        df_silver
        .filter(
            (F.col("total_amount") < limite_inferior) |
            (F.col("total_amount") > limite_superior)
        )
        .select(
            "tipo",
            "year",
            "month",
            "pickup_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "tolls_amount",
            "airport_fee",
            "total_amount",
        )
        .orderBy(F.desc("total_amount"))
    ).show(truncate=False)

    ####---- Valor cobrado por tempo de corrida, pra ver se outliers altos
    ####---- são justificados por corridas mais longas.
    df_tempos = (
        df_silver
        .withColumn(
            "duracao_minutos",
            (
                F.unix_timestamp("dropoff_datetime_tratado") -
                F.unix_timestamp("pickup_datetime_tratado")
            ) / 60.0
        )
        .filter(F.col("duracao_minutos") > 0)
    )

    ####---- Distribuição de outliers por faixa de duração. Nas faixas
    ####---- curtas (<10min, 10-20min) o valor médio do outlier é ~2x o da
    ####---- faixa — extremos reais. Nas faixas >=30min a proporção de
    ####---- outliers cresce, mas o valor médio do outlier fica perto do 
    ####---- valor médio da faixa inteira — o IQR global (dominado por 
    ####---- corridas curtas, >75% da base) superestima outliers nas faixas
    ####---- longas, já que o valor cresce naturalmente com a duração.
    df_faixas = (
        df_tempos
        .withColumn(
            "faixa_duracao",
            F.when(F.col("duracao_minutos") < 10, "<10 min")
             .when(F.col("duracao_minutos") < 20, "10-20 min")
             .when(F.col("duracao_minutos") < 30, "20-30 min")
             .when(F.col("duracao_minutos") < 60, "30-60 min")
             .otherwise(">60 min")
        )
        .withColumn(
            "outlier",
            (F.col("total_amount") < limite_inferior) |
            (F.col("total_amount") > limite_superior),
        )
    )

    print("\n[total_amount] Outliers por faixa de duração da corrida — "
          "avalia se outliers altos se justificam por corridas mais longas:")
    (
        df_faixas
        .groupBy("faixa_duracao")
        .agg(
            F.count("*").alias("corridas"),
            F.avg("duracao_minutos").alias("duracao_media"),
            F.avg("total_amount").alias("total_medio"),
            F.max("total_amount").alias("maior_total"),
            F.sum(F.col("outlier").cast("int")).alias("qtd_outliers"),
            F.avg(F.when(F.col("outlier"), F.col("total_amount"))).alias("total_medio_outlier"),
            F.expr("percentile(CASE WHEN outlier THEN total_amount END, 0.5)").alias("total_mediano_outlier"),
        )
        .orderBy("duracao_media")
    ).show(truncate=False)

    ####---- Valores negativos
    print("\n[total_amount] Total de registros com valor negativo, e faixa (min/max):")
    (
        df_silver.filter(F.col("total_amount") < 0)
        .agg(
            F.count("*").alias("total_negativos"),
            F.min("total_amount").alias("valor_minimo"),
            F.max("total_amount").alias("valor_maximo"),
        )
    ).show(truncate=False)

    ####---- Padrão de negativos por período — descartar problema pontual de
    ####---- ingestão.
    print("\n[total_amount] Negativos por ano/mês — checa se é problema pontual de ingestão:")
    (
        df_silver
        .filter(F.col("total_amount") < 0)
        .groupBy("year", "month")
        .count()
        .orderBy("year", "month")
    ).show(truncate=False)

    ####---- Negativos por data completa — checar concentração em dias
    ####---- específicos.
    print("\n[total_amount] Negativos por data completa — checa concentração em dias específicos:")
    (
        df_silver
        .filter(F.col("total_amount") < 0)
        .groupBy("data_corrida")
        .count()
        .orderBy("data_corrida")
    ).show(truncate=False)

    ####---- Todos os valores negativos estão associados ao VendorID = 2.
    ####---- O padrão sugere uma regra específica desse provedor, mas não é
    ####---- possível confirmá-la apenas com os dados disponíveis.
    print("\n[total_amount] Negativos por VendorID (concentração encontrada aqui):")
    (
        df_silver
        .groupBy("VendorID")
        .agg(
            F.sum((F.col("total_amount") < 0).cast("int")).alias("total_negativos"),
        )
    ).show(truncate=False)

    ####---- Sem associação entre negativos e payment_type.
    print("\n[total_amount] Negativos por payment_type:")
    (
        df_silver
        .filter(F.col("total_amount") < 0)
        .groupBy("payment_type")
        .count()
        .orderBy("payment_type")
    ).show(truncate=False)

    ####---- Recompõe total_amount pela soma dos componentes financeiros
    ####---- para verificar a consistência dos valores negativos.
    df_negativos = (
        df_silver
        .filter(F.col("total_amount") < 0)
        .withColumn(
            "total_calculado",
            F.coalesce(F.col("fare_amount"), F.lit(0.0)) +
            F.coalesce(F.col("extra"), F.lit(0.0)) +
            F.coalesce(F.col("mta_tax"), F.lit(0.0)) +
            F.coalesce(F.col("tip_amount"), F.lit(0.0)) +
            F.coalesce(F.col("tolls_amount"), F.lit(0.0)) +
            F.coalesce(F.col("improvement_surcharge"), F.lit(0.0)) +
            F.coalesce(F.col("congestion_surcharge"), F.lit(0.0)) +
            F.coalesce(F.col("airport_fee"), F.lit(0.0))
        )
    )

    print("\n[total_amount] Recomposição dos negativos: soma dos componentes "
          "financeiros bate com total_amount?")
    (
        df_negativos
        .withColumn(
            "validacao",
            F.when(
                F.abs(F.col("total_amount") - F.col("total_calculado")) <= 0.01,
                "Sem diferença",
            ).otherwise("Com diferença")
        )
        .groupBy("validacao")
        .count()
    ).show(truncate=False)

    ####---- 218 registros com diferença entre soma dos componentes e
    ####---- total_amount — inspeção individual das divergências.
    print("\n[total_amount] Detalhe dos registros negativos com divergência na recomposição:")
    (
        df_negativos
        .withColumn(
            "diferenca",
            F.round(F.col("total_amount") - F.col("total_calculado"), 4),
        )
        .filter(F.abs(F.col("diferenca")) > 0.01)
        .select(
            "year",
            "month",
            "pickup_datetime",
            "fare_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "improvement_surcharge",
            "congestion_surcharge",
            "airport_fee",
            "total_amount",
            "total_calculado",
            "diferenca",
        )
    ).show(truncate=False)



####-----------------------------------------####
####----  5. Documentação Camada Silver  ----####
####-----------------------------------------####    

####---- Documenta a tabela Silver e todos os seus atributos no Glue Data
####---- Catalog. Executada somente na criação inicial da tabela.
####---- Alterações futuras no layout, no significado dos atributos ou nas
####---- regras de tratamento devem atualizar esta função.
def documentar_tabela_silver(spark, tabela_silver):

    spark.sql(f"""
        COMMENT ON TABLE {tabela_silver} IS
        'Camada Silver consolidando os dados de Yellow e Green Taxi do
        NYC TLC Trip Record Data. Contém os registros padronizados,
        validados e tratados, particionados pelo ano e mês da corrida.'
    """)


    ####---- Identificação e origem da corrida

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN VendorID COMMENT
        'Identificador do provedor responsável pelo envio do registro da corrida.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN tipo COMMENT
        'Tipo de serviço de táxi de origem do registro: yellow ou green.'
    """)


    ####---- Data e hora da corrida

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN pickup_datetime COMMENT
        'Data e hora de início da corrida, conforme recebida na origem.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN dropoff_datetime COMMENT
        'Data e hora de término da corrida, conforme recebida na origem.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN pickup_datetime_tratado COMMENT
        'Data e hora de início após a correção dos registros com timestamps invertidos.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN dropoff_datetime_tratado COMMENT
        'Data e hora de término após a correção dos registros com timestamps invertidos.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN data_corrida COMMENT
        'Data da corrida derivada de pickup_datetime_tratado.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN year COMMENT
        'Ano da corrida derivado de pickup_datetime_tratado e usado como chave de partição física.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN month COMMENT
        'Mês da corrida derivado de pickup_datetime_tratado e usado como chave de partição física.'
    """)


    ####---- Passageiros e informações operacionais

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN passenger_count COMMENT
        'Quantidade de passageiros informada na corrida. Valores nulos e iguais a zero foram substituídos pela mediana dos valores válidos.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN store_and_fwd_flag COMMENT
        'Indicador de armazenamento temporário do registro no veículo antes do envio ao provedor.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN RatecodeID COMMENT
        'Código da tarifa aplicada à corrida.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN payment_type COMMENT
        'Código da forma de pagamento utilizada na corrida.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN trip_type COMMENT
        'Código do tipo de corrida informado para Green Taxi. Preenchido com zero para Yellow Taxi.'
    """)


    ####---- Localização e distância

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN PULocationID COMMENT
        'Identificador da zona de início da corrida.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN DOLocationID COMMENT
        'Identificador da zona de término da corrida.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN trip_distance COMMENT
        'Distância da corrida informada pelo provedor.'
    """)


    ####---- Componentes financeiros

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN fare_amount COMMENT
        'Valor da tarifa principal da corrida.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN extra COMMENT
        'Valor de cobranças adicionais aplicadas à corrida.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN mta_tax COMMENT
        'Valor do tributo MTA aplicado à corrida.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN tip_amount COMMENT
        'Valor da gorjeta registrada na corrida.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN tolls_amount COMMENT
        'Valor total de pedágios registrado na corrida.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN ehail_fee COMMENT
        'Valor da taxa de chamada eletrônica. Preenchido com zero para Yellow Taxi.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN improvement_surcharge COMMENT
        'Valor da taxa de melhoria aplicada à corrida.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN congestion_surcharge COMMENT
        'Valor da taxa de congestionamento aplicada à corrida.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN airport_fee COMMENT
        'Valor da taxa de aeroporto. Preenchido com zero quando não informado ou não aplicável ao tipo de táxi.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_silver}
        ALTER COLUMN total_amount COMMENT
        'Valor total registrado para a corrida. Outliers e valores negativos foram investigados e mantidos por não apresentarem evidência suficiente de inconsistência.'
    """)

    print(f"Tabela {tabela_silver} documentada.")





####---------------####
####---  Main  ----####
####---------------####

def main():


    ####---- 1. Tratamento inicial
    ####---------------------------

    args = parse_args()

    anos_meses = [item.strip() for item in args.anos_meses.split(",") if item.strip()]
    anos_meses = validar_anos_meses(anos_meses)

    data_inicio, data_fim_exclusivo = calcular_intervalo_datas(anos_meses)
    print(f"Período a processar: {anos_meses} ({data_inicio} até {data_fim_exclusivo})")

    ####---- Sessão Spark com catálogo Iceberg apontando pro Glue Data
    ####---- Catalog. Config explícita aqui pra o script rodar sozinho,
    ####---- independente do spark-defaults do cluster.
    warehouse = f"s3://{args.silver_bucket}/warehouse/"

    spark = (
        SparkSession.builder
        .appName("build_silver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{args.catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{args.catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{args.catalog}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{args.catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )

    boto_config = Config(
        region_name="us-east-2",
        connect_timeout=10,
        read_timeout=10,
        retries={"max_attempts": 2, "mode": "standard"},
    )
    s3_client = boto3.client("s3", config=boto_config)


    ####---- 2. Leitura dos arquivos
    ####-----------------------------

    ####---- Yellow
    yellow_files = listar_arquivos_bronze(s3_client, args.bronze_bucket, "yellow", anos_meses)
    print(f"Arquivos Yellow encontrados: {len(yellow_files)}")

    yellow_dfs = [ler_yellow_silver(spark, path) for path in yellow_files]

    df_yellow_silver = yellow_dfs[0]
    for df in yellow_dfs[1:]:
        df_yellow_silver = df_yellow_silver.unionByName(df)

    print("\n[Yellow] Schema após padronização de nomes/tipos:")
    df_yellow_silver.printSchema()
    print("[Yellow] Amostra de 6 registros:")
    df_yellow_silver.show(6)

    ####---- Green
    green_files = listar_arquivos_bronze(s3_client, args.bronze_bucket, "green", anos_meses)
    print(f"Arquivos Green encontrados: {len(green_files)}")

    green_dfs = [ler_green_silver(spark, path) for path in green_files]

    df_green_silver = green_dfs[0]
    for df in green_dfs[1:]:
        df_green_silver = df_green_silver.unionByName(df)

    print("\n[Green] Schema após padronização de nomes/tipos:")
    df_green_silver.printSchema()
    print("[Green] Amostra de 6 registros:")
    df_green_silver.show(6)



    ####---- 3. Tratamento Inicial dos Dados
    ####-------------------------------------

    ####---- União Yellow + Green, antes do cast final.
    df_silver_bruto = df_yellow_silver.unionByName(df_green_silver)

    print("\nVolume de registros por fonte, antes do cast final:")
    print(f"Yellow: {df_yellow_silver.count():,} registros")
    print(f"Green : {df_green_silver.count():,} registros")
    print(f"Silver: {df_silver_bruto.count():,} registros")

    ####---- Cast das colunas. year/month são colunas inteiras (não uma
    ####---- string "yyyy-MM") — usadas tanto nas análises abaixo quanto,
    ####---- mais adiante, como chave de partição física do Iceberg.
    df_silver = (
        aplicar_schema(df_silver_bruto, SCHEMA_SILVER)
        .withColumn("data_corrida", F.to_date("pickup_datetime"))
        .withColumn("year", F.year("pickup_datetime"))
        .withColumn("month", F.month("pickup_datetime"))
    )

    ####---- Validação dos casts — antes de qualquer análise/tratamento
    ####---- posterior, pra já sinalizar cedo se algum cast descartou dado.
    df_validacao_linhas = validar_casts(df_silver_bruto, df_silver, SCHEMA_SILVER)

    print("\nResumo das perdas no cast:")

    for coluna, antes, depois, perdidos in df_validacao_linhas:
        if perdidos > 0:
            percentual = (perdidos / antes) * 100 if antes > 0 else 0.0

            print(
                f"⚠️ {coluna}: "
                f"{perdidos:,} valores perdidos "
                f"({percentual:.4f}%) "
                f"no cast ({antes:,} → {depois:,})"
            )
        else:
            print(
                f"✅ {coluna}: "
                f"nenhuma perda "
                f"({antes:,} valores não nulos)"
            )

    
    ####---- Cache antecipado — a partir daqui até filtrar_periodo_processado()
    ####---- rodam ~20 ações (.show()/.count()) em tratar_passenger_count(),
    ####---- tratar_timestamps() e nas checagens de duplicidade abaixo. Sem
    ####---- cache, cada uma dessas ações reexecutaria a leitura + union +
    ####---- cast inteiros da Bronze. Materializado pela primeira ação
    ####---- abaixo (checagem de duplicidade); liberado após
    ####---- filtrar_periodo_processado() persistir a versão final filtrada,
    ####---- quando esta cópia intermediária deixa de ser necessária.
    df_silver_cache_inicial = df_silver = df_silver.persist(StorageLevel.MEMORY_AND_DISK)

    ####---- Os arquivos do TLC Trip Record Data não possuem uma chave primária natural. 
    ####---- Para avaliar a existência de registros duplicados, foram realizadas duas 
    ####---- abordagens de validação:
    ####---- * Duplicidade exata: considerando todos os atributos do conjunto de dados,
    ####----   não foram identificados registros duplicados.
    ####---- * Duplicidade por chave lógica: considerando os atributos VendorID, 
    ####----   pickup_datetime, dropoff_datetime, PULocationID, DOLocationID, 
    ####----   passenger_count e total_amount, foram identificadas apenas 2 ocorrências.
    print("\nChecagem de duplicidade exata (todos os atributos):")
    distintos = df_silver.dropDuplicates().count()
    print(f"Registros distintos: {distintos}")

    print("\nChecagem de duplicidade por chave lógica (VendorID, horários, "
          "localização, passageiros e valor total):")
    chave = ["VendorID", "pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID", "passenger_count", "total_amount"]
    dup_chave = (
        df_silver.groupBy(chave)
        .count()
        .filter("count > 1")
    )
    dup_chave.show(truncate=False)



    ####---- 4. Análise Exploratória e Validação da Qualidade dos Dados
    ####----------------------------------------------------------------

    ####---- VendorID
    ####--------------
    ####---- Sem problema identificado no atributo.
    print("\n[VendorID] Volume de registros por fornecedor:")
    (
        df_silver
        .groupBy("VendorID")
        .agg(F.count("*").alias("total_registros"))
        .orderBy("VendorID")
    ).show(truncate=False)



    ####---- passenger_count
    ####---------------------
    ####---- Investigação (distribuição, nulos, zero), estatísticas e
    ####---- tratamento (nulos e zeros -> mediana) isolados em função
    ####---- própria — ver tratar_passenger_count().
    df_silver = tratar_passenger_count(df_silver)



    ####---- pickup_datetime e dropoff_datetime
    ####----------------------------------------
    ####---- Investigação e correção de timestamps invertidos, isoladas em
    ####---- função própria — ver tratar_timestamps().
    df_silver = tratar_timestamps(df_silver)



    ####---- Período processado
    ####-------------------------
    ####---- Investigação de data_corrida fora do escopo de --anos-meses e
    ####---- remoção desses registros, isoladas em função própria — ver
    ####---- filtrar_periodo_processado().
    df_silver = filtrar_periodo_processado(df_silver, data_inicio, data_fim_exclusivo)

    ####---- Cache antecipado liberado — a versão final (filtrada) já está
    ####---- persistida por filtrar_periodo_processado(), a cópia
    ####---- intermediária não é mais referenciada daqui em diante.
    df_silver_cache_inicial.unpersist()



    ####---- total_amount
    ####---------------------
    ####---- Investigação de outliers e valores negativos, isolada em
    ####---- função própria — ver analisar_total_amount(). Só leitura, sem
    ####---- tratamento (conclusão do case: não é necessário).
    analisar_total_amount(df_silver)




    ####---- 5. Grava Camada Silver
    ####----------------------------

    tabela_silver = f"{args.catalog}.{args.glue_database}.trips"

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.catalog}.{args.glue_database}")

    ####---- year/month já existem em df_silver (derivados de
    ####---- pickup_datetime_tratado no tratamento de timestamps) —
    ####---- reaproveitados aqui como chave de partição física, sem
    ####---- recalcular.

    ####---- overwritePartitions() sobrescreve dinamicamente só as partições
    ####---- (year, month) presentes neste df — reprocessar um mês existente
    ####---- deleta e refaz aquela partição; um mês novo só é adicionado.
    ####---- Partições fora do escopo de --anos-meses ficam intocadas.
    ####---- createOrReplace() só roda na primeira execução, quando a tabela
    ####---- ainda não existe (overwritePartitions exige tabela já criada).
    tabela_ja_existe = spark.catalog.tableExists(tabela_silver)

    ####---- Número de partições explícito (len(anos_meses), um por
    ####---- combinação year/month processada) — sem isso, repartition usa
    ####---- spark.sql.shuffle.partitions (200 por padrão), fragmentando bem
    ####---- mais arquivos por partição física do que o necessário. O CALL
    ####---- rewrite_data_files logo abaixo já reorganiza fisicamente, mas
    ####---- não precisa compensar uma fragmentação evitável aqui.
    writer = (
        df_silver
        .repartition(len(anos_meses), "year", "month")
        .writeTo(tabela_silver)
        .using("iceberg")
    )

    if tabela_ja_existe:
        writer.overwritePartitions()
        print(f"Tabela {tabela_silver} atualizada — partições de {anos_meses} sobrescritas.")
    else:
        writer.partitionedBy(F.col("year"), F.col("month")).createOrReplace()
        documentar_tabela_silver(spark, tabela_silver)
        print(f"Tabela {tabela_silver} criada e documentada (primeira execução).")


    ####---- Escrita concluída — o resto do main só faz SQL de metadado,
    ####---- não precisa mais do DataFrame em cache.
    df_silver.unpersist()

    ####---- 'tipo' não é partição (só 2 valores), mas o Iceberg rastreia
    ####---- min/max por arquivo, permitindo pular arquivos num
    ####---- "WHERE tipo = ..." sem partição física. Fixado 'full' pra não
    ####---- depender do truncamento padrão de 16 chars (embora
    ####---- 'yellow'/'green' já caibam).
    spark.sql(f"""
        ALTER TABLE {tabela_silver} SET TBLPROPERTIES (
            'write.metadata.metrics.column.tipo' = 'full'
        )
    """)

    ####---- Reorganização física dos arquivos por partição, pra filtros por
    ####---- data — equivalente a OPTIMIZE ... ZORDER do Delta.
    spark.sql(f"""
        CALL {args.catalog}.system.rewrite_data_files(
            table => '{args.glue_database}.trips',
            strategy => 'sort',
            sort_order => 'zorder(data_corrida)'
        )
    """)



if __name__ == "__main__":
    main()