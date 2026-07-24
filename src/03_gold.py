"""
## Gold
### Modelagem e Disponibilização da Camada de Consumo

*Case Técnico — Engenharia de Dados · iFood*

---

**Objetivo**

Implementar a camada Gold do Lakehouse, agregando os dados da camada Silver em
métricas de negócio, para responder às perguntas analíticas do case de forma
performática e reutilizável.

**Fonte dos dados**

| | |
|---|---|
| Origem   | {catalog}.{silver-database}.trips |
| Período  | Janeiro a Maio de 2023 |
| Serviços | Yellow Cab, Green Cab |
| Formato  | Iceberg Table (camada Silver) |

**Atividades realizadas**

- Leitura da camada Silver, escopada por --anos-meses;
- Agregação de métricas por tipo de táxi, ano/mês e hora do dia;
- Gravação de duas tabelas Gold: trips (grão individual) e trip_metrics
  (agregada);
- Validação informativa respondendo às perguntas de negócio do case.

---

Adaptação do notebook 03_Gold.ipynb (Databricks) para script PySpark em EMR,
considerando:
- leitura via spark.table("ifood_case.silver.trips") vira leitura via
  {catalog}.{silver-database}.trips (Iceberg/Glue Data Catalog), escopada por
  --anos-meses (mesmo padrão da Bronze/Silver) via filtro em year/month, em
  vez de ler a tabela inteira a cada execução.
- ano_mes (coluna string "yyyy-MM") não existe mais na Silver — decisão
  tomada durante o desenvolvimento da Silver (ver build_silver.py): year e
  month (colunas inteiras) usados no lugar, tanto nas agregações quanto como
  chave de partição física. Os grãos que o case pedia por "mês" viram
  (year, month) aqui.
- Saída registrada como duas tabelas Iceberg no Glue Data Catalog
  (glue_catalog.<db>.trips e glue_catalog.<db>.trip_metrics), no lugar de
  Delta + saveAsTable (Unity Catalog).
- Particionamento por year/month (Hive-style), no lugar da coluna única
  ano_mes — mesmo padrão adotado na Bronze/Silver.
- OPTIMIZE + ZORDER (sintaxe Delta) vira CALL ...rewrite_data_files
  (procedimento Iceberg equivalente), aplicado só em trips (grão individual,
  tabela grande); trip_metrics é pequena o bastante pra não precisar.
- Período parametrizado via --anos-meses (mesmo padrão da Bronze/Silver).
  Leitura da Silver escopada só às partições pedidas; escrita usa overwrite
  dinâmico por partição (year/month) no Iceberg — reprocessar um mês
  existente substitui só aquela partição, mês novo só é adicionado.
- As perguntas de negócio do case (%sql no notebook) viram uma validação
  informativa ao final do script, lendo a tabela trip_metrics recém-gravada.

**Validações**

As validações desta etapa têm caráter informativo e visam confirmar que a
camada Gold responde corretamente às perguntas de negócio do case. O
acompanhamento é feito visualmente pelo resultado impresso na execução.

Uso:
    spark-submit build_gold.py \
        --gold-bucket <bucket-gold> \
        --catalog catalog \
        --silver-database silver \
        --gold-database gold \
        --anos-meses 2023-01,2023-02,2023-03,2023-04,2023-05
"""

####-------------------####
####----  Imports  ----####
####-------------------####
import argparse
import re
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


####---------------------####
####----  Arguments  ----####
####---------------------####

PADRAO_ANO_MES = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


####---- Recebe o bucket do warehouse Gold, o catalog, os databases de
####---- origem (Silver) e destino (Gold), e o período (--anos-meses) a
####---- processar.
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--gold-bucket",
        required=True,
    )
    parser.add_argument("--silver-database", default="silver")
    parser.add_argument("--gold-database", default="gold")
    parser.add_argument(
        "--catalog",
        default="catalog",
        help="Nome do catalog Spark, único e compartilhado por bronze/silver/gold via --*-database; aponta para o Glue Data Catalog via Iceberg",
    )
    parser.add_argument(
        "--anos-meses",
        required=True,
        help="Lista separada por vírgula, ex: 2023-01,2023-02,2023-03,2023-04,2023-05. Mesmo formato usado na Bronze/Silver.",
    )

    ####---- Utiliza parse_known_args para ignorar argumentos adicionais que
    ####---- possam ser passados pelo ambiente de execução.
    args, _ = parser.parse_known_args()
    return args


####---- Valida o formato AAAA-MM de cada item de --anos-meses (mesma regra
####---- da Bronze/Silver) e remove duplicatas preservando a ordem original.
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


####-------------------------------####
####----  1. Leitura da Silver  ----####
####-------------------------------####

####---- Lê {catalog}.{silver_database}.trips escopado às combinações
####---- (year, month) de anos_meses — aproveita o pruning de partição do
####---- Iceberg, não lê a tabela inteira. Seleciona só as colunas exigidas
####---- pelo case (VendorID, passenger_count, total_amount,
####---- pickup/dropoff_datetime — usando as versões _tratado da Silver,
####---- já com timestamps invertidos corrigidos) mais tipo/year/month,
####---- necessárias pra agregação.
def ler_silver(spark, catalog: str, silver_database: str, anos_meses: list):
    tabela_silver = f"{catalog}.{silver_database}.trips"

    condicao_periodo = None

    for periodo in anos_meses:
        ano, mes = map(int, periodo.split("-"))

        condicao = (
            (F.col("year") == ano)
            & (F.col("month") == mes)
        )

        if condicao_periodo is None:
            condicao_periodo = condicao
        else:
            condicao_periodo = condicao_periodo | condicao

    return (
        spark.table(tabela_silver)
        .filter(condicao_periodo)
        .select(
            "VendorID",
            "tipo",
            "year",
            "month",
            "total_amount",
            "passenger_count",
            F.col("pickup_datetime_tratado").alias("pickup_datetime"),
            F.col("dropoff_datetime_tratado").alias("dropoff_datetime"),
        )
    )


####---------------------------------####
####----  2. Cálculo das Métricas  ----####
####---------------------------------####

####---- Agrega no grão (tipo, year, month, hora_do_dia), guardando soma e
####---- contagem — não médias já calculadas — pra permitir recombinação
####---- correta em qualquer agregação superior, evitando o problema de
####---- média das médias. hora_do_dia extraída de pickup_datetime, que já
####---- reflete a correção de timestamps invertidos feita na Silver.
def calcular_metricas_agregadas(df_silver):
    return (
        df_silver
        .withColumn("hora_do_dia", F.hour("pickup_datetime"))
        .groupBy("tipo", "year", "month", "hora_do_dia")
        .agg(
            F.count("*").alias("qtd_corridas"),
            F.sum("total_amount").alias("soma_total_amount"),
            F.sum("passenger_count").alias("soma_passenger_count"),
        )
    )


####-------------------------####
####----  3. Documentação  ----####
####-------------------------####

####---- Documenta a tabela trips (grão individual) no Glue Data Catalog.
####---- Executada somente na criação inicial da tabela.
def documentar_tabela_gold_trips(spark, tabela_trips):

    spark.sql(f"""
        COMMENT ON TABLE {tabela_trips} IS
        'Camada de consumo com grão de corrida individual, contendo as colunas exigidas pelo
        case (VendorID, passenger_count, total_amount, pickup_datetime, dropoff_datetime —
        equivalentes a tpep_/lpep_ na origem). pickup_datetime e dropoff_datetime já refletem
        a correção de timestamps invertidos realizada na Silver. Complementa a tabela
        trip_metrics desta mesma camada Gold, que serve às perguntas de negócio já agregadas
        para performance.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_trips}
        ALTER COLUMN VendorID COMMENT
        'Identificador do provedor responsável pelo envio do registro da corrida.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_trips}
        ALTER COLUMN tipo COMMENT
        'Tipo de serviço de táxi de origem do registro: yellow ou green.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_trips}
        ALTER COLUMN year COMMENT
        'Ano da corrida, usado como chave de partição física.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_trips}
        ALTER COLUMN month COMMENT
        'Mês da corrida, usado como chave de partição física.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_trips}
        ALTER COLUMN total_amount COMMENT
        'Valor total registrado para a corrida, conforme tratado na Silver.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_trips}
        ALTER COLUMN passenger_count COMMENT
        'Quantidade de passageiros, conforme tratado na Silver (nulos e zeros imputados).'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_trips}
        ALTER COLUMN pickup_datetime COMMENT
        'Data e hora de início da corrida, já corrigida (pickup_datetime_tratado na Silver).'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_trips}
        ALTER COLUMN dropoff_datetime COMMENT
        'Data e hora de término da corrida, já corrigida (dropoff_datetime_tratado na Silver).'
    """)

    print(f"Tabela {tabela_trips} documentada.")


####---- Documenta a tabela trip_metrics (agregada) no Glue Data Catalog.
####---- Executada somente na criação inicial da tabela.
def documentar_tabela_gold_trip_metrics(spark, tabela_metrics):
    spark.sql(f"""
        COMMENT ON TABLE {tabela_metrics} IS
        'Camada Gold com métricas agregadas de corridas por tipo de táxi, ano, mês e hora do
        dia. Grão: (tipo, year, month, hora_do_dia). Guarda soma e contagem para permitir
        recombinação correta em qualquer agregação superior, evitando o problema de média das
        médias. hora_do_dia extraída de pickup_datetime_tratado (camada Silver).'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_metrics}
        ALTER COLUMN hora_do_dia COMMENT
        'Hora do dia (0-23) do início da corrida, extraída de pickup_datetime_tratado.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_metrics}
        ALTER COLUMN qtd_corridas COMMENT
        'Quantidade de corridas no grão (tipo, year, month, hora_do_dia).'
    """)
    spark.sql(f"""
        ALTER TABLE {tabela_metrics}
        ALTER COLUMN soma_total_amount COMMENT
        'Soma de total_amount no grão — dividir por qtd_corridas para obter a média.'
    """)

    spark.sql(f"""
        ALTER TABLE {tabela_metrics}
        ALTER COLUMN soma_passenger_count COMMENT
        'Soma de passenger_count no grão — dividir por qtd_corridas para obter a média.'
    """)

    print(f"Tabela {tabela_metrics} documentada.")




####-------------------------------------####
####----  4. Perguntas de Negócio  ----####
####-------------------------------------####

####---- Valida a tabela trip_metrics recém-gravada respondendo às duas
####---- perguntas de negócio do case. Validação informativa (ver docstring
####---- do módulo) — não interrompe o processamento, só imprime o
####---- resultado. A pergunta 2 é fixa a maio/2023 porque é assim que o
####---- case a formula ("mês de maio"); se maio/2023 não estiver no escopo
####---- de --anos-meses processado, a consulta retorna vazio.
def validar_perguntas_negocio(spark, tabela_metrics):

    ####---- Pergunta 1: média de total_amount por mês, considerando todos
    ####---- os yellow táxis da frota.
    spark.sql(f"""
        SELECT year, month
             , ROUND(SUM(soma_total_amount) / SUM(qtd_corridas), 2) AS media_total_amount
          FROM {tabela_metrics}
         WHERE tipo = 'yellow'
         GROUP BY year, month
         ORDER BY year, month
    """).show(truncate=False)

    ####---- Pergunta 2: média de passenger_count por hora do dia em
    ####---- maio/2023, considerando todos os táxis da frota.
    spark.sql(f"""
        SELECT hora_do_dia
             , ROUND(SUM(soma_passenger_count) / SUM(qtd_corridas), 2) AS media_passenger_count
          FROM {tabela_metrics}
         WHERE year = 2023 AND month = 5
         GROUP BY hora_do_dia
         ORDER BY hora_do_dia
    """).show(truncate=False)


####-----------------------------------------------------------------####
####----  5. Ponteiros públicos para o painel DuckDB-Wasm          ----####
####-----------------------------------------------------------------####

####---- O Glue Data Catalog guarda o metadata_location atual de cada
####---- tabela Iceberg como um parametro da tabela -- so acessivel via
####---- API autenticada (boto3/Glue), nao anonimamente. Como o painel
####---- roda 100% no navegador (sem credenciais AWS), ele nao consegue
####---- perguntar ao Glue "qual e o metadata.json atual?". Solucao:
####---- este job (que ja tem permissao Glue via IAM role) escreve um
####---- arquivo de texto PUBLICO com esse caminho, a cada execucao. O
####---- navegador le esse texto (GET simples, sem credencial) e manda
####---- direto pro iceberg_scan() do DuckDB -- dali em diante e tudo
####---- leitura publica de S3, sem tocar no Glue.
def publicar_ponteiros_iceberg(gold_bucket, catalog_glue_database, tabelas):
    import boto3

    glue_client = boto3.client("glue", region_name="us-east-2")
    s3_client = boto3.client("s3", region_name="us-east-2")

    for tabela in tabelas:
        resposta = glue_client.get_table(DatabaseName=catalog_glue_database, Name=tabela)
        metadata_location = resposta["Table"]["Parameters"]["metadata_location"]

        chave_ponteiro = f"public-pointers/{tabela}.txt"
        s3_client.put_object(
            Bucket=gold_bucket,
            Key=chave_ponteiro,
            Body=metadata_location.encode("utf-8"),
            ContentType="text/plain",
        )
        print(f"Ponteiro publico atualizado: s3://{gold_bucket}/{chave_ponteiro} -> {metadata_location}")


####---------------####
####---  Main  ----####
####---------------####

def main():

    ####---- 1. Tratamento inicial
    ####---------------------------

    args = parse_args()
    anos_meses = [item.strip() for item in args.anos_meses.split(",") if item.strip()]
    anos_meses = validar_anos_meses(anos_meses)

    ####---- Sessão Spark com catálogo Iceberg apontando pro Glue Data
    ####---- Catalog. Config explícita aqui pra o script rodar sozinho,
    ####---- independente do spark-defaults do cluster.
    warehouse = f"s3://{args.gold_bucket}/warehouse/"

    spark = (
        SparkSession.builder
        .appName("build_gold")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{args.catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{args.catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{args.catalog}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{args.catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.catalog}.{args.gold_database}")



    ####---- 2. Leitura da Silver
    ####-----------------------------

    df_silver = ler_silver(spark, args.catalog, args.silver_database, anos_meses)
    df_silver = df_silver.persist()
    qtd_trips = df_silver.count()
    print(f"Silver (escopo processado): {qtd_trips:,} registros")
    df_silver.printSchema()



    ####---- 3. Cálculo das métricas
    ####---------------------------------

    df_gold_metrics = calcular_metricas_agregadas(df_silver)
    df_gold_metrics = df_gold_metrics.persist()
    qtd_metrics = df_gold_metrics.count()
    print(f"Gold trip_metrics (agregado): {qtd_metrics:,} registros")



    ####---- 4. Grava Camada Gold
    ####------------------------------

    tabela_trips = f"{args.catalog}.{args.gold_database}.trips"
    tabela_metrics = f"{args.catalog}.{args.gold_database}.trip_metrics"

    ####---- trips — grão individual, tabela grande. Número de partições
    ####---- explícito (len(anos_meses), um por combinação year/month
    ####---- processada) — sem isso, repartition usa
    ####---- spark.sql.shuffle.partitions (200 por padrão), fragmentando
    ####---- bem mais arquivos por partição física do que o necessário.
    trips_ja_existe = spark.catalog.tableExists(tabela_trips)

    writer_trips = (
        df_silver
        .repartition(len(anos_meses), "year", "month")
        .writeTo(tabela_trips)
        .using("iceberg")
    )

    if trips_ja_existe:
        writer_trips.overwritePartitions()
        print(f"Tabela {tabela_trips} atualizada — partições de {anos_meses} sobrescritas.")
    else:
        writer_trips.partitionedBy(F.col("year"), F.col("month")).createOrReplace()
        documentar_tabela_gold_trips(spark, tabela_trips)
        print(f"Tabela {tabela_trips} criada e documentada (primeira execução).")

    print(f"Registros gravados em {tabela_trips}: {qtd_trips:,}")

    ####---- Reorganização física dos arquivos por partição, pra filtros
    ####---- por data — equivalente a OPTIMIZE ... ZORDER do Delta.
    ####---- Só em trips: trip_metrics é pequena o bastante pra não
    ####---- precisar.
    spark.sql(f"""
        CALL {args.catalog}.system.rewrite_data_files(
            table => '{args.gold_database}.trips',
            strategy => 'sort',
            sort_order => 'zorder(pickup_datetime)'
        )
    """)

    ####---- trip_metrics — agregada, poucas linhas (tipo x year x month x
    ####---- hora_do_dia). coalesce(1) mantém tudo num único arquivo por
    ####---- partição, evitando fragmentação numa tabela tão pequena.
    metrics_ja_existe = spark.catalog.tableExists(tabela_metrics)

    writer_metrics = (
        df_gold_metrics
        .coalesce(1)
        .writeTo(tabela_metrics)
        .using("iceberg")
    )

    if metrics_ja_existe:
        writer_metrics.overwritePartitions()
        print(f"Tabela {tabela_metrics} atualizada — partições de {anos_meses} sobrescritas.")
    else:
        writer_metrics.partitionedBy(F.col("year"), F.col("month")).createOrReplace()
        documentar_tabela_gold_trip_metrics(spark, tabela_metrics)
        print(f"Tabela {tabela_metrics} criada e documentada (primeira execução).")

    print(f"Registros gravados em {tabela_metrics}: {qtd_metrics:,}")

    ####---- Escrita concluída — os DataFrames em cache não são mais
    ####---- necessários para o restante do main.
    df_silver.unpersist()
    df_gold_metrics.unpersist()



    ####---- 5. Ponteiros públicos para o painel DuckDB-Wasm
    ####---------------------------------------------------

    publicar_ponteiros_iceberg(
        gold_bucket=args.gold_bucket,
        catalog_glue_database=args.gold_database,
        tabelas=["trips", "trip_metrics"],
    )



    ####---- 6. Perguntas de Negócio
    ####----------------------------

    validar_perguntas_negocio(spark, tabela_metrics)


if __name__ == "__main__":
    main()