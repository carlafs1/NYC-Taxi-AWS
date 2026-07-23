"""
## Bronze
### Ingestão e Auditoria Inicial dos Dados

*Projeto de treino — Engenharia de Dados AWS · NYC-Taxi-AWS*

---

**Objetivo**

Implementar a camada Bronze do Lakehouse: baixar os arquivos brutos do portal
NYC TLC, publicá-los em S3 sem qualquer transformação, e auditar a carga.

**Fonte dos dados**

| | |
|---|---|
| Dataset  | NYC TLC Trip Record Data |
| Período  | Janeiro a Maio de 2023 |
| Serviços | Yellow Cab, Green Cab |
| Formato  | Parquet |

**Validações**

- Validação da quantidade de registros;
- Validação do schema;
- Identificação de divergências de tipo entre arquivos do mesmo serviço.

---

Clone do notebook 01_bronze.ipynb (Databricks) para Glue Python Shell, considerando: 
- no notebook os arquivos já chegavam prontos num Volume (upload manual); aqui 
o próprio job baixa do portal NYC TLC.
- A auditoria é a mesma lógica do notebook — trocando só o motor: sem Spark, 
leitura via `pyarrow` (schema, min/max, contagem de nulos por coluna).
- Registra duas tabelas no Glue Data Catalog (bronze.yellow, bronze.green),
exatamente como o dado chega, particionadas por year/month (duas partições 
separadas). 
- O catálogo é registrado logo após a gravação no S3 (não espera a auditoria
completa), e suporta reprocessamento.
- Os arquivos baixados são mantidos em memória e reaproveitados para o
upload, a auditoria e o schema do catálogo, sem reler o S3.

Uso:
    python build_bronze.py \
        --bronze-bucket <bucket-bronze> \
        --anos-meses 2023-01,2023-02,2023-03,2023-04,2023-05 \
        --glue-database bronze
"""

####-------------------####
####----  Imports  ----####
####-------------------####
import argparse
import io
import re
import sys
import urllib.request
from urllib.error import HTTPError, URLError

import boto3
import pyarrow.compute as pc
import pyarrow.parquet as pq



####---------------------####
####----  Arguments  ----####
####---------------------####

NYC_TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
PADRAO_ANO_MES = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


####---- Recebe o nome do bucket Bronze e o período de processamento informado na execução do script.
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-bucket", required=True)
    parser.add_argument(
        "--anos-meses",
        required=True,
        help="Lista separada por vírgula, ex: 2023-01,2023-02,2023-03,2023-04,2023-05",
    )
    parser.add_argument(
        "--glue-database",
        required=True,
        help="Database do Glue Data Catalog onde bronze.yellow e bronze.green serão registradas",
    )

    ####--- Utiliza parse_known_args para ignorar argumentos adicionais que possam ser passados
    ####--- pelo ambiente de execução (ex.: Spark/EMR).
    args, _ = parser.parse_known_args()
    return args


####---- Valida o formato AAAA-MM (mês entre 01 e 12) de cada item recebido em
####---- --anos-meses, e remove duplicatas preservando a ordem original.
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


####--------------------####
####----  Ingestão  ----####
####--------------------####

####---- Remove do S3 os objetos das partições que serão reingeridas nesta
####---- execução, antes de baixar os arquivos novos. Considera ambos os taxi_type  
####---- para os year=/month= dos anos_meses informados.
def limpar_particoes_para_reprocessamento(s3_client, bucket: str, anos_meses: list):
    paginator = s3_client.get_paginator("list_objects_v2")

    for taxi_type in ["yellow", "green"]:
        for ano_mes in anos_meses:
            ano, mes = ano_mes.split("-")
            prefixo = f"{taxi_type}/year={ano}/month={mes}/"

            objetos_para_apagar = []
            for pagina in paginator.paginate(Bucket=bucket, Prefix=prefixo):
                for obj in pagina.get("Contents", []):
                    objetos_para_apagar.append({"Key": obj["Key"]})

            if not objetos_para_apagar:
                continue

            for i in range(0, len(objetos_para_apagar), 1000):
                lote = objetos_para_apagar[i:i + 1000]
                s3_client.delete_objects(Bucket=bucket, Delete={"Objects": lote})

            print(f"Reprocessamento: {len(objetos_para_apagar)} objeto(s) removido(s) em s3://{bucket}/{prefixo}")


####---- Baixa e publica Yellow + Green para cada ano/mes. Retorna, por tipo,
####---- a lista de (key, conteudo) — o conteúdo fica em memória e é
####---- reaproveitado depois pela auditoria e pelo catálogo, sem reler o S3.
####---- * Timeout evita que uma falha de rede deixe o job esperando indefinidamente.
####---- * Pastas year=AAAA/month=MM (Hive-style, duas partições) em vez de
####----   uma só ano_mes combinada.
####---- * Buffer em memória (BytesIO) simula um arquivo para o upload_fileobj do S3,
####----   evitando gravar em disco.
def ingerir(s3_client, bucket: str, anos_meses: list) -> dict:
    arquivos = {"yellow": [], "green": []}
    falhas = []

    for taxi_type in ["yellow", "green"]:
        for ano_mes in anos_meses:
            try:
                url = f"{NYC_TLC_BASE_URL}/{taxi_type}_tripdata_{ano_mes}.parquet"
                print(f"Baixando {url}")
                resposta = urllib.request.urlopen(url, timeout=60)
                conteudo = resposta.read()

                ano, mes = ano_mes.split("-")
                key = f"{taxi_type}/year={ano}/month={mes}/{taxi_type}_tripdata_{ano_mes}.parquet"
                tamanho_mb = len(conteudo) / 1_048_576
                print(f"Publicando s3://{bucket}/{key} ({tamanho_mb:.1f} MB)")

                buffer = io.BytesIO(conteudo)
                s3_client.upload_fileobj(buffer, bucket, key)

                arquivos[taxi_type].append((key, conteudo))
            
            except HTTPError as exc:
                if exc.code in (403, 404):
                    motivo = (
                        f"arquivo ainda não disponível na fonte NYC TLC "
                        f"para o período {ano_mes} (HTTP {exc.code})"
                    )
                else:
                    motivo = str(exc)

                print(
                    f"ERRO ao processar {taxi_type} {ano_mes}: {motivo}",
                    file=sys.stderr,
                )
                falhas.append((taxi_type, ano_mes, motivo))

            except Exception as exc:  # noqa: BLE001
                print(
                    f"ERRO ao processar {taxi_type} {ano_mes}: {exc}",
                    file=sys.stderr,
                )
                falhas.append((taxi_type, ano_mes, str(exc)))
            
    if falhas:
        raise RuntimeError(f"{len(falhas)} arquivo(s) falharam na ingestão: {falhas}")

    return arquivos


####--------------------####
####---  Auditoria  ----####
####--------------------####

####---- * Audita TODAS as colunas de cada arquivo. Se uma coluna não existir num 
####----   período, fica registrado.
####---- * Usa pc.min_max() (vetorizado, em C++) em vez de min()/max() do Python
####----   puro: min()/max() do Python precisam decodificar e processar cada
####----   elemento dentro do interpretador, enquanto pc.min_max() roda o loop
####----   inteiro em código compilado, sem custo por elemento. Faz diferença 
####----   em arquivos com milhões de linhas.
####---- * O loop por coluna aqui NÃO tem o custo de um loop por coluna em Spark:
####----   a tabela já está inteira em memória (lida uma vez), então cada
####----   tabela.column()/pc.min_max() é acesso local, sem I/O nem reprocessamento
####----   do arquivo.
####---- * Período (ex.: '2023-01') extraído direto da key, inline.
####---- * Lê os bytes já em memória (vindos de ingerir), sem tocar o S3.
def auditar_schema(files: list, taxi_type: str) -> list:
    linhas = []

    for key, conteudo in files:
        partes = key.split("/")
        periodo = partes[1].replace("year=", "") + "-" + partes[2].replace("month=", "")

        tabela = pq.read_table(io.BytesIO(conteudo))
        total_registros = tabela.num_rows
        schema = tabela.schema

        for nome_coluna in schema.names:
            valores = tabela.column(nome_coluna)
            campo = schema.field(nome_coluna)
            minimo_maximo = pc.min_max(valores).as_py()

            linha = {
                "taxi_type": taxi_type,
                "periodo": periodo,
                "total_registros": total_registros,
                "coluna": nome_coluna,
                "datatype": str(campo.type),
                "nullable": campo.nullable,
                "min": str(minimo_maximo["min"]),
                "max": str(minimo_maximo["max"]),
                "nulls": valores.null_count,
            }
            linhas.append(linha)

    return linhas


####---- Colunas que não aparecem em TODOS os períodos do mesmo taxi_type —
####---- schema incompleto/inconsistente entre meses.
def filtrar_colunas_ausentes(linhas: list, files_por_tipo: dict) -> list:
    ausentes = []

    for taxi_type, files in files_por_tipo.items():
        total_periodos = len(files)

        contagem_por_coluna = {}
        for linha in linhas:
            if linha["taxi_type"] != taxi_type:
                continue
            coluna = linha["coluna"]
            contagem_atual = contagem_por_coluna.get(coluna, 0)
            contagem_por_coluna[coluna] = contagem_atual + 1

        for coluna, contagem in contagem_por_coluna.items():
            if contagem < total_periodos:
                ausentes.append({
                    "taxi_type": taxi_type,
                    "coluna": coluna,
                    "presente_em": contagem,
                    "total_periodos": total_periodos,
                })

    ausentes.sort(key=lambda l: (l["taxi_type"], l["coluna"]))
    return ausentes


####---- * Mantém só (taxi_type, coluna) onde o datatype varia entre períodos.
####---- * 'tipos' aqui é o conjunto de datatypes vistos para essa coluna, não uma
####----   palavra genérica.
def filtrar_colunas_divergentes(linhas: list) -> list:

    tipos_por_coluna = {}
    for linha in linhas:
        chave = (linha["taxi_type"], linha["coluna"])
        tipos_ja_vistos = tipos_por_coluna.setdefault(chave, set())
        tipos_ja_vistos.add(linha["datatype"])

    colunas_divergentes = set()
    for chave, tipos in tipos_por_coluna.items():
        if len(tipos) > 1:
            colunas_divergentes.add(chave)

    resultado = []
    for linha in linhas:
        chave = (linha["taxi_type"], linha["coluna"])
        if chave in colunas_divergentes:
            resultado.append(linha)

    resultado.sort(key=lambda l: (l["taxi_type"], l["coluna"], l["periodo"]))
    return resultado


####--------------------------####
####----  Data Catalog  -------####
####--------------------------####

####---- Arrow -> tipo Hive/Glue. Cobre os tipos que aparecem no dado bruto da NYC TLC;
####---- qualquer tipo fora dessa lista cai em "string" (mais seguro que quebrar o registro).
TIPO_ARROW_PARA_GLUE = {
    "int32": "int",
    "int64": "bigint",
    "double": "double",
    "float": "float",
    "string": "string",
    "bool": "boolean",
}

####---- Config fixa do formato Parquet/Hive, usada em table_input e partition_input.
PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
PARQUET_SERDE = {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}


def tipo_glue(tipo_arrow: str) -> str:
    if tipo_arrow.startswith("timestamp"):
        return "timestamp"
    return TIPO_ARROW_PARA_GLUE.get(tipo_arrow, "string")


####---- Lê o schema do arquivo mais recente a partir dos bytes em memória
####---- (sem reler o S3) e registra a tabela no Glue: cria o database e 
####---- tabela se não existirem, senão só atualiza o schema.
def registrar_no_catalogo(glue_client, database: str, taxi_type: str, bucket: str, conteudo: bytes):
    tabela = pq.read_table(io.BytesIO(conteudo))
    schema = tabela.schema

    colunas = []
    for nome in schema.names:
        tipo_arrow = str(schema.field(nome).type)

        coluna = {
            "Name": nome,
            "Type": tipo_glue(tipo_arrow),
        }

        colunas.append(coluna)

    try:
        glue_client.get_database(Name=database)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_database(DatabaseInput={"Name": database})
        print(f"Database {database} criado.")

    table_input = {
        "Name": taxi_type,
        "Description": (
            f"Camada Bronze — dado bruto de {taxi_type} tripdata, exatamente como "
            "publicado pela NYC TLC e particionada por year/month."
        ),
        "PartitionKeys": [
            {"Name": "year", "Type": "int"},
            {"Name": "month", "Type": "int"},
        ],
        "StorageDescriptor": {
            "Location": f"s3://{bucket}/{taxi_type}/",
            "InputFormat": PARQUET_INPUT_FORMAT,
            "OutputFormat": PARQUET_OUTPUT_FORMAT,
            "SerdeInfo": PARQUET_SERDE,
            "Columns": colunas,
        },
        "TableType": "EXTERNAL_TABLE",
    }

    tabela_ja_existe = True
    try:
        glue_client.get_table(DatabaseName=database, Name=taxi_type)
    except glue_client.exceptions.EntityNotFoundException:
        tabela_ja_existe = False

    if tabela_ja_existe:
        glue_client.update_table(DatabaseName=database, TableInput=table_input)
        print(f"Tabela {database}.{taxi_type} atualizada (schema); partições existentes preservadas.")
    else:
        glue_client.create_table(DatabaseName=database, TableInput=table_input)
        print(f"Tabela {database}.{taxi_type} criada no Glue Data Catalog.")


####---- Uma partição (year, month) por ano_mes ingerido. Se a partição já
####---- existir (reprocessamento do mesmo mês), atualiza só ela — as demais
####---- partições/meses não são tocados.
def registrar_particoes(glue_client, database: str, taxi_type: str, bucket: str, anos_meses: list):
    for ano_mes in anos_meses:
        ano, mes = ano_mes.split("-")
        partition_input = {
            "Values": [ano, mes],
            "StorageDescriptor": {
                "Location": f"s3://{bucket}/{taxi_type}/year={ano}/month={mes}/",
                "InputFormat": PARQUET_INPUT_FORMAT,
                "OutputFormat": PARQUET_OUTPUT_FORMAT,
                "SerdeInfo": PARQUET_SERDE,
            },
        }

        try:
            glue_client.create_partition(
                DatabaseName=database,
                TableName=taxi_type,
                PartitionInput=partition_input,
            )
            print(f"Partição year={ano}/month={mes} criada em {database}.{taxi_type}.")
        except glue_client.exceptions.AlreadyExistsException:
            glue_client.update_partition(
                DatabaseName=database,
                TableName=taxi_type,
                PartitionValueList=[ano, mes],
                PartitionInput=partition_input,
            )
            print(f"Partição year={ano}/month={mes} já existia — atualizada (reprocessamento).")


####---------------####
####---  Main  ----####
####---------------####

def main():
    args = parse_args()
    anos_meses = [item.strip() for item in args.anos_meses.split(",") if item.strip()]
    anos_meses = validar_anos_meses(anos_meses)

    s3_client = boto3.client("s3")
    glue_client = boto3.client("glue")

    limpar_particoes_para_reprocessamento(s3_client, args.bronze_bucket, anos_meses)

    ####---- arquivos[taxi_type] é uma lista de (key, conteudo), reaproveitada
    ####---- abaixo sem reler o S3.
    arquivos = ingerir(s3_client, args.bronze_bucket, anos_meses)
    print(f"Ingestão concluída: {len(arquivos['yellow'])} Yellow, {len(arquivos['green'])} Green.")

    for taxi_type in ["yellow", "green"]:
        if not arquivos[taxi_type]:
            raise RuntimeError(f"Nenhum arquivo {taxi_type} foi ingerido.")

    ####---- Catálogo registrado logo após a gravação no S3: usa só o schema do
    ####---- arquivo mais recente de cada tipo.
    for taxi_type in ["yellow", "green"]:
        _, conteudo_mais_recente = arquivos[taxi_type][-1]
        registrar_no_catalogo(glue_client, args.glue_database, taxi_type, args.bronze_bucket, conteudo_mais_recente)
        registrar_particoes(glue_client, args.glue_database, taxi_type, args.bronze_bucket, anos_meses)

    ####---- Auditoria roda depois — é validação/observabilidade, não bloqueia o catálogo.
    audit_yellow = auditar_schema(arquivos["yellow"], "yellow")
    audit_green = auditar_schema(arquivos["green"], "green")
    todas_linhas = audit_yellow + audit_green

    divergencias_tipo = filtrar_colunas_divergentes(todas_linhas)
    colunas_ausentes = filtrar_colunas_ausentes(todas_linhas, arquivos)

    if divergencias_tipo:
        print(f"\n⚠️  {len(divergencias_tipo)} linha(s) com divergência de TIPO entre períodos:")
        for linha in divergencias_tipo:
            print(linha)
    else:
        print("\n✅ Nenhuma divergência de tipo encontrada entre os períodos auditados.")

    if colunas_ausentes:
        print(f"\n⚠️  {len(colunas_ausentes)} coluna(s) AUSENTES em algum período:")
        for linha in colunas_ausentes:
            print(linha)
    else:
        print("✅ Todas as colunas presentes em todos os períodos auditados.")

    if divergencias_tipo or colunas_ausentes:
        print(
            "\nPadronização de tipos/colunas fica a cargo da camada Silver "
            "(casts, nomenclatura, consolidação Yellow/Green)."
        )


if __name__ == "__main__":
    main()