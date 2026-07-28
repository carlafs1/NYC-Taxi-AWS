####--------------------------------------------------------------------####
####----                    conftest.py — tests/                    ----####
####----   Carrega os scripts de src/ (nomes numéricos) e fixtures  ----####
####--------------------------------------------------------------------####
####----                                                            ----####
####---- 01_bronze.py, 02_silver.py e 03_gold.py não são            ----####
####---- identificadores Python válidos (começam com dígito), então ----####
####---- `import 01_bronze` não funciona. carregar_modulo() importa ----####
####---- cada um pelo caminho do arquivo, sem precisar renomear os  ----####
####---- scripts de produção.                                       ----####
####----                                                            ----####
####---- Os três módulos importam pyspark e boto3 no topo do        ----####
####---- arquivo, então essas dependências precisam estar           ----####
####---- instaladas mesmo para testar só as funções puras — ver     ----####
####---- requirements-test.txt e a seção "Testes automatizados" do  ----####
####---- README.                                                    ----####
####----                                                            ----####
####--------------------------------------------------------------------####

import importlib.util
import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parent.parent / "src"


####------------------------------------------####
####----  1. Import dinâmico dos módulos  ----####
####------------------------------------------####

def carregar_modulo(nome_arquivo: str):
    caminho = SRC_DIR / nome_arquivo
    nome_modulo = nome_arquivo.replace(".py", "").replace("-", "_")

    spec = importlib.util.spec_from_file_location(nome_modulo, caminho)
    modulo = importlib.util.module_from_spec(spec)
    sys.modules[nome_modulo] = modulo
    spec.loader.exec_module(modulo)
    return modulo


####-----------------------####
####----  2. Fixtures  ----####
####-----------------------####

@pytest.fixture(scope="session")
def bronze():
    return carregar_modulo("01_bronze.py")


@pytest.fixture(scope="session")
def silver():
    return carregar_modulo("02_silver.py")


@pytest.fixture(scope="session")
def gold():
    return carregar_modulo("03_gold.py")


####---- SparkSession local (master=local[1], sem cluster/EMR) — só para
####---- testar funções que recebem/retornam DataFrame (ex: aplicar_schema).
####---- Requer Java instalado (ver README) mesmo em modo local: pyspark não
####---- é uma simulação em Python puro, roda sobre uma JVM real.
####---- scope="session": subir a JVM custa alguns segundos; reaproveitada
####---- por todos os testes que precisam dela.
@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    spark_session = (
        SparkSession.builder
        .appName("testes-nyc-taxi-aws")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()