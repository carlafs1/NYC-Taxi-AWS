"""
Os scripts em src/ (01_bronze.py, 02_silver.py, 03_gold.py) têm nome
numérico — não são identificadores Python válidos, então `import 01_bronze`
não funciona. carregar_modulo() importa cada um pelo caminho do arquivo,
contornando essa limitação sem precisar renomear os scripts de produção.

Os três módulos importam pyspark e boto3 no topo do arquivo (para as
funções que de fato usam Spark/S3), então essas duas dependências
precisam estar instaladas mesmo para testar só as funções puras abaixo —
ver README de testes para o motivo dessa limitação.
"""
import importlib.util
import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parent.parent / "src"


def carregar_modulo(nome_arquivo: str):
    caminho = SRC_DIR / nome_arquivo
    nome_modulo = nome_arquivo.replace(".py", "").replace("-", "_")

    spec = importlib.util.spec_from_file_location(nome_modulo, caminho)
    modulo = importlib.util.module_from_spec(spec)
    sys.modules[nome_modulo] = modulo
    spec.loader.exec_module(modulo)
    return modulo


@pytest.fixture(scope="session")
def bronze():
    return carregar_modulo("01_bronze.py")


@pytest.fixture(scope="session")
def silver():
    return carregar_modulo("02_silver.py")


@pytest.fixture(scope="session")
def gold():
    return carregar_modulo("03_gold.py")