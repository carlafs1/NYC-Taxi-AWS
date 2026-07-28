####--------------------------------------------------------------------####
####----          Testes unitários de aplicar_schema()              ----####
####----   Só existe em 02_silver.py — valida colunas obrigatórias  ----####
####----   e aplica cast via DataFrame Spark                        ----####
####--------------------------------------------------------------------####
####----                                                            ----####
####---- - Cast aplicado e colunas fora do schema descartadas       ----####
####---- - Coluna obrigatória ausente                               ----####
####---- - Comportamento real do cast inválido do Spark             ----####
####---- - Casos de borda (schema vazio)                            ----####
####----                                                            ----####
####----   Executado com pytest. Requer Java instalado (usa uma     ----####
####----   SparkSession local — ver fixture `spark` em conftest.py) ----####
####----                                                            ----####
####--------------------------------------------------------------------####

import pytest
from pyspark.sql import Row


####-------------------------------------####
####----  Cast e seleção de colunas  ----####
####-------------------------------------####

def test_aplica_cast_e_retorna_apenas_colunas_do_schema(spark, silver):
    df = spark.createDataFrame(
        [Row(a="1", b="2.5", c="ignorada")]
    )
    schema = {"a": "int", "b": "double"}

    resultado = silver.aplicar_schema(df, schema)

    ####---- Só as colunas do schema sobrevivem — 'c' é descartada.
    assert resultado.columns == ["a", "b"]

    linha = resultado.collect()[0]
    assert linha["a"] == 1
    assert isinstance(linha["a"], int)
    assert linha["b"] == 2.5
    assert isinstance(linha["b"], float)


####--------------------------------------####
####----  Coluna obrigatória ausente  ----####
####--------------------------------------####

def test_coluna_obrigatoria_ausente_levanta_value_error(spark, silver):
    df = spark.createDataFrame([Row(a="1")])
    schema = {"a": "int", "b": "double"}  # 'b' não existe no df

    with pytest.raises(ValueError, match="b"):
        silver.aplicar_schema(df, schema)


def test_mensagem_de_erro_lista_todas_as_colunas_faltantes(spark, silver):
    df = spark.createDataFrame([Row(a="1")])
    schema = {"a": "int", "b": "double", "c": "string"}

    with pytest.raises(ValueError) as exc_info:
        silver.aplicar_schema(df, schema)

    mensagem = str(exc_info.value)
    assert "b" in mensagem
    assert "c" in mensagem


####--------------------------------------####
####----  Comportamento real do cast  ----####
####--------------------------------------####

def test_cast_invalido_gera_nulo_em_vez_de_erro(spark, silver):
    ####---- Comportamento real do Spark, não uma escolha do projeto: cast
    ####---- de string não-numérica pra int não levanta exceção — vira
    ####---- NULL silenciosamente. Documentando esse comportamento porque
    ####---- é justamente o tipo de perda de dado silenciosa que
    ####---- validar_casts() (chamada logo depois no pipeline real) existe
    ####---- pra detectar, comparando contagem de não-nulos antes/depois.
    df = spark.createDataFrame([Row(a="não é um número")])
    schema = {"a": "int"}

    resultado = silver.aplicar_schema(df, schema)

    assert resultado.collect()[0]["a"] is None


####--------------------------####
####----  Casos de borda  ----####
####--------------------------####

def test_schema_vazio_retorna_dataframe_sem_colunas(spark, silver):
    df = spark.createDataFrame([Row(a="1", b="2")])

    resultado = silver.aplicar_schema(df, {})

    assert resultado.columns == []
    assert resultado.count() == 1  # linha continua existindo, só sem colunas