####-----------------------------------------------------------------####
####----          Testes unitários de validar_anos_meses()       ----####
####----   Duplicada em 01_bronze.py, 02_silver.py e 03_gold.py  ----####
####-----------------------------------------------------------------####
####----                                                         ----####
####---- - Listas válidas, vazias e com duplicatas               ----####
####---- - Formatos inválidos (mês fora de 01-12, sem zero à     ----####
####----   esquerda, ano com menos de 4 dígitos, formato livre)  ----####
####---- - Mensagem de erro                                      ----####
####---- - Consistência entre as três cópias duplicadas          ----####
####----                                                         ----####
####----   Executado com pytest.                                 ----####
####----                                                         ----####
####-----------------------------------------------------------------####

import pytest


@pytest.fixture(params=["bronze", "silver", "gold"])
def modulo(request, bronze, silver, gold):
    """Roda cada teste parametrizado contra os três módulos."""
    return {"bronze": bronze, "silver": silver, "gold": gold}[request.param]


####----------------------------------------------------####
####----  Listas válidas — devem retornar sem erro  ----####
####----------------------------------------------------####

def test_lista_valida_sem_duplicatas_retorna_igual(modulo):
    entrada = ["2023-01", "2023-02", "2023-03"]
    assert modulo.validar_anos_meses(entrada) == entrada


def test_lista_vazia_retorna_vazia(modulo):
    assert modulo.validar_anos_meses([]) == []


def test_remove_duplicatas_preservando_ordem(modulo):
    entrada = ["2023-03", "2023-01", "2023-03", "2023-02", "2023-01"]
    assert modulo.validar_anos_meses(entrada) == ["2023-03", "2023-01", "2023-02"]


####----------------------------------------------------------####
####----  Formatos inválidos — devem levantar ValueError  ----####
####----------------------------------------------------------####

def test_mes_fora_do_intervalo_01_12_rejeitado(modulo):
    with pytest.raises(ValueError, match="2023-13"):
        modulo.validar_anos_meses(["2023-13"])

    with pytest.raises(ValueError, match="2023-00"):
        modulo.validar_anos_meses(["2023-00"])


def test_mes_sem_zero_a_esquerda_rejeitado(modulo):
    ####---- Formato exigido é AAAA-MM com zero à esquerda — "2023-1" não
    ####---- bate com o regex, mesmo sendo um mês válido (janeiro).
    with pytest.raises(ValueError, match="2023-1\\b"):
        modulo.validar_anos_meses(["2023-1"])


def test_ano_com_menos_de_4_digitos_rejeitado(modulo):
    with pytest.raises(ValueError):
        modulo.validar_anos_meses(["23-01"])


def test_formato_totalmente_invalido_rejeitado(modulo):
    with pytest.raises(ValueError):
        modulo.validar_anos_meses(["janeiro-2023"])


def test_um_item_invalido_entre_validos_rejeita_lista_inteira(modulo):
    ####---- A validação roda sobre a lista inteira antes de aceitar
    ####---- qualquer item — um único formato ruim barra o processamento
    ####---- de todo o lote, não só do item inválido.
    with pytest.raises(ValueError, match="2023-99"):
        modulo.validar_anos_meses(["2023-01", "2023-99", "2023-02"])


####----------------------------####
####----  Mensagem de erro  ----####
####----------------------------####

def test_mensagem_de_erro_lista_todos_os_invalidos(modulo):
    with pytest.raises(ValueError) as exc_info:
        modulo.validar_anos_meses(["2023-13", "2023-01", "abc"])

    mensagem = str(exc_info.value)
    assert "2023-13" in mensagem
    assert "abc" in mensagem
    assert "2023-01" not in mensagem  # item válido não deve aparecer como erro


####---------------------------------------------####
####----  Consistência entre as três cópias  ----####
####---------------------------------------------####

def test_tres_implementacoes_identicas(bronze, silver, gold):
    ####---- As três cópias precisam se comportar exatamente igual pro
    ####---- mesmo input — se alguém corrigir um bug numa cópia e
    ####---- esquecer as outras duas, esse teste falha.
    entrada = ["2023-03", "2023-01", "2023-01", "2023-12"]

    resultado_bronze = bronze.validar_anos_meses(entrada)
    resultado_silver = silver.validar_anos_meses(entrada)
    resultado_gold = gold.validar_anos_meses(entrada)

    assert resultado_bronze == resultado_silver == resultado_gold