####----------------------------------------------------------------####
####----        Testes unitários de calcular_intervalo_datas()  ----####
####----   Só existe em 02_silver.py — calcula [data_inicio,    ----####
####----   data_fim_exclusivo) a partir de --anos-meses         ----####
####----------------------------------------------------------------####
####----                                                        ----####
####---- - Um único mês, meses contíguos, período não contíguo  ----####
####---- - Virada de ano em dezembro (caso mais frágil)         ----####
####---- - Ordem de entrada e múltiplos anos                    ----####
####----                                                        ----####
####----   Executado com pytest.                                ----####
####----                                                        ----####
####----------------------------------------------------------------####


####------------------------------------------------####
####----  Casos simples — mesmo ano, contíguos  ----####
####------------------------------------------------####

def test_um_unico_mes(silver):
    assert silver.calcular_intervalo_datas(["2023-01"]) == ("2023-01-01", "2023-02-01")


def test_meses_contiguos_mesmo_ano(silver):
    entrada = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05"]
    assert silver.calcular_intervalo_datas(entrada) == ("2023-01-01", "2023-06-01")


def test_ano_com_mes_de_um_digito_recebe_zero_a_esquerda(silver):
    assert silver.calcular_intervalo_datas(["2023-03"]) == ("2023-03-01", "2023-04-01")


####-----------------------------------------------####
####----  Virada de ano — o caso mais frágil  -----####
####-----------------------------------------------####

def test_virada_de_ano_em_dezembro(silver):
    ####---- Caso mais frágil: mes_max == 12 precisa virar o ano
    ####---- (ano_max + 1, mês 01), não simplesmente mes_max + 1 (que
    ####---- daria o mês inválido "13").
    entrada = ["2023-11", "2023-12"]
    assert silver.calcular_intervalo_datas(entrada) == ("2023-11-01", "2024-01-01")


def test_dezembro_isolado(silver):
    assert silver.calcular_intervalo_datas(["2023-12"]) == ("2023-12-01", "2024-01-01")


def test_intervalo_cruzando_multiplos_anos(silver):
    entrada = ["2022-11", "2023-03"]
    assert silver.calcular_intervalo_datas(entrada) == ("2022-11-01", "2023-04-01")


####-----------------------------------------------------####
####----  Ordem de entrada e períodos não contíguos  ----####
####-----------------------------------------------------####

def test_ordem_de_entrada_nao_importa(silver):
    ####---- min()/max() em string "AAAA-MM" funcionam por ordenação
    ####---- lexicográfica — só é seguro porque o formato tem largura
    ####---- fixa (4 dígitos de ano, 2 de mês com zero à esquerda).
    entrada_desordenada = ["2023-03", "2023-01", "2023-02"]
    entrada_ordenada = ["2023-01", "2023-02", "2023-03"]

    assert (
        silver.calcular_intervalo_datas(entrada_desordenada)
        == silver.calcular_intervalo_datas(entrada_ordenada)
    )


def test_periodo_nao_contiguo_usa_so_min_e_max(silver):
    ####---- calcular_intervalo_datas não sabe que faltam meses no meio —
    ####---- o intervalo cobre de min a max, mesmo que 2023-02/03/04 não
    ####---- estejam na lista. Documentando esse comportamento, não uma
    ####---- limitação nova.
    entrada = ["2023-01", "2023-05"]
    assert silver.calcular_intervalo_datas(entrada) == ("2023-01-01", "2023-06-01")