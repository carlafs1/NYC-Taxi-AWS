# Dicionário de Dados — NYC TLC Trip Record Data

Fonte oficial: PDFs do TLC (18/03/2025) — *Data Dictionary – Yellow Taxi Trip Records* e
*Data Dictionary - LPEP Trip Records* — http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml

Tradução literal dos campos descritos nos dicionários oficiais, mapeados para os nomes de
coluna padronizados na camada Silver deste projeto (`glue_catalog.silver.trips`). Nenhuma
descrição foi complementada ou reinterpretada além do texto original — onde a fonte não
documenta um campo ou um valor, isso é sinalizado explicitamente em vez de presumido.

> Este dicionário parte da mesma base do
> [`ifood-case`](https://github.com/carlafs1/ifood-case/blob/main/docs/data_dictionary.md).
> A principal diferença estrutural é o uso de `year` e `month` no lugar de
> `ano_mes`: o `ifood-case` processa um lote histórico fechado, enquanto este
> projeto foi desenvolvido para cargas mensais incrementais.

---

## Colunas — Camada Silver

| Coluna (Silver) | Origem Yellow | Origem Green | Tipo (Silver) | Descrição (tradução literal do dicionário oficial) |
|---|---|---|---|---|
| `VendorID` | VendorID | VendorID | int | Código indicando o provedor TPEP (Yellow) / LPEP (Green) que forneceu o registro. Ver domínio de valores abaixo. |
| `pickup_datetime` | tpep_pickup_datetime | lpep_pickup_datetime | timestamp_ntz | Data e hora em que o taxímetro foi acionado. |
| `dropoff_datetime` | tpep_dropoff_datetime | lpep_dropoff_datetime | timestamp_ntz | Data e hora em que o taxímetro foi desligado. |
| `store_and_fwd_flag` | store_and_fwd_flag | store_and_fwd_flag | string | Indica se o registro da corrida foi mantido na memória do veículo antes de ser enviado ao provedor ("store and forward"), por o veículo não ter conexão com o servidor. **Domínio de valores (Y/N) documentado apenas no dicionário Yellow** — ver nota no domínio de valores abaixo. |
| `RatecodeID` | RatecodeID | RatecodeID | long | Código da tarifa final em vigor ao término da corrida. Ver domínio de valores abaixo. |
| `PULocationID` | PULocationID | PULocationID | long | Zona de táxi do TLC em que o taxímetro foi acionado. |
| `DOLocationID` | DOLocationID | DOLocationID | long | Zona de táxi do TLC em que o taxímetro foi desligado. |
| `passenger_count` | passenger_count | passenger_count | long | Número de passageiros no veículo. |
| `trip_distance` | trip_distance | trip_distance | double | Distância percorrida da corrida, em milhas, registrada pelo taxímetro. |
| `fare_amount` | fare_amount | fare_amount | double | Tarifa calculada pelo taxímetro em função do tempo e distância. Para informações adicionais, ver https://www.nyc.gov/site/tlc/passengers/taxi-fare.page |
| `extra` | extra | extra | double | Acréscimos e sobretaxas diversas. |
| `mta_tax` | mta_tax | mta_tax | double | Taxa acionada automaticamente com base na tarifa registrada no taxímetro. |
| `tip_amount` | tip_amount | tip_amount | double | Valor da gorjeta. Preenchido automaticamente para gorjetas em cartão de crédito. Gorjetas em dinheiro não são incluídas. |
| `tolls_amount` | tolls_amount | tolls_amount | double | Valor total de todos os pedágios pagos na corrida. |
| `ehail_fee` | *(campo não documentado no dicionário oficial vigente)* | *(campo não documentado no dicionário oficial vigente)* | double | **Não consta em nenhum dos dois dicionários oficiais (18/03/2025).** Campo presente no schema físico dos arquivos parquet processados, sem descrição oficial atual disponível. |
| `improvement_surcharge` | improvement_surcharge | improvement_surcharge | double | Sobretaxa de melhoria cobrada na bandeirada. A sobretaxa de melhoria começou a ser cobrada em 2015. |
| `total_amount` | total_amount | total_amount | double | Valor total cobrado dos passageiros. Não inclui gorjetas em dinheiro. |
| `payment_type` | payment_type | payment_type | long | Código numérico indicando como o passageiro pagou a corrida. Ver domínio de valores abaixo. |
| `trip_type` | *(campo não existe no dicionário Yellow)* | trip_type | long | Código indicando se a corrida foi uma parada na rua (street-hail) ou um despacho (dispatch), atribuído automaticamente com base na tarifa em uso, podendo ser alterado pelo motorista. Ausente no dicionário/origem Yellow. |
| `congestion_surcharge` | congestion_surcharge | congestion_surcharge | double | Valor total cobrado na corrida referente à sobretaxa de congestionamento do estado de NY (NYS). |
| `airport_fee` | airport_fee | *(campo não existe no dicionário Green)* | double | Aplicável somente para embarques nos aeroportos LaGuardia e John F. Kennedy. Ausente no dicionário/origem Green. |
| `tipo` | *(derivada)* | *(derivada)* | string | Coluna adicionada no pipeline (não faz parte do dicionário oficial do TLC) para identificar o serviço de origem do registro: `yellow` ou `green`. |

> **Não incluído na camada Silver:** `cbd_congestion_fee` — "Per-trip charge for MTA's Congestion
> Relief Zone starting Jan. 5, 2025" (cobrança por corrida para a Zona de Alívio de Congestionamento
> da MTA, a partir de 5 de janeiro de 2025). Documentado em ambos os dicionários oficiais, mas fora
> do período de dados deste projeto (jan-mai/2023).

---

## Colunas derivadas / tratadas — exclusivas da Silver

Estas colunas não existem na origem TLC nem em seus dicionários oficiais; foram criadas durante
o tratamento de dados documentado em `src/02_silver.py`.

| Coluna | Tipo | Descrição |
|---|---|---|
| `pickup_datetime_tratado` | timestamp_ntz | Versão corrigida de `pickup_datetime`: para os registros em que `dropoff_datetime` era anterior a `pickup_datetime` (timestamps invertidos na origem), os dois valores foram trocados entre si (`least`/`greatest` dos dois campos). |
| `dropoff_datetime_tratado` | timestamp_ntz | Versão corrigida de `dropoff_datetime`, correspondente a `pickup_datetime_tratado`. |
| `data_corrida` | date | Data (sem hora), extraída de `pickup_datetime_tratado` — **não** de `pickup_datetime` original. |
| `year` | int | Ano, extraído de `pickup_datetime_tratado`. Usado, junto com `month`, como chave de particionamento físico das tabelas Silver e Gold (Hive-style: `year=.../month=...`). |
| `month` | int | Mês (1–12), extraído de `pickup_datetime_tratado`. |

> **Diferença em relação ao `ifood-case`:** neste projeto, `data_corrida`,
> `year` e `month` são derivados de `pickup_datetime_tratado`. Em
> jan-mai/2023, isso não alterou a data de nenhuma corrida, pois as inversões
> ocorreram dentro do mesmo dia. O recálculo evita que uma futura inversão
> atravessando a meia-noite grave a corrida na partição errada.

---

## Tratamentos aplicados sobre colunas existentes

Mantêm o nome de origem, mas o valor pode ter sido alterado na Silver.

| Coluna | Tratamento |
|---|---|
| `passenger_count` | Nulo ou zero é tratado como ausência de informação e substituído pela mediana dos valores válidos do lote processado. Na carga inicial de jan-mai/2023, a mediana foi `1`. Essa é uma hipótese de qualidade adotada no projeto; o dicionário oficial do TLC não define zero como ausência de informação. |

---

## Domínio de valores

Valores exatamente como documentados nos dicionários oficiais (18/03/2025).

### VendorID (idêntico em ambos os dicionários)
| Código | Provedor |
|---|---|
| 1 | Creative Mobile Technologies, LLC |
| 2 | Curb Mobility, LLC |
| 6 | Myle Technologies Inc |
| 7 | Helix *(exclusivo do dicionário Yellow — não consta no dicionário Green)* |

### RatecodeID (idêntico em ambos os dicionários)
| Código | Tarifa |
|---|---|
| 1 | Standard rate |
| 2 | JFK |
| 3 | Newark |
| 4 | Nassau or Westchester |
| 5 | Negotiated fare |
| 6 | Group ride |
| 99 | Null/unknown |

### payment_type (idêntico em ambos os dicionários)
| Código | Forma de pagamento |
|---|---|
| 0 | Flex Fare trip |
| 1 | Credit card |
| 2 | Cash |
| 3 | No charge |
| 4 | Dispute |
| 5 | Unknown |
| 6 | Voided trip |

### trip_type (documentado apenas no dicionário Green)
| Código | Tipo |
|---|---|
| 1 | Street-hail |
| 2 | Dispatch |

> Yellow não possui este campo em sua origem (decisão do pipeline, não do dicionário oficial).

### store_and_fwd_flag (domínio documentado apenas no dicionário Yellow)
| Valor | Significado |
|---|---|
| Y | store and forward trip |
| N | not a store and forward trip |

> O dicionário Green descreve o campo, mas não lista seu domínio. O uso de
> `Y`/`N` para as duas origens é uma inferência baseada no campo equivalente
> do Yellow.

---

## Colunas — Camada Gold

A camada Gold expõe duas tabelas Iceberg, ambas lidas publicamente pelo painel via `iceberg_scan()`
direto do bucket `nyc-taxi-aws-gold` (S3). Diferente da Silver, aqui o schema é um recorte — só as
colunas exigidas pelo caso de negócio, não o schema completo do TLC.

### `glue_catalog.gold.trips` — grão individual

| Coluna | Tipo | Origem | Descrição |
|---|---|---|---|
| `VendorID` | int | Silver, sem alteração | Ver domínio de valores. |
| `tipo` | string | Silver, sem alteração | `yellow` ou `green`. |
| `year` | int | Silver, sem alteração | Chave de partição física do Iceberg. |
| `month` | int | Silver, sem alteração | Chave de partição física do Iceberg. |
| `total_amount` | double | Silver, sem alteração | Ver descrição na tabela da Silver. |
| `passenger_count` | long | Silver, sem nova alteração na Gold | Já reflete o tratamento da Silver (nulo/zero substituído pela mediana). |
| `pickup_datetime` | timestamp_ntz | Alias de `pickup_datetime_tratado` (Silver) | **Atenção:** apesar do nome, este campo já reflete a correção de timestamps invertidos — não é o `tpep_`/`lpep_pickup_datetime` bruto da origem. |
| `dropoff_datetime` | timestamp_ntz | Alias de `dropoff_datetime_tratado` (Silver) | Mesma observação acima. |

Particionada por `(year, month)` e reorganizada fisicamente por `zorder(pickup_datetime)`
(`CALL system.rewrite_data_files`). A organização favorece filtros temporais; o ganho não foi
medido formalmente neste projeto.

### `glue_catalog.gold.trip_metrics` — grão agregado

Pré-agregada por `tipo`, `year`, `month` e `hora_do_dia` (extraída de `pickup_datetime_tratado`),
guardando soma e contagem — não médias prontas — para que consultas possam recompor médias
corretamente ao agregar por múltiplos meses, sem o problema de "média das médias".

| Coluna | Tipo | Descrição |
|---|---|---|
| `tipo` | string | `yellow` ou `green`. |
| `year` | int | Ano da corrida. |
| `month` | int | Mês da corrida (1–12). |
| `hora_do_dia` | int | Hora do dia (0–23), extraída de `pickup_datetime_tratado`. |
| `qtd_corridas` | long | `COUNT(*)` das corridas no grupo. |
| `soma_total_amount` | double | `SUM(total_amount)` no grupo — dividir por `qtd_corridas` para obter a média correta. |
| `soma_passenger_count` | long | `SUM(passenger_count)` no grupo — dividir por `qtd_corridas` para obter a média correta. |

Particionada por `(year, month)`. Como a tabela é pequena, `coalesce(1)` reduz
o número de partições Spark antes da escrita e evita a geração de vários
arquivos pequenos. A tabela não passa por `rewrite_data_files`.

---

## Linhagem

```
Bronze (s3://nyc-taxi-aws-bronze, particionado por tipo/year/month, catalogado no Glue)
    |
    v
Silver (glue_catalog.silver.trips, Iceberg, EMR Serverless)
    |  - Uniao Yellow + Green Cab
    |  - Padronizacao de schema e nomenclatura
    |  - Imputacao de passenger_count nulo/zero pela mediana
    |  - Correcao de timestamps invertidos
    |  - Validacao do periodo processado
    |  - Checagem de duplicidade (nenhuma removida)
    |  - data_corrida/year/month recalculados pos-tratamento de timestamps
    |
    +--> Gold (glue_catalog.gold.trips)
    |      Grao de corrida individual - camada de consumo publica (painel)
    |      (VendorID, tipo, year, month, total_amount, passenger_count,
    |       pickup_datetime, dropoff_datetime)
    |
    +--> Gold (glue_catalog.gold.trip_metrics)
           Grao agregado (tipo, year, month, hora_do_dia) - perguntas de
           negocio respondidas sem reprocessar a Silver
```

Orquestração: Step Functions dispara Bronze (Glue) → Silver (EMR Serverless) → Gold (EMR
Serverless) para o(s) `year-month` informado(s) no payload da execução (`anos_meses`).
Reprocessamento é idempotente por partição: reexecutar um `year/month` já existente sobrescreve
só aquela partição (`overwritePartitions()`), sem duplicar dado.

Detalhes de cada transformação: `src/01_bronze.py`, `src/02_silver.py`, `src/03_gold.py`.