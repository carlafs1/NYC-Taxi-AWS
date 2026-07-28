<div align="center">

# 🚕 NYC-Taxi-AWS

**Lakehouse serverless na AWS para os dados de corridas de táxi da NYC TLC (arquitetura Medalhão, Terraform, CI/CD)**

![AWS S3](https://img.shields.io/badge/AWS%20S3-569A31?style=flat&logo=amazons3&logoColor=white)
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-FF9900?style=flat&logo=amazonaws&logoColor=white)
![EMR Serverless](https://img.shields.io/badge/EMR%20Serverless-232F3E?style=flat&logo=amazonaws&logoColor=white)
![Step Functions](https://img.shields.io/badge/Step%20Functions-CD2264?style=flat&logo=amazonaws&logoColor=white)
![EventBridge](https://img.shields.io/badge/EventBridge-FF4F8B?style=flat&logo=amazonaws&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=flat&logo=terraform&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=flat&logo=githubactions&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-0468DB?style=flat&logo=apacheiceberg&logoColor=white)
![Pytest](https://img.shields.io/badge/Pytest-0A9EDC?style=flat&logo=pytest&logoColor=white)

</div>

<br>

Os dados deste projeto podem ser consultados de forma interativa, sem sair do navegador,
através do painel disponível no [site pessoal da autora](https://carlasampaio.com.br):
**carlasampaio.com.br** → **Projetos** → **Case IFood - AWS**.

---

## 📑 Sumário

- [Objetivo](#-objetivo)
- [Arquitetura](#-arquitetura)
- [Qualidade dos dados](#-qualidade-dos-dados)
- [Testes automatizados](#-testes-automatizados)
- [Execução mensal e reprocessamento](#-execução-mensal-e-reprocessamento)
- [Consumo pelo painel](#-consumo-pelo-painel)
- [Infraestrutura e orquestração](#-infraestrutura-e-orquestração)
- [Segurança, observabilidade e custo](#-segurança-observabilidade-e-custo)
- [CI/CD](#-cicd)
- [Decisões técnicas](#-decisões-técnicas)
- [Limitações e próximos passos](#-limitações-e-próximos-passos)
- [Dicionário de dados](#-dicionário-de-dados)
- [Ambiente e execução](#-ambiente-e-execução)
- [Estrutura do repositório](#-estrutura-do-repositório)

---

## 🎯 Objetivo

Ingerir os dados de corridas de táxi de NY (Yellow/Green Cab), disponibilizá-los para consumo
via SQL e manter o pipeline rodando mensalmente com serviços gerenciados da AWS e
Infraestrutura como Código.

Jan-mai/2023 foi só a carga inicial. Dali em diante, o EventBridge dispara o pipeline todo mês,
sem precisar de nenhuma mudança manual.

Este projeto reaproveita um [case anterior em Databricks](https://github.com/carlafs1/ifood-case),
sobre os mesmos dados. Lá, o recorte era um lote histórico fechado. Aqui o pipeline nasceu para
rodar todo mês, e essa mudança de mentalidade aparece em várias decisões deste documento.

---

## 🏗️ Arquitetura

```
Bronze (S3, bronze.yellow / bronze.green, Parquet brutos)
    │  Glue Python Shell (01_bronze.py): baixa do portal NYC TLC,
    │  audita schema e contagem, sem transformação
    ▼
Silver (glue_catalog.silver.trips, tabela Iceberg)
    │  EMR Serverless (02_silver.py): une Yellow + Green,
    │  padroniza schema, trata qualidade
    ▼
Gold
    ├── glue_catalog.gold.trips          → grão de corrida individual
    └── glue_catalog.gold.trip_metrics   → grão agregado (tipo, year, month, hora)
         EMR Serverless (03_gold.py)
```

| Camada | O que faz |
|---|---|
| **Bronze** | Baixa os arquivos do portal NYC TLC e publica sem transformação, em tabelas separadas por tipo (`bronze.yellow`, `bronze.green`), cada uma particionada por `year`/`month`. Roda em Glue Python Shell (1 DPU): carga leve, sem cluster Spark. |
| **Silver** | Consolida Yellow e Green num schema único e tipado, como tabela Iceberg. Schema completo em [`docs/data_dictionary.md`](docs/data_dictionary.md). |
| **Gold** | `gold.trips` (grão individual) e `gold.trip_metrics` (soma e contagem por tipo/mês/hora, não médias prontas, para evitar o problema de "média das médias"). |

**Reprocessamento na Bronze.** Antes de baixar os arquivos de um `year`/`month` pedido, a
Bronze apaga primeiro tudo que já existir naquela partição. É proposital: uma vez decidido
reprocessar um período, o dado antigo dali já não é confiável, e misturar as duas versões seria
pior. Se a nova ingestão falhar, a partição fica vazia. Vazio é mais seguro do que servir um 
dado que pode estar errado.

---

## ✅ Qualidade dos dados

A Silver não só consolida Yellow e Green. Ela audita e trata a base antes de publicar, com
evidência quantitativa no log de cada execução (`src/02_silver.py`):

- **Schema**: nomes e tipos divergentes entre Yellow e Green, unificados (ver dicionário de dados).
- **Casts**: contagem de não-nulos antes e depois de cada conversão, para pegar perda de dado.
- **`passenger_count`**: nulos e zeros investigados (distribuição, relação com outras colunas) e
  substituídos pela mediana dos valores válidos.
- **Timestamps invertidos**: `dropoff_datetime` antes de `pickup_datetime` é identificado,
  quantificado e corrigido. `data_corrida`/`year`/`month` são recalculados a partir do valor
  já corrigido.
- **Período fora do escopo**: registros fora do intervalo pedido são quantificados e removidos.
- **Duplicidade**: checada por todos os atributos e por uma chave lógica (vendor, horários,
  localização, passageiros, valor).
- **`total_amount`**: outliers checados por IQR e cruzados com a duração da corrida. Mantidos
  sem alteração, por falta de evidência de inconsistência.

Essas validações são informativas, não bloqueiam a execução. Isso é escolha, não falta de
controle: as ocorrências conhecidas já são tratadas, e ainda não há histórico mensal suficiente
para definir um limiar confiável de bloqueio. O log de cada mês é a base para essa decisão
futura.

---

## 🧪 Testes automatizados

**Pré-requisito:** Java (JRE 11+) precisa estar instalado — `pyspark` depende de uma JVM real
mesmo em modo local (`master="local[1]"`), não é uma simulação em Python puro. Em Ubuntu/Debian:
`sudo apt install openjdk-21-jre-headless`. Sem isso, os testes que usam Spark falham com
`JAVA_GATEWAY_EXITED`.

```
pip install -r requirements-test.txt
pytest
```

Cobre as funções de lógica pura dos três scripts — `validar_anos_meses()` (duplicada
identicamente em Bronze/Silver/Gold, já que cada um roda sozinho num job EMR/Glue) e
`calcular_intervalo_datas()` (só na Silver), incluindo o caso mais frágil: virada de ano em
dezembro. Um teste garante que as três cópias de `validar_anos_meses()` continuam idênticas
entre si — o risco real de duplicar código em vez de compartilhar um módulo.

Também cobre `aplicar_schema()` com uma `SparkSession` local (sem cluster, sem EMR) — validação
de colunas obrigatórias e o comportamento real do cast do Spark (valor não-numérico vira `NULL`
silenciosamente, não levanta erro; é justamente esse tipo de perda de dado silenciosa que
`validar_casts()`, chamada logo depois no pipeline real, existe para detectar).

O que ainda fica de fora, por escolha: as transformações com mais lógica de negócio embutida
(tratamento de `passenger_count`, correção de timestamps invertidos, escrita Iceberg) não têm
teste automatizado — cobri-las exigiria simular DataFrames maiores e um cenário de dados mais
elaborado, custo que não se justifica no estágio atual do projeto. A garantia de correção delas
hoje vem da auditoria quantitativa da seção anterior, revisada manualmente a cada execução —
suficiente para volume mensal e time de uma pessoa, mas o próximo passo natural se o projeto
crescesse.

---

## 🔁 Execução mensal e reprocessamento

O escopo de cada execução vem do parâmetro `--anos-meses` (`AAAA-MM`, um ou vários), usado nas
três camadas. Bronze baixa só os meses pedidos. Silver e Gold leem só as partições
correspondentes e escrevem via `overwritePartitions()` do Iceberg: só as partições do lote são
tocadas, o resto da tabela fica intacto. Reprocessar um mês substitui a partição; um mês novo é
só adicionado. Ao final da Gold, o ponteiro público do painel é atualizado com o snapshot mais
recente.

**Por que `mês atual − 2`, e não `mês atual − 1`.** O portal NYC TLC costuma publicar o Parquet 
de um mês só semanas depois do fechamento dele. Tentar `mês atual − 1` arriscaria pegar um arquivo 
ainda indisponível ou incompleto. 

Se uma etapa falhar, a Step Function tenta de novo, até duas novas tentativas além da execução
inicial (até três no total, `MaxAttempts = 2` no retry). No final, sucesso ou falha, sai um
e-mail via SNS com o motivo. Execução manual continua disponível a qualquer momento, com o
período que for preciso.

---

## 🌐 Consumo pelo painel

```
EMR Serverless grava gold.trips / gold.trip_metrics (Iceberg)
    │  publica um ponteiro em texto com o metadata.json mais recente
    ▼
S3, bucket gold público por escolha (GetObject + ListBucket)
    ▼
Navegador do visitante, DuckDB-Wasm resolve metadados, manifests e Parquet direto do bucket
```

O painel lê a tabela Iceberg direto do S3, sem exportação intermediária, aproveitando o
*partition pruning* nativo do Iceberg. Tem gráficos prontos e um campo de SQL livre. A
exposição pública do bucket gold é uma escolha de arquitetura, não um descuido: os dados já são
públicos por natureza (dataset aberto do TLC, sem informação pessoal), e só assim o
`iceberg_scan()` do DuckDB-Wasm consegue resolver metadados, manifests e arquivos Parquet
direto do navegador, sem backend e sem exportar uma cópia para outro lugar.

---

## 🧱 Infraestrutura e orquestração

```
push em src/**        → GitHub Actions (OIDC) → S3 (scripts)
EventBridge (mensal)  → Step Functions → Glue Bronze → EMR Silver → EMR Gold
qualquer falha        → SNS (e-mail)
```

Um arquivo Terraform por domínio:

| Arquivo | Recursos |
|---|---|
| `s3.tf` | Buckets do lakehouse, bloqueio de acesso público (exceto leitura na Gold), CORS |
| `glue.tf` | Job da Bronze, databases do Data Catalog |
| `emr.tf` | Application EMR Serverless |
| `step_function.tf` | State machine Bronze → Silver → Gold |
| `eventbridge.tf` | Agendamento mensal |
| `iam.tf` | Roles de execução |
| `iam_github_actions.tf` | Role do GitHub Actions via OIDC |
| `sns.tf` | Notificação por e-mail |
| `ssm.tf` | Parâmetros operacionais (dia do agendamento, e-mail) |
| `backend.tf` | State remoto em S3 |

O bucket de state e os parâmetros do SSM ficam fora do Terraform, por escolha: o backend não
pode se autogerenciar, e parâmetros operacionais mudam rápido demais para justificar um `apply`
a cada ajuste.

---

## 🔒 Segurança, observabilidade e custo

**Segurança.** O S3 público expõe só o bucket gold, dados de consumo já públicos por natureza.
A policy libera `s3:GetObject` em todos os objetos do bucket e `s3:ListBucket` no bucket
inteiro, sem restrição a prefixo. O `ListBucket` é público de propósito: o `iceberg_scan()` do
DuckDB-Wasm precisa listar o bucket para resolver os manifests do Iceberg direto do navegador.
Escrita e exclusão permanecem bloqueadas. O GitHub Actions autentica via OIDC, sem chaves
permanentes. Cada role (Glue, EMR, Step Functions, EventBridge) tem só a permissão do seu
papel.

**Observabilidade.** Logs da Bronze no console do Glue. Logs da Silver e Gold no CloudWatch.
Histórico de cada execução no console do Step Functions, com o grafo completo. Falhas notificam
por SNS depois do retry.

**Custo.** Nada fica ligado entre execuções. O EMR Serverless desliga sozinho após 15 minutos
sem job. Os custos são a execução pontual do Glue e do EMR, armazenamento no S3 e leitura do
painel.

---

## 🔄 CI/CD

```
push em src/**  →  GitHub Actions (deploy-scripts.yml)  →  aws s3 sync src/ s3://nyc-taxi-aws-scripts/ --delete
```

Autenticação via OIDC, sem Access Keys no repositório. A role usada pelo GitHub Actions é
restrita, por *trust policy*, a este repositório e à branch `main`, e só tem permissão de
`PutObject`/`DeleteObject`/`ListBucket` no bucket de scripts. `terraform apply` continua manual:
infraestrutura muda pouco, e o custo de um erro ali é maior que num script.

O painel (`docs/painel.html`) sobe via GitHub Pages, sem workflow próprio.

---

## 🧭 Decisões técnicas

| Decisão | Motivo |
|---|---|
| Glue Python Shell (não Spark) na Bronze | Carga leve, sem necessidade de processamento distribuído. |
| EMR Serverless (não EMR em cluster) | Sem cluster para gerenciar, escala sob demanda e desliga sozinho. |
| Apache Iceberg | Partition pruning, schema evolution e sobrescrita dinâmica de partições. |
| `year`/`month` como colunas inteiras | Reflete a mudança de lote fechado para fluxo mensal, casando melhor com o particionamento. |
| Mediana para `passenger_count` nulo/zero | Menos sensível a extremos que a média. |
| `percentile_approx` em vez de `percentile` | Evita o custo de ordenação completa do cálculo exato. |
| `max_capacity = 1` DPU no Glue | Corrigido de `0.0625`, que estourava memória processando vários meses juntos. |
| Z-order por data em `gold.trips` | Preparação para o crescimento do volume. Ganho ainda não medido na escala atual. |
| CI/CD via OIDC, não Access Keys | Elimina chave permanente armazenada no GitHub. |
| `expire_snapshots` após cada escrita (Silver e Gold) | Evita o acúmulo indefinido de snapshots. São removidos os snapshots com mais de 45 dias, mantendo sempre pelo menos os dois mais recentes (retain_last = 2). |

---

## ⚠️ Limitações e próximos passos

- **Z-order sem benchmark.** Aplicado, mas o ganho de leitura ainda não foi medido.
- **Painel depende do dispositivo do visitante.** Consultas amplas pesam na memória local, não
  num backend.
- **Sem CDN na frente do S3 público.** Cada acesso ao painel gera leitura direta do bucket.
- **Backend do Terraform com bootstrap manual.** O bucket de state é criado uma vez, fora do
  código.
- **Testes cobrem lógica de validação, não as transformações de negócio mais complexas.**
  Tratamento de `passenger_count`, correção de timestamps e escrita Iceberg seguem validados via
  log da execução, não via suíte de testes — ver seção "Testes automatizados" para o porquê.
- **`rewrite_data_files` roda sempre.** Uma versão futura poderia compactar só partições que de
  fato fragmentaram.

---

## 📖 Dicionário de dados

Schema completo (Silver e Gold), domínio de valores, tratamentos aplicados e linhagem em
[`docs/data_dictionary.md`](docs/data_dictionary.md), construído a partir do dicionário do
[case anterior em Databricks](https://github.com/carlafs1/ifood-case), com as diferenças entre
os dois projetos sinalizadas.

---

## 🛠️ Ambiente e execução

| Etapa | Motor | Configuração |
|---|---|---|
| Bronze | AWS Glue Python Shell | `glue_version = 3.0`, `max_capacity = 1` DPU |
| Silver / Gold | Amazon EMR Serverless | `release_label = emr-7.1.0` (Spark 3.5, Iceberg nativo) |
| Região | `us-east-2` (Ohio) | |

Execução manual via Step Functions:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-2:<account-id>:stateMachine:nyc-taxi-aws-pipeline \
  --name "manual-$(date +%Y%m%d%H%M%S)" \
  --input '{"anos_meses": "2023-01,2023-02,2023-03,2023-04,2023-05"}'
```

`anos_meses` aceita um ou vários períodos `AAAA-MM` separados por vírgula.

<details>
<summary><strong>⚠️ Outras limitações de ambiente</strong></summary>
<br>

- **`maximum_capacity` do EMR Serverless** (16 vCPU) espelha a cota padrão da conta AWS, não
  restringe nada além do que já é o limite hoje. Documentado no Terraform para deixar a decisão
  explícita, não para mudar comportamento.
- **Reprodução local não é o objetivo.** O projeto depende de uma conta AWS própria (buckets,
  Glue Catalog, EMR Serverless). Não há um modo de rodar o pipeline inteiro fora da AWS.

</details>

---

## 🗂️ Estrutura do repositório

```
NYC-Taxi-AWS/
├─ .github/workflows/          # Deploy automático de src/ via OIDC
├─ src/                        # Pipeline de ingestão e transformação
│  ├─ 01_bronze.py             # Ingestão dos Parquet oficiais do TLC
│  ├─ 02_silver.py             # Padronização, qualidade e tratamento de dados
│  └─ 03_gold.py               # Modelagem da camada de consumo, escrita no S3
├─ tests/                      # Testes automatizados (lógica pura)
├─ terraform/                  # Infraestrutura como código
├─ docs/
│  ├─ data_dictionary.md       # Dicionário de dados completo (Silver e Gold)
│  ├─ config.json              # Aponta o painel pro bucket gold
│  └─ painel.html              # Painel web (DuckDB-Wasm) para consulta interativa
├─ requirements-test.txt       # Dependências para rodar os testes
├─ pytest.ini
└─ README.md
```

> O painel (`docs/painel.html`) está disponível através do
> [site pessoal da autora](https://carlasampaio.com.br), na seção "Projetos".