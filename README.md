<div align="center">

# 🚕 NYC-Taxi-AWS

**Lakehouse serverless na AWS para processamento dos dados de corridas de táxi da NYC TLC — arquitetura Medalhão (Bronze → Silver → Gold), Infraestrutura como Código com Terraform e CI/CD**

![AWS S3](https://img.shields.io/badge/AWS%20S3-569A31?style=flat&logo=amazons3&logoColor=white)
![AWS Glue](https://img.shields.io/badge/AWS%20Glue-FF9900?style=flat&logo=amazonaws&logoColor=white)
![EMR Serverless](https://img.shields.io/badge/EMR%20Serverless-232F3E?style=flat&logo=amazonaws&logoColor=white)
![Step Functions](https://img.shields.io/badge/Step%20Functions-CD2264?style=flat&logo=amazonaws&logoColor=white)
![EventBridge](https://img.shields.io/badge/EventBridge-FF4F8B?style=flat&logo=amazonaws&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=flat&logo=terraform&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-2088FF?style=flat&logo=githubactions&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-0468DB?style=flat&logo=apacheiceberg&logoColor=white)

</div>

<br>

Os dados deste projeto podem ser consultados de forma interativa por meio do painel
disponível no portfólio da autora:
[carlasampaio.com.br](https://carlasampaio.com.br/) → Projetos → Case IFood - AWS

---

## 📑 Sumário

- [Objetivo](#-objetivo)
- [Visão geral e resultados](#-visão-geral-e-resultados)
- [Arquitetura](#-arquitetura)
- [Qualidade dos dados](#-qualidade-dos-dados)
- [Incrementalidade, reprocessamento e idempotência lógica](#-incrementalidade-reprocessamento-e-idempotência-lógica)
- [Consumo pelo painel](#-consumo-pelo-painel)
- [Infraestrutura e orquestração](#-infraestrutura-e-orquestração)
- [Segurança](#-segurança)
- [Observabilidade](#-observabilidade)
- [Custo](#-custo)
- [CI/CD (GitHub Actions + OIDC)](#-cicd-github-actions--oidc)
- [Decisões técnicas](#-decisões-técnicas)
- [Particionamento e otimizações físicas](#-particionamento-e-otimizações-físicas)
- [Limitações e próximos passos](#-limitações-e-próximos-passos)
- [Dicionário de dados](#-dicionário-de-dados)
- [Ambiente e execução](#-ambiente-e-execução)
- [Estrutura do repositório](#-estrutura-do-repositório)

---

## 🎯 Objetivo

Implementar, com serviços gerenciados da AWS e Infraestrutura como Código, um Lakehouse
serverless completo para os dados de corridas de táxi de NYC (jan-mai/2023): ingestão,
padronização, tratamento de qualidade e uma camada de consumo publicada com atualização
mensal automática — sem backend de consulta dedicado entre os dados e o navegador de quem
acessa o painel.

---

## 📊 Visão geral e resultados

| Item | Resultado |
|---|---|
| Fonte | NYC TLC Trip Record Data |
| Período processado | Janeiro a maio de 2023 |
| Registros Yellow | 16.186.386 |
| Registros Green | 339.630 |
| Total processado (camada Silver) | 16.526.016 |
| Camadas | Bronze → Silver → Gold |
| Formato de tabela | Apache Iceberg |
| Execução | Mensal, incremental, reprocessável por período |
| Infraestrutura | Terraform (bootstrap mínimo do backend fora do código, ver [Infraestrutura e orquestração](#-infraestrutura-e-orquestração)) |
| Orquestração | AWS Step Functions + EventBridge |
| Consumo | Painel web, DuckDB-Wasm lendo Iceberg direto do S3 |

> Números sujeitos a mudar a cada reprocessamento — refletem o estado mais recente do
> pipeline no momento da última atualização deste README, não um valor fixo do case.

---

## 🏗️ Arquitetura

```
Bronze (S3 — bronze.yellow / bronze.green, Parquet brutos)
    │  Glue Python Shell (01_bronze.py) — download NYC TLC, auditoria de
    │  schema/contagem, sem transformação
    ▼
Silver (glue_catalog.silver.trips — tabela Iceberg)
    │  EMR Serverless (02_silver.py)
    │  União Yellow + Green, padronização de schema e nomenclatura
    │  Tratamento: nulos, timestamps invertidos, período inválido, duplicatas
    ▼
Gold
    ├── glue_catalog.gold.trips          → grão de corrida individual (consumo)
    └── glue_catalog.gold.trip_metrics   → grão agregado (tipo, year, month, hora_do_dia)
         EMR Serverless (03_gold.py)
```

| Camada | O que faz |
|---|---|
| **Bronze** | Baixa os arquivos originais do portal NYC TLC e publica sem transformação, particionados por `year`/`month`. Roda em **Glue Python Shell** (1 DPU) — carga leve, sem necessidade de um cluster Spark. |
| **Silver** | Consolida Yellow e Green em um schema único e tipado como tabela **Iceberg**, preservando os atributos relevantes da origem em um schema unificado. Schema completo em [`docs/data_dictionary.md`](docs/data_dictionary.md). |
| **Gold** | Duas tabelas Iceberg: `gold.trips` (grão individual) e `gold.trip_metrics` (pré-agregada por tipo/mês/hora, guardando **soma e contagem** — não médias prontas — evitando o problema de "média das médias"). |

---

## ✅ Qualidade dos dados

A camada Silver não só consolida Yellow e Green — ela audita e trata a base antes de
publicar, com evidência quantitativa impressa no log a cada etapa (`src/02_silver.py`):

- **Padronização de schema** entre Yellow e Green (nomes de coluna e tipos divergentes na
  origem, unificados em [`docs/data_dictionary.md`](docs/data_dictionary.md)).
- **Validação de casts** — contagem de não-nulos antes/depois de cada conversão de tipo,
  para detectar perda de dado causada pelo cast. Na carga inicial de janeiro a maio de 2023,
  não foram identificadas perdas causadas pelos casts; divergências de cargas futuras ficam
  registradas no log de cada execução.
- **`passenger_count`** — investigação de nulos e zeros (distribuição, relação com
  VendorID/trip_type/store_and_fwd_flag/total_amount) antes de decidir o tratamento;
  substituídos pela mediana dos valores válidos.
- **Timestamps invertidos** — registros com `dropoff_datetime` anterior a `pickup_datetime`
  identificados, quantificados por faixa de diferença, e corrigidos (troca de valores);
  `data_corrida`/`year`/`month` recalculados a partir dos timestamps corrigidos.
- **Período fora do escopo** — registros com `data_corrida` fora do intervalo pedido em
  `--anos-meses` são quantificados e removidos.
- **Duplicidade** — checada tanto por todos os atributos quanto por uma chave lógica
  (VendorID, horários, localização, passageiros, valor total).
- **`total_amount`** — outliers investigados pelo critério IQR e cruzados com a duração da
  corrida (para distinguir valor alto genuíno de inconsistência); valores negativos
  recompostos a partir dos componentes financeiros para checar consistência interna. Mantidos
  sem alteração — sem evidência suficiente de inconsistência que justificasse remoção.

Todas essas análises rodam como parte do próprio job da Silver, não como um processo à parte
— qualquer reprocessamento gera as mesmas evidências novamente, no log da execução.

---

## 🔁 Incrementalidade, reprocessamento e idempotência lógica

O escopo de cada execução é definido pelo parâmetro `--anos-meses` (`AAAA-MM`, um ou vários
separados por vírgula), propagado de forma consistente às três camadas:

- **Bronze** baixa/republica só os arquivos dos meses pedidos.
- **Silver** e **Gold** leem só as partições Bronze/Silver correspondentes e escrevem via
  `overwritePartitions()` do Iceberg — a escrita afeta **só** as partições `year`/`month`
  presentes no lote processado; o restante da tabela não é tocado.
- Reprocessar um mês já existente **substitui** aquela partição; um mês novo é só adicionado.
  Rodar o mesmo período duas vezes produz o mesmo escopo de saída (idempotente em relação ao
  conjunto de partições afetadas).
- Ao final da Gold, o ponteiro público (`public-pointers/{tabela}.txt`) é reescrito com o
  `metadata_location` mais recente — o painel passa a refletir o snapshot novo automaticamente.

---

## 🌐 Consumo pelo painel

```
EMR Serverless (grava gold.trips / gold.trip_metrics — tabelas Iceberg)
    │  Publica um ponteiro em texto plano com o metadata.json mais recente
    ▼
S3 — leitura pública restrita aos objetos necessários (ver Segurança)
    ▼
Navegador — DuckDB-Wasm lê o ponteiro, monta iceberg_scan() e executa SQL
    100% no dispositivo do visitante — sem cópia dos dados, sempre atualizado
```

O painel lê a tabela Iceberg real, direto do S3 (sem exportação intermediária para outro
formato/local), aproveitando o *partition pruning* do próprio Iceberg — uma consulta
filtrando `year`/`month` restringe os arquivos de dados lidos às partições correspondentes.

O painel expõe gráficos prontos e um campo de **SQL livre**. Como toda a consulta roda no
navegador de quem acessa (sem custo de processamento em um backend dedicado), vale registrar:
- Os dados publicados são intencionalmente públicos e não contêm informação pessoal sensível
  (é o dataset aberto do NYC TLC).
- O processamento acontece no dispositivo do visitante — consultas amplas podem consumir
  mais memória/tráfego local, sem consumir um serviço de processamento dedicado no backend.

---

## 🧱 Infraestrutura e orquestração

**Fluxo de dependências:**

```
push em src/**        → GitHub Actions (OIDC) → S3 (scripts)
EventBridge (mensal)  → Step Functions
Step Functions        → Glue Bronze → EMR Silver → EMR Gold
qualquer falha         → SNS (e-mail)
Glue/EMR                → S3 (dados) + Glue Data Catalog (metadados Iceberg)
```

**Terraform**, um arquivo por domínio de serviço:

| Arquivo | Recursos |
|---|---|
| `s3.tf` | Buckets do lakehouse (bronze/silver/gold/scripts), bloqueio de acesso público (exceto leitura pontual na Gold), CORS |
| `glue.tf` | Job Python Shell da Bronze, databases do Data Catalog |
| `emr.tf` | Application EMR Serverless (Spark 3.5, suporte nativo a Iceberg) |
| `step_function.tf` | State machine que encadeia Bronze → Silver → Gold |
| `eventbridge.tf` | Regra de agendamento mensal |
| `iam.tf` | Roles/policies de execução (Glue, EMR Serverless, Step Functions, EventBridge) |
| `iam_github_actions.tf` | Role assumida pelo GitHub Actions via OIDC |
| `sns.tf` | Tópico e assinatura de notificação por e-mail |
| `ssm.tf` | Leitura de parâmetros operacionais (dia do agendamento, e-mail) |
| `backend.tf` | Configuração do state remoto em S3 |

**O que fica fora do Terraform, deliberadamente**: o bucket de state (bootstrap único,
manual, por natureza — o backend não pode se autogerenciar) e os parâmetros operacionais no
SSM (dia do agendamento, e-mail de notificação) — mudam com mais frequência que a infra em
si e não justificam um `apply` a cada ajuste. A infraestrutura do pipeline é declarada em
Terraform; o bootstrap do backend e os parâmetros operacionais ficam fora, por escolha.

---

## 🔒 Segurança

- **S3 com leitura pública restrita aos objetos necessários** para o painel funcionar — não
  o bucket inteiro. A policy permite somente `s3:GetObject`, limitado aos prefixos
  necessários à leitura da tabela Iceberg (ponteiros, metadados, manifests e os arquivos
  Parquet referenciados); `s3:ListBucket`, escrita e exclusão permanecem bloqueados.
- **CI/CD sem chaves permanentes** — o GitHub Actions autentica via OIDC (detalhes na seção
  [CI/CD](#-cicd-github-actions--oidc)), sem Access Keys permanentes armazenadas como secrets.
- **Privilégio mínimo nas roles de execução** — cada role (Glue, EMR Serverless, Step
  Functions, EventBridge, GitHub Actions) tem só as permissões necessárias ao seu papel
  específico, declaradas em `terraform/iam.tf` e `terraform/iam_github_actions.tf`.

---

## 🔍 Observabilidade

- **Logs da Bronze** publicados pelo próprio AWS Glue (console Glue → Jobs → execuções).
- **Logs da Silver e Gold** no CloudWatch Logs, via EMR Serverless.
- **Histórico e status de cada etapa** disponíveis no console do Step Functions — inclusive
  o grafo de execução (Bronze → Silver → Gold → notificação), consultável por execução.
- **Falhas notificadas por SNS** (e-mail), com retry automático (2 tentativas) antes de
  disparar a notificação.
- **Evidência quantitativa por execução** — cada etapa da Silver imprime contagens de
  entrada, saída e registros afetados por cada regra de tratamento (ver
  [Qualidade dos dados](#-qualidade-dos-dados)), permitindo auditar o efeito de cada
  reprocessamento sem precisar consultar a tabela final.

---

## 💰 Custo

O projeto evita recursos permanentes: nada fica ligado entre execuções. Os principais
custos são a execução pontual do Glue (Bronze) e do EMR Serverless (Silver/Gold — que
desliga sozinho após 15min de ociosidade), armazenamento no S3 e requisições de leitura. A
consulta pelo painel não exige servidor de aplicação, embora gere leitura e transferência
dos objetos publicados no S3 a cada acesso.

---

## 🔄 CI/CD (GitHub Actions + OIDC)

Os scripts do pipeline (`src/*.py`) são publicados automaticamente no S3 a cada push na
branch `main` que altere algo em `src/`:

```
push em src/**  →  GitHub Actions (deploy-scripts.yml)  →  aws s3 sync src/ s3://nyc-taxi-aws-scripts/ --delete
```

- **Autenticação via OIDC** — o GitHub Actions assume uma IAM Role
  (`nyc-taxi-aws-github-actions-deploy`) usando um token temporário assinado, validado pela
  AWS contra o Identity Provider OIDC do GitHub. A Role é restrita, na *trust policy*, a este
  repositório e à branch `main`.
- **Privilégio mínimo** — só `PutObject`/`DeleteObject`/`ListBucket` no bucket de scripts;
  sem permissão de `terraform apply` (infraestrutura aplicada manualmente, com revisão do
  `plan` antes de cada mudança).
- **Credenciais de vida curta** — `role-duration-seconds: 900` (piso do STS) e
  `timeout-minutes: 5` no job.
- **O repositório Git é a fonte de verdade versionada**; o bucket de scripts contém apenas
  os artefatos implantados, sempre espelhando (`--delete`) o que está em `src/` — nunca uma
  segunda versão divergente.

O deploy de `docs/painel.html` é automático via GitHub Pages, sem workflow próprio.

---

## 🧭 Decisões técnicas

| Decisão | Motivo |
|---|---|
| Glue Python Shell (não Spark) na Bronze | Carga leve — download de arquivos e auditoria via `pyarrow`, sem necessidade de processamento distribuído. |
| EMR Serverless (não EMR em cluster) na Silver/Gold | Sem cluster para gerenciar; escala sob demanda e desliga sozinho (`auto_stop_configuration`, 15min de ociosidade). |
| Apache Iceberg como formato de tabela | Suporte nativo a *partition pruning*, *schema evolution* e sobrescrita dinâmica de partições — reprocessar um mês substitui só aquela partição. |
| `year`/`month` como colunas inteiras de partição | Em vez de uma string `ano_mes` intermediária — usadas como chave de partição física e nas análises de qualidade. |
| Mediana para tratar `passenger_count` nulo/zero | Menos sensível a extremos que a média; calculada só sobre valores válidos para não incluir os próprios valores a substituir. |
| `percentile_approx` em vez de `percentile` | Evita o custo de ordenação completa exigido pelo cálculo exato, nos quantis usados (mediana, IQR). |
| `persist()` com liberação explícita | Cache antecipado antes das ~20 ações de validação na Silver, liberado assim que a versão filtrada final é persistida. |
| `max_capacity = 1` DPU no Glue Bronze | Corrigido de `0.0625` (causava OOM processando múltiplos meses de uma vez) — Python Shell só aceita `0.0625` ou `1`. |
| Z-order por data, como preparação para crescimento | Ver [Particionamento e otimizações físicas](#-particionamento-e-otimizações-físicas) — benefício ainda não medido na escala atual. |
| CI/CD via OIDC, não Access Keys | Elimina chaves de acesso permanentes armazenadas no GitHub. |

---

## 🧩 Particionamento e otimizações físicas

As tabelas `trips` (Silver e Gold) são particionadas fisicamente por `year`/`month`. Após a
escrita, os arquivos são reorganizados por data com `rewrite_data_files` (conceitualmente
semelhante ao `OPTIMIZE ... ZORDER` do Delta Lake), buscando melhorar as estatísticas min/max
usadas na poda de arquivos e blocos Parquet.

Na escala atual, muitas partições mensais têm apenas um arquivo. Nesse cenário, a
possibilidade de eliminar arquivos inteiros é limitada, mas ainda pode haver benefício na
organização interna do Parquet, por meio de estatísticas de row groups e, quando disponíveis
e utilizadas pela engine, índices no nível de página.

O impacto real ainda não foi medido por plano de execução ou volume lido, e deve ser tratado
como uma otimização preparatória para o crescimento do volume, não como um ganho comprovado
na escala atual.

---

## ⚠️ Limitações e próximos passos

- **Z-order sem benchmark** — reorganização física aplicada, mas o ganho de leitura ainda
  não foi medido por plano de execução na escala atual (ver
  [Particionamento e otimizações físicas](#-particionamento-e-otimizações-físicas)).
- **Consulta no painel limitada pelo dispositivo** — DuckDB-Wasm roda no navegador de quem
  acessa; consultas muito amplas dependem da memória/CPU disponíveis localmente.
- **Dados públicos no S3 geram transferência** — cada acesso ao painel lê objetos do S3
  diretamente; sem CDN/cache intermediário hoje.
- **Backend Terraform com bootstrap manual** — o bucket de state é criado uma única vez, fora
  do código (ver [Infraestrutura e orquestração](#-infraestrutura-e-orquestração)).
- **Sem testes automatizados** — validação de qualidade acontece via evidência impressa no
  log de cada execução, não via suíte de testes.
- **Compactação condicionada à fragmentação** — hoje o `rewrite_data_files` roda
  incondicionalmente a cada carga; uma versão futura poderia só compactar partições que de
  fato acumularam múltiplos arquivos pequenos.

---

## 📖 Dicionário de dados

Schema completo das camadas Silver e Gold, com descrição de cada coluna traduzida fielmente
dos dicionários oficiais do TLC, domínio de valores e linhagem completa:
[`docs/data_dictionary.md`](docs/data_dictionary.md).

---

## 🛠️ Ambiente e execução

| Etapa | Motor | Configuração |
|---|---|---|
| Bronze | AWS Glue Python Shell | `glue_version = 3.0`, `max_capacity = 1` DPU, `pyarrow==14.0.1` |
| Silver / Gold | Amazon EMR Serverless | `release_label = emr-7.1.0` (Spark 3.5, suporte nativo a Iceberg), arquitetura x86_64 |
| Região | `us-east-2` (Ohio) | |

Execução manual do pipeline completo (Bronze → Silver → Gold) via Step Functions:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-2:<account-id>:stateMachine:nyc-taxi-aws-pipeline \
  --name "manual-$(date +%Y%m%d%H%M%S)" \
  --input '{"anos_meses": "2023-01,2023-02,2023-03,2023-04,2023-05"}'
```

`anos_meses` aceita um ou vários períodos `AAAA-MM` separados por vírgula, no mesmo formato
em todas as três etapas.

---

## 🗂️ Estrutura do repositório

```
NYC-Taxi-AWS/
├─ .github/
│  └─ workflows/
│     └─ deploy-scripts.yml    # CI/CD: sync automático de src/ -> S3 via OIDC
├─ src/                        # Pipeline de ingestão e transformação
│  ├─ 01_bronze.py             # Ingestão e auditoria dos dados brutos (Glue Python Shell)
│  ├─ 02_silver.py             # Padronização, qualidade e tratamento de dados (EMR Serverless)
│  └─ 03_gold.py               # Modelagem da camada de consumo (EMR Serverless)
├─ terraform/                  # Infraestrutura como código (ver seção Infraestrutura e orquestração)
├─ docs/
│  ├─ data_dictionary.md       # Dicionário de dados completo (schema Silver/Gold)
│  ├─ painel.html              # Painel web (DuckDB-Wasm), acessível pelo site pessoal da autora
│  └─ config.json              # Aponta o painel para o bucket Gold público
└─ README.md
```