####----------------------------------------------------------------####
####----  Locals centralizados -- evita repetir os mesmos       ----####
####----  nomes/ARNs em varios arquivos (s3.tf/glue.tf/iam.tf)  ----####
####----------------------------------------------------------------####

locals {
  ####---- Nomes das camadas do lakehouse: buckets S3 e databases
  ####---- do Glue Data Catalog usam os mesmos 3 nomes.
  db_bronze = "bronze"
  db_silver = "silver"
  db_gold   = "gold"
  db_layers = [local.db_bronze, local.db_silver, local.db_gold]



  ####---- Buckets S3: as 3 camadas + o bucket de scripts.
  s3_layers = concat(local.db_layers, ["scripts"])



  ####---- Prefixo comum de ARN do Glue (catalog, database, table, job) --
  ####---- monta o resto em cada policy que precisar.
  glue_arn_prefix = "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}"



  ####---- Parametros tecnicos de execucao dos jobs.
  glue_bronze_python_modules = "pyarrow==14.0.1"



  ####---- Config do catalogo Iceberg (Silver/Gold), reaproveitada nos
  ####---- dois job-runs do Step Functions -- so muda o "warehouse".
  iceberg_catalog_name = "glue_catalog"

  iceberg_catalog_conf = {
    "spark.sql.catalog.${local.iceberg_catalog_name}"              = "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.${local.iceberg_catalog_name}.catalog-impl" = "org.apache.iceberg.aws.glue.GlueCatalog"
    "spark.sql.catalog.${local.iceberg_catalog_name}.io-impl"      = "org.apache.iceberg.aws.s3.S3FileIO"
  }

  emr_app_config = [
    {
      Classification = "iceberg-defaults"
      Properties     = { "iceberg.enabled" = "true" }
    }
  ]

  emr_spark_submit_params = "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

  

  ####---- Retry/catch identicos em Bronze, Silver e Gold -- 1 definicao,
  ####---- reaproveitada nos 3 estados do Step Functions.
  job_retry_policy = [{
    ErrorEquals     = ["States.ALL"]
    IntervalSeconds = 60
    MaxAttempts     = 2
    BackoffRate     = 2.0
  }]

  job_catch_to_failure = [{
    ErrorEquals = ["States.ALL"]
    ResultPath  = "$.error"
    Next        = "NotificarFalha"
  }]
}

