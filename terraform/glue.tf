####-----------------------------------------------------------####
####----  Glue Data Catalog: databases bronze/silver/gold  ----####
####-----------------------------------------------------------####
resource "aws_glue_catalog_database" "layers" {
  for_each = toset(local.db_layers)

  name = each.key
}


####-------------------------------------------####
####----  Script 01_bronze.py -> scripts/  ----####
####-------------------------------------------####
resource "aws_s3_object" "bronze_script" {
  bucket = aws_s3_bucket.lakehouse["scripts"].id
  key    = "01_bronze.py"
  source = "${path.module}/../src/01_bronze.py"
  etag   = filemd5("${path.module}/../src/01_bronze.py")
}


####-----------------------------------------------------------####
####----  Glue Job: 01_bronze.py (Python Shell)            ----####
####----  --anos-meses e --bronze-bucket vem da Step       ----####
####----  Function em tempo de execucao (nao fixos aqui).  ----####
####-----------------------------------------------------------####
resource "aws_glue_job" "bronze" {
  name         = "${var.app_name}-bronze"
  role_arn     = aws_iam_role.glue_ingest.arn
  glue_version = "3.0"
  max_capacity = 1 

  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.lakehouse["scripts"].id}/${aws_s3_object.bronze_script.key}"
    python_version  = "3.9"
  }

  default_arguments = {
    "--glue-database"             = local.db_bronze
    "--additional-python-modules" = local.glue_bronze_python_modules
  }

  timeout = 60 # minutos
}