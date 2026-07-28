####---------------------------------------------------------####
####----  Scripts 02_silver.py e 03_gold.py -> scripts/  ----####
####---------------------------------------------------------####
resource "aws_s3_object" "silver_script" {
  bucket = aws_s3_bucket.lakehouse["scripts"].id
  key    = "02_silver.py"
  source = "${path.module}/../src/02_silver.py"
  etag   = filemd5("${path.module}/../src/02_silver.py")
}


resource "aws_s3_object" "gold_script" {
  bucket = aws_s3_bucket.lakehouse["scripts"].id
  key    = "03_gold.py"
  source = "${path.module}/../src/03_gold.py"
  etag   = filemd5("${path.module}/../src/03_gold.py")
}



####---------------------------------------------------------------####
####----  EMR Serverless Application (roda 02_silver.py e      ----####
####----  03_gold.py). Uma application so; cada job-run        ----####
####----  (StartJobRun, feito pela Step Function) escolhe o    ----####
####----  script e o Glue Data Catalog/Iceberg via parametros  ----####
####----  de spark-submit -- nao configurados aqui.            ----####
####---------------------------------------------------------------####
resource "aws_emrserverless_application" "spark" {
  name          = "${var.app_name}-spark"
  release_label = "emr-7.13.0" # Spark 3.5.6 + suporte nativo a Iceberg
  type          = "SPARK"
  architecture  = "X86_64"

  ####--------------------------------------------------------------####
  ####---- Encerra automaticamente após 15 minutos sem jobs em  ----####         
  ####---- execução, evitando custo ocioso.                     ----####
  ####--------------------------------------------------------------####                                                  
  auto_stop_configuration {
    enabled              = true
    idle_timeout_minutes = 15
  }

  ####--------------------------------------------------------------------####
  ####---- Sem initial_capacity: a aplicação começa sem workers       ----####
  ####---- pré-aquecidos e provisiona capacidade sob demanda.         ----####
  ####--------------------------------------------------------------------####
  ####---- maximum_capacity espelha a cota padrão da conta            ----####
  ####---- (16 vCPUs concorrentes por região). Mantido explícito      ----####
  ####---- para documentar a decisão, não para restringir ainda mais  ----####
  ####---- a capacidade disponível.                                   ----####
  ####--------------------------------------------------------------------####
  maximum_capacity {
    cpu    = "16vCPU"
    memory = "64GB"
    disk   = "200GB"
  }
}