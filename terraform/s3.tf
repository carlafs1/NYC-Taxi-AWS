####--------------------------------------------------------####
####----  Buckets S3 do Lakehouse (bronze/silver/gold)  ----####
####----  + bucket de scripts para Glue/EMR Serverless  ----####
####--------------------------------------------------------####

resource "aws_s3_bucket" "lakehouse" {
  for_each = toset(local.s3_layers)

  bucket = "${var.app_name}-${each.key}"

  tags = {
    Project = var.app_name
    Layer   = each.key
  }
}


####--------------------------------------------------------------------------####
####----  Bloqueia acesso público em todos os buckets do lakehouse,       ----####
####----  EXCETO o gold: dados de consumo, liberado para leitura pública  ----####
####----  (ver bucket policy + CORS abaixo) para o painel DuckDB-Wasm     ----####
####----  ler as tabelas Iceberg direto do navegador via iceberg_scan(),  ----####
####----  sem duplicar dados nem expor credenciais AWS.                   ----####
####--------------------------------------------------------------------------####
resource "aws_s3_bucket_public_access_block" "lakehouse" {
  for_each = aws_s3_bucket.lakehouse

  bucket = each.value.id

  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = each.key == "gold" ? false : true
  restrict_public_buckets = each.key == "gold" ? false : true
}

data "aws_iam_policy_document" "lakehouse_gold_read" {
  statement {
    sid       = "PublicReadGetObject"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lakehouse["gold"].arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }
  }

  ####---- ListBucket é necessário para o iceberg_scan/httpfs do
  ####---- DuckDB-Wasm resolver os manifests do Iceberg no navegador.
  statement {
    sid       = "PublicListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse["gold"].arn]

    principals {
      type        = "*"
      identifiers = ["*"]
    }
  }
}

resource "aws_s3_bucket_policy" "lakehouse_gold" {
  bucket = aws_s3_bucket.lakehouse["gold"].id
  policy = data.aws_iam_policy_document.lakehouse_gold_read.json

  depends_on = [aws_s3_bucket_public_access_block.lakehouse]
}

resource "aws_s3_bucket_cors_configuration" "lakehouse_gold" {
  bucket = aws_s3_bucket.lakehouse["gold"].id

  cors_rule {
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["*"]
    allowed_headers = ["Range", "Content-Type", "x-host-override"]
    expose_headers  = ["Content-Length", "Content-Range", "ETag"]
    max_age_seconds = 3600
  }
}


####------------------------------------------------------------------####
####----  Criptografia padrão SSE-S3 (AES-256) — sem custo extra  ----####
####------------------------------------------------------------------####
resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  for_each = aws_s3_bucket.lakehouse

  bucket = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
