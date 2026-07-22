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


####--------------------------------------------------------------------####
####----  Bloqueia acesso público em todos os buckets do lakehouse  ----####
####--------------------------------------------------------------------####
resource "aws_s3_bucket_public_access_block" "lakehouse" {
  for_each = aws_s3_bucket.lakehouse

  bucket = each.value.id

  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = true
  restrict_public_buckets = true
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
