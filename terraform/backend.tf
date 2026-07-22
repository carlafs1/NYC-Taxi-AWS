####----------------------------------------------------------------------------####
####----            Backend remoto para o Terraform State no S3             ----####
####----------------------------------------------------------------------------####
####---- Bucket criado manualmente (fora do Terraform), uma única vez:      ----####
####---- ver comando em docs/ ou no histórico do projeto. Não é gerenciado  ----####
####---- por este código.                                                   ----####
####----------------------------------------------------------------------------####
terraform {
  backend "s3" {
    bucket = "nyc-taxi-aws-tfstate"
    key    = "terraform.tfstate"
    region = "us-east-2"
  }
}