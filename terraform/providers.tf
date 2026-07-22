####------------------------------------------------------####
####----  Define os provedores utilizados no projeto  ----####
####------------------------------------------------------####
terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}


####-------------------------------------------------------####
####----  Define a região AWS utilizada pelo provedor  ----####
####-------------------------------------------------------####
provider "aws" {
  region = var.aws_region
}
