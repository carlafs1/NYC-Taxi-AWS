####---------------------------------------####
####----  Variáveis gerais do projeto  ----####
####---------------------------------------####

variable "app_name" {
  description = "Nome do projeto, usado como prefixo/tag em todos os recursos."
  type        = string
  default     = "nyc-taxi-aws"
}

variable "aws_region" {
  description = "Região AWS onde a infraestrutura é provisionada."
  type        = string
  default     = "us-east-2" # Ohio
}
