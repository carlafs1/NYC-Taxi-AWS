####---------------------------------------------------------------####
####----  SSM Parameter Store: config operacional do pipeline  ----####
####----  Parametros criados por fora do Terraform (AWS CLI/   ----####
####----  console). Muda em redeploy. Terraform so LE (data    ----####
####----  source).                                             ----####
####---------------------------------------------------------------####

data "aws_ssm_parameter" "scheduler_day" {
  name = "/${var.app_name}/scheduler-day"
}

data "aws_ssm_parameter" "notification_email" {
  name            = "/${var.app_name}/notification-email"
  with_decryption = true
}