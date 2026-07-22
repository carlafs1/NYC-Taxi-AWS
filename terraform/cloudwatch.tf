####---------------------------------------------------------------####
####----  CloudWatch Logs: Silver e Gold (EMR Serverless)      ----####
####----  A role ja tinha permissao (iam.tf) mas nada mandava  ----####
####----  logs pra ca -- sem isso, o EMR usa so o managed      ----####
####----  storage (30 dias, via console).                      ----####
####---------------------------------------------------------------####
resource "aws_cloudwatch_log_group" "emr_serverless" {
  name              = "/aws/emr-serverless/${var.app_name}"
  retention_in_days = 30

  tags = {
    Project = var.app_name
  }
}
