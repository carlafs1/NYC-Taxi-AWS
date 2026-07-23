####--------------------------------------------------------------####
####----  CloudWatch Logs: EMR Serverless (Silver e Gold)     ----####
####----  Cria o Log Group para centralizar os logs dos jobs  ----####
####----  do EMR Serverless no CloudWatch, com retenção de    ----####
####----  30 dias.                                            ----####
####--------------------------------------------------------------####
resource "aws_cloudwatch_log_group" "emr_serverless" {
  name              = "/aws/emr-serverless/${var.app_name}"
  retention_in_days = 30

  tags = {
    Project = var.app_name
  }
}
