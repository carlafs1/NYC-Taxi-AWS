####------------------------------------------------------------####
####----  SNS: notificacoes de sucesso e falha do pipeline  ----####
####------------------------------------------------------------####
resource "aws_sns_topic" "pipeline_notifications" {
  name = "${var.app_name}-notifications"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.pipeline_notifications.arn
  protocol  = "email"
  endpoint  = data.aws_ssm_parameter.notification_email.value
}
