####--------------------------------------------------------------####
####----  EventBridge: dispara a state machine todo mes       ----####
####----  Dia vem do SSM (fora do Terraform, ver ssm.tf).     ----####
####----  Execucao manual continua possivel via console/CLI,  ----####
####----  sem depender desta regra.                           ----####
####--------------------------------------------------------------####

resource "aws_cloudwatch_event_rule" "monthly_trigger" {
  name                = "${var.app_name}-monthly-trigger"
  description         = "Dispara o pipeline NYC-Taxi-AWS mensalmente (dia definido no SSM)."
  schedule_expression = "cron(0 6 ${data.aws_ssm_parameter.scheduler_day.value} * ? *)"
}

resource "aws_cloudwatch_event_target" "pipeline" {
  rule      = aws_cloudwatch_event_rule.monthly_trigger.name
  target_id = "${var.app_name}-pipeline"
  arn       = aws_sfn_state_machine.pipeline.arn
  role_arn  = aws_iam_role.eventbridge_invoke_sfn.arn
}
