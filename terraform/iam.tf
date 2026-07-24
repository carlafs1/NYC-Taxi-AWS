####-----------------------------------------------------------####
####----  Dados da conta/regiao, usados para montar ARNs   ----####
####-----------------------------------------------------------####
####---- data aws_caller_identity → consulta a identidade  ----#### 
####---- autenticada na AWS. Usado para montar o ARN sem   ----####
####---- precisar escrever o número da conta no código. Se ----####
####---- executar o projeto em outra conta o Terraform     ----####
####---- descobrirá o novo account_id sozinho.             ----####
####-----------------------------------------------------------####
data "aws_caller_identity" "current" {}




####-------------------------------------------------------------------####
####----  Role: Glue Python Shell (01_bronze.py)                   ----####
####----  Ingere NYC TLC -> bronze/, cataloga bronze.yellow/green  ----####
####-------------------------------------------------------------------####

data "aws_iam_policy_document" "glue_ingest_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_ingest" {
  name               = "${var.app_name}-glue-ingest"
  assume_role_policy = data.aws_iam_policy_document.glue_ingest_trust.json
}

data "aws_iam_policy_document" "glue_ingest_permissions" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${aws_s3_bucket.lakehouse["scripts"].arn}/*",
      "${aws_s3_bucket.lakehouse["bronze"].arn}/*",
    ]
  }

  statement {
    effect  = "Allow"
    actions = ["s3:ListBucket"]
    resources = [
      aws_s3_bucket.lakehouse["scripts"].arn,
      aws_s3_bucket.lakehouse["bronze"].arn,
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:CreateDatabase",
      "glue:GetTable",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetPartition",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:BatchCreatePartition",
    ]
    resources = [
      "${local.glue_arn_prefix}:catalog",
      "${local.glue_arn_prefix}:database/${local.db_bronze}",
      "${local.glue_arn_prefix}:table/${local.db_bronze}/*",
    ]
  }

  statement {
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup"]
    resources = [
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*",
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/*:*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_ingest" {
  name   = "${var.app_name}-glue-ingest-policy"
  role   = aws_iam_role.glue_ingest.id
  policy = data.aws_iam_policy_document.glue_ingest_permissions.json
}




####---------------------------------------------------------------####
####----  Role: EMR Serverless (02_silver.py e 03_gold.py)     ----####
####----  Le bronze, escreve silver/gold como tabelas Iceberg  ----####
####---------------------------------------------------------------####

data "aws_iam_policy_document" "emr_serverless_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_serverless_job" {
  name               = "${var.app_name}-emr-serverless-job"
  assume_role_policy = data.aws_iam_policy_document.emr_serverless_trust.json
}

data "aws_iam_policy_document" "emr_serverless_permissions" {
  statement {
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.lakehouse["scripts"].arn}/*",
      "${aws_s3_bucket.lakehouse["bronze"].arn}/*",
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${aws_s3_bucket.lakehouse["silver"].arn}/*",
      "${aws_s3_bucket.lakehouse["gold"].arn}/*",
    ]
  }

  statement {
    effect  = "Allow"
    actions = ["s3:ListBucket"]
    resources = [
      aws_s3_bucket.lakehouse["scripts"].arn,
      aws_s3_bucket.lakehouse["bronze"].arn,
      aws_s3_bucket.lakehouse["silver"].arn,
      aws_s3_bucket.lakehouse["gold"].arn,
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "glue:GetDatabase",
      "glue:CreateDatabase",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:GetPartition",
      "glue:GetPartitions",
      "glue:CreatePartition",
      "glue:UpdatePartition",
      "glue:BatchCreatePartition",
    ]
    resources = concat(
      ["${local.glue_arn_prefix}:catalog"],
      [for db in local.db_layers : "${local.glue_arn_prefix}:database/${db}"],
      [for db in local.db_layers : "${local.glue_arn_prefix}:table/${db}/*"],
    )
  }

  statement {
    effect    = "Allow"
    actions   = ["logs:CreateLogGroup"]
    resources = [
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/emr-serverless/*",
    ]
  }

  ####---- Permite ao EMR Serverless localizar os grupos de logs.
  ####---- DescribeLogGroups é uma ação de listagem e não oferece
  ####---- restrição por ARN de um log group específico.
  statement {
    effect = "Allow"
    actions = [
      "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }

  ####---- Permite criar streams e publicar eventos somente nos
  ####---- grupos usados pelo projeto no EMR Serverless.
  ####---- DescribeLogStreams e exigido pelo EMR Serverless para
  ####---- publicar os streams SPARK_DRIVER/stdout e stderr no
  ####---- CloudWatch -- sem essa permissao, apenas o job-metadata-log
  ####---- e criado.
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:DescribeLogStreams",
      "logs:PutLogEvents",
    ]
    resources = [
      "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:/aws/emr-serverless/*:*",
    ]
  }
}  

resource "aws_iam_role_policy" "emr_serverless_job" {
  name   = "${var.app_name}-emr-serverless-job-policy"
  role   = aws_iam_role.emr_serverless_job.id
  policy = data.aws_iam_policy_document.emr_serverless_permissions.json
}


####---------------------------------------------------------------------####
####----  Role: Step Functions (orquestra bronze -> silver -> gold)  ----####
####---------------------------------------------------------------------####

data "aws_iam_policy_document" "step_functions_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "step_functions" {
  name               = "${var.app_name}-step-functions"
  assume_role_policy = data.aws_iam_policy_document.step_functions_trust.json
}

data "aws_iam_policy_document" "step_functions_permissions" {
  ####---- AWS Glue nao suporta controle por resource ARN nessas actions
  ####---- (StartJobRun, GetJobRun, etc.) -- vale para qualquer caller,
  ####---- nao so Step Functions. Por isso resource = "*". Tambem nao usa
  ####---- EventBridge managed rule (isso e exclusivo da integracao .sync
  ####---- do EMR Serverless).
  statement {
    effect = "Allow"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
    ]
    resources = ["*"]
  }

  statement {
    effect = "Allow"
    actions = [
      "emr-serverless:StartJobRun",
      "emr-serverless:GetJobRun",
      "emr-serverless:CancelJobRun",
    ]
    resources = [
      aws_emrserverless_application.spark.arn,
      "${aws_emrserverless_application.spark.arn}/jobruns/*",
    ]
  }

  ####---- Somente EMR Serverless: StartJobRun exige passar a role de
  ####---- execucao do job explicitamente. Glue nao passa role nenhuma
  ####---- no StartJobRun (a role ja esta fixada no Job criado no glue.tf).
  statement {
    effect    = "Allow"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.emr_serverless_job.arn]

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["emr-serverless.amazonaws.com"]
    }
  }

  statement {
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.pipeline_notifications.arn]
  }

  ####---- EventBridge managed rule usada pelo .sync do EMR Serverless
  ####---- para acompanhar o job de forma assincrona. Nome exato da regra
  ####---- confirmado na doc oficial da AWS.
  statement {
    effect = "Allow"
    actions = [
      "events:PutTargets",
      "events:PutRule",
      "events:DescribeRule",
    ]
    resources = [
      "arn:aws:events:${var.aws_region}:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventsForEMRServerlessJobRule",
    ]
  }
}

resource "aws_iam_role_policy" "step_functions" {
  name   = "${var.app_name}-step-functions-policy"
  role   = aws_iam_role.step_functions.id
  policy = data.aws_iam_policy_document.step_functions_permissions.json
}


####-------------------------------------------------------------####
####----  Role: EventBridge (dispara a state machine dia 5)  ----####
####-------------------------------------------------------------####

data "aws_iam_policy_document" "eventbridge_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["events.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "eventbridge_invoke_sfn" {
  name               = "${var.app_name}-eventbridge-invoke-sfn"
  assume_role_policy = data.aws_iam_policy_document.eventbridge_trust.json
}

data "aws_iam_policy_document" "eventbridge_invoke_sfn_permissions" {
  statement {
    effect    = "Allow"
    actions   = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.pipeline.arn]
  }
}

resource "aws_iam_role_policy" "eventbridge_invoke_sfn" {
  name   = "${var.app_name}-eventbridge-invoke-sfn-policy"
  role   = aws_iam_role.eventbridge_invoke_sfn.id
  policy = data.aws_iam_policy_document.eventbridge_invoke_sfn_permissions.json
}
