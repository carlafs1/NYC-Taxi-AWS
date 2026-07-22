####------------------------------------------------------------------####
####----  Step Functions: orquestra Bronze -> Silver -> Gold      ----####
####----  Primeiro estado usa JSONata (unico) so para calcular o  ----####
####----  --anos-meses = mes anterior; o resto da state machine   ----####
####----  segue em JSONPath classico (padrao mais maduro/testado  ----####
####----  para integracoes com Glue/EMR/SNS).                     ----####
####------------------------------------------------------------------####

resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.app_name}-pipeline"
  role_arn = aws_iam_role.step_functions.arn

  definition = jsonencode({
    Comment = "Bronze (Glue) -> Silver (EMR) -> Gold (EMR), com retry e notificacao SNS."
    StartAt = "DefinirPeriodo"

    States = {

      ####---- Unico estado em JSONata: calcula o mes anterior ao atual
      ####---- em formato AAAA-MM. Execucao manual pode sobrescrever
      ####---- --anos-meses no input, ignorando esse calculo.
      DefinirPeriodo = {
        Type          = "Pass"
        QueryLanguage = "JSONata"
        Output = {
          "anos_meses" = "{% $exists($states.input.anos_meses) ? $states.input.anos_meses : $fromMillis($toMillis($fromMillis($toMillis($now()), '[Y0001]-[M01]-01T00:00:00Z')) - 86400000, '[Y0001]-[M01]') %}"
        }
        Next = "Bronze"
      }

      Bronze = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.bronze.name
          Arguments = {
            "--bronze-bucket" = aws_s3_bucket.lakehouse["bronze"].id
            "--anos-meses.$"  = "$.anos_meses"
          }
        }
        ResultPath = "$.bronzeResult"
        Retry = local.job_retry_policy
        Catch = local.job_catch_to_failure
        Next  = "Silver"
      }

      Silver = {
        Type     = "Task"
        Resource = "arn:aws:states:::emr-serverless:startJobRun.sync"
        Parameters = {
          ApplicationId     = aws_emrserverless_application.spark.id
          ExecutionRoleArn  = aws_iam_role.emr_serverless_job.arn
          Name              = "${var.app_name}-silver"
          JobDriver = {
            SparkSubmit = {
              EntryPoint = "s3://${aws_s3_bucket.lakehouse["scripts"].id}/${aws_s3_object.silver_script.key}"
              "EntryPointArguments.$" = "States.Array('--bronze-bucket', '${aws_s3_bucket.lakehouse["bronze"].id}', '--silver-bucket', '${aws_s3_bucket.lakehouse["silver"].id}', '--glue-database', '${local.db_silver}', '--catalog', '${local.iceberg_catalog_name}', '--anos-meses', $.anos_meses)"
              SparkSubmitParameters = local.emr_spark_submit_params
            }
          }
          ConfigurationOverrides = {
            ApplicationConfiguration = concat(local.emr_app_config, [
              {
                Classification = "spark-defaults"
                Properties = merge(local.iceberg_catalog_conf, {
                  "spark.sql.catalog.${local.iceberg_catalog_name}.warehouse" = "s3://${aws_s3_bucket.lakehouse["silver"].id}/"
                })
              }
            ])
          }
        }
        ResultPath = "$.silverResult"
        Retry = local.job_retry_policy
        Catch = local.job_catch_to_failure
        Next  = "Gold"
      }

      Gold = {
        Type     = "Task"
        Resource = "arn:aws:states:::emr-serverless:startJobRun.sync"
        Parameters = {
          ApplicationId     = aws_emrserverless_application.spark.id
          ExecutionRoleArn  = aws_iam_role.emr_serverless_job.arn
          Name              = "${var.app_name}-gold"
          JobDriver = {
            SparkSubmit = {
              EntryPoint = "s3://${aws_s3_bucket.lakehouse["scripts"].id}/${aws_s3_object.gold_script.key}"
              "EntryPointArguments.$" = "States.Array('--silver-bucket', '${aws_s3_bucket.lakehouse["silver"].id}', '--gold-bucket', '${aws_s3_bucket.lakehouse["gold"].id}', '--silver-database', '${local.db_silver}', '--gold-database', '${local.db_gold}', '--catalog', '${local.iceberg_catalog_name}', '--anos-meses', $.anos_meses)"
              SparkSubmitParameters = local.emr_spark_submit_params
            }
          }
          ConfigurationOverrides = {
            ApplicationConfiguration = concat(local.emr_app_config, [
              {
                Classification = "spark-defaults"
                Properties = merge(local.iceberg_catalog_conf, {
                  "spark.sql.catalog.${local.iceberg_catalog_name}.warehouse" = "s3://${aws_s3_bucket.lakehouse["gold"].id}/"
                })
              }
            ])
          }
        }
        ResultPath = "$.goldResult"
        Retry = local.job_retry_policy
        Catch = local.job_catch_to_failure
        Next  = "NotificarSucesso"
      }

      NotificarSucesso = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.pipeline_notifications.arn
          Message  = "Pipeline NYC-Taxi-AWS concluido com sucesso."
        }
        End = true
      }

      NotificarFalha = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn      = aws_sns_topic.pipeline_notifications.arn
          "Message.$"   = "States.Format('Pipeline NYC-Taxi-AWS falhou. Detalhes: {}', $.error)"
        }
        Next = "Falhou"
      }

      Falhou = {
        Type  = "Fail"
        Error = "PipelineFailed"
        Cause = "Uma das etapas (Bronze/Silver/Gold) falhou apos as tentativas de retry."
      }
    }
  })
}
