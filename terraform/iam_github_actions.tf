####-------------------------------------------------------------------####
####----  Identity Provider OIDC do GitHub (ja existe na           ----####
####----  conta - criado manualmente, fora deste Terraform,        ----####
####----  para o projeto do website. E generico: nao amarra        ----####
####----  nenhum repo especifico, por isso referenciado via        ----####
####----  data source em vez de criado como resource.              ----####
####-------------------------------------------------------------------####

data "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"
}


####-------------------------------------------------------------------####
####----  Role: GitHub Actions -> deploy dos scripts (S3)          ----####
####----  Assumida via OIDC, restrita ao repo                      ----####
####----  carlafs1/NYC-Taxi-AWS, branch main. Escopo minimo:       ----####
####----  so escreve no bucket de scripts. NAO tem permissao       ----####
####----  de terraform apply -- infra continua manual,             ----####
####----  decisao deliberada (baixa frequencia de mudanca,         ----####
####----  custo de erro maior que em scripts).                     ----####
####-------------------------------------------------------------------####

data "aws_iam_policy_document" "github_actions_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [data.aws_iam_openid_connect_provider.github.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = ["repo:carlafs1/NYC-Taxi-AWS:ref:refs/heads/main"]
    }
  }
}

resource "aws_iam_role" "github_actions_deploy" {
  name               = "${var.app_name}-github-actions-deploy"
  assume_role_policy = data.aws_iam_policy_document.github_actions_trust.json
}


####-------------------------------------------------------------------####
####----  CONTEUDO da policy: so montado em memoria pelo           ----####
####----  Terraform -- nao cria nada na AWS por si so.             ----####
####-------------------------------------------------------------------####
data "aws_iam_policy_document" "github_actions_permissions" {
  statement {
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:DeleteObject",
    ]
    resources = [
      "${aws_s3_bucket.lakehouse["scripts"].arn}/*",
    ]
  }

  statement {
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.lakehouse["scripts"].arn]
  }
}


####-------------------------------------------------------------------####
####----  RECURSO que efetivamente cria a policy na AWS e          ----####
####----  anexa esta policy a role acima.                          ----####
####-------------------------------------------------------------------####
resource "aws_iam_role_policy" "github_actions_deploy" {
  name   = "${var.app_name}-github-actions-deploy-policy"
  role   = aws_iam_role.github_actions_deploy.id
  policy = data.aws_iam_policy_document.github_actions_permissions.json
}
