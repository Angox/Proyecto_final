# Archivo ZIP (se crea dinámicamente en terraform apply si existe la carpeta)
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "../src/lambda_function.py"
  output_path = "lambda_function.zip"
}

# IAM Role para Lambda
resource "aws_iam_role" "lambda_role" {
  name = "crypto_lambda_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

# Permisos Lambda (Logs, VPC, S3, Neptune)
resource "aws_iam_role_policy" "lambda_policy" {
  role = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
        Effect = "Allow",
        Resource = [aws_s3_bucket.data_bucket.arn, "${aws_s3_bucket.data_bucket.arn}/*"]
      },
      {
        Action = ["neptune-db:*"],
        Effect = "Allow",
        Resource = "*" # En prod restringir al ARN del cluster
      },
      {
        Action = ["ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DeleteNetworkInterface"],
        Effect = "Allow",
        Resource = "*"
      },
      {
        Action = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
        Effect = "Allow",
        Resource = "*"
      }
    ]
  })
}

# La Función Lambda
resource "aws_lambda_function" "processor" {
  filename         = "lambda_function.zip"
  function_name    = "CryptoGraphProcessor"
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.9"
  timeout          = 900 # 15 minutos (máximo)
  memory_size      = 2048 # 2GB RAM para pandas

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id]
    security_group_ids = [aws_security_group.lambda_sg.id]
  }

  environment {
    variables = {
      S3_BUCKET_NAME          = aws_s3_bucket.data_bucket.id
      NEPTUNE_ENDPOINT        = aws_neptune_cluster.default.endpoint
      NEPTUNE_LOADER_IAM_ROLE = aws_iam_role.neptune_s3_role.arn
    }
  }

  # Layer para Pandas/Numpy (AWS Data Wrangler) - Región EU-WEST-1
  layers = ["arn:aws:lambda:eu-west-1:336392948345:layer:AWSSDKPandas-Python39:25"]
}

# EventBridge Scheduler (Cada 5 minutos)
resource "aws_cloudwatch_event_rule" "every_5_minutes" {
  name                = "every-5-minutes"
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "check_foo" {
  rule      = aws_cloudwatch_event_rule.every_5_minutes.name
  target_id = "lambda"
  arn       = aws_lambda_function.processor.arn
}

resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.processor.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_5_minutes.arn
}
