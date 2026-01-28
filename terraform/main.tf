provider "aws" {
  region = "eu-west-1" # Cambia según tu preferencia
}

# --- 1. RED (VPC) ---
# Necesitamos VPC para que Lambda hable con Neptune
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags = { Name = "crypto-vpc" }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
}

# Subnets Públicas (Para el NAT Gateway)
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "eu-west-1a"
  map_public_ip_on_launch = true
}

# Subnets Privadas (Para Lambda y Neptune)
resource "aws_subnet" "private_a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = "eu-west-1a"
}
resource "aws_subnet" "private_b" { # Neptune necesita 2 AZs min
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.3.0/24"
  availability_zone = "eu-west-1b"
}

# NAT Gateway (Coste $$$): Permite a Lambda salir a internet
resource "aws_eip" "nat" { domain = "vpc" }
resource "aws_nat_gateway" "gw" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public.id
}

# Rutas
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
}
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.gw.id
  }
}
resource "aws_route_table_association" "pub" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}
resource "aws_route_table_association" "priv_a" {
  subnet_id      = aws_subnet.private_a.id
  route_table_id = aws_route_table.private.id
}
resource "aws_route_table_association" "priv_b" {
  subnet_id      = aws_subnet.private_b.id
  route_table_id = aws_route_table.private.id
}

# --- 2. SECURITY GROUPS ---
resource "aws_security_group" "lambda_sg" {
  vpc_id = aws_vpc.main.id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "neptune_sg" {
  vpc_id = aws_vpc.main.id
  ingress {
    from_port       = 8182
    to_port         = 8182
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda_sg.id] # Solo permite acceso desde Lambda
  }
}

# --- 3. ALMACENAMIENTO (S3 & ECR) ---
resource "aws_s3_bucket" "data_bucket" {
  bucket_prefix = "crypto-analysis-output-"
}

resource "aws_ecr_repository" "repo" {
  name                 = "crypto-analyzer"
  image_tag_mutability = "MUTABLE"
  force_delete         = true
}

# --- 4. BASE DE DATOS (Neptune) ---
resource "aws_neptune_subnet_group" "default" {
  name       = "neptune-subnet-group"
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]
}

resource "aws_neptune_cluster" "default" {
  cluster_identifier                  = "crypto-neptune-cluster"
  engine                              = "neptune"
  neptune_subnet_group_name           = aws_neptune_subnet_group.default.name
  vpc_security_group_ids              = [aws_security_group.neptune_sg.id]
  skip_final_snapshot                 = true
  apply_immediately                   = true
  
  # --- CORRECCIÓN: ---
  # HE BORRADO LA LÍNEA: instance_class = "db.t3.medium" DE AQUÍ.
  # El cluster no lleva tamaño, las instancias sí.
}

resource "aws_neptune_cluster_instance" "example" {
  count              = 1
  cluster_identifier = aws_neptune_cluster.default.id
  engine             = "neptune"
  
  # Aquí es donde debe estar:
  instance_class     = "db.t3.medium" 
  
  apply_immediately  = true
}

# --- 5. COMPUTACIÓN (Lambda) ---
resource "aws_iam_role" "lambda_exec" {
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

# Permisos para Lambda (Logs, VPC, S3)
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}
resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}
resource "aws_iam_role_policy_attachment" "lambda_s3" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# Nota: Terraform creará la lambda, pero fallará la primera vez si la imagen Docker no existe en ECR.
# Usamos un placeholder o ejecutamos el apply en dos fases (infra -> push docker -> lambda).
# Para este ejemplo, asumiremos que GitHub Actions hará el push antes de actualizar la lambda.
resource "aws_lambda_function" "analyzer" {
  function_name = "crypto-analyzer"
  role          = aws_iam_role.lambda_exec.arn
  package_type  = "Image"
  # URI dummy inicial, GitHub Actions lo actualizará
  image_uri     = "${aws_ecr_repository.repo.repository_url}:latest" 
  timeout       = 300 # 5 minutos
  memory_size   = 2048 # Pandas necesita memoria

  vpc_config {
    subnet_ids         = [aws_subnet.private_a.id]
    security_group_ids = [aws_security_group.lambda_sg.id]
  }

  environment {
    variables = {
      NEPTUNE_ENDPOINT = aws_neptune_cluster.default.endpoint
      S3_BUCKET        = aws_s3_bucket.data_bucket.id
    }
  }
  
  depends_on = [aws_nat_gateway.gw] # Asegura que la red esté lista
}

# --- 6. SCHEDULER (EventBridge / CloudWatch Events) ---

# 1. La Regla: Definimos la frecuencia
resource "aws_cloudwatch_event_rule" "every_5_minutes" {
  name                = "crypto-every-5-minutes"
  description         = "Ejecuta el analisis de cripto cada 5 minutos"
  schedule_expression = "rate(5 minutes)"
}

# 2. El Objetivo: Conectamos la regla con la Lambda
resource "aws_cloudwatch_event_target" "trigger_lambda" {
  rule      = aws_cloudwatch_event_rule.every_5_minutes.name
  target_id = "CallCryptoLambda"
  arn       = aws_lambda_function.analyzer.arn
}

# 3. El Permiso: Dejamos que EventBridge invoque la función
resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.analyzer.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_5_minutes.arn
}


# --- 7. ACCESO VISUALIZACIÓN (Bastion Host) ---

# Seguridad para el Bastion (Permitir SSH desde tu casa)
resource "aws_security_group" "bastion_sg" {
  name        = "bastion-sg"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # En prod, pon tu IP de casa aquí
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Permitir que el Bastion hable con Neptune
resource "aws_security_group_rule" "neptune_allow_bastion" {
  type                     = "ingress"
  from_port                = 8182
  to_port                  = 8182
  protocol                 = "tcp"
  security_group_id        = aws_security_group.neptune_sg.id
  source_security_group_id = aws_security_group.bastion_sg.id
}


# --- 8. NOTEBOOK PARA VISUALIZACIÓN (SageMaker) ---

# 1. Seguridad: Permitir que el Notebook hable con Neptune
resource "aws_security_group" "notebook_sg" {
  name        = "crypto-notebook-sg"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"] # Para descargar librerías de internet (vía NAT)
  }
}

# Regla: Dejar entrar al Notebook en Neptune
resource "aws_security_group_rule" "neptune_allow_notebook" {
  type                     = "ingress"
  from_port                = 8182
  to_port                  = 8182
  protocol                 = "tcp"
  security_group_id        = aws_security_group.neptune_sg.id
  source_security_group_id = aws_security_group.notebook_sg.id
}

# 2. Permisos IAM: El Notebook necesita permisos básicos
resource "aws_iam_role" "notebook_role" {
  name = "crypto_notebook_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "sagemaker.amazonaws.com" }
    }]
  })
}

# Adjuntamos política full access (para desarrollo es ok)
resource "aws_iam_role_policy_attachment" "notebook_sagemaker_full" {
  role       = aws_iam_role.notebook_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"
}

# 3. La Instancia del Notebook
resource "aws_sagemaker_notebook_instance" "neptune_notebook" {
  name                    = "crypto-graph-visualizer"
  role_arn                = aws_iam_role.notebook_role.arn
  instance_type           = "ml.t3.medium" # Capa económica
  
  # CRÍTICO: Ponerlo en la misma subred que la Lambda (Privada con NAT)
  subnet_id               = aws_subnet.private_a.id
  security_groups         = [aws_security_group.notebook_sg.id]
  
  # Acceso a internet a través del NAT Gateway (necesario para pip install)
  direct_internet_access  = "Disabled" 
}

# Output para ir directo a la consola
output "notebook_url" {
  value = "https://eu-west-1.console.aws.amazon.com/sagemaker/home?region=eu-west-1#/notebook-instances/crypto-graph-visualizer"
}


# --- 1. NUEVO BUCKET PARA LAS SEÑALES DE TRADING ---
resource "aws_s3_bucket" "signals_bucket" {
  # Nombre único (cambia los números si da error)
  bucket = "crypto-trading-signals-2026-output" 
}

