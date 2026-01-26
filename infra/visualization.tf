# 1. Rol para que el Notebook acceda a Neptune y S3
resource "aws_iam_role" "notebook_role" {
  name = "neptune_notebook_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "sagemaker.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "notebook_policy" {
  role = aws_iam_role.notebook_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = ["neptune-db:*"], # Acceso total al grafo
        Effect = "Allow",
        Resource = aws_neptune_cluster.default.arn
      },
      {
        Action = ["s3:GetObject", "s3:ListBucket"], # Por si quieres leer los CSVs manual
        Effect = "Allow",
        Resource = "*"
      }
    ]
  })
}

# 2. Grupo de seguridad para el Notebook
resource "aws_security_group" "notebook_sg" {
  name   = "crypto-notebook-sg"
  vpc_id = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# Permitir que el Notebook hable con Neptune (puerto 8182)
resource "aws_security_group_rule" "neptune_allow_notebook" {
  type                     = "ingress"
  from_port                = 8182
  to_port                  = 8182
  protocol                 = "tcp"
  security_group_id        = aws_security_group.neptune_sg.id
  source_security_group_id = aws_security_group.notebook_sg.id
}

# 3. La Instancia del Notebook (SageMaker)
resource "aws_sagemaker_notebook_instance" "graph_viz" {
  name                    = "CryptoGraphVisor"
  role_arn                = aws_iam_role.notebook_role.arn
  instance_type           = "ml.t3.medium" # Capa económica
  subnet_id               = aws_subnet.private_a.id # Debe estar en la misma VPC
  security_groups         = [aws_security_group.notebook_sg.id]
  direct_internet_access  = "Enabled" # Para instalar librerías pip si hace falta
  
  # Script de inicio para configurar la conexión a Neptune automáticamente
  lifecycle_config_name   = aws_sagemaker_notebook_instance_lifecycle_configuration.neptune_config.name

  tags = {
    Name = "Neptune Visualizer"
  }
}

# 4. Configuración automática del Notebook (Magia)
resource "aws_sagemaker_notebook_instance_lifecycle_configuration" "neptune_config" {
  name = "configure-neptune-connection"
  on_start = base64encode(<<EOF
#!/bin/bash
set -e
# Crear configuración de Neptune para el plugin graph-notebook
mkdir -p /home/ec2-user/SageMaker/Neptune
cat <<EOT > /home/ec2-user/SageMaker/Neptune/neptune_config.json
{
  "host": "${aws_neptune_cluster.default.endpoint}",
  "port": 8182,
  "auth_mode": "DEFAULT",
  "load_from_s3_arn": "",
  "ssl": true,
  "aws_region": "eu-west-1"
}
EOT
EOF
  )
}
