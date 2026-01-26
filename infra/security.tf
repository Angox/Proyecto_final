resource "aws_security_group" "lambda_sg" {
  name   = "crypto-lambda-sg"
  vpc_id = aws_vpc.main.id

  egress { # Salida a Internet (vía NAT)
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group" "neptune_sg" {
  name   = "crypto-neptune-sg"
  vpc_id = aws_vpc.main.id

  ingress { # Solo permitir entrada desde la Lambda
    from_port       = 8182
    to_port         = 8182
    protocol        = "tcp"
    security_groups = [aws_security_group.lambda_sg.id]
  }
}
