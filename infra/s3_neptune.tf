# Bucket S3 de Datos
resource "aws_s3_bucket" "data_bucket" {
  bucket_prefix = "crypto-graph-data-"
}

# IAM Role para que Neptune lea de S3
resource "aws_iam_role" "neptune_s3_role" {
  name = "neptune-s3-access-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "rds.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "neptune_s3_attach" {
  role       = aws_iam_role.neptune_s3_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

# Neptune Cluster
resource "aws_neptune_subnet_group" "default" {
  name       = "main"
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]
}

resource "aws_neptune_cluster" "default" {
  cluster_identifier                  = "crypto-graph-cluster"
  engine                              = "neptune"
  neptune_subnet_group_name           = aws_neptune_subnet_group.default.name
  vpc_security_group_ids              = [aws_security_group.neptune_sg.id]
  iam_roles                           = [aws_iam_role.neptune_s3_role.arn]
  skip_final_snapshot                 = true
  apply_immediately                   = true
}

resource "aws_neptune_cluster_instance" "instance" {
  cluster_identifier = aws_neptune_cluster.default.id
  instance_class     = "db.t3.medium" # Capa "barata"
  engine             = "neptune"
}
