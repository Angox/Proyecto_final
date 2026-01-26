provider "aws" {
  region = "eu-west-1"
}

# 1. El Bucket para guardar el estado
resource "aws_s3_bucket" "tf_state" {
  bucket = "terraform-state-crypto-graph-eu-west-1" # Debe ser único globalmente
  
  lifecycle {
    prevent_destroy = true # Evita borrarlo por error
  }
}

resource "aws_s3_bucket_versioning" "tf_state_ver" {
  bucket = aws_s3_bucket.tf_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

# 2. Tabla DynamoDB para "Locking" (evita que dos personas desplieguen a la vez)
resource "aws_dynamodb_table" "tf_lock" {
  name         = "terraform-lock-table"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}
