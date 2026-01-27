terraform {
  backend "s3" {
    bucket = "mi-estado-terraform-crypto-xyz" # El nombre que creaste arriba
    key    = "crypto-app/terraform.tfstate"
    region = "us-east-1"
  }
}
