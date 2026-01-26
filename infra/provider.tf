provider "aws" {
  region = "eu-west-1"
}

terraform {
  backend "s3" {
    bucket = "terraform-state-crypto-graph-eu-west-1" # CAMBIA ESTO por un nombre único
    key    = "state/terraform.tfstate"
    region = "eu-west-1"
  }
}
