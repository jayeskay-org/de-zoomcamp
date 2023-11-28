terraform {
    required_version = ">= 1.0"
    backend "local" {}
    required_providers {
        aws = {
        source  = "hashicorp/aws"
        version = "~> 5.0"
        }
    }
}

# Configure AWS provider
provider "aws" {
    region  = var.region
    profile = var.profile
}

# Data lake bucket
resource "aws_s3_bucket" "data-lake-bucket" {
    bucket = "${var.project}-${local.data_lake_bucket}"

    tags = {
        Name        = "tf-bucket"
        Environment = "dez"
    }
}

# Bucket versioning
resource "aws_s3_bucket_versioning" "bucket_versioning" {
    bucket = aws_s3_bucket.data-lake-bucket.id

    versioning_configuration {
        status = "Enabled"
    }
}

# Redshift (BigQuery equivalent)
resource "aws_redshift_cluster" "redshift-cluster" {
  cluster_identifier = "dez-redshift-cluster"
  database_name      = "dez_db"
  master_username    = "dez"
  node_type          = "dc2.large"
  cluster_type       = "single-node"

  manage_master_password = true
}
