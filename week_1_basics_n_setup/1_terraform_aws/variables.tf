locals {
  data_lake_bucket = "dez-data-lake"
}

variable "project" {
  description = "Name of project--to use as prefix for various AWS resources"
  type = string
  default = "dez2023"
}

variable "region" {
  description = "Region for AWS resources. Choose as per your location: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html."
  type = string
  default = "us-west-2"
}

variable "profile" {
    description = "Profile to use when building AWS infrastructure--should point to IAM user credentials used for DEZ organization (not root)."
    type = string
    default = "dez"
}
