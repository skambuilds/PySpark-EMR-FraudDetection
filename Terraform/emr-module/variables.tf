variable "project" {
  default = "Unknown"
}

variable "environment" {
  default = "Unknown"
}

variable "name" {}

variable "vpc_id" {}

variable "release_label" {
  default = "emr-5.32.0"
}

variable "applications" {
  default = ["Hive","Spark","Livy","JupyterEnterpriseGateway"]
  type    = list(string)
}

variable "configurations" {}

variable "key_name" {}

variable "subnet_id" {}

variable "instance_type" {
  default = "m5.xlarge"
}

variable "master_instance_count" {
  default = 1
}

variable "core_instance_count" {
  default = 2
}

variable "bootstrap_name" {}

variable "bootstrap_uri" {}

variable "bootstrap_args" {
  default = []
  type    = list(string)
}

variable "log_uri" {}


variable "action_on_failure" {
  default = "CONTINUE"
}

variable "step_name" {
  default = "FraudDetectionModel"
}

variable "step_jar_path" {
  default = "command-runner.jar"
}

variable "step_args" {
  default = []
  type    = list(string)
}