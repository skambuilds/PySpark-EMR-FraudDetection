provider "aws" {
  version = "3.21.0"
  region  = "us-east-1"
}

module "emr" {
  source = ".emr-module/"

  name          = "cluster-name"
  vpc_id        = "vpc-id"
  release_label = "emr-5.32.0"

  applications = [
    "Hive",
    "Spark",
    "Livy",
    "JupyterEnterpriseGateway",
  ]

  configurations        = data.template_file.emr_configurations.rendered
  key_name              = "key-pair-name"
  subnet_id             = "subnet-id"
  instance_type         = "m5.xlarge"
  master_instance_count = "1"
  core_instance_count   = "2"

  bootstrap_name = "runif"
  bootstrap_uri  = "s3://elasticmapreduce/bootstrap-actions/run-if"
  bootstrap_args = ["instance.isMaster=true", "echo running on master node"]

  log_uri     = "s3://your-bucket-name/logs/"
  project     = "FraudDetection"
  environment = "Test"

  action_on_failure = "CONTINUE"
  step_name         = "FraudDetectionModel"
  step_jar_path     = "command-runner.jar"

  step_args = [
    "spark-submit",
    "--deploy-mode",
    "client",
    "--master",
    "yarn",
    "--conf",
    "spark.yarn.submit.waitAppCompletion=true",
    "--executor-memory",
    "2g",
    "s3://your-bucket-name/code/fraud_detection_model.py"
  ]
}

provider "template" {
  version = "2.2.0"
}

data "template_file" "emr_configurations" {
  template = file("configurations/default.json")
}

resource "aws_security_group_rule" "emr_master_ssh_ingress" {
  type              = "ingress"
  from_port         = "22"
  to_port           = "22"
  protocol          = "tcp"
  cidr_blocks       = ["1.2.3.4/32"]
  security_group_id = module.emr.master_security_group_id
}

resource "aws_security_group_rule" "emr_master_all_egress" {
  type              = "egress"
  from_port         = "0"
  to_port           = "0"
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = module.emr.master_security_group_id
}

resource "aws_security_group_rule" "emr_slave_all_egress" {
  type              = "egress"
  from_port         = "0"
  to_port           = "0"
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = module.emr.slave_security_group_id
}
