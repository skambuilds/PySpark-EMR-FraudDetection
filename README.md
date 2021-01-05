# PySpark-EMR-FraudDetection
A PySpark fraud detection project on AWS EMR with Terraform

## Introduction
In this project, we build a machine learning model to accurately predict whether the transactions in the dateset are fraudolent or not. We use Spark ML Libraries in PySpark and we execute the model script on Amazon AWS EMR. We perform the EMR cluster infrastructure creation and management via Terraform.

Terraform is the infrastructure as code tool from HashiCorp. It is a tool for building, changing, and managing infrastructure in a safe, repeatable way. Operators and Infrastructure teams can use Terraform to manage environments with a configuration language called the HashiCorp Configuration Language (HCL) for human-readable, automated deployments.

We use a dataset from [IEEE-CIS Fraud Detection competition](https://www.kaggle.com/c/ieee-fraud-detection) that is available on Kaggle. 
The purpose of the competition is predicting the probability that an online transaction is fraudulent, as denoted by the binary target isFraud.
The data is broken into two files identity and transaction, which are joined by TransactionID. Not all transactions have corresponding identity information.

Categorical Features - Transaction:

    ProductCD
    card1 - card6
    addr1, addr2
    P_emaildomain
    R_emaildomain
    M1 - M9

Categorical Features - Identity:

    DeviceType
    DeviceInfo
    id_12 - id_38

The TransactionDT feature is a timedelta from a given reference datetime (not an actual timestamp).
You can read more about the data from [this post by the competition host](https://www.kaggle.com/c/ieee-fraud-detection/discussion/101203).

## Project Realization

The realization of this project can be divided into the following phases:

- Terraform Installation
- AWS Prerequisites
- Terraform module for EMR
- Rule modification for SSH
- SSH Connection
- PySpark Script Model
- Step execution

Let's dive into them.

### Terraform Installation

To install Terraform see [the official guide](https://learn.hashicorp.com/tutorials/terraform/install-cli?in=terraform/aws-get-started) in the HashiCorp website.

### AWS Prerequisites

To replicate this project you will need:

- An [AWS account](https://aws.amazon.com/it/free/?all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc) (if you are a student like us you can use an [Educate account](https://aws.amazon.com/it/education/awseducate/))
- An [AWS EC2 key pair](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html) in order to execute the terraform module. You must provide the name of the key pair as descibed in the next section.
- An [AWS S3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/create-bucket.html) with two directories inside it:
	- **code/** - You have to upload the **/MLModel/fraud_detection_model.py** file in this directory;
	- **input/** - You have to upload the [kaggle competition data](https://www.kaggle.com/c/ieee-fraud-detection/data) csv files in this other one.
- The [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html) installed
- Your AWS credentials configured locally.

In order to perform this last point you can proceed as follows:

1. With your account created and the CLI installed configure the AWS CLI:

    $ aws configure

2. Follow the prompts to input your AWS Access Key ID and Secret Access Key, which you'll find [on this page](https://signin.aws.amazon.com/signin?redirect_uri=https%3A%2F%2Fconsole.aws.amazon.com%2Fiam%2Fhome%3Fstate%3DhashArgs%2523security_credential%26isauthcode%3Dtrue&client_id=arn%3Aaws%3Aiam%3A%3A015428540659%3Auser%2Fiam&forceMobileApp=0&code_challenge=0PnMq9kl_B7Z_WeFz9d2bJFPoYxEFMahW6Zw0shoJzo&code_challenge_method=SHA-256).

If you are using the Educate account you have also to provide the Session Token. You can find these information in the Vocareum AWS console login page by clicking on the *Account Details* button. 

The configuration process creates a file at **~/.aws/credentials** on MacOS and Linux or **%UserProfile%\.aws\credentials** on Windows, where your credentials are stored.

### Terraform module for EMR
Modules in Terraform are units of Terraform configuration managed as a group. For example, an Amazon EMR module needs configuration for an Amazon EMR cluster resource, but it also needs multiple security groups, IAM roles, and an instance profile.

We encapsulated all of the necessary configuration into a reusable module in order to manage the infrastructure complexity only one-time. You can find the Terraform code in the **Terraform/** directory of this repo. This directory has been organized as follows:

- **Terraform/test.tf** - Terraform configuration file
- **Terraform/emr-module/** - Contains the Terraform module code to create an AWS EMR cluster. The contents of this directory will be specified in the next section.
- **Terraform/configurations/** - Contains a specific configuration file for the EMR cluster.

In the list below we specify the data source and resource configurations we have used:

- module.emr.aws_emr_cluster.cluster
- module.emr.aws_iam_instance_profile.emr_ec2_instance_profile
- module.emr.aws_iam_policy_document.ec2_assume_role
- module.emr.aws_iam_policy_document.emr_assume_role
- module.emr.aws_iam_role.emr_ec2_instance_profile
- module.emr.aws_iam_role.emr_service_role
- module.emr.aws_iam_role_policy_attachment.emr_ec2_instance_profile
- module.emr.aws_iam_role_policy_attachment.emr_service_role
- module.emr.aws_security_group.emr_master
- module.emr.aws_security_group.emr_slave

#### Module Description
On a fundamental level, Terraform modules consist of inputs, outputs, and Terraform configuration. Inputs feed configuration, and when configuration gets evaluated, it computes outputs that can route into other workflows. In the following we describe the module structure and then we provide a guide to execute it.

##### Inputs
Inputs are variables we provide to a module in order for it to perform its task. The **Terraform/emr-module/variables.tf** contains the variables declaration.

##### Configuration
As inputs come in, they get layered into the data source and resource configurations listed above. Below are examples of each data source or resource type used in the EMR cluster module, along with some detail around its use. The **Terraform/emr-module/main.tf** file contains this code.

###### Identity and Access Management (IAM)
An [aws_iam_policy_document](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) is a declarative way to assemble IAM policy objects in Terraform. Here, it is being used to create a trust relationship for an IAM role such that the EC2 service can assume the associated role and make AWS API calls on our behalf.

    data "aws_iam_policy_document" "emr_assume_role" {
	  statement {
	    effect = "Allow"

	    principals {
	      type        = "Service"
	      identifiers = ["elasticmapreduce.amazonaws.com"]
	    }

	    actions = ["sts:AssumeRole"]
	  }
	}

An [aws_iam_role](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role) resource encapsulates trust relationships and permissions. For EMR, one role associates with the EMR service itself, while the other associates with the EC2 instances that make up the compute capacity for the EMR cluster. Linking the trust relationship policy above with a new IAM role is demonstrated below.

    resource "aws_iam_role" "emr_ec2_instance_profile" {
        name               = "${var.environment}JobFlowInstanceProfile"
        assume_role_policy = data.aws_iam_policy_document.ec2_assume_role.json
    }

To connect permissions with an IAM role, there is an [aws_iam_role_policy_attachment](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) resource. In this case, we’re using a canned policy (referenced via Amazon Resource Name, or ARN) supplied by AWS. This policy comes close to providing a set of permissions (S3, DynamoDB, SQS, SNS, etc.) to the role.

    resource "aws_iam_role_policy_attachment" "emr_ec2_instance_profile" {
        role       = aws_iam_role.emr_ec2_instance_profile.name
        policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
    }

Finally, there is [aws_iam_instance_profile](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_instance_profile), which is a container for an IAM role that passes itself to an EC2 instance when the instance starts. This type of resource is only necessary when associating a role with an EC2 instance, not other AWS services.

    resource "aws_iam_instance_profile" "emr_ec2_instance_profile" {
        name = aws_iam_role.emr_ec2_instance_profile.name
        role = aws_iam_role.emr_ec2_instance_profile.name
    }


###### Security groups
Security groups house firewall rules for compute resources in a cluster. This module creates two security groups without rules. Rules are automatically populated by the EMR service (to support cross-node communication), but you can also grab a handle to the security group via its ID and add more rules through the [aws_security_group_rule](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) resource.

    resource "aws_security_group" "emr_master" {
        vpc_id                 = "${var.vpc_id}"
        revoke_rules_on_delete = true

        tags = {
            Name        = "sg${var.name}Master"
            Project     = "${var.project}"
            Environment = "${var.environment}"
        }
    }
    
A special thing to note here is the usage of revoke_rules_on_delete. This setting ensures that all the remaining rules contained inside a security group are removed before deletion. This is important because EMR creates cyclic security group rules (rules with other security groups referenced), which prevent security groups from deleting gracefully.

###### EMR Cluster
Last, but not least, is the [aws_emr_cluster](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/emr_cluster) resource. As you can see, almost all the module variables are being used in this resource.

    resource "aws_emr_cluster" "cluster" {
      name           = "${var.name}"
      release_label  = "${var.release_label}"
      applications   = "${var.applications}"
      configurations = "${var.configurations}"
    
      ec2_attributes {
        key_name                          = "${var.key_name}"
        subnet_id                         = "${var.subnet_id}"
        emr_managed_master_security_group = "${aws_security_group.emr_master.id}"
        emr_managed_slave_security_group  = "${aws_security_group.emr_slave.id}"
        instance_profile                  = "${aws_iam_instance_profile.emr_ec2_instance_profile.arn}"
      }
    
      master_instance_group {
        instance_type = "${var.instance_type}"
        instance_count = "${var.master_instance_count}"
      }    
    
      core_instance_group {
        instance_type = "${var.instance_type}"
        instance_count = "${var.core_instance_count}"
      }
    
      bootstrap_action {
        path = "${var.bootstrap_uri}"
        name = "${var.bootstrap_name}"
        args = "${var.bootstrap_args}"
      }
    
      log_uri      = "${var.log_uri}"
      service_role = "${aws_iam_role.emr_service_role.arn}"
    
      tags = {
        Name        = "${var.name}"
        Project     = "${var.project}"
        Environment = "${var.environment}"
      }
    }

The MASTER instance group contains the head node in your cluster, or a group of head nodes with one elected leader via a consensus process. CORE usually contains nodes responsible for Hadoop Distributed File System (HDFS) storage, but more generally applies to instances you expect to stick around for the entire lifetime of your cluster. TASK instance groups are meant to be more ephemeral, so they don’t contain HDFS data, and by default don’t accommodate YARN (resource manager for Hadoop clusters) application master tasks.

##### Outputs
As configuration gets evaluated, resources compute values, and those values can be emitted from the module as outputs. These are typically IDs or DNS endpoints for resources within the module. In this case, we emit the cluster ID so that you can use it as an argument to out-of-band API calls, security group IDs so that you can add extra security group rules, and the head node FQDN so that you can use SSH to run commands or check status. The **Terraform/emr-module/outputs.tf** file contains this setting.

#### Module Configuration
In this section we clarify how to configure the module. The **Terraform/test.tf** file contains the Terraform configuration. We describe its content in the following.

First of all we want to make sure that our AWS provider is properly configured. If you make use of named AWS credential profiles, then all you need to set in the provider block is a version and a region as shown below. Furthermore, exporting AWS_PROFILE with the desired AWS credential profile name before invoking Terraform ensures that the underlying AWS SDK uses the right set of credentials.

    provider "aws" {
        version = "3.21.0"
        region  = "us-east-1"
    }

From there, we create a module block in order to call the emr module we described previously. You have to update this module block with your own AWS parameters. The source argument has been set to the path of the emr module code which you can find in the **Terraform/emr-module/** directory of this repo.  

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

      log_uri     = "logs-bucket-path"
      project     = "FraudDetection"
      environment = "Test"
    }

More specifically you must provide: 

- `name` - Name of the EMR cluster
- `vpc_id` - ID of VPC meant to hold the cluster
- `release_label` - EMR release version to use (default: `emr-5.32.0`)
- `applications` - A list of EMR release applications (default: `["Spark"]`)
- `configurations` - JSON array of EMR application configurations
- `key_name` - EC2 Key pair name
- `subnet_id` - Subnet used to house the EMR nodes
- `instance_groups` - List of objects for each desired instance group (see secti on below)
- `bootstrap_name` - Name for the bootstrap action
- `bootstrap_uri` - S3 URI for the bootstrap action script
- `bootstrap_args` - A list of arguments to the bootstrap action script (default: `[]`)
- `log_uri` - S3 URI of the EMR log destination, must begin with `s3://` and end with trailing slashes
- `project` - Name of project this cluster is for (default: `Unknown`)
- `environment` - Name of environment this cluster is targeting (default: `Unknown`)

Besides the EMR module, we also make use of a [template_file](https://registry.terraform.io/providers/hashicorp/template/latest/docs/data-sources/file) resource to pull in a file containing the JSON required for [EMR cluster configuration](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html). Once retrieved, this resource renders the file contents (we have no variables defined, so no actual templating is going to occur) into the value of the configurations module variable. You can add your configuration rules simply updating the **Terraform/configurations/default.json** file.

    provider "template" {
        version = "2.2.0"
    }

    data "template_file" "emr_configurations" {
        template = file("configurations/default.json")
    }
    
Finally, we want to add a few rules to the cluster security groups. For the head node security group, we want to open port 22 for SSH, and for both security groups we want to allow all egress traffic. As you can see, we’re able to do this via the available module.emr.master_security_group_id and module.emr.slave_security_group_id outputs.

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

#### Module Execution
Now we can use Terraform to create and destroy the cluster. Clone this repo and navigate into the **Terraform/** directory. Terraform loads all files in the working directory that end in **.tf**, in our case the **test.tf** configuration file.

	$ cd Terraform/

##### Initialize the directory
When you create a new configuration — or check out an existing configuration from version control — you need to initialize the directory with `terraform init`.

Terraform uses a plugin-based architecture to support hundreds of infrastructure and service providers. Initializing a configuration directory downloads and installs providers used in the configuration, which in this case is the `aws` provider. Subsequent commands will use local settings and data during initialization.

	$ terraform init

Terraform downloads the aws provider and installs it in a hidden subdirectory of the current working directory. The output shows which version of the plugin was installed.

##### Format and validate the configuration

The `terraform fmt` command automatically updates configurations in the current directory for easy readability and consistency.

	$ terraform fmt

Terraform will return the names of the files it formatted. In this case, the configuration file was already formatted correctly, so Terraform won't return any file names.

If you are copying configuration snippets or just want to make sure your configuration is syntactically valid and internally consistent, the built in `terraform validate` command will check and report errors within modules, attribute names, and value types.

	$ terraform validate

If your configuration is valid, Terraform will return a success message.

##### Create infrastructure

First, we assemble a plan with the available configuration. This gives Terraform an opportunity to inspect the state of the world and determine exactly what it needs to do to make the world match our desired configuration.

	$ terraform plan -out=test.tfplan
	
From here, we inspect the command output (the infrastructure equivalent of a diff) of all the data sources and resources Terraform plans to create, modify, or destroy. If that looks good, the next step is to apply the plan.

	$ terraform apply test.tfplan

	...

	Apply complete! Resources: 11 added, 0 changed, 0 destroyed.
	
##### Monitoring the Step Execution

You can inspect the fraud detection model execution via the AWS Console. Just login and select the EMR service. Then click on the active cluster with the name you insert in the **test.tf** terraform configuration file. Finally go into the step tab to control its status.

##### Destroy infrastructure

After the step execution has been completed we want to clean up all the AWS resources. This can be performed with the following command:

	$ terraform destroy
