![Fraud Detection](/repo-image.png)

# PySpark-EMR-FraudDetection
A PySpark fraud detection project on AWS EMR with Terraform

## Project Summary

This document has been split into three main parts:

- [Introduction](https://github.com/skambuilds/PySpark-EMR-FraudDetection#introduction)
- [How to Replicate this Project](https://github.com/skambuilds/PySpark-EMR-FraudDetection/blob/main/README.md#how-to-replicate-this-project)
	- [Step 1: Terraform Installation](https://github.com/skambuilds/PySpark-EMR-FraudDetection#step-1-terraform-installation)
	- [Step 2: AWS Prerequisites](https://github.com/skambuilds/PySpark-EMR-FraudDetection#step-2-aws-prerequisites)
	- [Step 3: Setting Up the Bucket](https://github.com/skambuilds/PySpark-EMR-FraudDetection#step-3-setting-up-the-bucket)
	- [Step 4: Module Configuration](https://github.com/skambuilds/PySpark-EMR-FraudDetection#step-4-module-configuration)
	- [Step 5: Module Execution](https://github.com/skambuilds/PySpark-EMR-FraudDetection#step-5-module-execution)
- [In-depth Project Information](https://github.com/skambuilds/PySpark-EMR-FraudDetection#in-depth-project-information)
	- [Terraform EMR Module](https://github.com/skambuilds/PySpark-EMR-FraudDetection#terraform-emr-module)
	- [Fraud Detection Model](https://github.com/skambuilds/PySpark-EMR-FraudDetection#fraud-detection-model)
	- [Results and Conclusions](https://github.com/skambuilds/PySpark-EMR-FraudDetection#results-and-conclusions)
	- [References](https://github.com/skambuilds/PySpark-EMR-FraudDetection#references)

In the introduction we provide a brief overview of the context we are investigating. After that, we proceed with a step by step guide to replicate this project on your machine. Finally we explain more deeply the organization of the terraform module code and the design choices of our fraud detection algorithm.

Let's dive into them.

## Introduction
In this project, we build a machine learning model to predict whether the transactions in the dateset are fraudolent or not. We use Spark ML Libraries in PySpark and we execute the model script on Amazon AWS EMR. We perform the EMR cluster infrastructure creation and management via Terraform.

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

## How to replicate this project

To replicate this project you will need to accomplish the following steps:

### Step 1: Terraform Installation

First of all you need to install Terraform. Please refer to [the official guide](https://learn.hashicorp.com/tutorials/terraform/install-cli?in=terraform/aws-get-started) in the HashiCorp website.

### Step 2: AWS Prerequisites

Here we specify the AWS settings you need to perform:

1. Create an **AWS account**. Simply visit this [page](https://aws.amazon.com/it/free/?all-free-tier.sort-by=item.additionalFields.SortRank&all-free-tier.sort-order=asc) to create a free account. If you are a student like us you can apply for an [Educate account](https://aws.amazon.com/it/education/awseducate/).
2. Create an **AWS EC2 key pair**. Follow the [official guide](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html#prepare-key-pair) to perform this task. You must provide the name of the key pair in order to execute the terraform module as descibed in the next section.
3. Create an **AWS S3 bucket**. To accomplish this step please refer to this [guide](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/create-bucket.html).
	- Once you created the bucket, you should see its name on the Buckets list - click on it to enter the bucket page.
  	- From the bucket page you have to create the following list of directories:
		- **code/** - Will contain the PySpark fraud detection algorithm
		- **input/** - Will contain the csv files of the kaggle competition
		- **logs/** - This will be the EMR cluster log destination
		
		Just click on the "Create folder" button, then assign a name to the new folder and leave all the rest untouched.
4. Download and install the **AWS CLI Ver. 2**. To complete this task please refer to the [official guide](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html).
5. Configure your AWS credentials locally:
	- Reach for the prompt/console on your system where you installed the AWS CLI
	- Type the following command:
	
		$ aws configure
	
	- Follow the prompts to input your AWS Access Key ID and Secret Access Key, which you'll find [on this page](https://signin.aws.amazon.com/signin?redirect_uri=https%3A%2F%2Fconsole.aws.amazon.com%2Fiam%2Fhome%3Fstate%3DhashArgs%2523security_credential%26isauthcode%3Dtrue&client_id=arn%3Aaws%3Aiam%3A%3A015428540659%3Auser%2Fiam&forceMobileApp=0&code_challenge=0PnMq9kl_B7Z_WeFz9d2bJFPoYxEFMahW6Zw0shoJzo&code_challenge_method=SHA-256).

	If you are using the Educate account you have also to provide the Session Token. You can find these information in the Vocareum AWS console login page by clicking on the *Account Details* button. 

	The configuration process creates a file at **~/.aws/credentials** on MacOS and Linux or **%UserProfile%\.aws\credentials** on Windows, where your credentials are stored.

### Step 3: Setting Up the Bucket

1. Clone this repository on your local machine.
2. Open your local copy of the [**ModelCode/fraud_detection_model.py**](ModelCode/fraud_detection_model.py) file with a text editor and insert the name of the bucket you created previously in the following variable:
	
		bucket_name = 's3://your-bucket-name'
3. Login into your [AWS Console](https://aws.amazon.com/it/console/) and choose the S3 service using the search tool. Now select your bucket and navigate into the **code/** directory. Simply click on the *Upload* button on the top right, then click the *Add files* button and finally navigate on your filesystem and select the [**ModelCode/fraud_detection_model.py**](ModelCode/fraud_detection_model.py) file you have just modified.
4. Create your [Kaggle Account](https://www.kaggle.com/)
5. Download the kaggle competition dataset by clicking on the following four csv files and then clicking on the download button located on the top right corner of the dataset description table:
	- [train_transaction.csv](https://www.kaggle.com/c/ieee-fraud-detection/data?select=train_transaction.csv)
	- [train_identity.csv](https://www.kaggle.com/c/ieee-fraud-detection/data?select=train_identity.csv)
	- [test_transaction.csv](https://www.kaggle.com/c/ieee-fraud-detection/data?select=test_transaction.csv)
	- [test_identity.csv](https://www.kaggle.com/c/ieee-fraud-detection/data?select=test_identity.csv)
5. Now go back to your AWS Console, select the **input/** directory of your bucket and upload here the csv files you have just downloaded.

### Step 4: Module Configuration
In this section we clarify how to configure the module. The [**Terraform/test.tf**](Terraform/test.tf) file contains the Terraform configuration. We describe its content in the following.

First of all we want to make sure that our AWS provider is properly configured. If you make use of named AWS credential profiles, then all you need to set in the provider block is a version and a region as shown below. We have already perform this task for you adding the following code snippet:

    provider "aws" {
        version = "3.21.0"
        region  = "us-east-1"
    }

Furthermore, exporting AWS_PROFILE with the desired AWS credential profile name before invoking Terraform ensures that the AWS SDK uses the right set of credentials.

From there, we create a module block in order to call the emr module. The source argument has been set to the path of the emr module code which you can find in the [**Terraform/emr-module/**](Terraform/emr-module/) directory of this repo. The emr module code has been described in detail in the [in-depth information section](https://github.com/skambuilds/PySpark-EMR-FraudDetection#terraform-emr-module) of this document.

    module "emr" {
      source = "./emr-module/"

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
      step_name = "FraudDetectionModel"
      step_jar_path = "command-runner.jar"

      step_args = [
    	"spark-submit",
    	"--deploy-mode",
    	"client",
    	"--master",
    	"yarn",
    	"--conf",
    	"spark.yarn.submit.waitAppCompletion=true",    	
    	"s3://your-bucket-name/code/fraud_detection_model.py"
      ]
    }

You have to update this module block with your own AWS parameters. Open your local copy of the [**Terraform/test.tf**](Terraform/test.tf) file with a text editor and provide the following values:

- `name` - A name for your EMR cluster
- `vpc_id` - ID of VPC meant to hold the cluster
	- In order to retrieve this information just login into your AWS Console and search for the VPC Dashboard using the search tool. Then go to *Your VPC* and perform copy and paste on the "VPC ID" value of an already available VPC or follow this [guide](https://docs.aws.amazon.com/directoryservice/latest/admin-guide/gsg_create_vpc.html#create_vpc) to create a new VPC.
- `key_name` - EC2 Key pair name (you have to insert the key pair name you created previously)
- `subnet_id` - Subnet used to house the EMR nodes
	- In order to retrieve this information just login into your AWS Console and search for the VPC Dashboard using the search tool. Then go to *Subnets* and perform copy and paste on the "Subnet ID" value of a subnet which is related to the VPC you have chosen previously or follow this [guide](https://docs.aws.amazon.com/directoryservice/latest/admin-guide/gsg_create_vpc.html#add_subnet) to create a new Subnet related to the VPC you have created previously.
- `log_uri` - S3 URI of the EMR log destination (you just have to put "your-bucket-name" in the path)
- `step_args` - List of command line arguments passed to the JAR file's main function when executed. In this case we use the spark-submit in order to execute the fraud detection model algorithm (you just have to put "your-bucket-name" in the s3 model code path)

For the following ones we have already set up the right values for you:
- `release_label` - EMR release version to use
- `applications` - A list of EMR release applications
- `configurations` - JSON array of EMR application configurations
- `instance_type` - Instance type for the master and core instance groups
- `master_instance_count` - Number of master instances
- `core_instance_count` - Number of core instances
- `bootstrap_name` - Name for the bootstrap action
- `bootstrap_uri` - S3 URI for the bootstrap action script
- `bootstrap_args` - A list of arguments to the bootstrap action script
- `project` - Name of project this cluster is for
- `environment` - Name of environment this cluster is targeting
- `action_on_failure` - The action to take if the step fails. Valid values: `TERMINATE_JOB_FLOW`, `TERMINATE_CLUSTER`, `CANCEL_AND_WAIT`, and `CONTINUE`
- `step_name` - The name of the step
- `step_jar_path` - Path to a JAR file run during the step

### Step 5: Module Execution
Now you can use Terraform to create and destroy the cluster. The cluster creation includes a step phase which performs the fraud detection model execution. 

First of all you have to navigate into the **Terraform/** directory of your local copy of this repository simply typing the following command on your system prompt/console:

	$ cd ~/PySpark-EMR-FraudDetection/Terraform/

#### Initialize the directory
Terraform loads all files in the working directory that end in **.tf**, in our case the **test.tf** configuration file. In order to complete this task you need to initialize the directory with the following command:

	$ terraform init

Terraform uses a plugin-based architecture to support hundreds of infrastructure and service providers. Initializing a configuration directory downloads and installs providers used in the configuration, which in this case is the `aws` provider. The output shows which version of the plugin was installed. Subsequent commands will use local settings and data during initialization.

#### Format and validate the configuration

Now execute the following command which automatically updates configurations in the current directory for easy readability and consistency.

	$ terraform fmt

Terraform will return the names of the files it formatted. In this case, the configuration file was already formatted correctly, so Terraform won't return any file names.

If you are copying configuration snippets or just want to make sure your configuration is syntactically valid and internally consistent, the following command will check and report errors within modules, attribute names, and value types.

	$ terraform validate

If your configuration is valid, Terraform will return a success message.

#### Create Infrastructure

First, you have to assemble a plan with the available configuration. This gives Terraform an opportunity to inspect the state of your AWS account and determine exactly what it needs to do to make it match our desired configuration:

	$ terraform plan -out=test.tfplan
	
From here, you can inspect the command output of all the data sources and resources Terraform plans to create, modify, or destroy. Now the next step is to apply the plan:

	$ terraform apply test.tfplan

	...

	Apply complete! Resources: 11 added, 0 changed, 0 destroyed.
	
#### Monitoring the Step Execution

You can inspect the fraud detection model execution via the AWS Console. Just login and select the EMR service. Then click on the active cluster name you provide in the **test.tf** terraform configuration file. Finally go into the step tab to control its status. You can inspect the model result by clicking on *view logs* and selecting the **stdout** log file.

#### Destroy Infrastructure

After the step execution has been completed we want to clean up all the AWS resources. This can be performed with the following command:

	$ terraform destroy

## In-depth Project Information

In this section we provide a detail description of the Terraform EMR Module and the Fraud Detection Model algorithm with a consequent examination of the obteined results.

### Terraform EMR Module
Modules in Terraform are units of Terraform configuration managed as a group. For example, an Amazon EMR module needs configuration for an Amazon EMR cluster resource, but it also needs multiple security groups, IAM roles, and an instance profile.

We encapsulated all of the necessary configuration into a reusable module in order to manage the infrastructure complexity only one-time. You can find the Terraform code in the [**Terraform/**](Terraform/) directory of this repo. This directory has been organized as follows:

- [**Terraform/test.tf**](Terraform/test.tf) - Terraform configuration file which you modified following the above guidelines
- [**Terraform/emr-module/**](Terraform/emr-module/) - Contains the Terraform module code to create an AWS EMR cluster. The contents of this directory will be specified in the next section.
- [**Terraform/configurations/**](Terraform/configurations/) - Contains a specific configuration file for the EMR cluster.

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

On a fundamental level, Terraform modules consist of inputs, outputs, and Terraform configuration. Inputs feed configuration, and when configuration gets evaluated, it computes outputs that can route into other workflows. In the following we describe the module structure and then we provide a guide to execute it.

#### Inputs
Inputs are variables we provide to a module in order for it to perform its task. The [**Terraform/emr-module/variables.tf**](Terraform/emr-module/variables.tf) contains the variables declaration.

#### Configuration
As inputs come in, they get layered into the data source and resource configurations listed above. Below are examples of each data source or resource type used in the EMR cluster module, along with some detail around its use. The [**Terraform/emr-module/main.tf**](Terraform/emr-module/main.tf) file contains this code.

##### Identity and Access Management (IAM)
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


##### Security groups
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

##### EMR Cluster
Last, but not least, is the [aws_emr_cluster](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/emr_cluster) resource. As you can see, almost all the module variables are being used in this resource. Here we have also specified a step section in order to execute the PySpark script which contains our fraud detection model algorithm.

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
      
      step {
    	action_on_failure = "${var.action_on_failure}"
    	name   = "${var.step_name}"
	
    	hadoop_jar_step {
      	  jar  = "${var.step_jar_path}"
      	  args = "${var.step_args}"
    	}
      }
    }

The MASTER instance group contains the head node in your cluster, or a group of head nodes with one elected leader via a consensus process. CORE usually contains nodes responsible for Hadoop Distributed File System (HDFS) storage, but more generally applies to instances you expect to stick around for the entire lifetime of your cluster.

#### Outputs
As configuration gets evaluated, resources compute values, and those values can be emitted from the module as outputs. These are typically IDs or DNS endpoints for resources within the module. In this case, we emit the cluster ID so that you can use it as an argument to out-of-band API calls, security group IDs so that you can add extra security group rules, and the head node FQDN so that you can use SSH to run commands or check status. The [**Terraform/emr-module/outputs.tf**](Terraform/emr-module/outputs.tf) file contains this setting.

### Fraud Detection Model

In this section we are going to describe the structure of our fraud detection algorithm. The code has been organized into five main parts:
1. Competition Data Loading from the S3 Bucket
2. Feature Selection
3. Feature Engineering
4. Model Training and Execution
5. Model Evaluation

#### Competition Data Loading from the S3 Bucket
In this phase we simply load the data csv files from the S3 bucket and we join the *transaction dataset* with the *identity dataset*.

	train_ts = spark.read.csv(train_ts_location, header = True, inferSchema = True)
	train_id = spark.read.csv(train_id_location, header = True, inferSchema = True)
	train_df = train_ts.join(train_id, "TransactionID", how='left')
	
	test_ts = spark.read.csv(test_ts_location, header = True, inferSchema = True)
	test_id = spark.read.csv(test_id_location, header = True, inferSchema = True)
	test_df = test_ts.join(test_id, "TransactionID", how='left')

#### Feature Selection
Exploring the dataset we noticed that there were so many NAN values, consequently we take inspiration from this [Exploratory Data Analysis](https://www.kaggle.com/cdeotte/eda-for-columns-v-and-id) to perform the feature selection. The authors analyzed all the columns of train_transaction.csv to determine which columns are related by the number of NANs present. They see that D1 relates to a subset of V281 thru V315, and D11 relates to V1 thru V11. They also find groups of Vs with similar NAN structure. And they see that M1, M2, M3 related and M8, M9 related.

The V columns appear to be redundant and correlated. Therefore for each block of V columns with similar NAN structure, we could find subsets within the block that are correlated. Then we can replace the entire block with one column from each subset.

For example in block V1-V11, we see that the subsets [[1],[2,3],[4,5],[6,7],[8,9],[10,11]] exist and we can choose [1, 3, 4, 6, 8, 11] to represent the V1-V11 block without losing that much information. Here below the sets of V columns we decide to preserve:

	v =  [1, 3, 4, 6, 8, 11]
	v += [13, 14, 17, 20, 23, 26, 27, 30]
	v += [36, 37, 40, 41, 44, 47, 48]
	v += [54, 56, 59, 62, 65, 67, 68, 70]
	v += [76, 78, 80, 82, 86, 88, 89, 91]
	
	v += [96, 98, 99, 104] 
	v += [107, 108, 111, 115, 117, 120, 121, 123] 
	v += [124, 127, 129, 130, 136]
	
	v += [138, 139, 142, 147, 156, 162]
	v += [165, 160, 166]
	v += [178, 176, 173, 182]
	v += [187, 203, 205, 207, 215]
	v += [169, 171, 175, 180, 185, 188, 198, 210, 209]
	v += [218, 223, 224, 226, 228, 229, 235]
	v += [240, 258, 257, 253, 252, 260, 261]
	v += [264, 266, 267, 274, 277]
	v += [220, 221, 234, 238, 250, 271]
	
	v += [294, 284, 285, 286, 291, 297] 
	v += [303, 305, 307, 309, 310, 320] 
	v += [281, 283, 289, 296, 301, 314]
	v += [332, 325, 335, 338] 
	cols += ['V'+str(x) for x in v]

#### Feature Engineering
The preliminary step we made in order to complete this phase was a classification of the dataset columns based on their data type. We built two main sets:
1. Categorical columns
2. Non categorical columns
	
Then we performed the following tasks:
- String casting of the categorical columns values

		for col in categoricalColumns:
			train_transaction = train_transaction.withColumn(col, train_transaction[col].cast(StringType()))
    
- Null values substitution for categorical and non categorical columns

		for _col in categoricalColumns:
			train_transaction = train_transaction.withColumn(_col, when(train_transaction[_col].isNull(), 'none').otherwise(train_transaction[_col]))
		
		for _col in nonCategoricalColumns:
			train_transaction = train_transaction.withColumn(_col, when(train_transaction[_col].isNull(), 0).otherwise(train_transaction[_col]))
			
- Target variable (isFraud) values convertion from 0 to “No”, and from 1 to “Yes”

		trgStrConv_udf = udf(lambda val: "no" if val==0 else "yes", StringType())
		train_transaction=train_transaction.withColumn("isFraud", trgStrConv_udf('isFraud'))

Finally, we applied five important tranformers/estimators from the PySpark.ml library in order to perform the hot encoding:
1. *StringIndexer* - Converts a single column to an index column. It simply replaces each category with a number. The most frequent values gets the first index value, which is (0.0), while the most rare ones take the biggest index value.
2. *OneHotEncoderEstimator* - Converts categorical variables into binary SparseVectors. With OneHotEncoder, we create a dummy variable for each value in categorical columns and give it a value 1 or 0.
3. *VectorAssembler* - Transforms all features into a vector.
4. *LabelIndexer* - Converts label into label indices using the StringIndexer. “No” has been assigned with the value "0.0", "yes" is assigned with the value "1.0".
5. *StandardScaler* - Standardization of a dataset is a common requirement for many machine learning estimators: they might behave badly if the individual features do not look like more or less normally distributed data (e.g. Gaussian with 0 mean and unit variance). StandardScaler standardizes features by removing the mean and scaling to unit variance.

After applying them, the data will be ready to build the model.	

##### Model Pipeline
We use a pipeline to chain multiple Transformers and Estimators together to specify our machine learning workflow. The Pipeline’s stages are specified as an ordered array.
First of all we determine categorical columns. Then, it indexes each categorical column using the StringIndexer. After that, it converts the indexed categories into one-hot encoded variables. The resulting output has the binary vectors appended to the end of each row. We use the StringIndexer again to encode our labels to label indices. Next, we use the VectorAssembler to combine all the feature columns into a single vector column. As a final step, we use StandardScaler to distribute our features normally.

	stages = []
	
	for categoricalCol in categoricalColumns:
        	stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
        	encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
        	stages += [stringIndexer, encoder]
		
	label_stringIdx = StringIndexer(inputCol = 'isFraud', outputCol = 'label')
	stages += [label_stringIdx]
	
	assemblerInputs = [c + "classVec" for c in categoricalColumns] + nonCategoricalColumns
	assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="vectorized_features")
        stages += [assembler]
        scaler = StandardScaler(inputCol="vectorized_features", outputCol="features")
        stages += [scaler]

Run the stages as a Pipeline. This puts the data through all of the feature transformations we described in a single call.

	pipeline = Pipeline(stages = stages)
	pipelineModel = pipeline.fit(train_transaction)
	df = pipelineModel.transform(train_transaction)
	selectedCols = ['label', 'features'] + cols
	df = df.select(selectedCols)    

#### Model Training and Execution
The first step of this phase has been splitting the data into training and test sets (30% held out for testing).

	train, test = df.randomSplit([0.7, 0.3], seed=2021)
	print("Training Dataset Count: " + str(train.count()))
	print("Test Dataset Count: " + str(test.count()))

Then we built two functions, the first one has the task of train the model using the training set and then execute it using the test set. The resulting predictions will be evaluted by the second function which performs the model evalutation.

	def classifier_executor(classifier, train, test):
		model = classifier.fit(train)
		predictions = model.transform(test)
		predictions.select('TransactionID', 'label', 'rawPrediction', 'prediction', 'probability').show(10)
		metrics_calc(predictions)
	    
	def metrics_calc(predictions):
		evaluator = BinaryClassificationEvaluator()
		print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))
		#print('Test Area Under ROC', evaluator.evaluate(predictions))
		
		numSuccesses = predictions.where("""(prediction = 0 AND isFraud = 'no') OR (prediction = 1 AND (isFraud = 'yes'))""").count()
		numInspections = predictions.count()
		
		print('There were', numInspections, 'inspections and there were', numSuccesses, 'successful predictions')
		print('This is a', str((float(numSuccesses) / float(numInspections)) * 100) + '%', 'success rate')
		
		true_positive = predictions.filter((predictions.prediction==1) & (predictions.isFraud=='yes')).count()
		false_positive = predictions.filter((predictions.prediction==1) & (predictions.isFraud=='no')).count()
		true_negative = predictions.filter((predictions.prediction==0) & (predictions.isFraud=='no')).count()		
		false_negative = predictions.filter((predictions.prediction==0) & (predictions.isFraud=='yes')).count()
		
		print("True positive: " + str(true_positive)) 
		print("False positive: " + str(false_positive)) 
		print("True negative: " + str(true_negative)) 
		print("False negative: " + str(false_negative)) 
		
		sensitivity = true_positive/(true_positive+false_negative)
		fallout = false_positive/(false_positive+true_negative)
		specificity = true_negative/(true_negative+false_positive)
		miss_rate = false_negative/(false_negative+true_positive)
		
		print("Sensitivity: " + str(sensitivity))
		print("Fallout: " + str(fallout))
		print("Specificity: " + str(specificity))
		print("Miss_rate: " + str(miss_rate))

For this project, we have chosen to study the predictive performance of two different classification algorithms:
1. Logistic Regression
2. Decision Trees

In the following we show the classifiers initialization and the call to the `classifier_executor` function described previously:

	## LR
	if logReg:
		classifier = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10)
		classifier_executor(classifier, train, test)
	
	# DT
	if decTree:
		classifier = DecisionTreeClassifier(featuresCol = 'features', labelCol = 'label', maxDepth = 3)
		classifier_executor(classifier, train, test)
		
#### Model Evaluation

In order to perform the model evaluation the `classifier_executor` function calls the `metrics_calc` function. Here we use the BinaryClassificationEvaluator to evaluate our models. 
Note that the default metric for the BinaryClassificationEvaluator is areaUnderROC. ROC is a probability curve and AUC represents degree or measure of separability. ROC tells how much model is capable of distinguishing between classes. Higher the AUC, better the model is at distinguishing between fraudolent or no fraudolent transactions.

#### Classification Evaluation Metrics

In this last function we also perform the calculation of some metrics. When making predictions on events we can get four type of results:
1. TP - True Positives: transactions correctly classified as fraudulent
2. FP - False Positives: legitimate transactions erroneously classified as fraudulent
3. TN - True Negatives: transactions correctly classified as legitimate
4. FN - False Negatives: fraudulent transactions erroneously classified as legitimate

Combining these results we compute the following metrics:
- Sensitivity: TP/(TP+FN)
- Fall-out: FP/(FP+TN)
- Specificity: TN/(TN+FP)
- Miss-rate: FN/(FN+TP)

The first two metrics have been chosen because they provide information about the performance in terms of fraudulent transactions correctly classified, while the other two have been chosen in order to evaluate the algorithm performance in terms of correct and incorrect classification of the legitimate transactions.

### Results and Conclusions

The dataset has been split as follows:
- Training Dataset Count: 413264
- Test Dataset Count: 177276

#### Qualitative results

Below we show the results of the chosen classifiers:
- Logistic Regression results:
		
		Test Area Under ROC: 0.8313290563191054
		There were 177276 inspections and there were 172035 successful predictions
		This is a 97.04359304135923% success rate
		
		True positive: 1347
		False positive: 295
		True negative: 170688
		False negative: 4902
		
		Sensitivity: 0.2155544887181949
		Fallout: 0.0017253177216448418
		Specificity: 0.9982746822783551
		Miss_rate: 0.7844455112818051

- Decision Trees results:

		Test Area Under ROC: 0.38546968276899746
		There were 177276 inspections and there were 171842 successful predictions
		This is a 96.93472325639117% success rate
		
		True positive: 1222
		False positive: 363
		True negative: 170620
		False negative: 5071
		
		Sensitivity: 0.1941840139837915
		Fallout: 0.002123018077820602
		Specificity: 0.9978769819221794
		Miss_rate: 0.8058159860162085

Taking a look at the results we can see that the Logistic Regression classifier gives the best performance, but we can also notice that there are a lot of fraudulent transactions erroneously classified as legitimate. This result is due to the intrinsic nature of the problem we are facing. In the context of fraud detection, indeed, the datasets are characterized by a highly unbalanced distribution of classes which therefore determines poor performance of the classifiers.
In order to contain this effect and consequently improving the performance of the Logistic Regression classifier we may need to adopt the following strategies:
1. Class weighing technique in order to assign higher weightage to the minority class
2. Analyze and tune the hyperparameters

This two options could represent the future improvements of our work.

#### Quantative results

The following table indicates the execution time of our algorithm in three different cluster configurations:
\- | Configuration 1 | Configuration 2 | Configuration 3
------------ | :---: | :---: | :---: 
**Instance type** | m5.xlarge | m5.xlarge | m5.xlarge
**# Master instances** | 1 | 1 | 1
**# Core instances** | 2 | 4 | 6
**Pre-processing** | 11 min | 8 min | 7 min
**Logistic Regression** | 3 min | 2 min | 2 min
**LR Metrics Calculation**  | 11 min | 6 min | 5,5 min
**Decision Tree**  | 8 min | 5 min | 3 min
**DT Metrics Calculation**  | 11 min | 6 min | 5,5 min
**Total Execution Time** | **44 min** | **27 min** | **23 min**

We can see that passing from the first to the second configuration there is an evident improvement in the execution time, while from the second to the third configuration the improvement is less significant.

### References

- [Brent Lemieux - Getting Started with PySpark on AWS EMR](https://towardsdatascience.com/getting-started-with-pyspark-on-amazon-emr-c85154b6b921)
- [Hector Castro - Azavea - A Terraform Module for Amazon Elastic MapReduce](https://www.azavea.com/blog/2017/12/06/a-terraform-module-for-amazon-emr/)
- [Chris Deotte - EDA for Columns V and ID](https://www.kaggle.com/cdeotte/eda-for-columns-v-and-id)
- [Victor Roman - Finding Donors: Classification Project With PySpark](https://towardsdatascience.com/finding-donors-classification-project-with-pyspark-485fb3c94e5e)
- [Gülcan Öğündür - Logistic Regression with PySpark](https://medium.com/swlh/logistic-regression-with-pyspark-60295d41221)
- [Dhiraj Rai - Logistic Regression in Spark ML](https://medium.com/@dhiraj.p.rai/logistic-regression-in-spark-ml-8a95b5f5434c)
- [D. Reforgiato Recupero, S. Carta, G. Fenu, R. Saia - Fraud detection for E-commerce transactions by employing a prudential Multiple Consensus model](https://www.sciencedirect.com/science/article/abs/pii/S2214212618304216)
