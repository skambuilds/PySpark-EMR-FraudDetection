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
- The [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html) installed
- Your AWS credentials configured locally.

In order to perform this last point you can proceed as follows:

1. With your account created and the CLI installed configure the AWS CLI:

    $ aws configure

2. Follow the prompts to input your AWS Access Key ID and Secret Access Key, which you'll find [on this page](https://signin.aws.amazon.com/signin?redirect_uri=https%3A%2F%2Fconsole.aws.amazon.com%2Fiam%2Fhome%3Fstate%3DhashArgs%2523security_credential%26isauthcode%3Dtrue&client_id=arn%3Aaws%3Aiam%3A%3A015428540659%3Auser%2Fiam&forceMobileApp=0&code_challenge=0PnMq9kl_B7Z_WeFz9d2bJFPoYxEFMahW6Zw0shoJzo&code_challenge_method=SHA-256).

If you are using the Educate account you have also to provide the Session Token. You can find these information in the Vocareum AWS console login page by clicking on the *Account Details* button. 

The configuration process creates a file at ~/.aws/credentials on MacOS and Linux or %UserProfile%\.aws\credentials on Windows, where your credentials are stored.

### Terraform module for EMR

Modules in Terraform are units of Terraform configuration managed as a group. For example, an Amazon EMR module needs configuration for an Amazon EMR cluster resource, but it also needs multiple security groups, IAM roles, and an instance profile.

We encapsulated all of the necessary configuration into a reusable module in order to manage the infrastructure complexity only one-time.

On a fundamental level, Terraform modules consist of inputs, outputs, and Terraform configuration. Inputs feed configuration, and when configuration gets evaluated, it computes outputs that can route into other workflows.
