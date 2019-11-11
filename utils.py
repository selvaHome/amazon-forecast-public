import logging
import boto3
import json
import time


from botocore.exceptions import ClientError

def create_iam_policy():
    iam = boto3.client("iam")

    role_name = "ForecastRoleDemo"
    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "forecast.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    create_role_response = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(assume_role_policy_document)
    )

    # AmazonPersonalizeFullAccess provides access to any S3 bucket with a name that includes "personalize" or "Personalize"
    # if you would like to use a bucket with a different name, please consider creating and attaching a new policy
    # that provides read access to your bucket or attaching the AmazonS3ReadOnlyAccess policy to the role
    policy_arn = "arn:aws:iam::aws:policy/AmazonForecastFullAccess"
    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn=policy_arn
    )

    # Now add S3 support
    iam.attach_role_policy(
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3FullAccess',
        RoleName=role_name
    )
    time.sleep(60)  # wait for a minute to allow IAM role policy attachment to propagate

    role_arn = create_role_response["Role"]["Arn"]
    print(role_arn)
    return role_arn
