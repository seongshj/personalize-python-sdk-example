import boto3
import json
import os
from botocore.exceptions import ClientError
import load_config  as conf
import datetime

s3 = boto3.client("s3", region_name= conf.app_env['region'])
personalize = boto3.client("personalize", region_name= conf.app_env['region'])
iam = boto3.client("iam", region_name= conf.app_env['region'])


def upload_to_s3(bucket, prefix, file):

    if boto3.resource('s3').Bucket(bucket).creation_date is None:
        s3.create_bucket(ACL='private', Bucket=bucket)
    filename = "{}/{}".format(prefix, os.path.split(file)[1])
    boto3.Session().resource('s3').Bucket(bucket).Object(filename).upload_file(file)

    return "s3://{}/{}".format(bucket, filename)


def initialize_policy():

    #s3 bucket permission
    bucket =  conf.app_env['s3']['bucket']
    policy = {
        "Version": "2012-10-17",
        "Id": "PersonalizeS3BucketAccessPolicy",
        "Statement": [
            {
                "Sid": "PersonalizeS3BucketAccessPolicy",
                "Effect": "Allow",
                "Principal": {
                    "Service": "personalize.amazonaws.com"
                },
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::{}".format(bucket),
                    "arn:aws:s3:::{}/*".format(bucket)
                ]
            }
        ]
    }
    s3.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))

    #Personalise IAM Role
    role_name="Personalize-{}".format(conf.app_env['appname'])
    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {
                    "Service": "personalize.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        ]
    }

    role_arn = None
    try:
        create_role_response = iam.create_role(
            RoleName = role_name,
            AssumeRolePolicyDocument = json.dumps(assume_role_policy_document)
        )

        iam.attach_role_policy(
            RoleName = role_name,
            PolicyArn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        role_arn = create_role_response["Role"]["Arn"]
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            role_arn = iam.get_role(RoleName=role_name)['Role']['Arn']
        else:
            raise
    return role_arn


def create_schema(dataset_type, schema_file, name=None, add=False):
    if not dataset_type in ['Interactions', 'Users', 'Items']:
        return None

    schema_name = "{}-{}-schema".format(conf.app_env['appname'], dataset_type)
    schema_arn = None

    with open(schema_file) as f:
        schema = f.read()

    try:
        response = personalize.create_schema(name=schema_name, schema=schema)
        schema_arn = response['schemaArn']

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
            if add is False:
                schema_list = personalize.list_schemas()
                for schema in schema_list['schemas']:
                    if schema['name'] == schema_name:
                            schema_arn = schema['schemaArn']
                            break
            else:
                schema_name = "{}-{}-schema-{}".format(conf.app_env['appname'], dataset_type,datetime.datetime.now().strftime("%Y%m%d%H%M%S") )
                response = personalize.create_schema(name=schema_name, schema=schema)
                schema_arn = response['schemaArn']

        else:
            raise

    return schema_arn


def create_dataset_group():
    dataset_group_name = "{}-dataset-group".format(conf.app_env['appname'])
    group_arn = None
    try:
        response =  personalize.create_dataset_group(name = dataset_group_name)
        group_arn = response['datasetGroupArn']
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':

                rlist = personalize.list_dataset_groups()
                for group in rlist['datasetGroups']:
                    if group['name'] == dataset_group_name:
                        group_arn =  group['datasetGroupArn']
                        break
    return group_arn

def create_dataset(dataset_type, dataset_group_arn, schema_arn):
    dataset_name = "{}-{}-dataset".format(conf.app_env['appname'], dataset_type)
    dataset_arn = None
    try:
        response = personalize.create_dataset(
            datasetType=dataset_type,
            datasetGroupArn=dataset_group_arn,
            schemaArn=schema_arn,
            name= dataset_name
        )
        dataset_arn = response['datasetArn']
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':

                rlist = personalize.list_datasets()
                for rec in rlist['datasets']:
                    if rec['name'] == dataset_name:
                        dataset_arn = rec['datasetArn']
                        break
        else:
            raise
    return dataset_arn



def create_dataset_import_job(dataset_type, dataset_arn, role_arn, data_source, add=False):
    import_job_name = "{}-{}-import_job".format(conf.app_env['appname'], dataset_type)
    import_job_arn = None
    try:
        response = personalize.create_dataset_import_job(
            jobName = import_job_name,
            datasetArn=dataset_arn,
            dataSource={"dataLocation": data_source},
            roleArn=role_arn
        )
        import_job_arn = response['datasetImportJobArn']
    except  ClientError as e:
        if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
            if add is False:
                rlist = personalize.list_dataset_import_jobs()
                for rec in rlist['datasetImportJobs']:
                    if rec['jobName'] == import_job_name:
                        import_job_arn = rec['datasetImportJobArn']
                        break
            else:
                import_job_name = "{}-{}-import_job-{}".format(conf.app_env['appname'], dataset_type,  datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
                response = personalize.create_dataset_import_job(
                    jobName = import_job_name,
                    datasetArn=dataset_arn,
                    dataSource={"dataLocation": data_source},
                    roleArn=role_arn
                )
                import_job_arn = response['datasetImportJobArn']


        else:
            raise

    return  import_job_arn


##-------------------------------------------
##-------------------------------------------

## 0. setup s3 policy and create role
role_arn = initialize_policy()
print(role_arn)


## 1. create dataset group
group_arn = create_dataset_group()
print(group_arn)

conf.save_variable("group_arn", group_arn)


##### 2. Interactions dataset
## 2.1 upload interactions data
interation_data_location = upload_to_s3(conf.app_env['s3']['bucket'], conf.app_env['s3']['prefix'],  conf.app_env['local']['interactions'])

## 2.2 create interaction schema
schema_arn = create_schema("Interactions", conf.app_env['data_schema']['interactions'])
#print(schema_arn)

## 2.3 create interaction dataset
dataset_arn = create_dataset("Interactions", group_arn, schema_arn )
# print(dataset_arn)

## 2.4 create dataset_import_job
import_job_arn = create_dataset_import_job("interations-import-job", dataset_arn, role_arn, interation_data_location)


##### 3. Users dataset
## 3.1 upload users data
user_data_location = users_data_location = upload_to_s3(conf.app_env['s3']['bucket'], conf.app_env['s3']['prefix'],  conf.app_env['local']['users'])
print(user_data_location)

## 3.2 create user schema
schema_arn = create_schema("Users", conf.app_env['data_schema']['users'])
print(schema_arn)
#
## 3.3 create user dataset
dataset_arn = create_dataset("Users", group_arn, schema_arn)
print(dataset_arn)

## 3.4 create dataset_import_job
import_job_arn = create_dataset_import_job("Users", dataset_arn, role_arn, user_data_location)
print(import_job_arn)
conf.save_variable("users_import_job_arn", import_job_arn)

##### 4.  Iterms dataset
## 4.1 upload item data
item_data_location = users_data_location = upload_to_s3(conf.app_env['s3']['bucket'], conf.app_env['s3']['prefix'],  conf.app_env['local']['items'])
# print(item_data_location)


## 4.2 create item schema
schema_arn = create_schema("Items", conf.app_env['data_schema']['items'])
# print(schema_arn)
#
## 4.3 create Item dataset
dataset_arn = create_dataset("Items", group_arn, schema_arn )
# print(dataset_arn)
#
## 4.4 create dataset_import_job
#
import_job_arn = create_dataset_import_job("Items", dataset_arn, role_arn, item_data_location)
# print(import_job_arn)
#
# conf.save_variable("items_import_job_arn", import_job_arn)


## monitor import_job
# status = None
# max_time = time.time() + 3*60*60 # 3 hours
# while time.time() < max_time:
#     describe_dataset_import_job_response = personalize.describe_dataset_import_job(
#         datasetImportJobArn = import_job_arn
#     )
#
#     dataset_import_job = describe_dataset_import_job_response["datasetImportJob"]
#     if "latestDatasetImportJobRun" not in dataset_import_job:
#         status = dataset_import_job["status"]
#         print("DatasetImportJob: {}".format(status))
#     else:
#         status = dataset_import_job["latestDatasetImportJobRun"]["status"]
#         print("LatestDatasetImportJobRun: {}".format(status))
#
#     if status == "ACTIVE" or status == "CREATE FAILED":
#         break
#
#     print(".")
#     time.sleep(60)
#


#
# a = conf.save_variable("dataset_group_arn", group_arn)
# print(a)
