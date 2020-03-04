
import boto3
from botocore.exceptions import ClientError
import load_config as conf
import datetime

personalize = boto3.client("personalize", region_name= conf.app_env['region'])

def create_campaign(solution_version_arn, min_provisioned_tps, add=False):
    name = "{}-campaign".format(conf.app_env['appname'])
    campaign_arn = None

    try:
        response = personalize.create_campaign(
            name = name,
            solutionVersionArn=solution_version_arn,
            minProvisionedTPS=min_provisioned_tps
        )
        campaign_arn = response['campaignArn']
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
            if add is False:
                rlist = personalize.list_campaigns()
                for rec in rlist['campaigns']:
                    if rec['name'] == name:
                        campaign_arn = rec['campaignArn']
                        break
            else:
                name = "{}-campaign-{}".format(conf.app_env['appname'], datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
                response = personalize.create_campaign(
                    name = name,
                    solutionVersionArn=solution_version_arn,
                    minProvisionedTPS=min_provisioned_tps
                )
                campaign_arn = response['campaignArn']
        else:
            raise

    return campaign_arn

def update_campaign(campaign_arn, solution_version_arn, min_provisioned_tps):
    response = personalize.update_campaign(
        campaignArn=campaign_arn,
        solutionVersionArn=solution_version_arn,
        minProvisionedTPS=min_provisioned_tps)
    return response['campaignArn']

##-------------------------------------------

## 1. create campaign
campaign_arn = create_campaign(conf.get_variable('solution_version_arn'), 1)

conf.save_variable("campaign_arn", campaign_arn)

#print(campaign_arn)

## 2. update campaign

#campaign_arn = update_campaign(campaign_arn=campaign_arn, solution_version_arn=solution_version_arn, min_provisioned_tps=5)
#print(campaign_arn)

