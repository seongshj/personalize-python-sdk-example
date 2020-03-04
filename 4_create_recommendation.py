
import boto3
import json
import load_config as conf
from botocore.exceptions import ClientError

personalize = boto3.client("personalize", region_name= conf.app_env['region'])
personalize_runtime = boto3.client('personalize-runtime', region_name= conf.app_env['region'])
iam = boto3.client("iam", region_name= conf.app_env['region'])


def get_recommendations (campaign_arn, item_id=None, user_id=None, num_results=25, context=None, only_data=True):
    if context:
        response = personalize_runtime.get_recommendations(
            campaignArn=campaign_arn,
            itemId=item_id if type(item_id)=='str' else str(item_id),
            userId=user_id if type(user_id)=='str' else str(user_id),
            numResults=num_results,
            context=context
        )
    else:
        response = personalize_runtime.get_recommendations(
            campaignArn=campaign_arn,
            itemId=item_id if type(item_id)=='str' else str(item_id),
            userId=user_id if type(user_id)=='str' else str(user_id),
            numResults=num_results
        )
    if only_data:
        return response["itemList"]
    return response

def get_personalized_ranking (campaign_arn, input_list, user_id,  context=None, only_data=True):

    if context:
        response = personalize_runtime.get_personalized_ranking(
            campaignArn=campaign_arn,
            userId=user_id if type(user_id)=='str' else str(user_id),
            inputList=input_list,
            context=context
        )
    else:
        response = personalize_runtime.get_personalized_ranking(
            campaignArn=campaign_arn,
            userId=user_id if type(user_id)=='str' else str(user_id),
            inputList=input_list,
        )

    if only_data:
        return response["personalizedRanking"]
    return response



##-------------------------------------------

## 1. get_recommendations
#r = get_recommendations(conf.get_variable('campaign_arn'), item_id=2, num_results=10 )
# print(json.dumps(r))

## 2. get_personalized_ranking
#r = get_personalized_ranking(conf.get_variable('campaign_arn'), user_id=2, input_list=['318', '590', '296'])
#print(json.dumps(r))

