
import boto3
from botocore.exceptions import ClientError
import load_config as conf
import datetime

personalize = boto3.client("personalize", region_name= conf.app_env['region'])


def create_solution(dataset_group_arn, recipe_arn=None, perform_hpo=False, perform_automl=False, event_type='', solution_config=None, add=False):
    solution_name="{}-solution".format(conf.app_env['appname'])
    solution_arn = None
    try:
        if solution_config == None:
            response = personalize.create_solution(name =solution_name,performHPO=perform_hpo,
                performAutoML=perform_automl,recipeArn=recipe_arn,datasetGroupArn=dataset_group_arn,eventType=event_type
            )
        else:
            response = personalize.create_solution( name =solution_name, performHPO=perform_hpo,
                performAutoML=perform_automl,recipeArn=recipe_arn, datasetGroupArn=dataset_group_arn,
                eventType=event_type, solutionConfig=solution_config
            )
        solution_arn = response['solutionArn']
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
            if add is False:
                solution_list = personalize.list_solutions()
                for solution in solution_list['solutions']:
                    if solution['name'] == solution_name:
                        solution_arn = solution['solutionArn']
                        break
            else:
                solution_name="{}-solution-{}".format(conf.app_env['appname'], datetime.datetime.now().strftime("%Y%m%d%H%M%S"))
                if solution_config == None:
                    response = personalize.create_solution(name =solution_name,performHPO=perform_hpo,
                        performAutoML=perform_automl,recipeArn=recipe_arn,datasetGroupArn=dataset_group_arn,eventType=event_type
                        )
                else:
                    response = personalize.create_solution( name =solution_name, performHPO=perform_hpo,
                        performAutoML=perform_automl,recipeArn=recipe_arn, datasetGroupArn=dataset_group_arn,
                        eventType=event_type, solutionConfig=solution_config )
                solution_arn = response['solutionArn']

        else:
            raise

    return solution_arn

def get_recipe(name):
    response = personalize.list_recipes()
    recipes ={}
    for recipe in response['recipes']:
        recipes.setdefault(recipe['name'], recipe['recipeArn'])

    recipe_arn = None
    if name in recipes:
        recipe_arn = recipes[name]
    return recipe_arn


def create_solution_version(solution_arn, training_mode='FULL'):
    response = personalize.create_solution_version(solutionArn = solution_arn, trainingMode=training_mode)
    solution_version_arn = response['solutionVersionArn']
    return solution_version_arn

def get_solution_metric_by_version (solution_version_arn):
    return personalize.get_solution_metrics(solutionVersionArn=solution_version_arn)

##-------------------------------------------


## 1. create solution
solution_arn = create_solution(
    dataset_group_arn=conf.get_variable('group_arn'),
    recipe_arn=get_recipe('aws-hrnn-metadata')
)

## 2. create solution version
solution_version_arn= create_solution_version(solution_arn)
print(solution_version_arn)
#
# print (get_solution_metric_by_version(conf.get_variable('solution_version_arn')))

conf.save_variable("solution_arn", solution_arn)
conf.save_variable("solution_version_arn", solution_version_arn)

# import pytz
# status = None
# max_time = time.time() + 3*60*60 # 3 hours
# while time.time() < max_time:
#     describe_solution_version_response = personalize.describe_solution_version(
#         solutionVersionArn = conf.get_variable('solution_version_arn')
#     )
#     status = describe_solution_version_response["solutionVersion"]["status"]
#     now = datetime.datetime.now(pytz.utc)
#     elapsed = now - describe_solution_version_response["solutionVersion"]["creationDateTime"]
#     print("SolutionVersion: {}   (elapsed = {})".format(status, elapsed))
#
#     if status == "ACTIVE" or status == "CREATE FAILED":
#         break
#
#     time.sleep(60)
