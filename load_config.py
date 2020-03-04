import yaml
import json
import sys
import os.path


global app_env

def load_environment():

    app_conf_file = './configuration/env.yaml'
    global app_env
    with open(app_conf_file) as f:
        app_env = yaml.load(f, Loader=yaml.FullLoader)

def get_variable(key):
    value = None
    load_environment()
    if os.path.isfile(app_env['stored_variables']):
        try:
            with open(app_env['stored_variables']) as f:
                vars = json.load(f)
            value = vars[key]
        except:
            pass

    return value

def save_variable(key, value):
    load_environment()
    vars ={}
    if os.path.isfile(app_env['stored_variables']):
        try:
            with open(app_env['stored_variables']) as f:
                vars = json.load(f)
        except:
            pass

    with open(app_env['stored_variables'], 'w+') as f:
        if key in vars:
            vars[key] = value
        else:
            vars.setdefault(key, value)
        json.dump(vars, f)

load_environment()

#save_variable("dataset_group_arn", 'group_arn')
# save_variable("dataset_group_arn", 'group_arn2')
# save_variable("dataset_group_arn", 'group_arn3')
# save_variable("da arn", 'grd_arn')
#
# print(get_variable('da arn'))
# print(get_variable('dataset_group_arn'))
# print(get_variable('da dataset_group_arn'))

