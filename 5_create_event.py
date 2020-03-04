
import boto3
from botocore.exceptions import ClientError
import load_config as conf
import time

personalize = boto3.client("personalize", region_name= conf.app_env['region'])
personalize_events = boto3.client('personalize-events', region_name= conf.app_env['region'])


def create_event_tracker (dataset_group_arn, name=None):
    tracker_name = name if name else "{}-tracker".format(conf.app_env['appname'])
    tracking_id = None
    try:
        response = personalize.create_event_tracker(name =tracker_name, datasetGroupArn=dataset_group_arn)
        tracking_id = response['trackingId']
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceAlreadyExistsException':
            rlist = personalize.list_event_trackers()
            for rec in rlist['eventTrackers']:
                if rec['name'] == tracker_name:

                    tracker = personalize.describe_event_tracker(eventTrackerArn=rec['eventTrackerArn'])
                    tracking_id = tracker['eventTracker']['trackingId']
                    break
        else:
            raise

    return tracking_id


def put_events (tracking_id, session_id, event_list, user_id=None):
    personalize_events.put_events(trackingId=tracking_id,
                                  sessionId= session_id if type(session_id)=='str' else str(session_id),
                                  userId= user_id if type(user_id)=='str' else str(user_id),
                                  eventList=event_list)


##-------------------------------------------

## 1. create_event_tracker
tracking_id = create_event_tracker( conf.get_variable('dataset_group_arn'))
# print(tracking_id)
# conf.save_variable("tracking_id", tracking_id)

## 2. put_events
event_list = [{
    'sentAt': int(time.time())+ 1000,
    'properties': '{"itemId":"1", "eventValue":5}',
    'eventType': 'RATING',
}]


put_events(tracking_id=tracking_id, session_id=1, user_id= 15, event_list=event_list )

