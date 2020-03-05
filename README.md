# Amazon Personalize's example using python sdk


## This example is Movie recommendation example using Amazon Personalize SDK to understand Personalize. 

This is consist of following scripts, and it might be helps to test and  understand Amazon Personalize easily.

* configuration/env.yaml: Setting configurations such as s3 bucket,file location of training data and schema and so on
* 0_prepare_data.py: Preprocess training data
* 1_create_dataset.py: Create dataset group, dataset, dataschema and import job
* 2_create_solution.py:	Create solution and recipe
* 3_create_campaign.py:	Create campaign
* 4_create_recommendation.py: get_recommendation, get_personalized_ranking
* 5_create_event.py: Put events


## Reference
- Boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/personalize.html
- MovieLens: https://grouplens.org/datasets/movielens/
- https://github.com/aws-samples/amazon-personalize-samples
- https://github.com/chrisking/PersonalizePOC
