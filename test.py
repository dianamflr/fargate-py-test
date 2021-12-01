import boto3

REGION_NAME = "us-east-1"

def main():
    ecs_client = boto3.client('ecs', region_name=REGION_NAME)
    response = run_ecs_task(ecs_client)
    return response

def run_ecs_task(ecs_client):
    INPUT_BUCKET = 'crc-data-lake-structured'
    INPUT_FILE_PATH = 'corporate_names/corporate_names.csv'
    OUTPUT_BUCKET= 'crc-data-lake-raw'
    OUTPUT_FILE_PATH = 'corporate_names/corporate_names.csv'
    OUTPUT_BUCKET_ATHENA = 'datalake-data-prep-staging'
    OUTPUT_FOLDER_ATHENA = 'results'    
    TOPIC_ARN = 'arn:aws:sns:us-east-1:265456890698:vre-manage-corp-name-dataset'

    PROPAGATION_CLUSTER = "crc-vre-ray-shared-cluster"
    COVLIB_WRAPPER_TASK_DEFINITION_FAMILY = "vremanagecorpnamedatasetvremanagecorpnamedatasettask724EEACE"
    COVLIB_WRAPPER_CONTAINER_NAME = "vre-manage-corp-name-dataset-container"

    SUBNET1 = "subnet-0fabc8a84758916ee"
    SUBNET2 = "subnet-0af28fcc702bf03f3"

    SECURITY_GROUP = "sg-095fe9cdde858be82"

    job_id = 1234

    response = ecs_client.run_task(
        cluster=PROPAGATION_CLUSTER,
        launchType = 'FARGATE',
        taskDefinition=COVLIB_WRAPPER_TASK_DEFINITION_FAMILY,
        count = 1,
        platformVersion='1.4.0',
        networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': [
                        SUBNET1,
                        SUBNET2
                    ],
                    'securityGroups': [
                        SECURITY_GROUP
                    ],
                    'assignPublicIp': 'DISABLED'
                }
            },
        overrides={
            'containerOverrides': [
                {
                    'name': COVLIB_WRAPPER_CONTAINER_NAME,
                    'environment': [{"name":"INPUT_BUCKET", "value":f'{INPUT_BUCKET}'},
                                    {"name":"INPUT_FILE_PATH", "value":f'{INPUT_FILE_PATH}'},
                                    {"name":"OUTPUT_BUCKET", "value":f'{OUTPUT_BUCKET}'},
                                    {"name":"OUTPUT_FILE_PATH", "value":f'{OUTPUT_FILE_PATH}'},
                                    {"name":"OUTPUT_BUCKET_ATHENA", "value":f'{OUTPUT_BUCKET_ATHENA}'},
                                    {"name":"OUTPUT_FOLDER_ATHENA", "value":f'{OUTPUT_FOLDER_ATHENA}'},
                                    {"name":"REGION", "value":f'{REGION_NAME}'},
                                    {"name":"TOPIC_ARN", "value":f'{TOPIC_ARN}'}
                        ]
                        
                }
            ],
        },
    )
    # logger.info ("CQLS - Run Task: " + str(['python', '/app/app.py'] + list(chain(*[(f'--{k}', f'{v}') for (k, v) in message_dict.items()]))))
    return response

if __name__ == '__main__':
    main()