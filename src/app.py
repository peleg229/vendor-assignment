import logging
import src.flow_runner as fr
import json

LOGGER = logging.getLogger(__name__)
def execute(data: dict):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.info('event parameter: {}'.format(data))
    # print("Received event: " + json.dumps(event, indent=2))
    body = data
    print("Received body:  " + str(body))
    try:
        return fr.run_flow(body)
    except Exception as e:
        logger.error(e)
        print(json.dumps({'error': str(e)}))
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

