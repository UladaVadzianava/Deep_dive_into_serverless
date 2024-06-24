import json
import os
import uuid
import boto3
from decimal import Decimal

import requests

from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda

_LOG = get_logger('Processor-handler')

dynamodb = boto3.client('dynamodb')
table_name = 'cmtr-d7243a10-Weather-test'


class Processor(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass

    def put_db_item(self, item):
        table = dynamodb.Table(table_name)
        table.put_item(Item=item)

    def handle_request(self, event, context):
        """
        Explain incoming event here
        """
        _LOG.info(event)

        if "rawPath" in event and (event["rawPath"] in ["/weather", "/"]):
            response = requests.get(
                "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m")
            weather = response.json()

            record = {}
            forecast = {
                "elevation": weather['elevation'],
                "generationtime_ms": weather['generationtime_ms'],
                "hourly": {
                    "temperature_2m": weather['hourly']['temperature_2m'],
                    "time": weather['hourly']['time']
                },
                "hourly_units": {
                    "temperature_2m": weather['hourly_units']['temperature_2m'],
                    "time": weather['hourly_units']['time']
                },
                "latitude": weather['latitude'],
                "longitude": weather['longitude'],
                "timezone": weather['timezone'],
                "timezone_abbreviation": weather['timezone_abbreviation'],
                "utc_offset_seconds": weather['utc_offset_seconds']
            }
            record["id"] = str(uuid.uuid4())
            record["forecast"] = forecast

            item = json.loads(json.dumps(record), parse_float=Decimal)
            self.put_db_item(item)

            return {
                "headers": {
                    "Content-Type": "application/json"
                },
                "statusCode": 200,
                "body": weather
            }


HANDLER = Processor()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
