import json
import boto3

from datetime                     import date
from boto3.dynamodb.conditions    import Key, Attr
from asgiref.sync                 import async_to_sync
from django.core.serializers.json import DjangoJSONEncoder
from channels.generic.websocket   import AsyncWebsocketConsumer

class OrderConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.order_name = 'order'
        self.order_group_name = 'order_%s' % self.order_name

        # join group
        await self.channel_layer.group_add(
            self.order_group_name,
            self.channel_name
        )
        await self.accept()


    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(
            self.order_group_name,
            self.channel_name
        )


    # receive message from websocket
    async def receive(self, text_data):
        # order status 통계
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('lastordr_order')

        today = date.today().isoformat()  # '2019-11-23'
        todaylist = table.scan(FilterExpression=Key('created_at').begins_with(today))["Items"]

        order_count = 0
        receipt_count = 0
        for el in todaylist:
            if el["status"] == 201:
                order_count += 1
            elif el["status"] == 210:
                receipt_count += 1

        orderStatus = {
            "order" : order_count,
            "receipt" : receipt_count
        }

        data = json.loads(text_data)
        message = {
            "data" : data,
            "orderStatus" : orderStatus
        }

        print("message : ", message)
        await self.channel_layer.group_send(
            self.order_group_name,
            {
                'type' : 'order_message',
                'message' : message
            }
        )


    # receive message from room group
    async def order_message(self, event):
        message = event['message']

        await self.send(text_data=json.dumps({
            'message': message
        }))

        
