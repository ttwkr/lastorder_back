import json
import boto3

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

        order_count = table.scan(FilterExpression=Key('status').eq(201))["Count"]
        receipt_count = table.scan(FilterExpression=Key('status').eq(210))["Count"]

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

        
