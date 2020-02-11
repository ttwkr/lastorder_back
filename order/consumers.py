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
        print("receive text_data : ", text_data)
        data = json.loads(text_data)

        await self.channel_layer.group_send(
            self.order_group_name,
            {
                'type':'order_message',
                'message':data
            }
        )


    # receive message from room group
    async def order_message(self, event):
        message = event['message']

        await self.send(text_data=json.dumps({
            'message': message
        }))

        
