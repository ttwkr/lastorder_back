import channels.layers
from asgiref.sync import async_to_sync
from channels.generic.websocket import WebsocketConsumer, JsonWebsocketConsumer
import json


class ProductConsumer(JsonWebsocketConsumer):

    def connect(self):
        self.room_name = 'product'
        self.room_group_name = 'product_%s' % self.room_name

        async_to_sync(self.channel_layer.group_add)(
            self.room_group_name,
            self.channel_name
        )
        # WebSocket 연결
        self.accept()

    def disconnect(self, close_code):
        async_to_sync(self.channel_layer.group_discard)(
            self.room_group_name,
            self.channel_name
        )
        self.close()

    # lambda handler

    def handle(event, context):
        layers = channels.layers.get_channel_layer()

        insert_data = [
            {
                'product_id': data['dynamodb']['NewImage']['product_id']['N'],
                'product': data['dynamodb']['NewImage']['product']['S'],
                'quantity': data['dynamodb']['NewImage']['quantity']['N'],
                'price': data['dynamodb']['NewImage']['price']['N'],
                'store': data['dynamodb']['NewImage']['store_name']['S'],
                'store_lng': data['dynamodb']['NewImage']['longitude']['S'],
                'store_lat': data['dynamodb']['NewImage']['latitude']['S'],
                'status': data['dynamodb']['NewImage']['status']['S'],
                'created_at': data['dynamodb']['NewImage']['created_at']['S'],
            }
            for data in event['Records'] if data['eventName'] == 'INSERT']

        async_to_sync(layers.group_send)('product_product', {
            'type': 'order_message',
            'data': insert_data
        })
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from lambda')
        }

    def order_message(self, event):
        message = event
        channel_layers = channels.layers.get_channel_layer()
        self.send(text_data=json.dumps(message))
