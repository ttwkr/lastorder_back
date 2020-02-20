import channels.layers
import json
import boto3
import datetime
from boto3.dynamodb.conditions import Key, Attr
from django.http import JsonResponse
from asgiref.sync import async_to_sync
from channels.generic.websocket import WebsocketConsumer, JsonWebsocketConsumer

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('realtime_product')


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
        data = event['Records']
        today = datetime.date.today().isoformat()
        status_1_count = 0
        status_2_count = 0
        send_data = ''
        status = ''
        diffkeys = []
        result = []

        # 상태코드 변환
        def statusCode():
            for i in data:
                if i['dynamodb']['NewImage']['status']['S'] == '1':
                    status = '판매중'

                elif i['dynamodb']['NewImage']['status']['S'] == '2':
                    status = "대기"

                return status

        # 총갯수, 상태에 따른 갯수 통계
        today_count = table.scan(
            FilterExpression=Key('created_at').begins_with(today))

        for el in today_count['Items']:
            if el["status"] == '1':
                status_1_count += 1
            elif el["status"] == '2':
                status_2_count += 1

        # eventName에 따른 분기
        if (eventName == "INSERT") or (eventName == "MODIFY"):

            # 전에 있던 데이터와 고친 데이터의 차이점을 찾아서 보낸다.
            if eventName == 'MODIFY':
                diffresult = ''

                newdict = list(data[0]['dynamodb']['NewImage'].items())
                olddict = list(data[0]['dynamodb']['OldImage'].items())

                for i in range(0, len(newdict)):
                    if newdict[i] != olddict[i]:
                        diffresult = newdict[i]
                        diffkeys.append(diffresult[0])
            for i in data:
                send_data = {
                    'type': i['eventName'],
                    'product_id': i['dynamodb']['NewImage']['product_id']['N'],
                    'product': i['dynamodb']['NewImage']['product']['S'],
                    'quantity': i['dynamodb']['NewImage']['quantity']['N'],
                    'price': i['dynamodb']['NewImage']['price']['N'],
                    'store_name': i['dynamodb']['NewImage']['store_name']['S'],
                    'store_lng': i['dynamodb']['NewImage']['longitude']['S'],
                    'store_lat': i['dynamodb']['NewImage']['latitude']['S'],
                    'status': statusCode(),
                    'created_at': i['dynamodb']['NewImage']['created_at']['S'],
                    'diffKeys': diffkeys,
                    'count': {
                        'today_count': today_count['Count'],
                        'status_1_count': status_1_count,
                        'status_2_count': status_2_count
                    }
                }
                result.append(send_data)

        elif eventName == 'REMOVE':
            for i in data:
                send_data = {
                    'type': i['eventName'],
                    'product_id': i['dynamodb']['OldImage']['product_id']
                }
                result.append(send_data)

        async_to_sync(layers.group_send)('product_product', {
            'type': 'order_message',
            'data': result,
        })
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from lambda')
        }

    def order_message(self, event):
        message = event
        channel_layers = channels.layers.get_channel_layer()
        self.send(text_data=json.dumps(message))
