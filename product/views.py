import json
import boto3
import datetime
from django.http import JsonResponse
from django.views import View
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('realtime_product')

now = datetime.date.today()


class Product(View):
    def get(self, request):

        products = table.scan(
            FilterExpression=Attr('created_at').begins_with(str(now))
        )

        product_list = sorted(products['Items'], key=lambda x: datetime.datetime.strptime(
            x['created_at'], '%Y-%m-%d %H:%M:%S.%f'), reverse=True)

        return JsonResponse({'data': product_list})
