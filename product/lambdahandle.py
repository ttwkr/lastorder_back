import json
from .consumers import ProductConsumer


def invokeHander(event, context):
    return (ProductConsumer.handle(event, context))
