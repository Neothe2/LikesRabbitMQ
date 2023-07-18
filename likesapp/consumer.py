import json
import threading
import time

import pika
import django
from sys import path
from os import environ

path.append(
    'C:/Users/neoja/OneDrive/Tutorials/RabbitMQ/LikesAndQuotes/Likes/')  # Your path to settings.py file
environ.setdefault('DJANGO_SETTINGS_MODULE', 'Likes.settings')
django.setup()
from likesapp.models import Quote




def callback(ch, method, properties, body):
    print("Received in likes...")
    print(body)
    data = json.loads(body)

    print(data)
    print('hi')

    if properties.content_type == 'quote_created':
        quote = Quote.objects.create(id=data['id'], title=data['title'])
        quote.save()
        print("quote created")
    elif properties.content_type == 'quote_updated':
        quote = Quote.objects.get(id=data['id'])
        quote.title = data['title']
        quote.save()
        print("quote updated")
    elif properties.content_type == 'quote_deleted':
        quote = Quote.objects.get(id=data)
        quote.delete()
        print("quote deleted")




def consume_messages():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters('localhost', heartbeat=600, blocked_connection_timeout=300))
            channel = connection.channel()
            channel.queue_declare(queue='likes')
            channel.basic_consume(queue='likes', on_message_callback=callback, auto_ack=True)

            print("Started Consuming...")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            print("Connection was closed, retrying...")
            time.sleep(10)

consumer_thread = threading.Thread(target=consume_messages)
consumer_thread.start()

# channel.basic_consume(queue='likes', on_message_callback=callback, auto_ack=True)
# print("Started Consuming...")
# channel.start_consuming()
