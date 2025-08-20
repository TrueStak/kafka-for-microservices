from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from .models import Order
from kafka import KafkaProducer
import json

class OrderCreateView(APIView):
    """
    View to create a new order and send a Kafka message to update product stock.
    """
    def post(self, request):
        customer_name = request.data.get('customer_name')
        product_name = request.data.get('product_name')
        quantity = request.data.get('quantity')

        if not customer_name or not product_name or not quantity:
            return Response({"error": "All fields are required."}, status=status.HTTP_400_BAD_REQUEST)

        # Create order in DB
        order = Order.objects.create(
            customer_name=customer_name,
            product_name=product_name,
            quantity=quantity
        )

        # Kafka producer
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )


        # Send message to 'order_created' topic
        message = {
            'order_id': order.id,
            'customer_name': customer_name,
            'product_name': product_name,
            'quantity': quantity
        }
        producer.send('order_created', value=message)
        producer.flush()

        return Response({"order_id": order.id}, status=status.HTTP_201_CREATED)
