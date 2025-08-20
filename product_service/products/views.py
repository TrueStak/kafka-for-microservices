from rest_framework.views import APIView
from rest_framework import status
from rest_framework.response import Response
from .models import Product
from kafka import KafkaConsumer
import json
import threading

# Product CRUD Views

class ProductCreateView(APIView):
    def post(self, request):
        name = request.data.get('name')
        description = request.data.get('description')
        price = request.data.get('price')
        stock = request.data.get('stock')

        if not all([name, description, price, stock]):
            return Response({"error": "All fields are required"}, status=status.HTTP_400_BAD_REQUEST)

        product = Product.objects.create(
            name=name,
            description=description,
            price=price,
            stock=stock
        )
        return Response(
            {"message": "Product created successfully", "product_id": product.id},
            status=status.HTTP_201_CREATED
        )

class ProductListView(APIView):
    def get(self, request):
        products = Product.objects.all()
        product_list = [
            {
                "id": product.id,
                "name": product.name,
                "description": product.description,
                "price": str(product.price),
                "stock": product.stock
            } for product in products
        ]
        return Response({"products": product_list}, status=status.HTTP_200_OK)


# Kafka Consumer to update stock

def start_kafka_consumer():
    consumer = KafkaConsumer(
    'order_created',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    group_id='product-service',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)


    for message in consumer:
        data = message.value
        product_name = data.get('product_name')
        quantity = int(data.get('quantity', 0))

        try:
            product = Product.objects.get(name=product_name)
            product.stock -= quantity
            product.save()
            print(f"Updated stock for {product_name}: {product.stock} left")
        except Product.DoesNotExist:
            print(f"Product {product_name} does not exist.")


# Run Kafka consumer in a background thread (optional, for development)
threading.Thread(target=start_kafka_consumer, daemon=True).start()
