# Reads the config file for the rabbitmq settings so the queues can be created and published
import os
import yaml

# modify to be directly called
# A second service will then establish all queues and make them discoverable

# Load configuration from YAML file

def load_config(config_file):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
        server = config['rabbitmq']['server']
        queues = config['rabbitmq']['ingestion']
    return server, queues

#async def send_response(channel, output_queue, response_message):
#    async with channel.default_exchange.publish(
#        aio_pika.Message(body=response_message.encode()),
#        routing_key=output_queue
#    ):
#        print(f" [x] Sent '{response_message}' to {output_queue}")

#async def on_message(message: aio_pika.IncomingMessage, channel, output_queue, response_message):
#    async with message.process():
#        print(f" [x] Received {message.body.decode()}")
#        await send_response(channel, output_queue, response_message)

def main():
    
    # define location of config file which is assumed to be one level up from the microservice
    script_dir = os.path.dirname(os.path.abspath(__file__))    
    config_dir = os.path.dirname(script_dir)
    config_file = os.path.join(script_dir, 'config.yaml')

    config = load_config(config_file)

    # will need error handling
    # publish queue
    # read queue and respond with success, error, etc.
    # if using queue to respond, can put all responses on queue for now

    print()
    print(script_dir)
    print(config_dir)
    print(config_file)
    print(config)
    print()

#    rabbitmq_host = config['rabbitmq']['host']
#    rabbitmq_port = config['rabbitmq']['port']
#    input_queue = config['rabbitmq']['input_queue']
#    output_queue = config['rabbitmq']['output_queue']
#    response_message = 'Processed successfully'

    # Connect to RabbitMQ
#    connection = await aio_pika.connect_robust(
#        host=rabbitmq_host, 
#        port=rabbitmq_port
#    )

#    async with connection:
        # Creating a channel
#        channel = await connection.channel()

        # Declare input and output queues
#        await channel.declare_queue(input_queue, durable=True)
#        await channel.declare_queue(output_queue, durable=True)

        # Set up subscription on the input queue
#       input_queue_obj = await channel.get_queue(input_queue)
#        await input_queue_obj.consume(
#            lambda message: on_message(message, channel, output_queue, response_message)
#        )

#        print(f' [*] Waiting for messages in {input_queue}. To exit press CTRL+C')
        
        # Keep the script running
        # while True:
        #    await asyncio.sleep(1)

if __name__ == "__main__":
    main()
    