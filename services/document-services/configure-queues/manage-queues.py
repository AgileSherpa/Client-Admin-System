# sets up all the queues for the document services microservices and publish them to 
# consul so the microservices can find them

import consul
import os
import pika
import yaml


# will read the config file and will return:
#       Server name and port for the rabbitmq server
#       specific queue names for the microservices defined by scope
#           all - returns all queues in the config file
#           service name - returns the queues for that specified service

def load_config(config_file,scope):
    
    queue_server = 'rabbitmq'
    server_section = 'server'
        
    with open(config_file, 'r') as f: 
        config = yaml.safe_load(f)[queue_server]        
        server = config[server_section]
        del config[server_section]    
        
        if scope == 'all':
            
            queues = config
            
        else:
            
            queues = config[scope]
            
        return server, queues


def establish_queue(server, queue_name):
    
    credentials = pika.PlainCredentials('guest','guest')
    parameters = pika.ConnectionParameters('localhost',5672,'/',credentials)
    
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue='my_queue', durable=True)
    
    connection.close()


# also add delete queue
# restart queue

def main():
    
    # define location of config file which is assumed to be one level up from the microservice
    script_dir = os.path.dirname(os.path.abspath(__file__))    
    config_dir = os.path.dirname(script_dir)
    config_file = os.path.join(script_dir, 'config.yaml')

    config = load_config(config_file, 'preprocessing-docx')

    # will need error handling
    # publish queue
    # read queue and respond with success, error, etc.
    # if using queue to respond, can put all responses on queue for now

    print()
    print(config)
    print()

    # establish_queue()    


if __name__ == "__main__":
    main()