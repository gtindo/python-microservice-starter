# Micro service 1.0.0

## Overview on How to Run this Project

1. Create a Python virtual environment
2. Install required packages
3. A running RabbitMQ Server

# Setup procedure
1. Create a Python Virtual Environment
    - Install virtualenv
        ````shell script
        sudo pip install virtualenv

    - Create a virtualenv
        ````shell script
        virtualenv -p python3 <name of virtualenv>

    - Install requirements
        ````shell script
        pip install -r requirements.txt

2. Create configuration file
    - Create file (using config.example.txt as template)
       
       ````shell script
        cd app && and touch config.txt

    - Write configurations on file \
         [RABBITMQ] \
         HOST = host_name \
         VIRTUAL_HOST = virtual_host \
         USERNAME = username \
         PASSWORD = password \
         PORT = port \
         REQUEST_QUEUE = request_queue_name \
         RESPONSE_QUEUE = response_queue_name \
         \
         [MODE] \
         DEBUG = True or False \
         \
         [APP] \
         APP_NAME = Name of application \
         APP_VERSION = Version of application \

3. Set DEBUG variable in app/settings.py
    - For development : DEBUG = True
    - For production : DEBUG = False

4. Run main.py
    ```shell script
    python -m app.main
   
    ./start.sh
   
## Run unit tests  
    cd tests
    python -m unittest
