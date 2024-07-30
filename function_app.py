import azure.functions as func
import logging
import urllib.parse
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubProducerClient
import os
import json
import asyncio

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="IncomingSMS", methods=['POST'])
def IncomingSMS(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body_bytes = req.get_body()
        req_body = req_body_bytes.decode("utf-8")
        resultParams = urllib.parse.parse_qsl(req_body)
        objectData = dict(resultParams)
        logging.info(f"Request: {req_body}")
        logging.info(f"Request Parse: {objectData}")

        if 'Body' in objectData.keys():
            #Decoding settings
            TNH_CONAGUA_PHONES = os.environ.get("TNH_CONAGUA_PHONES")
            config = json.loads(TNH_CONAGUA_PHONES)
            
            #Message received
            message = objectData['Body']
            arrMsg = message.split("|")
            medidor = arrMsg[4]

            phones = []
            clientId = ""
            dataEventHub = {}
            for phone in config:
                if medidor == phone["serial"]:
                    phones.append({"phone": phone["phone"]})
                    clientId = phone["clientId"]

            dataEventHub["phones"] = phones
            dataEventHub["clientId"] = clientId
            dataEventHub["message"] = message
            jsonstr = json.dumps(dataEventHub) 
            logging.info(f"Message Event Hub: {jsonstr}")
            asyncio.run(SendMessageEventHub(jsonstr))

            return func.HttpResponse(f"Mensaje Recibido: {message}")
        else:
            return func.HttpResponse(
                "The data sent is incorrect.",
                status_code=200
            )
    except Exception as ex:
        logging.error('An error ocurred: %s', repr(ex))
        return func.HttpResponse("Function error ocurred.", status_code=200)

async def SendMessageEventHub(msg):
    try:
        TNH_EVENT_HUB_CONNECTION = os.environ.get("TNH_EVENT_HUB_CONNECTION")
        TNH_EVENT_HUB_NAME = os.environ.get("TNH_EVENT_HUB_NAME")
        producerClient = EventHubProducerClient.from_connection_string(conn_str=TNH_EVENT_HUB_CONNECTION, eventhub_name=TNH_EVENT_HUB_NAME)

        async with producerClient:
            # Create a batch.
            event_data_batch = await producerClient.create_batch()

            # Add events to the batch.
            event_data_batch.add(EventData(msg))
            logging.info(f"Message sent: {msg}")

            # Send the batch of events to the event hub.
            await producerClient.send_batch(event_data_batch)
    except Exception as ex:
        logging.error('An error ocurred: %s', repr(ex))