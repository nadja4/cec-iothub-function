import azure.functions as func
import logging
import json

app = func.FunctionApp()

@app.event_hub_message_trigger(arg_name="azeventhub", event_hub_name="CECIoTHub",
                               connection="CECIoTHub_events_IOTHUB") 
@app.cosmos_db_output(arg_name="outputDocument", database_name="iothub-database", container_name="iothub-container", connection="CosmosDbConnectionSetting")
def iothubfunction(azeventhub: func.EventHubEvent, outputDocument: func.Out[func.Document]):
    event_data = azeventhub.get_body().decode('utf-8')
    logging.info('Python EventHub trigger processed an event: %s', event_data)

    # Umwandeln der Event-Daten in ein JSON-Objekt
    try:
        event_json = json.loads(event_data)
    except json.JSONDecodeError as e:
        logging.error('JSON decode error: %s', e)
        return

    # Hinzufügen eines eindeutigen Bezeichners für die Cosmos DB
    event_json['id'] = str(azeventhub.metadata['SequenceNumber'])

    # Schreiben der Daten in die Cosmos DB
    outputDocument.set(func.Document.from_dict(event_json))
