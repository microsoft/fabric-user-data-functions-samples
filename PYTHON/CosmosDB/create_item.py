import logging
from typing import Any
from datetime import datetime, timezone
from fabric.functions.cosmosdb import get_cosmos_client
from azure.cosmos import exceptions

@udf.generic_connection(argName="cosmosDb", audienceType="CosmosDB")
@udf.function()
def insert_product(cosmosDb: fn.FabricItem) -> list[dict[str, Any]]:

    '''
    Description: 
        Insert a new single document item into in a container for a Cosmos DB artifact.
        
        To run this sample, create a new Cosmos artifact, then click on SampleData in Cosmos Home screen.
        Next go to settings (gear icon), then Connection tab, and copy the URI to COSMOS_DB_URI variable below.
        Copy the artifact name to DB_NAME variable below. The Sample Data will create a SampleData container.

        Before running this function, go Library Management and add the azure-cosmos package, version 4.14.0 or later.

    Args:
    - cosmosDb (fn.FabricItem): The Cosmos DB connection information.

    Returns: 
    - list[dict[str, Any]]: JSON Object. List of dictionaries with string keys and values of Any type.
    '''

    COSMOS_DB_URI = "{my-cosmos-artifact-uri}"
    DB_NAME = "{my-cosmos-artifact-name}" 
    CONTAINER_NAME = "SampleData"

    try:
        cosmosClient = get_cosmos_client(cosmosDb, COSMOS_DB_URI)
        database = cosmosClient.get_database_client(DB_NAME)
        container = database.get_container_client(CONTAINER_NAME)

        # A new GUID for document id and product id
        productId = "8a82f850-a33b-4734-80ce-740ba16c39f1"  # = str(uuid.uuid4())
        
        # Get current date and time in ISO8601 format
        iso_now = datetime.now(timezone.utc).isoformat() + "Z"
        
        # Create the product document
        product = {
            "id": productId,
            "docType": "product",
            "productId": productId,
            "name": "UnSmart Phone",
            "description": "The UnSmart Phone features a SlackDragon CPU, CRT display, 8KB RAM, 4MB storage, and no camera. With big buttons and fat case, it is designed for people who only make phone calls.",
            "categoryName": "Devices, Smartphones",
            "countryOfOrigin": "China",
            "inventory": 279,
            "firstAvailable": iso_now,
            "currentPrice": 99.93,
            "priceHistory": [
                {
                    "date": iso_now,
                    "price": 99.93
                }
            ]
        }

        return container.create_item(body=product)

    except exceptions.CosmosResourceExistsError as e:
        logging.error(f"Cosmos error in insert_product: {e}")
        raise
    except exceptions.CosmosHttpResponseError as e:
        logging.error(f"Cosmos error in insert_product: {e}")
        raise
    except exceptions as e:
        logging.error(f"Unexpected error in insert_product: {e}")
        raise