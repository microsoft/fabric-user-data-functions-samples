import logging
from datetime import datetime
from typing import Any
from azure.cosmos import CosmosClient
from azure.cosmos import exceptions

COSMOS_URI = "{my-cosmos-artifact-uri}"
DB_NAME = "{my-cosmos-artifact-name}" 
CONTAINER_NAME = "SampleData"

@udf.connection(argName="cosmosClient", audienceType="CosmosDB", cosmos_endpoint=COSMOS_URI)
@udf.function()
def update_product(cosmosClient: CosmosClient, categoryName: str, productId: str, newPrice: float) -> list[dict[str, Any]]:

    '''
    Description: 
        Read and update a single document in a Cosmos DB container.
        
        This function first reads the document using the read_item operation, updates the currentPrice property,
        appends a new entry to the priceHistory array, then replaces the document in the container.
        
        The read operation on a Cosmos container takes two parameters, a partition key value and the id of the item (document).
        `categoryName` is the partition key for this container. Example value is, "Computers, Laptops". The `productId` is also
        the value for the id field so we will use that as the parameter name.
        
        The update operation, takes the modified document as the body parameter and the id of the item to replace.
        
        To run this sample...
        1. Create a new Cosmos artifact, copy its name to DB_NAME variable above.
        2. Click on SampleData in Cosmos Home screen to create a container called, SampleData.
        3. Go to settings (gear icon), then Connection tab, copy the URI to COSMOS_URI variable above.

        Before running this function, go Library Management and add the azure-cosmos package, version 4.14.0 or later.
        
        # Example values to use when calling this function
        # categoryName = "Computers, Laptops"
        # productId = "77be013f-4036-4311-9b5a-dab0c3d022be"
        # newPrice = 2899.99

    Args:
    - cosmosClient (CosmosClient): The Cosmos DB client object.
    - categoryName: The partition key property for this container.
    - productId: The productId and id are the same value for products.
    - newPrice: The new current price to set for the product.

    Returns: 
    - list[dict[str, Any]]: JSON Object. List of dictionaries with string keys and values of Any type.
    '''

    try:
        # Get the database and container clients
        database = cosmosClient.get_database_client(DB_NAME)
        container = database.get_container_client(CONTAINER_NAME)

        # Get the current product document
        product = container.read_item(item=productId, partition_key=categoryName)

        # Update the product's price
        product["currentPrice"] = newPrice
        
        now = datetime.now().replace(microsecond=0)
        current_time_iso = now.isoformat()
        
        # Append to the price history
        product["priceHistory"].append({
            "date": current_time_iso,
            "price": newPrice
        })

        # Replace the document in the container
        container.replace_item(item=productId, body=product)

        return json.dumps(product)

    except exceptions.CosmosResourceNotFoundError as e:
        logging.error(f"Item not found in update_product: {e}")
        raise
    except exceptions.CosmosHttpResponseError as e:
        logging.error(f"Cosmos error in update_product: {e}")
        raise  
    except exceptions as e:
        logging.error(f"Unexpected error in update_product: {e}")
        raise