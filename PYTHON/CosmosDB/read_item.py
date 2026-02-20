import logging
from typing import Any
from azure.cosmos import CosmosClient
from azure.cosmos import exceptions

COSMOS_URI = "{my-cosmos-artifact-uri}"
DB_NAME = "{my-cosmos-artifact-name}" 
CONTAINER_NAME = "SampleData"

@udf.connection(argName="cosmosClient", audienceType="CosmosDB", cosmos_endpoint=COSMOS_URI)
@udf.function()
def get_product(cosmosClient: CosmosClient, categoryName: str, productId: str) -> list[dict[str, Any]]:

    '''
    Description: 
        Get a single document from a Cosmos DB container.
        
        The read operation on a Cosmos container takes two parameters, a partition key value and the id of the item (document).
        The property used as the partition key for this container is `categoryName`. An example value would be, "Computers, Laptops".
        For products in this container, the id value is the same as its `productId` so we will use that as the parameter name.
        
        To run this sample...
        1. Create a new Cosmos artifact, copy its name to DB_NAME variable above.
        2. Click on SampleData in Cosmos Home screen to create a container called, SampleData.
        3. Go to settings (gear icon), then Connection tab, copy the URI to COSMOS_URI variable above.

        Before running this function, go Library Management and add the azure-cosmos package, version 4.14.0 or later.
        
        # Example values to use when calling this function
        # categoryName = "Computers, Laptops"
        # productId = "77be013f-4036-4311-9b5a-dab0c3d022be"

    Args:
    - cosmosClient (CosmosClient): The Cosmos DB client object.
    - categoryName: The partition key property for this container.
    - productId: The productId and id are the same value for products.

    Returns: 
    - list[dict[str, Any]]: JSON Object. List of dictionaries with string keys and values of Any type.
    '''

    try:
        # Get the database and container
        database = cosmosClient.get_database_client(DB_NAME)
        container = database.get_container_client(CONTAINER_NAME)

        # Read a single item
        product = container.read_item(item=productId, partition_key=categoryName)

        return product

    except exceptions.CosmosResourceNotFoundError as e:
        logging.error(f"Item not found in get_product: {e}")
        raise
    except exceptions as e:
        logging.error(f"Unexpected error in get_product: {e}")
        raise