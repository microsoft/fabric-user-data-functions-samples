import logging
from typing import Any
from fabric.functions.cosmosdb import get_cosmos_client
from azure.cosmos import exceptions

@udf.generic_connection(argName="cosmosDb", audienceType="CosmosDB")
@udf.function()
def get_product(cosmosDb: fn.FabricItem, categoryName: str, productId: str) -> list[dict[str, Any]]:

    '''
    Description: 
        Get a single document from a Cosmos DB container.
        
        The read operation on a Cosmos container takes two parameters, a partition key value and the id of the item (document).
        The property used as the partition key for this container is `categoryName`. An example value would be, "Computers, Laptops".
        For products in this container, the id value is the same as its `productId` so we will use that as the parameter name.
        
        To run this sample, create a new Cosmos artifact, then click on SampleData in Cosmos Home screen.
        Next go to settings (gear icon), then Connection tab, and copy the URI to COSMOS_DB_URI variable below.
        Copy the artifact name to DB_NAME variable below. The Sample Data will create a SampleData container.

        Before running this function, go Library Management and add the azure-cosmos package, version 4.14.0 or later.
        
        # Example values to use when calling this function
        # categoryName = "Computers, Laptops"
        # productId = "77be013f-4036-4311-9b5a-dab0c3d022be"

    Args:
    - cosmosDb (fn.FabricItem): The Cosmos DB connection information.
    - categoryName: The partition key property for this container.
    - productId: The productId and id are the same value for products.

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

        # Read a single item
        product = container.read_item(item=productId, partition_key=categoryName)

        return product

    except exceptions.CosmosResourceNotFoundError as e:
        logging.error(f"Item not found in get_product: {e}")
        raise
    except exceptions as e:
        logging.error(f"Unexpected error in get_product: {e}")
        raise