import logging
from typing import Any
from fabric.functions.cosmosdb import get_cosmos_client
from azure.cosmos import exceptions

@udf.generic_connection(argName="cosmosDb", audienceType="CosmosDB")
@udf.function()
def query_products(cosmosDb: fn.FabricItem, categoryName: str, productId: str, newPrice: float) -> list[dict[str, Any]]:

    '''
    Description: 
        Query for multiple items from a Cosmos DB container.

        The query operation on a Cosmos container takes three parameters, the query text, an array of parameters and a 
        boolean indicating whether to enable cross-partition query. Since we are using the partition key, `categoryName` 
        as a filter predicate for this query, we can set `enable_cross_partition_query=False` (this is the default). 
        Using the partition key in the query is essential to optimizing performance and reduce Fabric Capacity Units (CUs) consumed.
        The `docType` property is used to distinguish between product and review documents in the container. If you want to 
        query for one or more products and all their reviews, you can omit that parameter to get the reviews as well.
        
        # Example values to use when calling this function
        # categoryName = "Computers, Laptops"
        
        To run this sample, create a new Cosmos artifact, then click on SampleData in Cosmos Home screen.
        Next go to settings (gear icon), then Connection tab, and copy the URI to COSMOS_DB_URI variable below.
        Copy the artifact name to DB_NAME variable below. The Sample Data will create a SampleData container.

        Before running this function, go Library Management and add the azure-cosmos package, version 4.14.0 or later.

    Args:
    - cosmosDb (fn.FabricItem): The Cosmos DB connection information.
    - categoryName: The filter predicate for our query and the partition key property for this container.

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

        # Use parameterized query
        query = """
            SELECT
                c.categoryName,
                c.name, 
                c.description,
                c.currentPrice,
                c.inventory,
                c.priceHistory
            FROM c 
            WHERE 
                c.categoryName = @categoryName AND
                c.docType = @docType
            ORDER BY
                c.price DESC
        """

        parameters = [
            {"name": "@categoryName", "value": categoryName},
            {"name": "@docType", "value": 'product'}
        ]

        # Execute the query
        products = [p for p in container.query_items(
            query=query,
            enable_cross_partition_query=False,
            parameters=parameters
        )]

        return products
    
    except exceptions.CosmosHttpResponseError as e:
        logging.error(f"Cosmos DB query failed: {e}")
        raise
    except exceptions as e:
        logging.error(f"Unexpected error in search_products: {e}")
        raise