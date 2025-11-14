import logging
from typing import Any
from fabric.functions.cosmosdb import get_cosmos_client
from azure.cosmos import exceptions
from openai import AzureOpenAI

@udf.generic_connection(argName="cosmosDb", audienceType="CosmosDB")
@udf.function()
def product_vector_search(cosmosDb: fn.FabricItem, searchtext: str, similarity: float, limit: int) -> list[dict[str, Any]]:

    '''
    Description: 
        Vector search for multiple items from a Cosmos DB container.

        A vector search on a Cosmos container is a query that includes the `VectorDistance` system function. The VectorDistance 
        function takes two parameters, the property containing the vector embeddings and the embeddings themselves.
        
        The query operation on a Cosmos container takes three parameters, the query text, an array of parameters and a 
        boolean indicating whether to enable cross-partition query. Since we are not the partition key, `categoryName` 
        as a filter predicate for this query, we have to set `enable_cross_partition_query=True`.
        
        The `docType` property is used to distinguish between product and review documents in the container. If you want to 
        query for one or more products and all their reviews, you can omit that parameter to get the reviews as well.
        
        # Example values to use when calling this function
        # searchText = "gaming pc"
        # similarity = 0.824
        # limit = 5
        
        To run this sample, create a new Cosmos artifact, then click on SampleVectorData in Cosmos Home screen.
        This will create a container with vectorized data, generated with the text-embedding-ada-002 model.
        Next go to settings (gear icon), then Connection tab, and copy the URI to COSMOS_DB_URI variable below.
        Copy the artifact name to DB_NAME variable below. The Sample Data will create a SampleData container.

        Next, create an Azure OpenAI account and deploy the text-embedding-ada-002 model, and copy the endpoint 
        and key to the variables below in the generate_embeddings function. Be sure to validate the API version in the
        AI Foundry portal when deploying the model.

        Before running this function, go Library Management and add the azure-cosmos package, version 4.14.0 or later.
        and openai package, version 2.3.0 or later.

    Args:
    - cosmosDb (fn.FabricItem): The Cosmos DB connection information.
    - searchtext (str): The text to generate embeddings for vector search.
    - similarity (float): The minimum similarity score for results.
    - limit (int): The maximum number of results to return.

    Returns: 
    - list[dict[str, Any]]: JSON object. List of dictionaries with string keys and values of Any type.
    '''

    COSMOS_DB_URI = "{my-cosmos-artifact-uri}"
    DB_NAME = "{my-cosmos-artifact-name}" 
    CONTAINER_NAME = "SampleVectorData"

    try:
        cosmosClient = get_cosmos_client(cosmosDb, COSMOS_DB_URI)
        database = cosmosClient.get_database_client(DB_NAME)
        container = database.get_container_client(CONTAINER_NAME)

        # Generate embeddings for the search text
        embeddings = generate_embeddings(searchtext.strip())

        # Cosmos query with VectorDistance to perform similarity search
        query = """
            SELECT TOP @limit 
                VectorDistance(c.vectors, @embeddings) AS SimilarityScore,
                c.productId,
                c.categoryName,
                c.name, 
                c.description,
                c.currentPrice,
                c.inventory,
                c.priceHistory
            FROM c 
            WHERE 
                c.docType = @docType AND
                VectorDistance(c.vectors, @embeddings) >= @similarity
            ORDER BY 
                VectorDistance(c.vectors, @embeddings)
        """

        parameters = [
            {"name": "@limit", "value": limit},
            {"name": "@embeddings", "value": embeddings},
            {"name": "@docType", "value": "product"},
            {"name": "@similarity", "value": similarity}
        ]

        # Execute the query
        products = [p for p in container.query_items(
                query=query,
                enable_cross_partition_query=True,
                parameters=parameters
            )]

        # Always remove the vectors property if you accidentally project it
        # it is unnecessarily large and not needed in the results
        for p in products:
            p.pop('vectors', None)

        return products
    
    except exceptions.CosmosHttpResponseError as e:
        logging.error(f"Cosmos DB query failed: {e}")
        raise
    except exceptions as e:
        logging.error(f"Unexpected error in search_products: {e}")
        raise
    
# Generate embeddings on passed in text
def generate_embeddings(text: str) -> list[float]:

    OPENAI_URI = "{my-azure-openai-endpoint}"
    OPENAI_KEY = "{my-azure-openai-key}"
    OPENAI_API_VERSION = "2023-05-15"
    OPENAI_EMBEDDING_MODEL = "text-embedding-ada-002"
    OPENAI_EMBEDDING_DIMENSIONS = 1536

    try:
        # Initialize Azure OpenAI client
        OPENAI_CLIENT = AzureOpenAI(
            api_version=OPENAI_API_VERSION,
            azure_endpoint=OPENAI_URI,
            api_key=OPENAI_KEY
        )

        # Create embeddings
        response=OPENAI_CLIENT.embeddings.create(
            input=text, 
            model=OPENAI_EMBEDDING_MODEL
        )
        # Include dimensions when using models newer than text-embedding-ada-002
        #dimensions=OPENAI_EMBEDDING_DIMENSIONS
        
        embeddings = response.model_dump()
        return embeddings['data'][0]['embedding']

    except Exception as e:
        logging.error(f"Unexpected Error in generate_embeddings: {e}")
        raise