import datetime
# Select 'Manage connections' and add connections to an Event Schema Set item and a SQL Database
# Replace the alias "<My Event Schema Set Alias>" with your Event Schema Set connection alias.
# Replace the alias "<My SQL Database Alias>" with your SQL Database connection alias.
@udf.connection(argName="businessEventsClient", alias="<My Event Schema Set Alias>")
@udf.connection(argName="sqlDB", alias="<My SQL Database Alias>")
@udf.function()
def query_and_publish_inventory_event(businessEventsClient: fn.FabricBusinessEventsClient, sqlDB: fn.FabricSqlConnection, threshold: int = 10) -> str:
    '''
    Description: Query inventory data from a SQL Database and publish business events for low stock items.
    
        This sample demonstrates how to combine a SQL Database connection with Business Events
        to query data and publish multiple events in a single batch. This pattern is useful for
        data-driven event publishing scenarios like inventory alerts, threshold notifications,
        or data change events where you need to efficiently publish many events at once.
        
        Pre-requisites:
            * Create a Business Events in Microsoft Fabric with an event type (e.g., "inventory.low_stock")
            * Create a SQL Database with an Inventory table containing columns: ProductId, ProductName, StockLevel
            * Add connections to both the Schema Set item and the SQL Database in your User Data Function
    
    Args:
        businessEventsClient (fn.FabricBusinessEventsClient): Fabric Business Events connection client
            used to publish events to the Business Events item.
        sqlDB (fn.FabricSqlConnection): Fabric SQL Database connection
            used to query inventory data.
        threshold (int): Stock level threshold. Products with stock below this value will trigger an event.
            Defaults to 10.

    Returns:
        str: Summary message indicating how many low stock events were published.

    Workflow:
        1. Connect to the SQL Database.
        2. Query the Inventory table for products with stock below the threshold.
        3. Build a list of event data for all low stock products.
        4. Publish all events in a single batch call.
        5. Return a summary of events published.
        
    Example:
        query_and_publish_inventory_event(businessEventsClient, sqlDB, threshold=5) 
        returns "Published 3 low stock events for products below threshold of 5"
    '''
    
    # Connect to the SQL Database
    connection = sqlDB.connect()
    cursor = connection.cursor()
    
    # Query for products with low stock
    query = f"""
        SELECT ProductId, ProductName, StockLevel 
        FROM dbo.Inventory 
        WHERE StockLevel < {threshold}
    """
    cursor.execute(query)
    
    # Process results and build event data list
    rows = [row for row in cursor]
    column_names = [col[0] for col in cursor.description]
    
    # Build a list of events for batch publishing
    events_list = []
    
    for row in rows:
        # Build the event data from query results
        product_data = {}
        for col_name, value in zip(column_names, row):
            if isinstance(value, (datetime.date, datetime.datetime)):
                value = value.isoformat()
            product_data[col_name] = value
        
        # Add event to the list
        event_data = {
            "productId": product_data.get("ProductId"),
            "productName": product_data.get("ProductName"),
            "currentStock": product_data.get("StockLevel"),
            "threshold": threshold,
            "alertTimestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        events_list.append(event_data)
    
    # Publish all events in a single batch call
    if events_list:
        businessEventsClient.PublishEvent(
            type="inventory.low_stock", 
            event_data=events_list, 
            data_version="V1"
        )
    
    # Close the connection
    cursor.close()
    connection.close()
    
    return f"Published {len(events_list)} low stock events for products below threshold of {threshold}"
