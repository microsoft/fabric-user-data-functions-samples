# Select 'Manage connections' and add connections to an Event Schema Set item and a Lakehouse
# Replace the alias "<My Event Schema Set Alias>" with your Event Schema Set connection alias.
# Replace the alias "<My Lakehouse Alias>" with your Lakehouse connection alias.
import datetime
import json

@udf.connection(argName="businessEventsClient", alias="<My Event Schema Set Alias>")
@udf.connection(argName="myLakehouse", alias="<My Lakehouse Alias>")
@udf.function()
def publish_sale_summary_event(businessEventsClient: fn.FabricBusinessEventsClient, myLakehouse: fn.FabricLakehouseClient, customerKey: int, saleKey: int, salesPersonKey: int) -> str:
    '''
    Description: Query sale data from a lakehouse and publish a business event with the sale summary.
    
        This sample demonstrates how to query sales data from a Lakehouse, aggregate it by 
        stock item, and publish a business event containing the sale summary. This pattern 
        is useful for order confirmation notifications, sales reporting events, or invoice 
        generation triggers.
        
        Pre-requisites:
            * Create a Business Events item in Microsoft Fabric with an event type (e.g., "sale.summary")
            * Create a Lakehouse and start with sample data "Sales Data"
            * Add connections to both the Event Schema Set item and the Lakehouse in your User Data Function
    
    Args:
        businessEventsClient (fn.FabricBusinessEventsClient): Fabric Business Events connection client
            used to publish events to the Business Events item.
        myLakehouse (fn.FabricLakehouseClient): Fabric Lakehouse connection client
            used to query sale data.
        customerKey (int): The customer identifier to filter sales.
        saleKey (int): The sale identifier to filter sales.
        salesPersonKey (int): The sales person identifier to filter sales.

    Returns:
        str: Summary message indicating the event was published with item count.

    Workflow:
        1. Connect to the Lakehouse SQL endpoint.
        2. Query the fact_sale table filtering by customerKey, saleKey, and salesPersonKey.
        3. Aggregate the results by StockItemKey, summing quantities and totals.
        4. Publish a business event with the sale summary details.
        5. Return a confirmation message.
        
    Example:
        publish_sale_summary_event(businessEventsClient, myLakehouse, customerKey=100, saleKey=5001, salesPersonKey=25) 
        returns "Published sale summary event for sale 5001 with 3 line items totaling $1,234.56"
    '''
    
    # Connect to the Lakehouse SQL Endpoint
    connection = myLakehouse.connectToSql()
    cursor = connection.cursor()
    
    # Query and aggregate sale items by StockItemKey
    query = f"""
        SELECT 
            StockItemKey,
            Description,
            SUM(Quantity) AS TotalQuantity,
            SUM(TotalIncludingTax) AS TotalPrice
        FROM dbo.fact_sale 
        WHERE CustomerKey = {customerKey}
          AND SaleKey = {saleKey}
          AND SalespersonKey = {salesPersonKey}
        GROUP BY StockItemKey, Description
    """
    cursor.execute(query)
    
    # Process results into line items
    rows = cursor.fetchall()
    
    line_items = []
    grand_total = 0.0
    
    for row in rows:
        stock_item_key, description, total_quantity, total_price = row
        
        line_item = {
            "stockItemKey": stock_item_key,
            "description": description,
            "totalQuantity": int(total_quantity),
            "totalPrice": float(total_price)
        }
        line_items.append(line_item)
        grand_total += float(total_price)
    
    # Build the event data payload
    event_data = {
        "saleKey": saleKey,
        "customerKey": customerKey,
        "salesPersonKey": salesPersonKey,
        "lineItems": json.dumps(line_items),
        "lineItemCount": len(line_items),
        "grandTotal": grand_total,
        "eventTimestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    
    # Publish the business event
    businessEventsClient.PublishEvent(
        type="sale.summary", 
        event_data=event_data, 
        data_version="V1"
    )
    
    # Close the connection
    cursor.close()
    connection.close()
    
    return f"Published sale summary event for sale {saleKey} with {len(line_items)} line items totaling ${grand_total:,.2f}"
