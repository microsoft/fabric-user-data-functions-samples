# Select 'Manage connections' and add a connection to an Event Schema Set item
# Replace the alias "<My Event Schema Set Alias>" with your connection alias.
import fabric.functions as fn

udf = fn.UserDataFunctions()

@udf.connection(argName="businessEventsClient", alias="<My Event Schema Set Alias>")
@udf.function()
def publish_order_shipped_event(businessEventsClient: fn.FabricBusinessEventsClient, orderId: str, deliverAddress: str) -> str:
    '''
    Description: Publish a business event to notify downstream systems when an order has shipped.
    
        This sample demonstrates how to use the FabricBusinessEventsClient to publish 
        business events that can be consumed by other applications and services.
        
        Pre-requisites:
            * Create a Business Events in Microsoft Fabric
            * Add a connection to the Schema Set where the Business Events item is defined in your User Data Function
            * Define the event schema/type in your Business Events item (e.g., "order.shipped")
    
    Args:
        businessEventsClient (fn.FabricBusinessEventsClient): Fabric Business Events connection client
            used to publish events to the Business Events item.
        orderId (str): The unique identifier for the order that has shipped.
        deliverAddress (str): The delivery address for the shipped order.

    Returns:
        str: Confirmation message indicating the event was published successfully.

    Workflow:
        1. Prepare the event data payload with the provided order information.
        2. Use the PublishEvent method to publish the event.
        3. Return a confirmation message.
        
    Example:
        publish_order_shipped_event(businessEventsClient, orderId="12345", deliverAddress="123 Main St") 
        returns "Event 'order.shipped' published successfully for order 12345"
    '''
    
    # Prepare the event data payload
    event_data = {
        "orderId": orderId,
        "status": "shipped",
        "orderDeliverAddress": deliverAddress
    }
    
    # Publish the business event
    businessEventsClient.PublishEvent(
        type="order.shipped", 
        event_data=event_data, 
        data_version="V1"
    )
    
    return f"Event 'order.shipped' published successfully for order {orderId}"
