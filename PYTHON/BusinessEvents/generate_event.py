# Select 'Manage connections' and add a connection to an Event Definition item
# Replace the alias "<My Event Definition Alias>" with your connection alias.
import fabric.functions as fn

udf = fn.UserDataFunctions()

@udf.connection(argName="businessEventsClient", alias="<My Event Definition Alias>")
@udf.function()
def generate_order_shipped_event(businessEventsClient: fn.FabricBusinessEventsClient) -> str:
    '''
    Description: Generate a business event to notify downstream systems when an order has shipped.
    
        This sample demonstrates how to use the FabricBusinessEventsClient to publish 
        business events that can be consumed by other applications and services.
        
        Pre-requisites:
            * Create a Business Events item in Microsoft Fabric
            * Add a connection to the Schema Set where the Business Events item is defined in your User Data Function
            * Define the event schema/type in your Business Events item (e.g., "order.shipped")
    
    Args:
        businessEventsClient (fn.FabricBusinessEventsClient): Fabric Business Events connection client
            used to publish events to the Business Events item.

    Returns:
        str: Confirmation message indicating the event was generated successfully.

    Workflow:
        1. Prepare the event data payload with relevant order information.
        2. Use the GenerateEvent method to publish the event.
        3. Return a confirmation message.
        
    Example:
        generate_order_shipped_event(businessEventsClient) 
        returns "Event 'order.shipped' generated successfully for order 12345"
    '''
    
    # Prepare the event data payload
    event_data = {
        "orderId": "12345",
        "status": "shipped",
        "orderDeliverAddress": "123 Main St"
    }
    
    # Generate the business event
    businessEventsClient.GenerateEvent(
        type="order.shipped", 
        event_data=event_data, 
        version_id="V1"
    )
    
    return f"Event 'order.shipped' generated successfully for order {event_data['orderId']}"
