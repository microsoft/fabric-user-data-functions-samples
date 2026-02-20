# Select 'Manage connections' and add a connection to an Event Schema Set item
# Replace the alias "<My Event Schema Set Alias>" with your connection alias.
import fabric.functions as fn

udf = fn.UserDataFunctions()

@udf.connection(argName="businessEventsClient", alias="<My Event Schema Set Alias>")
@udf.function()
def publish_custom_event(
    businessEventsClient: fn.FabricBusinessEventsClient,
    # Add your custom parameters below - these are examples you can replace:
    param1: str,
    param2: int
) -> str:
    '''
    Description: Publish a custom business event with your own event type and properties.
    
        This sample provides a template for publishing business events using the 
        FabricBusinessEventsClient. Customize the event type, properties, and parameters
        to match your specific business scenario.
        
        Pre-requisites:
            * Create a Business Events item in Microsoft Fabric
            * Add a connection to the Schema Set where the Business Events item is defined
            * Define your event schema/type in your Business Events item
    
    Args:
        businessEventsClient (fn.FabricBusinessEventsClient): Fabric Business Events connection client
            used to publish events to the Business Events item.
        param1 (str): Replace with your first custom parameter.
        param2 (int): Replace with your second custom parameter.

    Returns:
        str: Confirmation message indicating the event was published successfully.

    Workflow:
        1. Prepare the event data payload with your custom properties.
        2. Use the PublishEvent method to publish the event.
        3. Return a confirmation message.
        
    Example:
        publish_custom_event(businessEventsClient, param1="value1", param2=123) 
        returns "Event '<your.event.type>' published successfully"
    '''
    
    # Prepare the event data payload
    # Replace these properties with your own event schema properties
    event_data = {
        "property1": param1,       # Replace with your property name
        "property2": param2,       # Replace with your property name
        "property3": "value3"      # Add more properties as needed
    }
    
    # Publish the business event
    # Replace "<your.event.type>" with your actual event type (e.g., "order.shipped", "inventory.updated")
    businessEventsClient.PublishEvent(
        type="<your.event.type>", 
        event_data=event_data, 
        data_version="V1"
    )
    
    return "Event '<your.event.type>' published successfully"
