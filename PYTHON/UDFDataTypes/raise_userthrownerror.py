# This sample function raises a UserThrownError if the age given is less than 18.
# The UserThrownError is a special type of error that can be used (or extended with a custom error class)
# to make an invocation fail and allow context to be provided about why the function failed.

import datetime

@udf.function()
def raise_userthrownerror(age: int)-> str:
    '''
    Description: Validate user age and return welcome message or raise error for underage users.
    
    Args:
        age (int): User's age to validate.
    
    Returns:
        str: Welcome message with current timestamp.
        
    Raises:
        fn.UserThrownError: If age is less than 18.
        
    Example:
        raise_userthrownerror(25) -> "Welcome to Fabric Functions at 2025-07-01 10:30:00!"
        raise_userthrownerror(16) -> Raises UserThrownError
    '''
    if age < 18:
        raise fn.UserThrownError("You must be 18 years or older to use this service.", {"age": age})

    return f"Welcome to Fabric Functions at {datetime.datetime.now()}!"
