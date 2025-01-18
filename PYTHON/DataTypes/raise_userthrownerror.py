# This sample function raises a UserThrownError if the age given is less than 18.
# The UserThrownError is a special type of error that can be used (or extended with a custom error class)
# to make an invocation fail and allow context to be provided about why the function failed.

import datetime

@udf.function()
def raise_userthrownerror(age: int)-> str:
    if age < 18:
        raise fn.UserThrownError("You must be 18 years or older to use this service.", {"age": age})

    return f"Welcome to Fabric Functions at {datetime.datetime.now()}!"