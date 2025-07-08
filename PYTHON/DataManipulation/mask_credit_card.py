
@udf.function()
def mask_credit_card(cardnumber: int) -> str:
    '''
    Description: Mask credit card number showing only the last 4 digits.

    Args:
    - cardnumber (int): Credit card number to be masked

    Returns: str: Masked credit card number with asterisks except last 4 digits
    '''
    # Convert the card number to a string
    numberstr = str(cardnumber)

    # Check if the card number is valid
    if not numberstr.isdigit() or not (13 <= len(numberstr) <= 19):
        raise ValueError("Invalid credit card number")
    
    # Mask all but the last four digits
    masked_number = '*' * (len(numberstr) - 4) + numberstr[-4:]
    
    return str(masked_number)
