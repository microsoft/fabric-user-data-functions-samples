
@udf.function()
def mask_credit_card(card_number: int) -> str:
    '''
    Description: Mask credit card number showing only the last 4 digits.

    Args:
    - card_number (int): Credit card number to be masked

    Returns: str: Masked credit card number with asterisks except last 4 digits
    '''
    # Convert the card number to a string
    card_number_str = str(card_number)
    
    # Check if the card number is valid
    if not card_number_str.isdigit() or not (13 <= len(card_number_str) <= 19):
        raise ValueError("Invalid credit card number")
    
    # Mask all but the last four digits
    masked_number = '*' * (len(card_number_str) - 4) + card_number_str[-4:]
    
    return str(masked_number)
