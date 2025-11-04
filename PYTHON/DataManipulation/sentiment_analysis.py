
from textblob import TextBlob

@udf.function()
def analyze_sentiment(text: str) -> str:
    '''
    Description: Analyze sentiment of input text using TextBlob and classify as Happy, Sad, or Neutral.

    Args:
    - text (str): Input text string to analyze for sentiment

    Returns: str: Formatted message with text and sentiment classification (Happy/Sad/Neutral)
    '''
    
    sentimentscore= TextBlob(text).sentiment.polarity
    sentiment= "N/A"
    # Classify sentiment based on polarity value
    if sentimentscore > 0.1:
        sentiment= "Happy"
    elif sentimentscore < -0.1:
        sentiment="Sad"
    else:
        sentiment="Neutral"

    return f"Sentiment for {text} is {sentiment}"
