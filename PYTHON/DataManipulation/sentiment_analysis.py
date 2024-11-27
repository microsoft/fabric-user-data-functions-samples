# This sample allows you to calculate the sentiment of given text that is passed to the function as input.
# Complete these steps before testing this funtion 
#   1. Select library management and add textblob library 

from textblob import TextBlob

@udf.function()
def analyze_sentiment(text: str) -> str:
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
