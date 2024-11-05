# This sample allows you to read data from Azure SQL database 
# Complete these steps before testing this funtion 
#   1. Select library management and add textblob library 

from textblob import TextBlob

@app.function("analyze_sentiment")
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
