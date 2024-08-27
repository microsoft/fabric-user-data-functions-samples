
# This sample vectorizes a string or text 
from sklearn.feature_extraction.text import CountVectorizer

@app.function("vectorize_string")
def vectorize_string(text: str)-> fabric.functions.UdfResponse:
    """
    Vectorizes a string using CountVectorizer from sklearn.

    :param text: The string to be vectorized.
    :return: A vectorized representation of the string as a sparse matrix.
    """
    try:
        # Initialize the CountVectorizer
        vectorizer = CountVectorizer()
        
        # Fit and transform the input text to vectorize it
        vectorized_text = vectorizer.fit_transform([text])
        vectors = ''.join(str(x) for x in vectorized_text.toarray())
        featurenames= " ,".join(str(x) for x in vectorizer.get_feature_names_out())
        print("Vectorized text:\n", vectorized_text.toarray())
        print("Feature names:\n",vectorizer.get_feature_names_out())
        return fabric.functions.UdfResponse("vectorized_text: " + vectors + "\nfeature_names: " + featurenames)
    except Exception as e:
        return fabric.functions.UdfResponse("An error occurred during vectorization:", e)