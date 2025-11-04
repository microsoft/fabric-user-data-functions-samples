from sklearn.feature_extraction.text import CountVectorizer

@udf.function()
def vectorize_string(text: str) -> str:
    '''
    Description: Vectorize a string of text using CountVectorizer and return vectorized representation.

    Args:
    - text (str): Input text string to be vectorized

    Returns: str: Formatted string containing vectorized text array and feature names
    '''
    try:
        # Initialize the CountVectorizer
        vectorizer = CountVectorizer()
        
        # Fit and transform the input text to vectorize it
        vectorized_text = vectorizer.fit_transform([text])
        vectors = ''.join(str(x) for x in vectorized_text.toarray())
        featurenames= " ,".join(str(x) for x in vectorizer.get_feature_names_out())
        print("Vectorized text:\n", vectorized_text.toarray())
        print("Feature names:\n",vectorizer.get_feature_names_out())
        return "vectorized_text: " + vectors + "\nfeature_names: " + featurenames
    except Exception as e:
        return "An error occurred during vectorization: " + str(e)
