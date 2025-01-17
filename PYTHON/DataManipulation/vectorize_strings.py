# This sample vectorizes a string or text which takes a string and returns a vectorized representation of the string. 
# Complete the following before testing this function
# 1. Select libraries management and add "scikit-learn" library 


from sklearn.feature_extraction.text import CountVectorizer

@udf.function()
def vectorize_string(text: str)-> str:
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
