
# Predicting the age of the user using API Vkontakte

**The data was load using API of the site, then it was transformed and cleaned from unnecessary information. The data from users post was also be loaded and concatenated with other profiles data to make a prediction about the age. Then CountVectorizer and TfidfVectorizer (with different ngram_range) were used to predict the user age. The results were stored in a separate file. The evaluation metric was ROC_AUC.**

*Used technologies:*
- Python, Pandas, Numpy;
- Matplotlib;
- Scikit-learn (LogisticRegression, CountVectorizer, TfidfVectorizer);
