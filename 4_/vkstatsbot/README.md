
# Predicting the age of the user using API Vkontakte

**The data was load using API of the site, then it was transformed and cleaned from unnecessary features. The data from user's post was also be loaded and concatenated with other profile information to make a preditcion about the age of user. Then CountVectorizer and TfidfVectorizer (with different ngram_range) model was used to predict the user age. The results was stored in separate file. The evaluation metric was ROC_AUC.**

*Used technologies:*
- Python, Pandas, Numpy;
- Matplotlib;
- Scikit-learn (LogisticRegression, CountVectorizer, TfidfVectorizer);
