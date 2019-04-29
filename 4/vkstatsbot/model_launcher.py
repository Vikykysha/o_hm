
import pandas as pd
import numpy as np
import datetime
import time
import pickle
import time
import regex
import re
import emoji

from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import  LabelBinarizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score

from storages import ObjectStorage

class ModelLauncher(object):

    @classmethod
    def _get_avg_age_from_grad_dates(cls, row):
        grad_school = int(row['schools_grad_year'])
        grad_uni = int(row['graduation'])
        years = []
        if grad_uni > 0:
            for year in range(21, 27):
                years.append(year + 2019 - grad_uni)
        if grad_school > 0:
            for year in range(17, 20):
                years.append(year + 2019 - grad_school)
        if len(years) != 0:
            return int(np.mean(years))
        else:
            return 0
    @classmethod
    def _get_age_from_brthd(cls,val):
        if val != '':
            if len(val.split('.')) == 3:
                return 2019 - int(val[-4:])
        return 0

    @classmethod
    def _get_avg_completeness(cls,row):
        counter = 0
        #отдельно обрабатываем те столбцы, где отсутствие - это пустая строка или  "Не указано"
        for col in ['about', 'activities', 'books','games','home_town','interests','movies', 'music', 'quotes' ,'political', 'langs', 'religion', 'inspired_by',
        'people_main', 'life_main', 'smoking', 'alcohol']:
            if df[col] != '' and df[col] != 'Не указано':
                counter += 1
        #отдельно обрабатываем числовые признаки
        for col in ['has_mobile','political','relation','has_facebook', 'has_instagram', 'has_livejournal',
        'has_twitter', 'has_skype', 'has_relative']:
            if col != -1 and col != 0:
                counter += 1
        return np.round(counter/len(feat_completeness_lst),2)

    @classmethod
    def _count_tags(cls,text):
            text = ' '.join(text)
            return len(regex.findall('#', text))

    @classmethod
    def _count_emoji(cls,text):
        text = ' '.join(text)
        emoji_list = []
        data = regex.findall(r'\X', text)
        for word in data:
            if any(char in emoji.UNICODE_EMOJI for char in word):
                emoji_list.append(word)

        return len(emoji_list)


    @classmethod
    def make_post_for_model(cls,df_train, df_test, feature_post, params,CountVect=True ):
        if CountVect:
            vect = CountVectorizer()
            vect.set_params(**params)

            X_tfidf_train = vect.fit_transform(df_train[feature_post])
            X_tfidf_test = vect.transform(df_test[feature_post])
        else:
            vect = TfidfVectorizer()
            vect.set_params(**params)
            
            X_tfidf_train = vect.fit_transform(df_train[feature_post])
            X_tfidf_test = vect.transform(df_test[feature_post])

        return X_tfidf_train, X_tfidf_test

    @classmethod
    def prepare_model(cls,df_train, df_test, post_train, post_test):
        scaler = StandardScaler()
        df_train_for_model = df_train.drop('post',axis=1)
        df_test_for_model = df_test.drop('post',axis=1)

        X_train_short = pd.DataFrame(data=scaler.fit_transform(df_train_for_model.values), 
                                        index=df_train_for_model.index, 
                                        columns=df_train_for_model.columns)

        X_test_short = pd.DataFrame(data=scaler.transform(df_test_for_model.values), 
                                        index=df_test_for_model.index, 
                                        columns=df_test_for_model.columns)

        X_train_short_sparse = sparse.csr_matrix(X_train_short)
        X_test_short_sparse = sparse.csr_matrix(X_test_short)

        x_train = hstack([X_train_short_sparse, post_train])
        x_test = hstack([X_test_short_sparse, post_test])
        
        return x_train, x_test

    @classmethod
    def run_model(cls,model, x_train, x_test, y_train, y_test):
        model.fit(x_train, y_train)
        y_pred_proba = model.predict_proba(x_test)
        y_pred =  model.predict(x_test)
        lb = LabelBinarizer()
        lb.fit(y_test)
        y_test_n = lb.transform(y_test)
        y_pred_n = lb.transform(y_pred)
        return (model,roc_auc_score(y_test_n,y_pred_n,average='micro'),accuracy_score(y_test,y_pred))

    @classmethod
    def model_maker(cls,x_train, x_test, y_train, y_test, df_for_results):
        lst_vectorizers = []
        for ngram in [(1,1),(1,2),(1,3)]:
            for bols in [True,False]:
                X_vect_train, X_vect_test = cls.make_post_for_model(x_train, x_test, 'post', {'ngram_range':ngram, 'analyzer':'word', 'max_features':10000},bols )
                lst_vectorizers.append((X_vect_train, X_vect_test))
                name =  "logistic ngram range:{},CountVectorizer is {}".format(ngram,bols)
                df_for_results = df_for_results.append({'model_name' :name,'ROC-AUC':None ,
                                            'Accuracy, test' : None},ignore_index=True)
                
        models_lst = []
        model = LogisticRegression()
        for num,i in enumerate(lst_vectorizers):
            X_vect_train, X_vect_test = i[0],i[1]
            x_train, x_test = cls.prepare_model(X_train, X_test, X_vect_train, X_vect_test )
            model, auc, accuracy = cls.run_model(model, x_train, x_test, y_train, y_test)
            models_lst.append(model)
            df_for_results.loc[num,'ROC-AUC'] = auc
            df_for_results['ROC-AUC'] = df_for_results['ROC-AUC'].astype('float')
            df_for_results.loc[num,'Accuracy, test'] = accuracy
        best_model_index = df_for_results['ROC-AUC'].idxmax()
        #TODO сохранить табличку с резульататми, а затем протранкейтить 
        return models_lst[best_model_index]
        
    @classmethod
    def get_model(cls,df):
        df.loc[df['schools_grad_year'] == '','schools_grad_year'] = -1
        df['guessed_age'] = df.apply(cls._get_avg_age_from_grad_dates, axis=1)
        df['guessed_age'] = df['bdate'].apply(cls._get_age_from_brthd)
        rang_occupation = {'Не указано' : 0, 'work' : 3, 'university' : 2, 'school' : 1}
        df['occupation'].replace(rang_occupation, inplace=True)
        #признаки для анализа
        feat_completeness_lst = ['about', 'activities', 'books','games','has_mobile', 'home_town','interests','movies', 'music', 'quotes', 'relation', 'political', 'langs', 'religion', 'inspired_by',
       'people_main', 'life_main', 'smoking', 'alcohol','has_facebook', 'has_instagram', 'has_livejournal',
       'has_twitter', 'has_skype', 'has_relative']
        df['avg_completeness'] = df.apply(lambda x: cls._get_avg_completeness(x),axis=1)
        # общее число символов в постах
        df['post_length'] = df['post'].apply(lambda text: len(text))

        df['tags_number'] = df['post'].apply(lambda x: cls._count_tags(x))
        df['emoji_number'] = df['post'].apply(cls._count_emoji)
        #Готово, теперь оставим только те признаки, которые нам нужны
        df = df[['age_cat','followers_count','sex','post','guessed_age','avg_completeness','post_length','tags_number','emoji_number']].copy()

        le = LabelEncoder()
        y = le.fit_transform(df['age_cat'])
        df.drop('age_cat', axis=1, inplace=True)

        X_train, X_test, y_train, y_test = train_test_split(df, y, test_size=0.2,  shuffle=True, stratify=y, random_state=121)

        X_train.loc[:,'post'] = X_train['post'].apply(lambda x: ' '.join(x))
        X_test.loc[:,'post'] = X_test['post'].apply(lambda x: ' '.join(x))

        #Результаты будем записывать в таблицу
        table_results = pd.DataFrame(columns=['model_name', 'ROC-AUC' ,
                                     'Accuracy, test'])

        best_model = cls.model_maker(X_train, X_test, y_train, y_test, table_results)