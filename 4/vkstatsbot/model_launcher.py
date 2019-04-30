
import pandas as pd
import logging
import numpy as np
import datetime
import time
import pickle
import time

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import  LabelBinarizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from scipy.sparse import csr_matrix, hstack

from process_data import DataFramePreprocessor
from storages import ObjectStorage

logger = logging.getLogger(__name__)

class ModelLauncher(object):

    
    @classmethod
    def make_post_for_model(cls,df_train, df_test, feature_post, params,CountVect=True ):
        """
        Подготавливает признак посты пользователей к возможности использовать в модели. (Строит векторайзер, разреженную матрицу мешка слов)
        Если CountVect=True - будет испольовать в качестве векторного представления CountVectorizer иначе - TfidfVectorizer
        """
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
        """
        Возращает обучащющую и тестовую выборки для работы в модели
        """
        scaler = StandardScaler()
        df_train_for_model = df_train.drop('post',axis=1)
        df_test_for_model = df_test.drop('post',axis=1)

        X_train_short = pd.DataFrame(data=scaler.fit_transform(df_train_for_model.values), 
                                        index=df_train_for_model.index, 
                                        columns=df_train_for_model.columns)

        X_test_short = pd.DataFrame(data=scaler.transform(df_test_for_model.values), 
                                        index=df_test_for_model.index, 
                                        columns=df_test_for_model.columns)

        X_train_short_sparse = csr_matrix(X_train_short)
        X_test_short_sparse = csr_matrix(X_test_short)

        x_train = hstack([X_train_short_sparse, post_train])
        x_test = hstack([X_test_short_sparse, post_test])
        
        return x_train, x_test

    @classmethod
    def run_model(cls,model, x_train, x_test, y_train, y_test):
        """
        Прогоняет модели и получает метрики
        """
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
        """
        Прогоняет модели и получает метрики
        """
        lst_vectorizers = []
        for ngram in [(1,1),(1,2),(1,3)]:
            for bols in [True,False]:
                X_vect_train, X_vect_test = cls.make_post_for_model(x_train, x_test, 'post', {'ngram_range':ngram, 'analyzer':'word', 'max_features':10000},bols )
                lst_vectorizers.append((X_vect_train, X_vect_test))
                name =  "logistic ngram range:{},CountVectorizer is {}".format(ngram,bols)
                df_for_results = df_for_results.append({'model_name' :name,'ROC-AUC':None ,
                                            'Accuracy, test' : None},ignore_index=True)
                
        models_lst = []
        model = LogisticRegression( multi_class  = 'auto',C=1000)
        for num,i in enumerate(lst_vectorizers):
            X_vect_train, X_vect_test = i[0],i[1]
            x_train_, x_test_ = cls.prepare_model(x_train, x_test, X_vect_train, X_vect_test )
            model, auc, accuracy = cls.run_model(model, x_train_, x_test_, y_train, y_test)
            models_lst.append(model)
            df_for_results.loc[num,'ROC-AUC'] = auc
            df_for_results['ROC-AUC'] = df_for_results['ROC-AUC'].astype('float')
            df_for_results.loc[num,'Accuracy, test'] = accuracy
        best_model_index = df_for_results['ROC-AUC'].idxmax()
        #сохраняем таблицу с резульататми
        ObjectStorage().save_obj(df_for_results, 'df_with_model_results')
        #выводим таблицу с результатами на экран
        print(df_for_results)

        return models_lst[best_model_index]
        
    @classmethod
    def get_model(cls,df):
        """
        Запускает методы обработки данных для модели, построение модели и получения результатов
        """
        logger.info('Запускаем работу модели')
        try:
            df = DataFramePreprocessor.prepare_df_for_model(df)
            
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

            #сохраняем лучшую модель
            ObjectStorage().save_obj(best_model, 'best_logisticregre_model')

            logger.info('Результаты работы моделей получены и сохранены. Лучшая модель сохранена.')

            return best_model
        except  Exception as ex:
            logger.exception(ex)
