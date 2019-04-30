import re
import string
import random
import logging
import regex

import pandas as pd
import numpy as np
import emoji

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from vk_api_method_handler import VKApiConnector
from storages import ObjectStorage

logger = logging.getLogger(__name__)

class DataFrameHelperFunctions(object):


    @classmethod
    def _get_avg_age_from_grad_dates(cls, row):
        """
        Генерит примерные возраст из даты окончания школы и института
        """
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
        """
        Генерит примерные возраст из даты рождения
        """
        if val != '':
            if len(val.split('.')) == 3:
                return 2019 - int(val[-4:])
        return 0

    @classmethod
    def _get_avg_completeness(cls,row,feat_completeness_lst):
        """
        Формирует признак средней заполняемости аккаунта
        """
        counter = 0
        #отдельно обрабатываем те столбцы, где отсутствие - это пустая строка или  "Не указано"
        for col in ['about', 'activities', 'books','games','home_town','interests','movies', 'music', 'quotes' ,'political', 'langs', 'religion', 'inspired_by',
        'people_main', 'life_main', 'smoking', 'alcohol']:
            if row[col] != '' and row[col] != 'Не указано':
                counter += 1
        #отдельно обрабатываем числовые признаки
        for col in ['has_mobile','political','relation','has_facebook', 'has_instagram', 'has_livejournal',
        'has_twitter', 'has_skype', 'has_relative']:
            if row[col] != -1 and row[col] != 0:
                counter += 1
        return np.round(counter/len(feat_completeness_lst),2)

    @classmethod
    def _count_tags(cls,text):
        """
        Считает количество тэгов в постах
        """
        text = ' '.join(text)
        return len(regex.findall('#', text))

    @classmethod
    def _count_emoji(cls,text):
        """
        Считает количество эмоджи в постах
        """
        text = ' '.join(text)
        emoji_list = []
        data = regex.findall(r'\X', text)
        for word in data:
            if any(char in emoji.UNICODE_EMOJI for char in word):
                emoji_list.append(word)

        return len(emoji_list)

    @classmethod
    def _add_post(cls,row,user_clean_doc_dict):
        uid = row['id']
        try:
            post = user_clean_doc_dict[uid]
            return post
        except Exception:
            return []

    @classmethod
    def _fill_missing_str(cls):
        return '' 
    
    @classmethod
    def _fill_missing_num(cls):
        return -1

    @classmethod
    def _get_city_name_through_api(cls, city_dict, vk_connector):
        """Способ достать название города через обращение к базе.Неэффективен, так как требует больше времени"""
        return vk_connector.get_city(city_dict)

    @classmethod
    def _get_city_name(cls, city_dict):
        if city_dict != '':
            return city_dict['title']
        return ''

    @classmethod
    def _get_country(cls, country_dict):
        if country_dict != '':
            return country_dict['title']
        return ''

    @classmethod
    def _deal_with_personal_col(cls, df):
        #Creating cols for extracting personal info
        for col in [ 'political' ,'langs', 'religion', 'inspired_by' ,'people_main', 'life_main', 'smoking',  'alcohol' ]:
            df[col] = ''
            df[col] = df[col].astype('object')
        #Possible fields: political langs religion inspired_by people_main life_main smoking  alcohol
        for x in df.index.values:
            if df.loc[x,'personal'] != '':
                col = df.loc[x,'personal']
                political = col.get('political')
                langs = col.get('langs')
                religion = col.get('religion')
                inspired_by = col.get('inspired_by')
                people_main = col.get('people_main')
                smoking = col.get('smoking')
                alcohol = col.get('alcohol')
                if political:
                    df.loc[x,'political'] = political
                if langs:
                    df.at[x, 'langs']  = langs
                    
                if religion:
                    df.at[x, 'religion']  = religion
                    
                if inspired_by:
                    df.at[x, 'inspired_by']  = inspired_by
                    
                if people_main:
                    df.loc[x,'people_main'] = people_main
                    
                if smoking :
                    df.loc[x,'smoking'] = smoking 
                    
                if alcohol:
                    df.loc[x,'alcohol'] = alcohol
                    
        df.loc[df['political'] == '','political'] = 'Не указано'
        df.loc[df['langs'] == '','langs'] = 'Не указано'
        df.loc[df['religion'] == '','religion'] = 'Не указано'
        df.loc[df['inspired_by'] == '','inspired_by'] = 'Не указано'
        df.loc[df['people_main'] == '','people_main'] = 'Не указано'
        df.loc[df['smoking'] == '','smoking'] = 'Не указано'
        df.loc[df['alcohol'] == '','alcohol'] = 'Не указано'
        df.drop('personal',axis=1,inplace=True)
        return df

    @classmethod
    def _deal_with_counters_col(cls, df):
        #Possible fields: albums  videos audios friends  groups  user_videos followers
        for col in [ 'albums',  'videos', 'audios', 'friends',  'groups',  'user_videos', 'followers']:
            #-1 будет означать, что инфа не указана
            df[col] = -1
            df[col] = df[col].astype('int64')
        for x in df.index.values:
            if df.loc[x,'counters'] != '':
                col = df.loc[x,'counters']
                
                albums = col.get('albums')
                videos = col.get('videos')
                audios = col.get('audios')
                friends = col.get('friends')
                groups = col.get('groups')
                user_videos = col.get('user_videos')
                followers = col.get('followers')
                if albums:
                    df.loc[x,'albums'] = albums
                    df.loc[df['albums'] == '','albums'] = -1
                if videos:
                    df.loc[x,'videos'] = videos
                    df.loc[df['videos'] == '','videos'] = -1
                if audios :
                    df.loc[x,'audios'] = audios
                    df.loc[df['audios'] == '','audios'] = -1
                if friends:
                    df.loc[x,'friends'] = friends
                    df.loc[df['friends'] == '','friends'] = -1
                if groups :
                    df.loc[x,'groups'] = groups
                    df.loc[df['groups'] == '','groups'] = -1
                if user_videos:
                    df.loc[x,'user_videos'] = user_videos
                    df.loc[df['user_videos'] == '','user_videos'] = -1
                if followers:
                    df.loc[x,'followers'] = followers
                    df.loc[df['followers'] == '','followers'] = -1
        df.drop(['counters','followers_count'],axis=1,inplace=True)  
        return df

    @classmethod
    def _deal_with_schools(cls, df):
        """
        Из писка школ пользователя берет только год окончания и название школы, которую закончил пользователь последней
        """
        for col in [ 'schools_name', 'schools_grad_year']:
            df[col] = ''
            df[col] = df[col].astype('object')
        for x in df.index.values:
            if df.loc[x,'schools'] != '':
                col = df.loc[x,'schools']
                lst = np.array([])
                for sc in col:
                    year = sc.get('year_graduated')
                    if year:
                        lst = np.append(lst,year)
                if lst.size > 0:
                    max_sc = lst.argmax()
                    df.loc[x,'schools_name'] = col[max_sc].get('name')
                    df.loc[x,'schools_grad_year'] = lst[max_sc]

        df.drop('schools',axis=1,inplace=True)
        return df



class DataFramePreprocessor(object):
    __funct_tools = DataFrameHelperFunctions()
    @classmethod
    def make_df_raw(cls,from_files, uids_lst):
        """
        Метод загружает собранные данные по юзерам из файлов, добавляет к ним столбец age_range, сами age_range берутся из названия фалов.
        Далее метод объединяет все датафреймы из файлов и сохраняет их в один, который сохраняет и возвращает из return
        from_files - список файлов, из которых надо загрузить данные
        uids_lst - лист id юзеров, по которым у нас есть посты с их стен и которые надо оставить в итоговом датафрейме
        """
        logger.info('Начинаем формировать начальный датафрейм...')
        lst_df = []
        for _, f in enumerate(from_files):
            parts = f.split('_')
            age_from = parts[3]
            age_to = parts[5]
            ds = ObjectStorage().load_obj(f[:-4])
            try:
                unique_uids =  set(x['id'] for x in ds)
            except KeyError as k:
                logger.exception(k)
                print('Such key doesn\'t exist in users data dict')
            if len(unique_uids) > 990:
                unique_uids = random.sample(unique_uids, 990)
            filtered_ds = list(filter(lambda x: x['id'] in  unique_uids, ds))
            df = pd.DataFrame(filtered_ds)
            df['age_cat'] = '{}_{}'.format(age_from, age_to)
            lst_df.append(df)
        df = pd.concat(lst_df,ignore_index=True)
        #оставляем только отобранные ранее id для которых есть скачанные посты
        df = df[df['id'].isin(uids_lst)]
        ObjectStorage().save_obj(df,'data_raw_df')
        logger.info('Датафрейм с необработанными данными сформирован и сохранен в отдельный файл.')
        return df

    @classmethod
    def add_post(cls,df, post_dict):
        """
        Добавляет посты со стены пользователей к готовму датафрейму с данными по пользователям
        df - датафрейм с данными по пользователям
        post_dict - файл c постами пользователей
        """
        logger.info('Пытаемся добавить посты...')
        df['post'] = df.apply(lambda x: cls.__funct_tools._add_post(x,post_dict),axis=1)
        logger.info('Посты добавлены.')
        return df

    @classmethod
    def clean_df(cls,df,vk_connector):
        
        """
        Очищает данные в датафрейме
        df - готовый датафрейм с данными по пользователями в необработанном виде
        """
        logger.info('Начинаем чистить данные в датафрейме...')

        df_col_lst = df.columns

        #заполняем пропуски в строках пустой строкой
        str_col = [x for x in df.columns if df[x].dtype == 'object']
        for col in str_col:
            df.loc[df[col].isnull(),col] = df[df[col].isnull()][col].apply(lambda x: cls.__funct_tools._fill_missing_str())

        #заполняем пропуски в номерах значением -1
        numrc_col = [x for x in df.columns if df[x].dtype == 'float64' or df[x].dtype == 'int64']
        for col in numrc_col:
            df.loc[df[col].isnull(),col] = df[df[col].isnull()][col].apply(lambda x: cls.__funct_tools._fill_missing_num())

        #приводим числовые значению к единому типу
        for col in numrc_col:
            df[col] = df[col].astype('int64')

        #достаем расшифровку города; вообще через базу доставать его не обязатльно, так как данные стали приходить в виде словаря ид города - название города
        if 'city' in df_col_lst:
            df['city'] = df['city'].apply(lambda x: cls.__funct_tools._get_city_name(x))
            df.loc[df['city'] == '','city'] = 'Не указан город'

        #достаем название страны
        if 'country' in df_col_lst:
            df['country'] = df['country'].apply(lambda x: cls.__funct_tools._get_country(x))
            df.loc[df['country'] == '','country'] = 'Не указана страна'

        if 'faculty' in df_col_lst and 'university' in df_col_lst:
        #удаляем колонки, так как названия факультета и университета мы уже имеем в других колонках
            df.drop(['faculty','university'], axis=1,inplace=True)
        if 'universities' in df_col_lst and 'relation_partner' in df_col_lst:
            df.drop(['universities','relation_partner'], axis=1,inplace=True)
        if 'facebook' in df_col_lst:
            df.drop(['facebook'], axis=1,inplace=True)
        if 'last_seen' in df_col_lst:
            df.drop(['last_seen'], axis=1,inplace=True)

        
        #разбираемся с занятостью; оставляем только тип занятости
        if 'occupation' in df_col_lst:
            df.loc[df['occupation'] != '','occupation'] = df[df['occupation'] != '']['occupation'].apply(lambda x: x['type'])
            df.loc[df['occupation'] == '','occupation'] = -1
            df.loc[df['occupation'] == -1,'occupation'] = 'Не указано'

        if 'personal' in df_col_lst:
        #разбираемся с персональными данными
            df = cls.__funct_tools._deal_with_personal_col(df)

        #разбираемся с полями по различным количествам чего то: треков музыки, друзей, видео и др. 
        if 'counters' in df_col_lst:
            df = cls.__funct_tools._deal_with_counters_col(df)

        #разбираемся с данными по школе пользователя
        if 'schools' in df_col_lst:
            df =  cls.__funct_tools._deal_with_schools(df)
        if 'domain' in df_col_lst:
            df.drop('domain', axis=1, inplace=True)
        if 'deactivated' in df_col_lst:
            df.drop(['deactivated'],axis=1,inplace=True)

        #разбираемся с соц.сетями
        if 'facebook_name' in df_col_lst:
            df['has_facebook'] = df['facebook_name'].apply(lambda x: 1 if len(x) > 2 else 0)
            df.drop('facebook_name',axis=1,inplace=True)
        if 'instagram' in df_col_lst:
            df['has_instagram'] = df['instagram'].apply(lambda x: 1 if len(x) >= 2 else 0)
            df.drop('instagram',axis=1,inplace=True)
        if 'livejournal' in df_col_lst:   
            df['has_livejournal'] = df['livejournal'].apply(lambda x: 1 if len(x) >= 2 else 0)
            df.drop('livejournal',axis=1,inplace=True)
        if 'twitter' in df_col_lst:   
            df['has_twitter'] = df['twitter'].apply(lambda x: 1 if len(x) >= 2 else 0)
            df.drop('twitter',axis=1,inplace=True)
        if 'skype' in df_col_lst:  
            df['has_skype'] = df['skype'].apply(lambda x: 1 if len(x) >= 2 else 0)
            df.drop('skype',axis=1,inplace=True)
        if 'mobile_phone' in df_col_lst:
            df.drop('mobile_phone',axis=1,inplace=True)
        if 'home_phone' in df_col_lst:
            df.drop('home_phone',axis=1,inplace=True)
        df.drop(['first_name','last_name'],axis=1,inplace=True)
        if 'can_access_closed' in df_col_lst:
            df.drop('can_access_closed',axis=1,inplace=True)
        if 'track_code' in df_col_lst:
            df.drop('track_code',axis=1,inplace=True)

        #разбираемся с полями по родственникам пользователя
        if 'relatives' in df_col_lst:
            df['has_relative'] = df['relatives'].apply(lambda x: 1 if x != '' or x != [] else 0)
            df.drop(['relatives'],axis=1,inplace=True)
        
        #рандомно мешаем строки в датафрейме (сейчас они упорядочены по возрастным категориям)
        np.random.seed(0)
        df = df.reindex(np.random.permutation(df.index))

        #готово! датафрейм относительно с чистыми данными. по ним можно построить различные статистики и визуализации и передавать дальше на подготовку данных к построению модели
        logger.info('Готово! Данные в датафрейме очищены. Очищенный датафрейм сохранен в отедльный файл')

        ObjectStorage().save_obj(df,'data_clean_df')
        return df

    @classmethod
    def prepare_df_for_model(cls, df):
        logger.info('Начинаем готовить датафрейм для модели...')
        try:
            df.loc[df['schools_grad_year'] == '','schools_grad_year'] = -1
            df['guessed_age'] = df.apply(cls.__funct_tools._get_avg_age_from_grad_dates, axis=1)
            df['guessed_age'] = df['bdate'].apply(cls.__funct_tools._get_age_from_brthd)
            rang_occupation = {'Не указано' : 0, 'work' : 3, 'university' : 2, 'school' : 1}
            df['occupation'].replace(rang_occupation, inplace=True)
            #признаки для анализа
            feat_completeness_lst = ['about', 'activities', 'books','games','has_mobile', 'home_town','interests','movies', 'music', 'quotes', 'relation', 'political', 'langs', 'religion', 'inspired_by',
        'people_main', 'life_main', 'smoking', 'alcohol','has_facebook', 'has_instagram', 'has_livejournal',
        'has_twitter', 'has_skype', 'has_relative']
            df['avg_completeness'] = df.apply(lambda x: cls.__funct_tools._get_avg_completeness(x,feat_completeness_lst),axis=1)
            # общее число символов в постах
            df['post_length'] = df['post'].apply(lambda text: len(text))

            df['tags_number'] = df['post'].apply(lambda x: cls.__funct_tools._count_tags(x))
            df['emoji_number'] = df['post'].apply(cls.__funct_tools._count_emoji)
            #Готово, теперь оставим только те признаки, которые нам нужны
            df = df[['age_cat','followers_count','sex','post','guessed_age','avg_completeness','post_length','tags_number','emoji_number']].copy()
            logger.info('Датафрей для работы с моделью готов.')
            return df
        except Exception as ex:
            logger.exception(ex)
            print('Неврзможно подготовить данные дя модели. Возможно, отсутствуют какие то столбцы')
  
        

