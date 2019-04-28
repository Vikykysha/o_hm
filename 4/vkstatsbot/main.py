# coding=utf-8
"""
Код работает на python2.7

Запуск приложения локально:
python main.py

Запуск приложения как сервис (использую supervisorctl, возможны другие
варианты, для работы нужна настройка конфига):
supervisorctl start vkstat

Для работы приложения необходимо скопировать файл vkstat_example.cfg в файл vkstat.cfg
Путь до конфига можно исправить

В конфиг нужно добавить токен бота и токен приложения ВК

Бота регистрировать через @botfather прямо в телеграме

Приложение в ВК регистрировать на странице https://vk.com/apps?act=manage
Получение токена для приложения ВК:
https://vk.com/dev/implicit_flow_user

Рекомендую токен получать бессрочный (обратить внимание на scope)

НЕЛЬЗЯ КОММИТИТЬ КОНФИГ В ГИТ РЕПОЗИТОРИЙ
не забывайте правильно настраивать .gitignore или использовать другие пути для конфига (что предпочтительнее)
(и вообще никогда никакие ключи не добавляйте в гит)
"""


import configparser
import logging
import time
import glob
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

from vk_api_method_handler import VKApiConnector
from get_data import GetData
from text_processing import TextProcessing
from process_data import DataFramePreprocessor
from storages import ObjectStorage
from visualisation import Visual

config = configparser.ConfigParser()
config.read('vkstat.cfg')
#token = config.get('VKStat', 'token', '')
vk_api_v = config.get('VKApi','v')
vk_api_token = config.get('VKApi', 'token')
client_id = config.get('VKApi', 'client_id')
VKApiConnector.config(vk_api_v, client_id, vk_api_token)

VKApiConnector.establish_vk_session()

age_ranges = config.get('Parameters','age_ranges').split(',')

if __name__ == '__main__':
    #TODO надо сохранять id пользователей в отдельный файл
    '''
    users_uids = {}
    for range_ in age_ranges:
        age_from, age_to = [int(x) for x in range_.split('-')]
        users_uids['{}-{}-male'.format(str(age_from),str(age_to))] = GetData.load_and_save_users(VKApiConnector,age_from,age_to,0)
        users_uids['{}-{}-female'.format(str(age_from),str(age_to))] = GetData.load_and_save_users(VKApiConnector,age_from,age_to,1)

    #uids_itog_lst - id юзеров, для которых были сораны посты с их стены и которые мы будем использовать в дальнейшем анализе;в фале с постами uid являются int
    users_raw_post = GetData.load_and_save_walls(VKApiConnector,users_uids)
    #очищаем текст с постов юзеров; в users_uids id имеют строковый тип, в датафреймах тоже
    user_clean_doc_dict = TextProcessing.clean_wall(users_raw_post)
    '''
    #в полученном списке id в формате int
    uids = ObjectStorage().load_obj('itog_uids_for_analysis')

    # составляем итоговый датафрейм из собранных данных по юзерам, пока без постов
    df_raw = DataFramePreprocessor.make_df_raw([f for f in glob.glob( "*_[1|2].pkl")],uids) 
    
    #чистим данные
    df_clean =  DataFramePreprocessor.clean_df(df_raw,VKApiConnector)

    #визуализируем некоторую статистику
    Visual.plot_alco_distribution(df_clean)
    Visual.plot_occupation_distribution(df_clean)
    Visual.plot_people_main(df_clean)
    Visual.plot_political_distribution(df_clean)
    Visual.plot_relation_distribution(df_clean)
    Visual.plot_smoke_distribution(df_clean)
    Visual.plot_bar_from_df(df_clean,'city','город')
    Visual.plot_bar_from_df(df_clean,'country','страна')

    
    
