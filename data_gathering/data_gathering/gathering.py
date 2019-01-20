import sys
import os
import time
import json
import re

import logging
import pandas as pd
import numpy as np

from scrappers.scrapper import Scrapper
from storages.file_storage import FileStorage
from parsers.html_parser import HtmlParser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#gather data from pages
def gather_process(user_id):
    """Process to load html pages, parse them and write data into files"""
    
    logger.info("gather")

    #specify with which page you want to start scrapping proccess
    page_num = 1
    #first empty user files with the same user id
    for user_file in os.listdir('./user_data'):
        if user_id in user_file and int(user_file.split('_')[2].replace('.json', '')) >= page_num:
            os.unlink(os.path.join(os.getcwd(),user_file))

    for user_file in os.listdir('./movie_data'):
        if user_id in user_file  and int(user_file.split('_')[2].replace('.json', '')) >= page_num:
            os.unlink(os.path.join(os.getcwd(),user_file))

    #load and parse data from site
    while True:
        #debug
        print(page_num)
        scrapper = Scrapper('http://www.kinopoisk.ru/user/%d/votes/list/ord/date/page/%d/#list' % (int(user_id),page_num))
        page_exists, soup_parser = scrapper.load_page()
        if page_exists:
            parser = HtmlParser(soup_parser)
            parser.parse_and_load_user_data(user_id,page_num)
            page_num += 1
            time.sleep(10)
        else:
            break   


def stats_of_data(user_id):
    """Try to load parsed data into pandas and get some statistics from it"""

    logger.info("stats")

    #load data from files to analyze

    #lines=True let read json file that have separate {} lines ; in os.path.join path user_data must be without the first / otherwise it will be interpreted as absolute path and all paths before will be discarded
    #user_data_df = pd.concat([pd.read_json(os.path.join(os.getcwd(),os.path.dirname('user_data/'),f)) for f in os.listdir('./user_data/') if user_id in f], ignore_index=True)
    #movies_data_df = pd.concat([pd.read_json(os.path.join(os.getcwd(),os.path.dirname('movie_data/'),f)) for f in os.listdir('./movie_data/') if user_id in f], ignore_index=True)

    df_user_list = []
    for f in os.listdir('./user_data/'):
        if user_id in f:
            path =  os.path.join(os.getcwd(),'user_data/',f)
            with open(path,'r', encoding = "utf-8",errors = 'ignore') as f:
                data = json.load(f)
            data_df = pd.DataFrame(data)
            df_user_list.append(data_df)
    user_data_df = pd.concat(df_user_list,ignore_index=True)

    df_movie_list = []
    for f in os.listdir('./movie_data/'):
        if user_id in f:
            path =  os.path.join(os.getcwd(),'movie_data/',f)
            with open(path,'r', encoding = "utf-8",errors = 'ignore') as f:
                data = json.load(f)
            data_df = pd.DataFrame(data)
            df_movie_list.append(data_df)
    movies_data_df = pd.concat(df_movie_list,ignore_index=True)

    #EDA
    print(user_data_df.info())
    print(user_data_df.describe())
    print(user_data_df.head())
    print(user_data_df.shape)

    #Cleaning data
    user_data_df['duration'] = pd.to_numeric(user_data_df['duration'] , errors='coerce')
    user_data_df['imdb_rating'] = pd.to_numeric(user_data_df['duration'] , errors='coerce')
    user_data_df['imdb_rating_cnt'] = pd.to_numeric(user_data_df['duration'] , errors='coerce')
    user_data_df['kp_rating'] = pd.to_numeric(user_data_df['duration'] , errors='coerce')
    user_data_df['kp_rating_cnt'] = pd.to_numeric(user_data_df['duration'] , errors='coerce')

    def find_age(age_str):
        prog = re.compile(r'\d+')
        matches = re.findall(prog,age_str)
        if matches != []:
            return matches[0]
        elif "любой" in age_str:
            return 0
        else:
            return np.nan
        
    user_data_df['age_restriction'] = user_data_df['age_restriction'].apply(find_age)
    #if data has  some other than numeric types we can escape errors by passing the errors coeff in the to_numeric method
    user_data_df['age_restriction'] = pd.to_numeric(user_data_df['age_restriction'] , errors='coerce')

    user_data_df['country'] = user_data_df['country'].fillna(np.nan)
    user_data_df['genre'] = user_data_df['genre'].astype('str')
    user_data_df['director'] = user_data_df['director'].astype('str')
    user_data_df['movie_desc'] = user_data_df['movie_desc'].astype('str')
    user_data_df['name_eng'] = user_data_df['name_eng'].astype('str')
    user_data_df['name_rus'] = user_data_df['name_rus'].astype('str')

    user_data_df['country'] = user_data_df.loc[:,'country'].apply(lambda x: 'не определено' if x == '' else x)

    #Print some statistics
    print("In which years did the user watch movies the most? Show top-10 years\n\n")
    print(user_data_df['release_year'].value_counts()[:10])
    print("Most watched countries of user\n\n")
    print(user_data_df['country'].value_counts()[:10])
    print('Most watched genres of user\n\n')
    print(user_data_df['genre'].value_counts()[1:5])
    print("In wich age restriction did user watch movies the most?\n\n")
    print(user_data_df['age_restriction'].value_counts(dropna = False))




if __name__ == '__main__':

    logger.info("Work started")

    if sys.argv[1] == 'gather':
        #sys.argv[2] is user's id
        gather_process(sys.argv[2])

    elif sys.argv[1] == 'stats':
        stats_of_data(sys.argv[2])

    logger.info("work ended")
