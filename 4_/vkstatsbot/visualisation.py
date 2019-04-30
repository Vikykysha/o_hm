import datetime
import io
from collections import Counter
import matplotlib 
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import numpy as np

class Visual(object):

    @classmethod
    def items_stats(cls,items):
        date_counts = Counter()
        for item in items:
            date = datetime.datetime.fromtimestamp(item["date"]).strftime("%Y-%m-%d")
            date_counts[date] += 1
        return [(k, date_counts[k]) for k in sorted(date_counts)]

    @classmethod
    def plot_hist_buffer(cls,hist_data):
        y_pos = np.arange(len(hist_data))
        y = [x[1] for x in hist_data]

        plt.figure(figsize=(20, 5))
        plt.bar(y_pos, y, align='center', alpha=0.5)
        plt.xticks(y_pos, [x[0] for x in hist_data], rotation='vertical')
        plt.ylabel('Post count')
        plt.title('Post count by date (last 100 posts)')

        buf = io.BytesIO()
        buf.name = 'stats.png'
        plt.savefig(buf, format='png', bbox_inches='tight')
        buf.seek(0)
        return buf
    @classmethod
    def plot_bar_from_df(cls,df,col,title_name,max_vals=15):
        plot_ = df[col].value_counts().head(max_vals).plot(kind='bar', figsize=(20,15),colormap='winter')
        plt.title('ТОП-{} {}, по которым есть данные'.format(max_vals,title_name))
        plt.xlabel('Название {}'.format(title_name))
        plt.ylabel('Количество пользователей')
        fig = plt.gcf()
        fig.savefig('./pictures/output_{}.png'.format(title_name))
    @classmethod
    def plot_relation_distribution(cls,df):
        relation_dict = {-1 : 'Не указано', 4 : 'Женат/Замужем', 1 : 'Одинок', 6 : 'В активном поиске', 7 : 'Влюблен', 2: 'Состоит в отношениях',
                    3: 'Помолвлен', 5 : 'Все сложно', 8 : 'В гражданском браке'}
        df_draw = df.copy()
        df_draw.loc[df_draw['relation'] == 0,:] = -1
        df_draw['relation'].replace(relation_dict,inplace=True)
        plot_ = df_draw['relation'].value_counts().plot(kind='bar', figsize=(20,15),colormap='winter')
        plt.title('Распределение семейного статуса')
        plt.xlabel('Название семейного статуса')
        plt.ylabel('Количество пользователей')
        fig = plt.gcf()
        fig.savefig('./pictures/output_relation.png')
    @classmethod
    def plot_political_distribution(cls,df):
        dict_politican = {-1 : 'не указано',1 : 'Communist',2 : 'Socialist',3 : 'Moderate',4 : 'Liberal',5 : 'Conservative',6  :'Monarchist',7 : 'Ultraconservative',8 : 'Apathetic',9 :'Libertarian'}
        df_draw = df.copy()
        df_draw.loc[df['political'] == '',:] = -1
        df_draw['political'].replace(dict_politican,inplace=True)
        plot_ = df_draw['political'].value_counts().plot(kind='bar', figsize=(20,15),colormap='winter')
        plt.title('Распределение политического статуса')
        plt.xlabel('Название  статуса')
        plt.ylabel('Количество пользователей')
        fig = plt.gcf()
        fig.savefig('./pictures/output_political.png')
    @classmethod
    def plot_occupation_distribution(cls,df):
        df['occupation'].value_counts().plot(kind='bar', figsize=(20,15),colormap='winter')
        plt.title('Распределение  статуса занятости')
        plt.xlabel('Название  статуса')
        plt.ylabel('Количество пользователей')
        fig = plt.gcf()
        fig.savefig('./pictures/output_occupation.png')
    @classmethod
    def plot_smoke_distribution(cls,df):
        smoke_dict = {1 : 'very negative',2 : 'negative',3 : 'neutral',4 : 'compromisable',5 : 'positive'}
        df_draw = df.copy()
        df_draw['smoking'].replace(smoke_dict ,inplace=True)
        plot_ = df_draw['smoking'].value_counts().plot(kind='bar', figsize=(20,15),colormap='winter')
        plt.title('Распределение отношения к курению')
        plt.xlabel('Название  статуса')
        plt.ylabel('Количество пользователей')
        fig = plt.gcf()
        fig.savefig('./pictures/output_smoking.png')
    @classmethod
    def plot_people_main(cls,df):
        people_dict = {1 : 'intellect and creativity',2 : 'kindness and honesty',3 : 'health and beauty',4 : 'wealth and power',5 : 'courage and persistance',6 : 'humor and love for life'}
        df_draw = df.copy()
        df_draw['people_main'].replace(people_dict ,inplace=True)
        plot_ = df_draw['people_main'].value_counts().plot(kind='bar', figsize=(20,15),colormap='winter')
        plt.title('Распределение главного в людях')
        plt.xlabel('Название  статуса')
        plt.ylabel('Количество пользователей')
        fig = plt.gcf()
        fig.savefig('./pictures/output_people_main.png')
    @classmethod
    def plot_alco_distribution(cls,df):
        alco_dict = {1 : 'very negative',2 : 'negative',3 : 'neutral',4 : 'compromisable',5 : 'positive'}
        df_draw = df.copy()
        df_draw['alcohol'].replace(alco_dict ,inplace=True)
        plot_ = df_draw['alcohol'].value_counts().plot(kind='bar', figsize=(20,15),colormap='winter')
        plt.title('Распределение отношения к курению')
        plt.xlabel('Название  статуса')
        plt.ylabel('Количество пользователей')
        fig = plt.gcf()
        fig.savefig('./pictures/output_alcohol.png')
