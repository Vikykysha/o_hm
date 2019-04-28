import logging
import time

from random import sample 

from vk_api_method_handler import VKApiConnector
from storages import ObjectStorage


logger = logging.getLogger(__name__)

class GetData(object):
    __fields = """sex,bdate,city,country,home_town,lists,has_mobile,
    contacts,connections,education,universities,schools,followers_count,counters,
    occupation,last_seen,relation,relatives,verified,personal,
    connections,activities,interests,music,movies,books,games,about,quotes
    """
    @classmethod
    def load_and_save_users(cls,vk_connector, age_from,age_to,sex,fields=None):
        """
        Загружает данные по пользователями с через API Вконтакте
        vk_connector - инстанс коннектора к апи
        age_from,age_to - ранг возрастов , для которых будем загружать данные
        sex - пол, по которому загружаем данные
        fields - поля с информацией по пользователям, которые будем грузить
        """
        fields = fields if fields else cls.__fields
        answer = vk_connector.get_users(age_from,age_to,sex,fields=fields)
        try:
            if answer['count']:
                user_len = len(answer['items'])
                ObjectStorage().save_obj(answer['items'], '{0}_user_from_{1}_to_{2}_sex_{3}'.format(user_len, age_from, age_to,sex))
                users_lst = answer['items']
                users_id = [user['id'] for user in users_lst]
                return users_id
            else:
                logger.info('Can not find users age {} - {} gender {}'.format(age_from, age_to,sex))
        except Exception as ex:
            logger.exception(ex)
            print('Users didnt load')

    @classmethod
    def load_and_save_walls(cls,vk_connector,uids,uids_from_range_cnt=150,post_cnt_to_extract=100):
        """
        Загружает посты со стены пользователей
        vk_connector - инстанс коннектора к апи
        uids - список id для которых грузим посты
        uids_from_range_cnt - у скольки пользователей будем грузить постов, задает максимальное количество пользователей из каждого возрастного диапазона, для которых загрузим их посты
        post_cnt_to_extract - сколько постов грузим со стены одного пользователя
        """
        uids_lst = []
        #формируем список id, по которым будем грузить посты
        for vals in uids.values():
            lnght = len(vals)
            #из каждой категории берем одинаковое количество id для дальнейшего анализа и скачки постов. если id меньше чем заданное количество, берем все, которые есть
            how_many = uids_from_range_cnt if uids_from_range_cnt <= lnght else lnght
            uids_lst.extend(sample(vals,how_many))
        users_wall_dict = {}
        counter=0

        """
        #FOR DEBUG ONLY!!!!
        uids_lst = uids_lst[:40]
        #FOR DEBUG ONLY!!!!
        """

        for uid in uids_lst:
            try:
                uid = int(uid)
                counter+=1
                answer = vk_connector.get_wall(uid)
                lngth = post_cnt_to_extract if post_cnt_to_extract <= answer['count'] else answer['count']
                #если по запросу на стену юзера вернулся ответ с ошибкой(прим стена недоступна для просмотра) то библиотека апи вк вернет просто пустой лист в items
                check = answer['items']
                user_doc = ''
                if answer['count'] != 0:
                    for post in check[:lngth]:
                        if post.get('marked_as_ads') != 1:
                            user_doc += post['text']
                users_wall_dict[uid] = user_doc
                time.sleep(0.8)
            except Exception as ex:
                logger.exception(ex)
                print('Unable to extract user {} wall'.format(uid))
            if counter % 100 == 0:
                print("{0} users extracted".format(counter))
            if  counter % 200 == 0:
                time.sleep(5)
        ObjectStorage().save_obj(users_wall_dict, 'users_wall_posts')
        #чтобы недопустить дубликатов по id
        uids_lst = list(set(uids_lst))
        ObjectStorage().save_obj(uids_lst, 'itog_uids_for_analysis')
        #возвращаем список id, для которых мы скачали посты и которые в дальнейшем будем использовать для анализа
        return users_wall_dict



        




