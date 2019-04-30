import logging
import requests
import vk_api

from storages import ObjectStorage

logger = logging.getLogger(__name__)


def encoded_dict(in_dict):
    out_dict = {}
    for k, v in in_dict.iteritems():
        if isinstance(v, unicode):
            v = v.encode('utf8')
        elif isinstance(v, str):
            v.decode('utf8')
        out_dict[k] = v
    return out_dict


class VKApiConnector(object):
    __base_url = "https://api.vk.com/method/"
    __v = NotImplemented
    __resolve_screen_name_method = "utils.resolveScreenName"
    __wall_get_method = "wall.get"
    __user_search_method = "users.search"
    __token = NotImplemented
    __client_id = NotImplemented
    __sleep_time = NotImplemented

    __vk_session = NotImplemented
    __tools = NotImplemented
    __fields = """sex,bdate,city,country,home_town,lists,has_mobile,
    contacts,connections,education,universities,schools,followers_count,counters,
    occupation,last_seen,relation,relatives,verified,personal,
    connections,activities,interests,music,movies,books,games,about,quotes
    """

    @classmethod
    def config(cls, version, client_id, token, sleep_time=1):
        """
        Получаем и устанавливаем конфигурационные данные
        """
        cls.__v = str(version)
        cls.__sleep_time = sleep_time
        cls.__client_id = int(client_id)
        cls.__token = str(token)

    @classmethod
    def establish_vk_session(cls):
        """Устанавливаем соединение с API"""
        cls.__vk_session = vk_api.VkApi(token= cls.__token, app_id=cls.__client_id,api_version=cls.__v)
        cls.__tools = vk_api.VkTools(cls.__vk_session)

    @classmethod
    def __get_base_params(cls):
        return {
            'v': cls.__v,
            'client_id': cls.__client_id,
            'access_token': cls.__token
        }

    @classmethod
    def resolve_screen_name(cls, screen_name):
        """Detects a type of object (e.g., user, community, application) and its ID by screen name"""
        try:
            logger.info("Access {} method".format(cls.__resolve_screen_name_method))
            request_params = cls.__get_base_params()
            request_params['screen_name'] = screen_name

            url = '{}{}'.format(cls.__base_url, cls.__resolve_screen_name_method)
            response = requests.post(url, encoded_dict(request_params) if request_params else None)

            if not response.ok:
                logger.error(response.text)
                return

            return response.json()['response']
        except Exception as ex:
            logger.exception(ex)

    @classmethod
    def get_wall(cls, owner_id):
        #TODO сделать так, чтобы сообщения  об ошибках из библиотеки vkapi не выводились на консоль при работе
        """
        Грузим посты с указанного id
        owner_id - id пользователя, со стены которого грузим посты
        """
        try:
            logger.info("Access {} method for {}".format(cls.__wall_get_method,owner_id))
            request_params = cls.__get_base_params()
            request_params['count'] = 100
        except Exception as ex:
            logger.exception(ex)
            print('Can not establish connections with cls params')
        try:
            wall = cls.__tools.get_all(cls.__wall_get_method, request_params['count'], {'owner_id': owner_id, 'filter': 'owner'})
            return wall       
        except ValueError as ex:
            logger.exception(ex)
            print('There are no items in response wall get')
        except Exception as ex:
            logger.exception(ex)
            print('API erorr')
        return []

    @classmethod
    def get_users(cls,age_from,age_to,sex,fields=None):
        """
        Грузим данные по пользователям по указанным параметрам
        age_from,age_to - ранг возрастов для которых грузим
        sex - пол
        fields - поля , которые грузис по пользователям
        """
        fields = cls.__fields if not fields else fields
        try:
            logger.info("Access {} method for users {} age {}-{}".format(cls.__user_search_method, sex, age_from, age_to))
            request_params = cls.__get_base_params()
        except Exception as ex:
            logger.exception(ex)
            print('Can not establish connections with cls params')
        try:
            users =  cls.__tools.get_all(cls.__user_search_method,1000, {'fields': fields, 'sex': str(sex), 'age_from':str(age_from), 'age_to':str(age_to)})
            return users     
        except ValueError as ex:
            logger.exception(ex)
            print('There are no items in response users search')
        except Exception as ex:
            logger.exception(ex)
            print('API erorr')
        return []

    @classmethod
    def get_city(cls,city_dict):
        """
        Загружает город по его id
        city_dict - id города
        """
        if city_dict != '':
            if city_dict['id'] != 0:
                try:          
                    city = cls.__vk_session.method( 'database.getCitiesById',{'city_ids': city_dict['id'],'lang': 'ru'})
                    city = city[0]['title']
                    return city
                except Exception as val:
                    print('Cannot extract city name bc of {}'.format(val))
                    return ''
        return ''




