import re
import string
import logging
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from sklearn.preprocessing import LabelEncoder
from scipy.sparse import csr_matrix, hstack

from tqdm import tqdm
import nltk
from bs4 import BeautifulSoup
from pymorphy2 import MorphAnalyzer
nltk.download('stopwords')
nltk.download('punkt')

from storages import ObjectStorage


logger = logging.getLogger(__name__)

class TextProcessing(object):
    chrs_to_delete = string.punctuation + u'»' + u'«' + u'—' + u'“' + u'„' + u'•' + u'#'
    translation_table = {ord(c): None for c in chrs_to_delete if c != u'-'}
    units = MorphAnalyzer.DEFAULT_UNITS
    morph = MorphAnalyzer(result_type=None, units=units)
    PortSt = PorterStemmer()
    stopw = set(
        [w for w in stopwords.words(['russian', 'english'])]
        + [u'это', u'году', u'года', u'также', u'етот',
        u'которые', u'который', u'которая', u'поэтому',
        u'весь', u'свой', u'мочь', u'eтот', u'например',
        u'какой-то', u'кто-то', u'самый', u'очень', u'несколько',
        u'источник', u'стать', u'время', u'пока', u'однако',
        u'около', u'немного', u'кроме', u'гораздо', u'каждый',
        u'первый', u'вполне', u'из-за', u'из-под',
        u'второй', u'нужно', u'нужный', u'просто', u'большой',
        u'хороший', u'хотеть', u'начать', u'должный', u'новый', u'день',
        u'метр', u'получить', u'далее', u'именно', u'апрель',
        u'сообщать', u'разный', u'говорить', u'делать',
        u'появиться', u'2016',
        u'2015', u'получить', u'иметь', u'составить', u'дать', u'читать',
        u'ничто', u'достаточно', u'использовать',
        u'принять', u'практически',
        u'находиться', u'месяц', u'достаточно', u'что-то', u'часто',
        u'хотеть', u'начаться', u'делать', u'событие', u'составлять',
        u'остаться', u'заявить', u'сделать', u'дело',
        u'примерно', u'попасть', u'хотя', u'лишь', u'первое',
        u'больший', u'решить', u'число', u'идти', u'давать', u'вопрос',
        u'сегодня', u'часть', u'высокий', u'главный', u'случай', u'место',
        u'конец', u'работать', u'работа', u'слово', u'важный', u'сказать']
    )
    url_start = 'http[s]?://'
    url_end = (
        '(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    )
    pattern = url_start + url_end

    @classmethod
    def clean_wall(cls, users_raw_post):
        users_clean_wall_dict = {}
        counter = 0
        logger.info("Cleaning posts ...")
        for user_id, doc in users_raw_post.items():
            try:
                soup = BeautifulSoup(doc, 'html.parser')
                body = ' '.join(
                    [tag.string.replace('\\n', ' ').replace('\\r', ' ')
                    for tag in soup.descendants if tag.string]
                )
                body = re.sub('\[.*?\]','', body)
                body = re.sub(cls.pattern,'', body)
            except Exception as ex:
                logger.exception(ex)
                print('Unable to parse posts of user {}'.format(user_id))
            if body != '':
                body_clean = body.translate(cls.translation_table).lower().strip()
                #токенизируем текст (грубо говоря, разбиваем на слова)
                words = word_tokenize(body_clean)
                tokens = []
                # stemming and text normalization
                for word in words:
                    if re.match('^[a-z0-9-]+$', word) is not None:
                        #если токен попадает под паттерн английского слова или цифры - делаем стемминг
                        tokens.append(cls.PortSt.stem(word))
                    elif word.count('-') > 1:
                        #если токен представляет собой набор символов --- , которые мы не удаляли, так как он может входить в слово ефис, просто добавляем это токен целиком
                        tokens.append(word)
                    else:
                        #если токен русское слово - применяем к нему нормальную форму
                        normal_forms = cls.morph.normal_forms(word)
                        tokens.append(normal_forms[0] if normal_forms else word)
                # remove stopwords and leave unique words only
                tokens = filter(
                    lambda token: token not in cls.stopw, sorted(set(tokens))
                )

                # leave all words with more than 3 chars
                tokens = filter(lambda token: len(token) > 3, tokens)
            else:
                tokens = []
            counter += 1
            if counter % 100 == 0:
                print("{0} docs processed".format(counter))
            #словарь, где id - это id группы, значения - ее очищенные токенизированные слова
            users_clean_wall_dict[user_id] = tokens
        #словарь id группы: очищенные слова по ее постам
        user_clean_doc_dict = {key: list(val) for key, val in users_clean_wall_dict.items()}
        # сохранить обработанные данные на диск
        ObjectStorage().save_obj(user_clean_doc_dict, 'users_clean_wall_dict')
        logger.info("Данные постов пользователей очищены, нормализованы и сохранены в отдельный файл.")
        return user_clean_doc_dict

