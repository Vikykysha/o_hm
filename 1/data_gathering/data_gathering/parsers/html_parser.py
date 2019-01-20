import sys
import re

import logging
from bs4 import BeautifulSoup as bs  

from .parser import Parser
from storages.file_storage import FileStorage

logger = logging.getLogger(__name__)

class HtmlParser(Parser):

    """Parse html pages from the Internet"""

    def __init__(self,what_to_parse,kind_parser='html.parser'):
        """
        Args:
            what_to_parse(html page): what source to parse.
            kind_parser(str): which type of parser to use.
        """
        self.parse_source =  what_to_parse
        self.kind_parser = kind_parser

    def exists_page(self):
        """Check if the page with certain number exist"""
        soup = bs(self.parse_source,self.kind_parser)
        film_list = soup.find('div', {'class': 'profileFilmsList'})
        return film_list, soup
    def parse_movie_data(self):
        """Parses movie html pages"""
        logger.info("trying to parse movie data")
        soup = bs(self.parse_source,self.kind_parser)

        try:
            info_table = soup.find('table', {'class' : 'info'}).tbody
            rating_table = soup.find('form', {'class' : 'rating_stars'})
        except AttributeError:
            logger.info("Cannot load info table")
            return None

        movie_data = {}
        try:
        #get the name_rus
            movie_data['name_rus'] = soup.find('h1', {'class': 'moviename-big'}).text
        except Exception:
             movie_data['name_rus'] = 'NaN'

        #debug
        print(movie_data['name_rus'] )

        #TODO make this part with regular expressions
        if info_table != None:
            for nbr, tr in enumerate(info_table.find_all('tr')):
                if len(info_table.find_all('tr')) <= 10:
                    logger.info("len <= 10")
                    return None
                if nbr == 0:
                    try:
                        #get the release_year
                        movie_data['release_year'] = tr.find_all('a')[0].text
                    except Exception:
                        movie_data['release_year'] = 'NaN'
                elif nbr == 1:
                    try:
                        #get the country
                        movie_data['country'] = tr.find('a').text
                    except Exception:
                        movie_data['country'] = 'NaN'
                elif nbr == 3:
                    try:
                        #get the director
                        movie_data['director'] = tr.find('a').text
                    except Exception:
                        movie_data['director'] = 'NaN'
                elif nbr == 10:
                    try:
                        #get the genre
                        movie_data['genre'] = tr.find('span').find('a').text
                    except Exception:
                        movie_data['genre'] = 'NaN'

            try:
                #get the age_restriction
                movie_data['age_restriction'] = info_table.find('tr', {'class' : 'ratePopup'}).find('span').text
            except Exception:
                movie_data['age_restriction'] = 'NaN'
            try:
                #get the duration
                movie_data['duration'] = int(re.match('(\d+)', info_table.find('td', {'class' : 'time'}).text).groups()[0])
            except Exception:
                movie_data['duration'] = 'NaN'
        else:
            movie_data['release_year'] = 'NaN'
            movie_data['country'] = 'NaN'
            movie_data['director'] = 'NaN'
            movie_data['genre'] = 'NaN'
            movie_data['age_restriction'] = 'NaN'
            movie_data['duration'] = 'NaN'

        try:
            #get kp_rating
            movie_data['kp_rating'] = float(rating_table.find('span', {'class' : 'rating_ball'}).text)
            movie_data['kp_rating_cnt'] = rating_table.find('span', {'class' : 'ratingCount'}).text
        except Exception:
            #if kp_rating is missed we say that this movie is not released yet and we don't want to analyze it
            logger.info("cannot find kp rating")
            return None
        #get imdb_rating
        imdb_rating = rating_table.find(id='block_rating').find('div', {'class' : 'block_2'}).find(text = re.compile('^IMDb'))
        if imdb_rating is not None:
            imdb_block_text = re.search('(\d+\.\d+)(?:.+?)(\d+\s*\d+)', str(imdb_rating))
            if imdb_block_text is not None:
                movie_data['imdb_rating'] = float(imdb_block_text.groups()[0])
                movie_data['imdb_rating_cnt'] = int(imdb_block_text.groups()[1].replace(' ',''))
            else:
                movie_data['imdb_rating'], movie_data['imdb_rating_cnt'] = 'NaN', 'NaN'
        else:
            movie_data['imdb_rating'], movie_data['imdb_rating_cnt'] = 'NaN', 'NaN'
        return movie_data

    def parse_and_load_user_data(self,user_id,page_num):
        """Parses user data

        Args:
            user_id(str): id of user which data we want to get
            page_num(int): number of page with user data wchich we want to parse
        """
        logger.info("trying to parse user data")
        results_user = []
        results_movie = []
        film_list = self.parse_source.find('div', {'class': 'profileFilmsList'})
        items = film_list.find_all('div', {'class': ['item', 'item even']})
        for item in items:
            logger.info("new item in a page")
            # getting movie_id
            movie_link = item.find('div', {'class': 'nameRus'}).find('a').get('href')
            movie_desc = item.find('div', {'class': 'nameRus'}).find('a').text
            movie_id = int(re.findall('\d+', movie_link)[0])
            
            # getting english name
            name_eng = item.find('div', {'class': 'nameEng'}).text
            
            #getting watch time
            watch_datetime = item.find('div', {'class': 'date'}).text
            date_watched, time_watched = re.match('(\d{2}\.\d{2}\.\d{4}), (\d{2}:\d{2})', watch_datetime).groups()
            
            # getting user rating
            user_rating = item.find('div', {'class': 'vote'}).text
            if user_rating:
                user_rating = int(user_rating)
            
            scrapper = Scrapper('https://www.kinopoisk.ru/film/%s/' % (movie_id))
            movie_data = scrapper.load_movie_data()
            if movie_data is None:
                continue
            results_user.append({
                    'user_id' : user_id,
                    'movie_id': movie_id,
                    'name_eng': name_eng,
                    'name_rus' : movie_data['name_rus'],
                    'date_watched': date_watched,
                    'time_watched': time_watched,
                    'user_rating': user_rating,
                    'movie_desc': movie_desc,
                    'kp_rating' : movie_data['kp_rating'],
                    'imdb_rating' : movie_data['imdb_rating'],
                    'kp_rating_cnt' : movie_data['kp_rating_cnt'],
                    'imdb_rating_cnt' : movie_data['imdb_rating_cnt'],
                    'release_year' : movie_data['release_year'],
                    'country' : movie_data['country'],
                    'duration' : movie_data['duration'],
                    'genre' : movie_data['genre'],
                    'age_restriction' : movie_data['age_restriction'],
                    'director' : movie_data['director']
                })
            results_movie.append({
                'movie_id': movie_id,
                'name_eng': name_eng,
                'name_rus' : movie_data['name_rus'],
                'movie_desc': movie_desc,
                'kp_rating' : movie_data['kp_rating'],
                'imdb_rating' : movie_data['imdb_rating'],
                'kp_rating_cnt' : movie_data['kp_rating_cnt'],
                'imdb_rating_cnt' : movie_data['imdb_rating_cnt'],
                'release_year' : movie_data['release_year'],
                'country' : movie_data['country'],
                'duration' : movie_data['duration'],
                'genre' : movie_data['genre'],
                'age_restriction' : movie_data['age_restriction'],
                'director' : movie_data['director']

            })
        
        file_users_date = FileStorage('./user_data/%d_page_%d.json' % (int(user_id), page_num))
        file_users_date.write_data(results_user)

        file_movies_data = FileStorage('./movie_data/movies_page_%d_id_%d.json' % (page_num, int(user_id)))
        file_movies_data.write_data(results_movie)

#in the end of file bc we need to handle cyclic imports
from scrappers.scrapper import Scrapper



