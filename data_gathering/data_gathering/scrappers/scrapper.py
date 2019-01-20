import sys

import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


logger = logging.getLogger(__name__)


class Scrapper(object):
    def __init__(self,url,driver_path = r"C:\Users\viktoria.sinditskaya\drivers",skip_objects=None):
        self.driver_path = driver_path
        self.skip_objects = skip_objects
        self.url = url

    def get_Firefox_driver(self):
        try:
            options = webdriver.FirefoxOptions()
            options.add_argument('-headless')
            driver = webdriver.Firefox(self.driver_path,options=options)
            driver.get(self.url)
            html = driver.page_source
            driver.close()
        except Exception:
            logging.exception("message")
            logger.info("cannot load the page through web driver")
            sys.exit(1)
        html_parser = HtmlParser(html)
        return html_parser


    def load_page(self,driver_kind='FireFox'):
        logger.info("trying to load page")
        if driver_kind  == 'FireFox':
            film_list, soup = self.get_Firefox_driver().exists_page()
        return (True if film_list != None else False, soup)

    def load_movie_data(self,driver_kind='FireFox'):
        logger.info("trying to load movie data")
        if driver_kind  == 'FireFox':    
            html_parser = self.get_Firefox_driver()
        return html_parser.parse_movie_data()

#in the end of file bc we need to handle cyclic imports
from parsers.html_parser import HtmlParser



