import os
import json
from .storage import Storage
import logging

logger = logging.getLogger(__name__)

class FileStorage(Storage):

    def __init__(self, file_name):
        self.file_name = file_name

    def read_data(self):
        if not os.path.exists(self.file_name):
            raise StopIteration

        with open(self.file_name) as f:
            for line in f:
                yield line.strip()

    def write_data(self, data_array):

        try:
            with open(self.file_name,'a+',  encoding="utf-8") as file:
                json.dump(data_array, file, ensure_ascii=False, indent=2)
        except UnicodeEncodeError as e:
            logging.exception("message")
            logger.info('utf-8 doesn\'t work')
            try:
                with open(self.file_name,'a+',  encoding="utf-16") as file:
                    json.dump(data_array, file, ensure_ascii=False, indent=2)
            except UnicodeEncodeError as e:
                logging.exception("message")
                logger.info('utf-16 doesn\'t work')
                try:
                    with open(self.file_name,'a+',  encoding="cp1251") as file:
                        json.dump(data_array, file, ensure_ascii=False, indent=2)
                except UnicodeEncodeError as e:
                    logging.exception("message")
                    logger.info('cp1251 doesn\'t work')
                    logging.exception("can not form json file")



    def append_data(self, data):
        """
        :param data: string
        """
        with open(self.file_name, 'a') as f:
            for line in data:
                if line.endswith('\n'):
                    f.write(line)
                else:
                    f.write(line + '\n')
