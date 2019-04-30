import os
import json
import logging
import pickle

logger = logging.getLogger(__name__)

class ObjectStorage(object):
    @classmethod
    #TODO переделать эти методы как работающие конкретно с типом pickle, иначе проверка на  + '.pkl' будет мешать при открытии других файлов
    def load_obj(self,name):
        if not os.path.exists(name + '.pkl'):
            print('Such file path doesn\'t exists')
            raise StopIteration
        with open(name + '.pkl', 'rb') as f:
            return pickle.load(f)
    def save_obj(self, obj,name):
        with open(name + '.pkl', 'wb') as f:
            pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

