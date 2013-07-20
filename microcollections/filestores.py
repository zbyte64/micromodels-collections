# -*- coding: utf-8 -*-


class BaseFileStore(object):
    def load(self, file_path):
        pass

    def save(self, file_obj):
        #CONSIDER: how to determine desired file path
        pass
