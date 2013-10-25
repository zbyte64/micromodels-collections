# -*- coding: utf-8 -*-
from microcollections.collections import BaseCollection
from microcollections.datastores.core import BaseDataStore, UnsupportedOperation
from .utils import FileProxy


class FileCollection(BaseCollection):
    '''
    A collection of file objects referenced through paths
    '''
    model = FileProxy
    object_id_field = 'path'

    def __init__(self, data_store):
        self.data_store = data_store  # of type file store

    def get_available_key(self, path):
        return self.data_store.get_available_key(path)


#TODO what about overwriting files?
class BaseFileStore(BaseDataStore):
    def get_available_key(self, path):
        #TODO
        return path

    def uri(self, path):
        return '://%s' % path

    def save_file(self, file_obj, path):
        raise NotImplementedError

    def open_file(self, path, mode='rb'):
        raise NotImplementedError

    def delete_file(self, path):
        raise NotImplementedError

    def file_exists(self, path):
        raise NotImplementedError

    def save(self, collection, instance, key=None):
        instance = self.execute_hooks('beforeSave',
            {'instance': instance, 'collection': collection})
        self.save_file(instance, key or instance.path)
        return self.execute_hooks('afterSave',
            {'instance': instance, 'collection': collection})

    def remove(self, collection, instance):
        instance = self.execute_hooks('beforeRemove',
            {'instance': instance, 'collection': collection})
        self.delete_file(instance.path)
        return self.execute_hooks('afterRemove',
            {'instance': instance, 'collection': collection})

    def get(self, collection, params):
        path = self._normalize_params(collection, params).get('pk', None)
        if path is None:
            raise UnsupportedOperation('Lookups must be by path')
        if not self.file_exists(path):
            raise KeyError('Path not found: %s' % path)
        return {
            'lazy_file': (self.open_file, path),
            'path': path,
        }

    def find(self, collection, params):
        params = self._normalize_params(collection, params)
        objects = list()
        if 'pk' in params:
            val = params['pk']
            if self.file_exists(val):
                objects.append({
                    'lazy_file': (self.open_file, val),
                    'path': val,
                })
        if 'pk__in' in params:
            for val in params['pk__in']:
                if self.file_exists(val):
                    objects.append({
                        'lazy_file': (self.open_file, val),
                        'path': val,
                    })
        return objects
        raise UnsupportedOperation('Lookups must be by path')

    def delete(self, collection, params):
        params = self._normalize_params(collection, params)
        if 'pk' in params:
            self.delete_file(params['pk'])
        if 'pk__in' in params:
            for val in params['pk__in']:
                self.delete_file(val)
        return self.execute_hooks('afterDelete',
            {'collection': collection})
