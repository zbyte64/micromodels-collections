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

    #CONSIDER: it is up to the collection to provide uri <=> path transalations


class BaseFileStore(BaseDataStore):
    def get_available_file_path(self, path):
        return path

    def save_file(self, file_obj, path):
        raise NotImplementedError

    def open_file(self, path, mode='rb'):
        raise NotImplementedError

    def delete_file(self, path):
        raise NotImplementedError

    def save(self, collection, instance):
        instance = self.execute_hooks('beforeSave',
            {'instance': instance, 'collection': collection})
        self.save_file(instance, instance.path)
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
        return {
            'lazy_file': (self.open_file, path),
            'path': path,
        }

    def find(self, collection, params):
        params = self._normalize_params(collection, params)
        if 'pk' in params:
            return [self.get(collection, params)]
        if 'pk__in' in params:
            objects = list()
            for val in params['pk__in']:
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
