# -*- coding: utf-8 -*-
import re

from microcollections.datastores.core import UnsupportedOperation
from .core import BaseCollection, BaseFileStore
from .utils import URIFileProxy


class URICollection(BaseCollection):
    '''
    Proxies to regex matched FileCollections
    '''
    #CONSIDER: it is up to the collection to provide uri <=> path transalations
    model = URIFileProxy
    object_id_field = 'uri'

    def __init__(self, file_stores):
        self.file_stores = file_stores
        self.data_store = ProxyFileStore(self)

    def lookup_data_store(self, uri):
        for pattern, fs in self.file_stores.items():
            found = re.compile(pattern).match(uri)
            if found:
                kwargs = found.groupdict() or {'path':found.groups()[0]}
                kwargs['file_store'] = fs
                return kwargs
        return None
        #TODO return NullFileStore

    def get_available_key(self, path):
        return self.data_store.get_available_key(path)


class ProxyFileStore(BaseFileStore):
    def __init__(self, collection):
        self.collection = collection

    def get_available_key(self, uri):
        kwargs = self.collection.lookup_data_store(uri)
        if not kwargs:
            raise KeyError('Could not match a file store for the uri')
        path = kwargs['file_store'].get_available_key(kwargs['path'])
        return kwargs['file_store'].uri(path)

    def save_file(self, file_obj, uri):
        kwargs = self.collection.lookup_data_store(uri)
        if not kwargs:
            raise KeyError('Could not match a file store for the uri')
        return kwargs['file_store'].save_file(file_obj, kwargs['path'])

    def open_file(self, uri, mode='rb'):
        kwargs = self.collection.lookup_data_store(uri)
        if not kwargs:
            raise KeyError('Could not match a file store for the uri')
        return kwargs['file_store'].open_file(kwargs['path'], mode)

    def delete_file(self, uri):
        kwargs = self.collection.lookup_data_store(uri)
        if not kwargs:
            raise KeyError('Could not match a file store for the uri')
        return kwargs['file_store'].delete_file(kwargs['path'])

    def file_exists(self, uri):
        kwargs = self.collection.lookup_data_store(uri)
        if not kwargs:
            return False
            raise KeyError('Could not match a file store for the uri')
        return kwargs['file_store'].file_exists(kwargs['path'])

    def save(self, collection, instance, key=None):
        instance = self.execute_hooks('beforeSave',
            {'instance': instance, 'collection': collection})
        self.save_file(instance, key or instance.uri)
        return self.execute_hooks('afterSave',
            {'instance': instance, 'collection': collection})

    def remove(self, collection, instance):
        instance = self.execute_hooks('beforeRemove',
            {'instance': instance, 'collection': collection})
        self.delete_file(instance.uri)
        return self.execute_hooks('afterRemove',
            {'instance': instance, 'collection': collection})

    def get(self, collection, params):
        uri = self._normalize_params(collection, params).get('pk', None)
        if uri is None:
            raise UnsupportedOperation('Lookups must be by uri, got: %s' % params)
        if not self.file_exists(uri):
            raise KeyError('URI not found: %s' % uri)
        return {
            'lazy_file': (self.open_file, uri),
            'uri': uri,
        }

    def find(self, collection, params):
        params = self._normalize_params(collection, params)
        objects = list()
        if 'pk' in params:
            val = params['pk']
            if self.file_exists(val):
                objects.append({
                    'lazy_file': (self.open_file, val),
                    'uri': val,
                })
        if 'pk__in' in params:
            for val in params['pk__in']:
                if self.file_exists(val):
                    objects.append({
                        'lazy_file': (self.open_file, val),
                        'uri': val,
                    })
        return objects
        raise UnsupportedOperation('Lookups must be by uri')
