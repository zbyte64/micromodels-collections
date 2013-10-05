# -*- coding: utf-8 -*-
import re

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
        for pattern, func in self.file_stores.items():
            found = re.compile(pattern).match(uri)
            if found:
                return func, found.groupdict() or {'path':found.groups()[0]}


class ProxyFileStore(BaseFileStore):
    def __init__(self, collection):
        self.collection = collection

    def get_available_file_uri(self, uri):
        file_store, kwargs = self.collection.lookup_data_store(uri)
        path = file_store.get_available_file_path(kwargs['path'])
        return path #TODO

    def save_file(self, file_obj, uri):
        file_store, kwargs = self.collection.lookup_data_store(uri)
        return file_store.save_file(file_obj, kwargs['path'])

    def open_file(self, uri, mode='rb'):
        file_store, kwargs = self.collection.lookup_data_store(uri)
        return file_store.open_file(kwargs['path'], mode)

    def delete_file(self, uri):
        file_store, kwargs = self.collection.lookup_data_store(uri)
        return file_store.delete_file(kwargs['path'])

    def file_exists(self, uri):
        file_store, kwargs = self.collection.lookup_data_store(uri)
        return file_store.file_exists(kwargs['path'])

    def save(self, collection, instance):
        instance = self.execute_hooks('beforeSave',
            {'instance': instance, 'collection': collection})
        self.save_file(instance, instance.uri)
        return self.execute_hooks('afterSave',
            {'instance': instance, 'collection': collection})

    def remove(self, collection, instance):
        instance = self.execute_hooks('beforeRemove',
            {'instance': instance, 'collection': collection})
        self.delete_file(instance.uri)
        return self.execute_hooks('afterRemove',
            {'instance': instance, 'collection': collection})
