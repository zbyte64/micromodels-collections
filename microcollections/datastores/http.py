# -*- coding: utf-8 -*-
import requests
import json

from .core import BaseDataStore, UnsupportedOperation


class HTTPDataStore(BaseDataStore):
    '''
    A DataStore that connects to a RESTful web service
    '''
    def __init__(self, url, session=None):
        self.url = url
        if session is None:
            session = requests.Session()
        self.session = session

    def get_object_lookup(self, collection, instance):
        return collection.get_object_id(instance)

    def get_object_url(self, pk):
        url = self.url
        if not url.endswith('/'):
            url += '/'
        return url + str(pk)

    def get_index_url(self):
        return self.url

    def get_add_url(self):
        return self.url

    def serialize_data(self, data):
        return json.dumps(data)

    def deserialize_response(self, response):
        return response.json()

    def save(self, collection, instance):
        instance = self.execute_hooks('beforeSave',
            {'instance': instance, 'collection': collection})
        pk = self.get_object_lookup(collection, instance)
        payload = self.serialize_data(instance.to_dict(serial=True))
        if pk:
            self.session.put(self.get_object_url(pk), data=payload)
        else:
            self.session.post(self.get_add_url(), data=payload)
        return self.execute_hooks('afterSave',
            {'instance': instance, 'collection': collection})

    def remove(self, collection, instance):
        instance = self.execute_hooks('beforeDelete',
            {'instance': instance, 'collection': collection})
        pk = self.get_object_lookup(collection, instance)
        self.session.delete(self.get_object_lookup(pk))
        return self.execute_hooks('afterDelete',
            {'instance': instance, 'collection': collection})

    def get(self, collection, params):
        if 'pk' in params:
            response = self.session.get(self.get_object_url(params['pk']))
            return self.deserialize_response(response)
        raise UnsupportedOperation('Lookups must be by pk')

    def find(self, collection, params):
        response = self.session.get(self.get_index_url())
        return self.deserialize_response(response)
