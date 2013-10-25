# -*- coding: utf-8 -*-
from .core import BaseDataStore


class MemoryDataStore(BaseDataStore):
    def __init__(self):
        super(MemoryDataStore, self).__init__()
        self.collections = dict()

    def _get_cstore(self, collection):
        if collection.name not in self.collections:
            self.collections[collection.name] = dict()
        return self.collections[collection.name]

    def save(self, collection, instance, key=None):
        instance = self.execute_hooks('beforeSave',
            {'instance': instance, 'collection': collection})
        pk = key or collection.get_object_id(instance)
        cstore = self._get_cstore(collection)
        cstore[pk] = collection.get_serializable(instance)
        return self.execute_hooks('afterSave',
            {'instance': instance, 'collection': collection})

    def remove(self, collection, instance):
        instance = self.execute_hooks('beforeRemove',
            {'instance': instance, 'collection': collection})
        pk = collection.get_object_id(instance)
        cstore = self._get_cstore(collection)
        cstore.pop(pk, None)
        return self.execute_hooks('afterRemove',
            {'instance': instance, 'collection': collection})

    def get(self, collection, params):
        params = self._normalize_params(collection, params)
        if 'pk' in params:
            cstore = self._get_cstore(collection)
            return cstore[params['pk']]
        return self.find(collection, params)[0]

    def find(self, collection, params):
        cstore = self._get_cstore(collection)
        if params:
            params = self._normalize_params(collection, params)
            objects = list()
            for pk, obj in cstore.items():
                match = True
                for param, value in params.items():
                    if param == 'pk':
                        match &= value == pk
                    elif param == 'pk__in':
                        match &= pk in value
                    elif param.endswith('__in'):
                        param = param[:-len('__in')]
                        match &= obj.get(param) in value
                    else:
                        match &= obj.get(param) == value
                    if not match:
                        break
                if match:
                    objects.append(obj)
            return objects
        return cstore.values()

    def delete(self, collection, params):
        cstore = self._get_cstore(collection)
        objects = self.find(collection, params)
        for obj in objects:
            pk = obj.get(collection.object_id_field)
            cstore.pop(pk)
        return self.execute_hooks('afterDelete',
            {'collection': collection})
