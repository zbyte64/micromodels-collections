# -*- coding: utf-8 -*-


class UnsupportedOperation(Exception):
    pass


class BaseDataStore(object):
    def execute_hooks(self, hook, kwargs):
        return getattr(kwargs.pop('collection'), hook)(**kwargs)

    def load_instance(self, collection, result):
        instance = collection.get_loader()(**result)
        return self.execute_hooks('afterInitialize',
            {'instance': instance, 'collection': collection})

    def save(self, collection, instance):
        raise UnsupportedOperation

    def remove(self, collection, instance):
        raise UnsupportedOperation

    def get(self, collection, params):
        raise UnsupportedOperation

    def find(self, collection, params):
        raise UnsupportedOperation

    def all(self, collection):
        return self.find(collection, {})

    def delete(self, collection, params):
        raise UnsupportedOperation

    def exists(self, collection, params):
        return bool(self.count(collection, params))

    def count(self, collection, params):
        return len(self.find(collection, params))