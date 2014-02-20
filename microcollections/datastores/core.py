# -*- coding: utf-8 -*-


class UnsupportedOperation(Exception):
    pass


class BaseDataStore(object):
    def __init__(self):
        self.subscribers = dict()

    def subsribe(self, topic, callback):
        self.subscribers.setdefault(topic, []).append(callback)

    def publish(self, topic, message):
        subscribers = self.subscribers.get(topic)
        if not subscribers:
            return
        message["topic"] = topic
        #stubs
        message["action_id"] = None #TODO uuid, useful for versioning
        message["transaction_id"] = None #TODO
        for callback in subscribers:
            callback(message)

    def execute_hooks(self, hook, kwargs):
        self.publish(hook, kwargs)
        return getattr(kwargs.pop('collection'), hook)(**kwargs)

    def load_instance(self, collection, result):
        instance = collection.get_loader()(**result)
        return self.execute_hooks('afterInitialize',
            {'instance': instance, 'collection': collection})

    def save(self, collection, instance, key=None):
        raise UnsupportedOperation

    def remove(self, collection, instance):
        raise UnsupportedOperation

    def _normalize_params(self, collection, params):
        id_field = collection.object_id_field
        if id_field:
            params = dict(params)
            if id_field in params:
                params['pk'] = params.pop(id_field)
            if '%s__in' % id_field in params:
                params['pk__in'] = params.pop('%s__in' % id_field)
        return params

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

    def keys(self, collection, params):
        for instance in self.find(collection, params):
            yield collection.get_object_id(instance)