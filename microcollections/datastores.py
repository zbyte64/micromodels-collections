# -*- coding: utf-8 -*-


class UnsupportedOperation(Exception):
    pass


class BaseDataStore(object):
    def execute_hooks(self, hook, kwargs):
        return getattr(kwargs.pop('collection'), hook)(**kwargs)

    def load_instance(self, collection, result):
        instance = collection.model(**result)
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


class MemoryDataStore(BaseDataStore):

    def __init__(self, collection):
        super(MemoryDataStore, self).__init__(collection)
        self.objects = dict()

    def get_object_lookup(self, collection, instance):
        pk = collection.get_object_id(instance)
        return self.get_lookup(collection, pk)

    def get_lookup(self, collection, pk):
        return hash('%s-%s' % (collection.name, pk))

    def save(self, collection, instance):
        instance = self.execute_hooks('beforeSave',
            {'instance': instance, 'collection': collection})
        pk = self.get_object_lookup(collection, instance)
        self.objects[pk] = instance.to_dict(serial=True)
        return self.execute_hooks('afterSave',
            {'instance': instance, 'collection': collection})

    def remove(self, collection, instance):
        instance = self.execute_hooks('beforeDelete',
            {'instance': instance, 'collection': collection})
        pk = self.get_object_lookup(collection, instance)
        self.objects.pop(pk, None)
        return self.execute_hooks('afterDelete',
            {'instance': instance, 'collection': collection})

    def get(self, collection, params):
        if 'pk' in params:
            return self.objects[self.get_lookup(collection, params['pk'])]
        raise UnsupportedOperation('Lookups must be by pk')

    def find(self, collection, params):
        if params:
            if 'pk__in' in params:
                results = list()
                for pk in params['pk__in']:
                    results.append(self.get(collection, pk))
                return results
            else:
                raise UnsupportedOperation('Lookups must be by pk')
        return self.objects.values()

    def delete(self, collection, params):
        if params:
            if 'pk__in' in params:
                for pk in params['pk__in']:
                    self.objects.pop(self.get(collection, pk), None)
            else:
                raise UnsupportedOperation('Lookups must be by pk')
        self.objects = dict()
