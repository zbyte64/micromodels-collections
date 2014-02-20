# -*- coding: utf-8 -*-
import json

from microcollections.datastores.core import BaseDataStore


class DjangoTableDataStore(BaseDataStore):
    def __init__(self, table,
            seeker=lambda col, pk: {'collection': col, 'identifier': pk},
            setter=lambda x: {'data': json.dumps(x)},
            getter=lambda obj: json.loads(obj.data)):
        super(DjangoTableDataStore, self).__init__()
        self.table = table
        self.manager = table.objects
        self.seeker = seeker
        self.setter = setter
        self.getter = getter

    def save(self, collection, instance, key=None):
        instance = self.execute_hooks('beforeSave',
            {'instance': instance, 'collection': collection})
        pk = key or collection.get_object_id(instance)
        payload = collection.get_serializable(instance)
        lookup = self.seeker(collection, pk)
        properties = self.setter(payload)
        try:
            entry = self.manager.get(**lookup)
        except self.table.DoesNotExist:
            lookup.update(properties)
            entry = self.manager.create(**lookup)
        else:
            for k, v in properties.items():
                setattr(entry, k, v)
            entry.save()
        return self.execute_hooks('afterSave',
            {'instance': instance, 'collection': collection})

    def remove(self, collection, instance):
        instance = self.execute_hooks('beforeRemove',
            {'instance': instance, 'collection': collection})
        pk = collection.get_object_id(instance)
        lookup = self.seeker(collection, pk)
        self.manager.filter(**lookup).delete()
        return self.execute_hooks('afterRemove',
            {'instance': instance, 'collection': collection})

    def _normalize_params(self, collection, params):
        params = super(DjangoTableDataStore, self)._normalize_params(collection, params)
        if 'pk' in params:
            params.update(self.seeker(collection, params.pop('pk')))
        if 'pk__in' in params:
            new_lookups = list()
            for pk in params['pk__in']:
                new_lookups.append(self.seeker(collection, pk))
            params['pk__in'] = new_lookups
        return params

    def get(self, collection, params):
        params = self._normalize_params(collection, params)
        return self.getter(self.manager.get(**params))

    def find(self, collection, params):
        params = self._normalize_params(collection, params)
        return map(self.getter, self.manager.filter(**params))

    def delete(self, collection, params):
        params = self._normalize_params(collection, params)
        self.manager.filter(**params).delete()
        return self.execute_hooks('afterDelete',
            {'collection': collection})
