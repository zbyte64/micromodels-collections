# -*- coding: utf-8 -*-
from types import GeneratorType

import micromodels


class CollectionQuery(object):

    def __init__(self, collection, params):
        self.collection = collection
        self.params = params
        self._cache = dict()

    @property
    def model(self):
        return self.collection.model

    @property
    def data_store(self):
        return self.collection.data_store

    @property
    def file_store(self):
        return self.collection.file_store

    def get(self, **params):
        if params:
            return self.clone(**params).get()
        if 'get' not in self._cache:
            result = self.data_store.get(self.collection, self.params)
            self._cache['get'] = \
                self.data_store.load_instance(self.collection, result)
        return self._cache['get']

    def __iter__(self):
        self._cache.setdefault('objects', dict())
        if 'results' not in self._cache:
            self.find()

        #yield cached objects
        index = 0
        while index in self._cache['objects']:
            yield self._cache['objects'][index]
            index += 1

        #yield objects not yet loaded
        for index, result in self._cache['results']:
            if index not in self._cache['objects']:  # redundant
                self._cache['objects'][index] = \
                    self.data_store.load_instance(self.collection, result)
            yield self._cache['objects'][index]

    def __getitem__(self, index):
        #TODO communicate to backend so that we don't fetch more then we need
        self._cache.setdefault('objects', dict())
        if 'results' not in self._cache:
            self.find()
        if isinstance(index, slice):
            def sliced_gen():
                for y, obj in enumerate(iter(self)):
                    if y >= index.start and y < index.stop:
                        yield obj
            return sliced_gen()
        else:
            if index in self._cache['objects']:
                return self._cache['objects'][index]
            else:
                for y, obj in enumerate(iter(self)):
                    if y == index:
                        return obj
        #TODO raise DoesNotExist

    def find(self, **params):
        if params:
            return self.clone(**params).find()
        if not self.params:
            return self.all()
        if 'results' not in self._cache:
            results = self.data_store.find(self.collection, self.params)
            self._cache['results'] = enumerate(results)
        return iter(self)

    def all(self):
        if self.params:
            return self.find()
        if 'results' not in self._cache:
            self._cache['results'] = self.data_store.all(self.collection)
        return iter(self)

    def delete(self):
        return self.data_store.delete(self.collection, self.params)

    def count(self):
        if 'count' not in self._cache:
            self._cache['count'] = \
                self.data_store.count(self.collection, self.params)
        return self._cache['count']

    def exists(self, **params):
        if params:
            return self.clone(**params).exists()
        if 'exists' not in self._cache:
            self._cache['exists'] = \
                self.data_store.exists(self.collection, self.params)
        return self._cache['exists']

    def clone(self, **params):
        new_params = dict(self.params)
        new_params.update(params)
        return type(self)(self.collection, new_params)


class CRUDHooks(object):
    def afterInitialize(self, instance):
        return instance

    def beforeCreate(self, params):
        return params

    def afterCreate(self, instance):
        return instance

    def beforeSave(self, instance):
        return instance

    def afterSave(self, instance):
        return instance

    def beforeDelete(self, instance):
        return instance

    def afterDelete(self, instance):
        return instance


class Collection(CRUDHooks):
    object_id_field = 'id'

    def __init__(self, model, data_store, file_store, name=None,
                 object_id_field=None, params=None):
        self.model = model
        self.data_store = data_store
        self.file_store = file_store
        self.name = name or model.__name__
        self.params = params
        if object_id_field:
            self.object_id_field = object_id_field

    def get_object_id(self, instance):
        object_id = getattr(instance, self.object_id_field, None)
        if callable(object_id):
            object_id = object_id()
        return object_id

    def get_query(self, **params):
        if self.params:
            params.update(self.params)
        return CollectionQuery(self, params)

    def get(self, **params):
        '''
        Returns a single object matching the query params
        '''
        return self.get_query(**params).get()

    def find(self, **params):
        '''
        Returns a query object that iterates over instances matching the query params
        '''
        return self.get_query(**params)

    def exists(self, **params):
        '''
        Returns a boolean on whether objects match the query params
        '''
        return self.get_query(**params).exists()

    def new(self, **params):
        '''
        Instantiates and returns a new instance
        '''
        instance = self.model(**params)
        return self.afterInitialize(instance)

    def create(self, **params):
        '''
        Saves a new instance
        '''
        instance = self.new(**params)
        return self.save(instance)
    
    def save(self, instance):
        return self.data_store.save(self, instance)
    
    def remove(self, instance):
        return self.data_store.remove(self, instance)

    def all(self):
        return self.get_query()

    def delete(self):
        return self.get_query().delete()

    def count(self):
        return self.get_query().count()

    def _process_file_fields(self, instance, callback):
        for key, field in instance._fields.items():
            if isinstance(field, micromodels.FileField):
                field.set_serializer(self.file_store.to_serial)
                val = getattr(instance, key)
                setattr(instance, key, callback(val))
            elif isinstance(field, micromodels.ModelField):
                val = getattr(instance, key)
                if val:
                    self._process_file_fields(val, callback)
            elif isinstance(field, micromodels.ModelCollectionField):
                for val in getattr(instance, key):
                    self._process_file_fields(val, callback)
            elif (isinstance(field, micromodels.FieldCollectionField) and
                  isinstance(field._instance, micromodels.FileField)):
                field._instance.set_serializer(self.file_store.to_serial)
                val = getattr(instance, key)
                new_val = list()
                for subval in val:
                    new_val.append(callback(subval))
                setattr(instance, key, new_val)

    def afterInitialize(self, instance):
        instance._collection = self
        if not hasattr(instance, 'delete'):
            instance.delete = lambda: self.remove(instance)
        if not hasattr(instance, 'save'):
            instance.save = lambda: self.save(instance)

        #load files
        def callback(file_path):
            return self.file_store.load(file_path)
        self._process_file_fields(instance, callback)

        return instance

    def beforeSave(self, instance):
        #save files
        def callback(file_obj):
            if not isinstance(file_obj, basestring):
                file_obj.save()
            return file_obj
        self._process_file_fields(instance, callback)

        return instance

    def afterDelete(self, instance):
        #remove files
        def callback(file_obj):
            file_obj.delete()
        self._process_file_fields(instance, callback)

        return instance


class PolymorphicLoader(object):
    def __init__(self, poly_collection):
        self.collection = poly_collection

    def __call__(self, **values):
        object_type = self.collection.get_object_type_from_values(values)
        model = self.collection.get_model(object_type)
        return model(**values)


class PolymorphicCollection(Collection):
    '''
    A collection representing mixed objects
    '''
    object_type_field = '_object_type'
    object_types_field = '_object_types'

    def __init__(self, model, *args, **kwargs):
        self.base_model = model
        #TODO monkey patch __new__ to record to model types
        # object_type => model
        self.descendent_registry = dict()

        # model => (object_type, object_types)
        self.reverse_descendent_registry = dict()
        model = PolymorphicLoader(self)
        super(PolymorphicCollection, self).__init__(model, *args, **kwargs)

    def get_model(self, object_type):
        if object_type not in self.descendent_registry:
            #TODO attempt import
            pass
        return self.descendent_registry.get(object_type, self.base_model)

    def extract_object_type(self, cls):
        return '%s.%s' (cls.__module__, cls.__name__)

    def add_new_model(self, model):
        if not issubclass(model, self.base_model):
            return
        object_type = self.extract_object_type(model)
        object_types = [object_type]

        def collect_parents(bases):
            for entry in bases:
                if isinstance(entry, tuple):
                    collect_parents(entry)
                elif issubclass(entry, micromodels.Model):
                    parent_type = self.extract_object_type(entry)
                    object_types.append(parent_type)

        collect_parents(model.__bases__)
        self.descendent_registry[object_type] = model
        self.reverse_descendent_registry[model] = (object_type, object_types)

    def get_object_type(self, instance):
        model = type(instance)
        if model in self.reverse_descendent_registry:
            return self.reverse_descendent_registry[model][0]
        object_type = getattr(instance, self.object_type_field, None)
        if object_type is None:
            object_type = self.extract_object_type(type(instance))
        if callable(object_type):
            object_type = object_type()
        return object_type

    def get_object_types(self, instance):
        model = type(instance)
        if model in self.reverse_descendent_registry:
            return self.reverse_descendent_registry[model][1]
        object_types = getattr(instance, self.object_types_field, None)
        if callable(object_types):
            object_types = object_types()
        return object_types

    def get_object_type_from_values(self, values):
        return values.get(self.object_type_field, None)

    def afterInitialize(self, instance):
        instance = super(PolymorphicCollection, self).afterInitialize(instance)
        object_type = self.get_object_type(instance)
        if object_type:
            instance.add_field(self.object_type_field, object_type,
                micromodels.CharField())
        else:
            assert False, 'Why is object type None?'
        object_types = self.get_object_types(instance)
        if object_types is not None:
            instance.add_field(self.object_types_field, object_types,
                micromodels.FieldCollectionField(micromodels.CharField()))
        else:
            assert False, 'Why is object types None?'
        return instance

    def findType(self, cls, **params):
        object_type = self.extract_object_type(cls)
        params[self.object_types_field] = object_type
        return self.find(**params)
