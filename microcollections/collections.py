# -*- coding: utf-8 -*-


class CollectionQuery(object):

    def __init__(self, collection, params):
        self.collection = collection
        self.params = params
        self._cache = dict()

    @property
    def model(self):
        return self.collection.model

    @property
    def datastore(self):
        return self.collection.datastore

    @property
    def filestore(self):
        return self.collection.filestore

    def get(self, **params):
        if params:
            return self.clone(**params).get()
        if 'get' in self._cache:
            result = self.datastore.get(self.params)
            self._cache['get'] = \
                self.datastore.load_instance(self.collection, result)
        return self._cache['get']

    def __iter__(self):
        self._cache.setdefault('objects', dict())
        for index, result in enumerate(self._cache['results']):
            if index not in self._cache['objects']:
                self._cache['objects'][index] = \
                    self.datastore.load_instance(self.collection, result)
            yield self._cache['objects'][index]

    def find(self, **params):
        if params:
            return self.clone(**params).find()
        if not self.params:
            return self.all()
        if 'results' not in self._cache:
            self._cache['results'] = \
                self.datastore.find(self.collection, self.params)
        return iter(self)

    def all(self):
        if self.params:
            return self.find()
        if 'results' not in self._cache:
            self._cache['results'] = self.datastore.all(self.collection)
        return iter(self)

    def delete(self):
        return self.datastore.delete(self.collection, self.params)

    def count(self):
        if 'count' in self._cache:
            self._cache['count'] = \
                self.datastore.count(self.collection, self.params)
        return self._cache['count']

    def exists(self, **params):
        if params:
            return self.clone(**params).exists()
        if 'exists' in self._cache:
            self._cache['exists'] = \
                self.datastore.exists(self.collection, self.params)
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

    def __init__(self, model, datastore, filestore, name=None,
                 object_id_field=None):
        self.model = model
        self.datastore = datastore
        self.filestore = filestore
        self.name = name or model.__name__
        if object_id_field:
            self.object_id_field = object_id_field

    def get_object_id(self, instance):
        object_id = getattr(instance, self.object_id_field, None)
        if callable(object_id):
            object_id = object_id()
        return object_id

    def get_query(self, **params):
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
        return self.datastore.save(instance)

    def all(self):
        return self.get_query()

    def delete(self):
        return self.get_query().delete()

    def count(self):
        return self.get_query().count()

    def _process_file_fields(self, instance, callback):
        from micromodels import FileField, ModelField, ModelCollectionField, FieldCollectionField
        for key, field in instance._fields.items():
            if isinstance(field, FileField):
                val = getattr(instance, key)
                setattr(instance, key, callback(val))
            elif isinstance(field, ModelField):
                val = getattr(instance, key)
                if val:
                    self._process_file_fields(val, callback)
            elif isinstance(field, ModelCollectionField):
                for val in getattr(instance, key):
                    self._process_file_fields(val, callback)
            elif (isinstance(field, FieldCollectionField) and
                  isinstance(field._instance, FileField)):
                val = getattr(instance, key)
                new_val = list()
                for subval in val:
                    new_val.append(callback(subval))
                setattr(instance, key, new_val)

    def afterInitialize(self, instance):
        instance.delete = lambda: self.datastore.remove(self, instance)
        instance.save = lambda: self.datastore.save(self, instance)

        #load files
        def callback(file_path):
            if isinstance(file_path, basestring):
                return self.filestore.load(file_path)
            return file_path
        self._process_file_fields(instance, callback)

        return instance

    def beforeSave(self, instance):
        #save files
        def callback(file_obj):
            if not isinstance(file_obj, basestring):
                return self.filestore.save(file_obj)
            return file_obj
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

    def __init__(self, model, **kwargs):
        self.base_model = model
        #TODO monkey patch __new__ to record to model types
        # object_type => model
        self.descendent_registry = dict()

        # model => (object_type, object_types)
        self.reverse_descendent_registry = dict()
        model = PolymorphicLoader(self)
        super(PolymorphicCollection, self).__init__(model, **kwargs)

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
                elif issubclass(entry, self.base_model):
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
            setattr(instance, self.object_type_field, object_type)
        object_types = self.get_object_types(instance)
        if object_types is not None:
            setattr(instance, self.object_types_field, object_types)
        return instance
