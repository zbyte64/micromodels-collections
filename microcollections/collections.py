# -*- coding: utf-8 -*-
import micromodels


class NotSet:
    pass


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
        raise KeyError('Not found: %s' % index)

    def __len__(self):
        return self.count()

    def find(self, **params):
        if params:
            return self.clone(**params).find()
        if not self.params:
            return self.all()
        if 'results' not in self._cache:
            results = self.data_store.find(self.collection, self.params)
            self._cache['results'] = enumerate(results)
        return iter(self)

    def first(self, **params):
        if params:
            return self.clone(**params).first()
        if not self.params:
            return self.all().next()
        if 'results' not in self._cache:
            results = self.data_store.find(self.collection, self.params)
            self._cache['results'] = enumerate(results)
        try:
            return iter(self).next()
        except StopIteration:
            return None

    def all(self):
        if self.params:
            return self.find()
        if 'results' not in self._cache:
            results = self.data_store.all(self.collection)
            self._cache['results'] = enumerate(results)
        return iter(self)

    def delete(self):
        return self.data_store.delete(self.collection, self.params)

    def count(self):
        if 'count' not in self._cache:
            self._cache['count'] = \
                self.data_store.count(self.collection, self.params)
        return self._cache['count']

    def keys(self):
        if 'keys' not in self._cache:
            self._cache['keys'] = \
                self.data_store.keys(self.collection, self.params)
        return self._cache['keys']

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
    def modelRegistered(self, model):
        return model

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

    def beforeRemove(self, instance):
        return instance

    def afterRemove(self, instance):
        return instance

    #CONSIDER: ids or params?
    def afterDelete(self):
        return


class BaseCollection(CRUDHooks):
    model = None
    object_id_field = None
    id_generator = None
    params = dict()

    def get_query(self, **params):
        if self.params:
            params.update(self.params)
        return CollectionQuery(self, params)

    def get_loader(self):
        return self.model

    def get_object_id(self, instance):
        object_id = instance.get(self.object_id_field, self.id_generator)
        if callable(object_id):
            object_id = object_id()
        return object_id

    def get_serializable(self, instance):
        '''
        Returns an object representation that can be easily serialized
        '''
        return instance

    ## Dictionary like methods ##

    def __setitem__(self, key, instance):
        if self.object_id_field:
            if hasattr(instance, '__setitem__'):
                instance[self.object_id_field] = key
            elif hasattr(instance, self.object_id_field):
                setattr(instance, self.object_id_field, key)
        return self.save(instance, key)

    def __getitem__(self, key):
        return self.get(pk=key)

    def __delitem__(self, key):
        return self.find(pk=key).delete()

    def __contains__(self, key):
        return self.exists(pk=key)

    def __len__(self):
        return self.count()

    def keys(self):
        return self.get_query().keys()

    def values(self):
        return self.all()

    def items(self):
        #TODO make efficient
        for key in self.keys():
            yield (key, self.get(key))

    def update(self, items):
        for key, value in items.items():
            self[key] = value

    def pop(self, key, default=NotSet):
        try:
            instance = self[key]
        except (KeyError, IndexError):
            if default == NotSet:
                raise
            return default
        instance.remove()
        return instance

    def has_key(self, key):
        return key in self

    def clear(self):
        self.delete()

    def setdefault(self, key, value):
        if key in self:
            return
        self[key] = value

    def copy(self):
        return dict(self.items())

    def get(self, pk=NotSet, _default=None, **params):
        '''
        Returns a single object matching the query params
        Raises exception if no object matches
        '''
        if pk is not NotSet:
            params['pk'] = pk
        try:
            return self.get_query(**params).get()
        except (KeyError, IndexError):
            return _default

    ## Query Methods ##

    def first(self, **params):
        '''
        Returns a single object matching the query params
        Returns None if no object matches
        '''
        return self.get_query(**params).first()

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

    def save(self, instance, key=None):
        return self.data_store.save(self, instance, key)

    def remove(self, instance):
        return self.data_store.remove(self, instance)

    def all(self):
        return self.get_query()

    def delete(self):
        return self.get_query().delete()

    def count(self):
        return self.get_query().count()

    def __iter__(self):
        return self.get_query().__iter__()


class RawCollection(BaseCollection):
    '''
    A collection that returns dictionaries and responds like a dictionary
    '''
    object_id_field = 'id'

    def __init__(self, data_store, model=dict, name=None,
                 object_id_field=None, id_generator=None, params=None):
        self.model = model
        self.data_store = data_store
        self.name = name
        self.params = params or dict()
        if object_id_field:
            self.object_id_field = object_id_field
        if id_generator:
            self.id_generator = id_generator

    ## Hooks ##

    def beforeSave(self, instance):
        #set the id field if we have one
        if self.object_id_field:
            key = self.get_object_id(instance)
            if hasattr(instance, '__setitem__'):
                instance[self.object_id_field] = key
            else:
                setattr(instance, self.object_id_field, key)
        return super(RawCollection, self).beforeSave(instance)


class Collection(RawCollection):
    '''
    A collection bound to a schema and returns model instances
    '''
    def __init__(self, model, data_store, name=None,
                 object_id_field=None, id_generator=None, params=None):
        if name is None:
            name = model.__name__
        super(Collection, self).__init__(model=model, data_store=data_store,
            name=name, object_id_field=object_id_field,
            id_generator=id_generator, params=params,)

    def prepare_model(self, model):
        '''
        Legacy hook, you shouldn't modify the model, but if you do return a new
        class
        '''
        return self.modelRegistered(model)

    def get_loader(self):
        '''
        Returns a callable that returns an instantiated model instance
        '''
        if not hasattr(self, '_prepped_model'):
            self._prepped_model = self.prepare_model(self.model)
        return self._prepped_model

    def get_object_id(self, instance):
        object_id = getattr(instance, self.object_id_field, self.id_generator)
        if callable(object_id):
            object_id = object_id()
        return object_id

    def get_serializable(self, instance):
        return instance.to_dict(serial=True)

    def modelRegistered(self, model):
        model._collection = self

        if not hasattr(model, 'remove'):
            def remove(instance):
                return self.remove(instance)
            model.remove = remove

        if not hasattr(model, 'save'):
            def save(instance):
                return self.save(instance)
            model.save = save

        return model


class PolymorphicLoader(object):
    '''
    Returns the proper model class based on the object type field
    '''
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
        self.prepped_base_model = self.prepare_model(model)

        # object_type => model
        self.descendent_registry = dict()

        # model => (object_type, object_types)
        self.reverse_descendent_registry = dict()
        super(PolymorphicCollection, self).__init__(model, *args, **kwargs)

    def get_loader(self):
        return PolymorphicLoader(self)

    def get_model(self, object_type):
        if object_type not in self.descendent_registry:
            self.load_model(object_type)
        return self.descendent_registry.get(object_type,
                                            self.prepped_base_model)

    def load_model(self, object_type):
        #import and add model here
        pass

    def extract_object_type(self, cls):
        return '%s.%s' (cls.__module__, cls.__name__)

    def register_model(self, model):
        '''
        Registers a new model to belong in the collection
        '''
        if not issubclass(model, (self.model, self.prepped_base_model)):
            return
        model = self.prepare_model(model)
        object_type = self.extract_object_type(model)
        object_types = [object_type]

        def collect_parents(bases):
            for entry in bases:
                if isinstance(entry, tuple):
                    collect_parents(entry)
                elif issubclass(entry, micromodels.Model):
                    parent_type = self.extract_object_type(entry)
                    if parent_type not in object_types:
                        object_types.append(parent_type)

        collect_parents(model.__bases__)
        self.descendent_registry[object_type] = model
        self.reverse_descendent_registry[model] = (object_type, object_types)

    def get_object_type(self, instance):
        '''
        Return a string representing the model instance type
        '''
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
        '''
        Return a list of strings representing the various inherritted types of
        the model instance
        '''
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
        if object_types:
            assert len(set(object_types)) == len(object_types), 'Duplicate object types detected'
            instance.add_field(self.object_types_field, object_types,
                micromodels.FieldCollectionField(micromodels.CharField()))
        else:
            assert False, 'Why is object types None?'
        return instance

    def findType(self, cls, **params):
        object_type = self.extract_object_type(cls)
        params[self.object_types_field] = object_type
        return self.find(**params)
