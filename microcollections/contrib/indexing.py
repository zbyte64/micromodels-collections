# -*- coding: utf-8 -*-
def index(srcColl, dstColl, indexF):
    """
    Creates an overslimpified index by
    listening on one collection and recording on another collection
    The index function takes a CRUD message and emits keys
    Removes the index for the object id when removed
    """
    def record_new_version(message):
        if message['collection'] != srcColl:
            return
        ident = srcColl.get_object_id(message['instance'])
        index_key = (':index', ident)
        stale_indexes = dstColl.get(index_key, [])
        new_indexes = list()
        for key, value in indexF(message):
            new_indexes.append(key)
            dstColl[key] = value
        remove = set(stale_indexes) - set(new_indexes)
        if remove:
            dstColl.find(pk__in=remove).delete()
        dstColl[index_key] = new_indexes

    def remove_index_refs(message):
        if message['collection'] != srcColl:
            return
        ident = srcColl.get_object_id(message['instance'])
        index_key = (':index', ident)
        stale_indexes = dstColl.get(index_key, [])
        stale_indexes.append(index_key)
        dstColl.find(pk__in=stale_indexes).delete()

    srcColl.data_source.subsribe('afterCreate', record_new_version)
    srcColl.data_source.subsribe('afterSave', record_new_version)
    srcColl.data_source.subsribe('afterRemove', remove_index_refs)

    def do_lookup(**kwargs):
        pass

    return do_lookup
