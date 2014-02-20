# -*- coding: utf-8 -*-
import datetime
import uuid
import time
import random


#our own uuid1 constructer to use UTC instead of local time zone
_last_timestamp = None


def uuid1(node=None, clock_seq=None):
    """Generate a UUID from a host ID, sequence number, and the current time.
    If 'node' is not given, getnode() is used to obtain the hardware
    address.  If 'clock_seq' is given, it is used as the sequence number;
    otherwise a random 14-bit sequence number is chosen."""
    global _last_timestamp
    nanoseconds = int(time.time() * 1e9)
    # 0x01b21dd213814000 is the number of 100-ns intervals between the
    # UUID epoch 1582-10-15 00:00:00 and the Unix epoch 1970-01-01 00:00:00.
    timestamp = int(nanoseconds//100) + 0x01b21dd213814000L
    if _last_timestamp is not None and timestamp <= _last_timestamp:
        timestamp = _last_timestamp + 1
    _last_timestamp = timestamp
    if clock_seq is None:
        clock_seq = random.randrange(1<<14L) # instead of stable storage
    time_low = timestamp & 0xffffffffL
    time_mid = (timestamp >> 32L) & 0xffffL
    time_hi_version = (timestamp >> 48L) & 0x0fffL
    clock_seq_low = clock_seq & 0xffL
    clock_seq_hi_variant = (clock_seq >> 8L) & 0x3fL
    if node is None:
        node = uuid.getnode()
    return uuid.UUID(fields=(time_low, time_mid, time_hi_version,
                        clock_seq_hi_variant, clock_seq_low, node), version=1)


def version(srcColl, dstColl, version_id_field='_version'):
    """
    Versions the entries of one collection into another collection.
    """
    def indexF(identity, version, payload):
        """
        Emits two indexes:
            <version> to record the new object
            <id> to hold an array of seen versions
        """
        timestamp = datetime.datetime.utcnow()
        yield version, payload
        #TODO locking would be a good idea
        versions = dstColl.get(identity, [])
        versions.append({"timestamp": timestamp,
                         "version": version})
        yield identity, versions

    def assign_version(message):
        if message['collection'] != srcColl:
            return
        instance = message['instance']
        #uuid1 is chosen because it encodes the timestamp
        version = uuid1()
        if instance is not None:
            if hasattr(instance, '__setitem__'):
                instance[version_id_field] = version
            else:
                setattr(instance, version_id_field, version)
        '''
        try:
            identity__uuid = uuid.UUID(identity)
        except ValueError:
            version = uuid.uuid4()
        else:
            version = uuid.uuid5(identity__uuid, count)
        '''

    def record_version(action):
        def func(message):
            if message['collection'] != srcColl:
                return
            instance = message['instance']
            payload = None
            if action != 'delete':
                payload = srcColl.get_serializable(instance)
            identity = srcColl.get_object_id(instance)
            if hasattr(instance, '__getitem__'):
                version = instance[version_id_field]
            else:
                version = getattr(instance, version_id_field)
            entries = indexF(version=version, identity=identity, payload=payload)
            for key, value in entries:
                dstColl[key] = value
        return func

    srcColl.data_store.subsribe('beforeSave', assign_version)
    srcColl.data_store.subsribe('afterSave', record_version('update'))
    srcColl.data_store.subsribe('afterRemove', record_version('delete'))
