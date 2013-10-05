#Shamelessly copied from django


class FileProxyMixin(object):
    """
    A mixin class used to forward file methods to an underlaying file
    object.  The internal file object has to be called "file"::

        class FileProxy(FileProxyMixin):
            def __init__(self, file):
                self.file = file
    """

    encoding = property(lambda self: self.file.encoding)
    fileno = property(lambda self: self.file.fileno)
    flush = property(lambda self: self.file.flush)
    isatty = property(lambda self: self.file.isatty)
    newlines = property(lambda self: self.file.newlines)
    read = property(lambda self: self.file.read)
    readinto = property(lambda self: self.file.readinto)
    readline = property(lambda self: self.file.readline)
    readlines = property(lambda self: self.file.readlines)
    seek = property(lambda self: self.file.seek)
    softspace = property(lambda self: self.file.softspace)
    tell = property(lambda self: self.file.tell)
    truncate = property(lambda self: self.file.truncate)
    write = property(lambda self: self.file.write)
    writelines = property(lambda self: self.file.writelines)
    xreadlines = property(lambda self: self.file.xreadlines)

    def __iter__(self):
        return iter(self.file)


class FileProxy(FileProxyMixin):
    def __init__(self, path, file=None, lazy_file=None):
        assert file or lazy_file
        self._file = file
        self.lazy_file = lazy_file
        self.path = path

    @property
    def file(self):
        if not self._file:
            self._file = self.lazy_file[0](*self.lazy_file[1:])
        return self._file


class URIFileProxy(FileProxy):
    def __init__(self, uri, file=None, lazy_file=None):
        assert file or lazy_file
        self._file = file
        self.lazy_file = lazy_file
        self.uri = uri
