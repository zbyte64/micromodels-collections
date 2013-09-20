# -*- coding: utf-8 -*-
import os
import io


class FileProxy(io.IOBase):
    def __init__(self, filestore, path=None, committed=False, overwrite=False):
        self.filestore = filestore
        self.path = path
        self.committed = committed
        self.overwrite = overwrite
        self._file = None

    def open(self, mode='r'):
        self._file = self.filestore.open(self.path, mode)
        if 'w' in mode:
            self.committed = False

    def close(self):
        self._file.close()
        super(FileProxy, self).close()

    def fileno(self):
        return self.file.fileno()

    def flush(self):
        return self._file.flush()

    def isatty(self):
        return self._file.isatty()

    def readline(self, *args, **kwargs):
        return self._file.readline(*args, **kwargs)

    def readlines(self, *args, **kwargs):
        return self._file.readlines(*args, **kwargs)

    def seek(self, *args, **kwargs):
        return self._file.seek(*args, **kwargs)

    def seekable(self):
        return self._file.seekable()

    def tell(self):
        return self._file.tell()

    def truncate(self, *args, **kwargs):
        return self._file.truncate(*args, **kwargs)

    def writable(self):
        return self._file.writable()

    def writelines(self, lines):
        return self._file.writelines(lines)

    def save(self, path=None):
        if self.committed:
            return
        self.filestore.save(self._file, path or self.path, self.overwrite)
        self.committed = True

    def delete(self):
        self.filestore.delete(self.path)
        self.committed = False


class BaseFileStore(object):
    def to_python(self, path):
        return FileProxy(filestore=self, path=path, committed=True, overwrite=True)

    def get_available_file_path(self, path):
        return path

    def save(self, file_obj, path, overwrite=False):
        return path

    def open(self, path, mode='r'):
        pass

    def delete(self, path):
        pass

    def to_serial(self, file_obj):
        if isinstance(file_obj, basestring):
            return file_obj
        if not isinstance(file_obj, FileProxy):
            desired_path = (getattr(file_obj, 'path', None) or
                            getattr(file_obj, 'name'))
            return self.save(file_obj, desired_path)
        return file_obj.path


class DirectoryFileStore(BaseFileStore):

    def __init__(self, directory):
        super(DirectoryFileStore, self).__init__()
        self.directory = directory

    def save(self, file_obj, path, overwrite=False):
        from werkzeug.utils import secure_filename
        path = secure_filename(path)
        if not overwrite:
            path = self.get_available_file_path(path)
        full_path = os.path.join(self.directory, path)
        open(full_path).write(file_obj)
        return path

    def open(self, path, mode='r'):
        from werkzeug.utils import secure_filename
        path = secure_filename(path)
        full_path = os.path.join(self.directory, path)
        return open(full_path, mode)

    def delete(self, path):
        from werkzeug.utils import secure_filename
        path = secure_filename(path)
        full_path = os.path.join(self.directory, path)
        os.unlink(full_path)
