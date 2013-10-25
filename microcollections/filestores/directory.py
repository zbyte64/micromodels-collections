# -*- coding: utf-8 -*-
import os

from werkzeug.utils import secure_filename

from .core import BaseFileStore


class DirectoryFileStore(BaseFileStore):

    def __init__(self, directory):
        super(DirectoryFileStore, self).__init__()
        self.directory = directory

    def save_file(self, file_obj, path):
        full_path = self.uri(path)
        directory = os.path.split(full_path)[0]
        if not os.path.exists(directory):
            os.makedirs(directory)
        outfile = open(full_path, 'wb')
        for chunk in file_obj:
            outfile.write(chunk)
        outfile.close()
        return path

    def open_file(self, path, mode='rb'):
        return open(self.uri(path), mode)

    def delete_file(self, path):
        os.unlink(self.uri(path))

    def file_exists(self, path):
        return os.path.exists(self.uri(path))

    def uri(self, path):
        path = secure_filename(path)
        return os.path.join(self.directory, path)
