# -*- coding: utf-8 -*-
import os

from werkzeug.utils import secure_filename

from .core import BaseFileStore


class DirectoryFileStore(BaseFileStore):

    def __init__(self, directory):
        super(DirectoryFileStore, self).__init__()
        self.directory = directory

    def save_file(self, file_obj, path):
        path = secure_filename(path)
        full_path = os.path.join(self.directory, path)
        directory = os.path.split(full_path)[0]
        if not os.path.exists(directory):
            os.makedirs(directory)
        outfile = open(full_path, 'wb')
        for chunk in file_obj:
            outfile.write(chunk)
        outfile.close()
        return path

    def open_file(self, path, mode='rb'):
        path = secure_filename(path)
        full_path = os.path.join(self.directory, path)
        return open(full_path, mode)

    def delete_file(self, path):
        path = secure_filename(path)
        full_path = os.path.join(self.directory, path)
        os.unlink(full_path)

    def file_exists(self, path):
        path = secure_filename(path)
        full_path = os.path.join(self.directory, path)
        return os.path.exists(full_path)
