# -*- coding: utf-8 -*-
import unittest
import tempfile
import io
import os
import shutil

from microcollections.collections import RawCollection
from microcollections.datastores.memory import MemoryDataStore
from microcollections.filestores import FileCollection
from microcollections.filestores.directory import DirectoryFileStore


class TestMemoryCollection(unittest.TestCase):
    def setUp(self):
        self.collection = RawCollection(MemoryDataStore())

    def test_creation(self):
        self.collection['obj1'] = {'foo': 'bar'}
        self.assertTrue('obj1' in self.collection)
        self.assertEqual(self.collection['obj1']['foo'], 'bar')


class TestFileDirectoryCollection(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp()
        self.collection = FileCollection(DirectoryFileStore(self.directory))

    def tearDown(self):
        shutil.rmtree(self.directory)

    def test_creation(self):
        mocked_file = io.BytesIO('my text file')
        self.collection['obj1'] = mocked_file
        self.assertTrue('obj1' in self.collection)
        self.assertEqual(self.collection['obj1'].read(), 'my text file')
