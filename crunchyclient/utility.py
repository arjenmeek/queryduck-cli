import os
import re
import subprocess
import tempfile

from base64 import b64encode
from pathlib import Path, PurePath


def call_text_editor(text):
    editor = os.environ.get('EDITOR','vim')
    fd, fname = tempfile.mkstemp(suffix=".tmp")
    with os.fdopen(fd, 'w') as f:
        f.write(text)
        f.close()

    before = os.path.getmtime(fname)
    first = True
    while first or (os.path.getmtime(fname) == before
            and input("File unchanged, [r]eopen or [c]ontinue? ") != 'c'):
        subprocess.call([editor, fname])
        first = False
    with open(fname, 'r') as f:
        result = f.read()
    os.unlink(fname)
    return result


class TreeFileIterator(object):

    def __init__(self, root, exclude=None):
        self.root = Path(root)
        self.stack = [self.root]
        self.exclude = exclude

    def __iter__(self):
        return self

    @staticmethod
    def sortkey(entry):
        if entry.is_dir():
            return bytes(entry) + b'/'
        else:
            return bytes(entry)

    def _is_excluded(self, path):
        if self.exclude is not None:
            for e in self.exclude:
                if path.match(e):
                    return True
        return False

    def __next__(self):
        try:
            p = self.stack.pop()
            while True:
                if p.is_symlink() or self._is_excluded(p):
                    p = self.stack.pop()
                elif p.is_dir():
                    self.stack += sorted(p.iterdir(),
                        key=self.sortkey, reverse=True)
                    p = self.stack.pop()
                else:
                    break
        except IndexError:
            raise StopIteration

        return p


class ApiFileIterator(object):

    preferred_limit = 10000

    def __init__(self, api, reference, nostatement=None):
        self.api = api
        self.reference = reference
        self.nostatement = nostatement
        self.results = None
        self.idx = 0

    def __iter__(self):
        return self

    def _load_next(self):
        if self.results is None:
            params = {'limit': self.preferred_limit}
            if self.nostatement:
                params['nostatement'] = 1
            response = self.api.get('volumes/{}/files'.format(self.reference),
                params=params)
        else:
            after = b64encode(os.fsencode(
                self.results[self.limit-1]['path'])).decode()
            params = {'after': after, 'limit': self.preferred_limit}
            if self.nostatement:
                params['nostatement'] = 1
            response = self.api.get('volumes/{}/files'.format(self.reference),
                params=params)
        self.results = response['results']
        self.limit = response['limit']
        self.idx = 0

    def __next__(self):
        if self.results is None or self.idx >= self.limit:
            self._load_next()
        try:
            api_file = self.results[self.idx]
            self.idx += 1
        except IndexError:
            raise StopIteration
        return api_file


class CombinedIterator(object):

    def __init__(self, left, right, left_key, right_key):
        self.left = left
        self.right = right
        self.left_key = left_key
        self.right_key = right_key
        self._advance_left()
        self._advance_right()

    def __iter__(self):
        return self

    def _advance_left(self):
        if self.left is not None:
            try:
                self.cur_left = next(self.left)
            except StopIteration:
                self.left = None
                self.cur_left = None

    def _advance_right(self):
        if self.right is not None:
            try:
                self.cur_right = next(self.right)
            except StopIteration:
                self.right = None
                self.cur_right = None

    def __next__(self):
        if self.left is None and self.right is None:
            raise StopIteration
        elif (self.right is None or
                self.left_key(self.cur_left) < self.right_key(self.cur_right)):
            retval = (self.cur_left, None)
            self._advance_left()
        elif (self.left is None or
                self.left_key(self.cur_left) > self.right_key(self.cur_right)):
            retval = (None, self.cur_right)
            self._advance_right()
        else:
            retval = (self.cur_left, self.cur_right)
            self._advance_left()
            self._advance_right()

        return retval
