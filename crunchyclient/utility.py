import json
import os
import re
import subprocess
import tempfile

from base64 import b64encode
from decimal import Decimal
from pathlib import Path, PurePath
from pprint import pprint

import magic

from .constants import MIME_TYPE_MAPPING
from .errors import MediaFileError

def safe_bytes(input_bytes):
    """Replace surrogates in UTF-8 bytes"""
    return input_bytes.decode('utf-8', 'replace').encode('utf-8')

def safe_string(input_string):
    """Replace surrogates in UTF-8 string"""
    return input_string.encode('utf-8', 'replace').decode('utf-8')


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

    def __init__(self, api, reference, without_statements=None):
        self.api = api
        self.reference = reference
        self.without_statements = without_statements
        self.results = None
        self.idx = 0

    def __iter__(self):
        return self

    def _load_next(self):
        if self.results is None:
            params = {'limit': self.preferred_limit}
            if self.without_statements:
                params['without_statements'] = 1
            response = self.api.get('volumes/{}/files'.format(self.reference),
                params=params)
        else:
            after = b64encode(os.fsencode(
                self.results[self.limit-1]['path'])).decode()
            params = {'after': after, 'limit': self.preferred_limit}
            if self.without_statements:
                params['without_statements'] = 1
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


class FileAnalyzer:

    def __init__(self, bindings):
        self.bindings = bindings

    def _get_mime_type(self, path):
        mime = magic.Magic(mime=True)
        file_type = mime.from_file(str(path))
        return file_type.split('/')[0:2]

    def _get_compressed_mime_type(self, path):
        mime = magic.Magic(mime=True, uncompress=True)
        file_type = mime.from_file(str(path))
        return file_type.split('/')[0:2]

    def _call_json_process(self, command):
        p = subprocess.Popen(command, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, stdin=subprocess.PIPE)
        out, err = p.communicate()
        if p.returncode == 0:
            info = json.loads(safe_bytes(out))
        else:
            info = {}
            #raise MediaFileError("Command returned error status, stderr output: {}".format(err))
        return info

    def analyze_image(self, path, info):
        b = self.bindings
        im_info = self._call_json_process(['convert', path, 'json:-'])
        image_info = im_info[0]['image']

        if 'scenes' in image_info and image_info['scenes'] > 1:
            # animated "picture" (GIF etc.), treat it as a video
            return self.analyze_video(path, info)

        info[b.type] = [
            b.ComputerFile,
            b.ImageFile,
        ]
        info[b.widthInPixels] = image_info['geometry']['width']
        info[b.heightInPixels] = image_info['geometry']['height']

        return info

    def analyze_video(self, path, info):
        ff_info = self._call_json_process(['ffprobe', '-print_format',
            'json', '-show_error', '-show_format', '-show_programs',
            '-show_streams', '-show_chapters',
            #'-count_frames', '-count_packets',
            path])

        video_streams = []
        audio_streams = []
        subtitle_streams = []

        for stream in ff_info['streams']:
            if stream['codec_type'] == 'video':
                video_streams.append(stream)
            elif stream['codec_type'] == 'audio':
                audio_streams.append(stream)
            elif stream['codec_type'] == 'subtitle':
                subtitle_streams.append(stream)
            else:
                raise MediaFileError("UNKNOWN STREAM CODEC TYPE {}".format(stream))
        if len(video_streams) != 1:
            raise MediaFileError("UNEXPECTED NUMBER OF VIDEO STREAMS")

        b = self.bindings
        info[b.type] = [
            b.ComputerFile,
            b.VideoFile,
        ]
        video_info = video_streams[0]
        info[b.widthInPixels] = video_info['width']
        info[b.heightInPixels] = video_info['height']
        if 'duration' in ff_info['format']:
            info[b.durationInSeconds] = Decimal(ff_info['format']['duration'])

        return info

    def analyze(self, path):
        b = self.bindings
        info = {}
        info[b.fileSize] = path.stat().st_size
        info[b.type] = [b.ComputerFile]
        info[b.label] = safe_string(path.name)

        main, sub = self._get_mime_type(path)

        mimemap = MIME_TYPE_MAPPING
        if (main, sub) in mimemap:
            filetype = mimemap[(main, sub)]
        elif (main,) in mimemap:
            filetype = mimemap[(main,)]
        else:
            raise MediaFileError("Unknown mime type: {}/{}".format(main, sub))

        if filetype == 'compressed':
            cmain, csub = self._get_compressed_mime_type(path)
            if (cmain, csub) in mimemap:
                filetype = mimemap[(cmain, csub)]
            elif (cmain,) in mimemap:
                filetype = mimemap[(cmain,)]
            else:
                raise MediaFileError("Unknown compressed mime type: {}/{}".format(cmain, csub))
            info[b.type].append(b.CompressedFile)

        if filetype == 'image' or filetype == 'imageorvideo':
            info = self.analyze_image(path, info)
        elif filetype == 'video':
            info = self.analyze_video(path, info)
        elif filetype == 'audio':
            info = self.analyze_audio(path, info)
        elif filetype == 'document':
            info[b.type].append(b.DocumentFile)
        elif filetype == 'archive':
            info[b.type].append(b.ArchiveFile)
        elif filetype == 'ignore':
            pass
        else:
            raise MediaFileError("Unknown file type: {}".format(filetype))

        return info
