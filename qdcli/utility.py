import json
import os
import re
import subprocess
import tempfile

from decimal import Decimal

import magic

from queryduck.utility import safe_bytes, safe_string

from .constants import MIME_TYPE_MAPPING, MORE_MAPPING
from .errors import MediaFileError


def call_text_editor(text):
    editor = os.environ.get("EDITOR", "vim")
    fd, fname = tempfile.mkstemp(suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        f.write(text)
        f.close()

    before = os.path.getmtime(fname)
    first = True
    while first or (
        os.path.getmtime(fname) == before
        and not input("File unchanged, [r]eopen or [c]ontinue? [c] ") != "r"
    ):
        subprocess.call([editor, fname])
        first = False
    with open(fname, "r") as f:
        result = f.read()
    os.unlink(fname)
    return result


class FileAnalyzer:
    def __init__(self, bindings):
        self.bindings = bindings

    def _get_mime_type(self, path):
        mime = magic.Magic(mime=True)
        file_type = mime.from_file(str(path))
        return file_type.split("/")[0:2]

    def _get_compressed_mime_type(self, path):
        mime = magic.Magic(mime=True, uncompress=True)
        file_type = mime.from_file(str(path))
        return file_type.split("/")[0:2]

    def _get_more_type(self, path):
        m = magic.Magic(mime=False)
        file_type = m.from_file(str(path))
        return file_type

    def _call_json_process(self, command):
        p = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
        )
        out, err = p.communicate()
        if p.returncode == 0:
            info = json.loads(safe_bytes(out))
        else:
            # info = {}
            raise MediaFileError(
                "Command returned error status, stderr output: {}".format(err)
            )
        return info

    def analyze_image(self, path, info, preview_path=None):
        b = self.bindings
        im_info = self._call_json_process(["convert", path, "json:-"])
        image_info = im_info[0]["image"]

        if "scenes" in image_info and image_info["scenes"] > 1:
            # animated "picture" (GIF etc.), treat it as a video
            return self.analyze_video(path, info)

        info[b.fileType] = [
            b.ImageFile,
        ]
        info[b.widthInPixels] = image_info["geometry"]["width"]
        info[b.heightInPixels] = image_info["geometry"]["height"]

        if preview_path:
            self.make_preview(path, preview_path)

        return info

    def make_preview(self, image_path, preview_path):
        os.makedirs(os.path.dirname(preview_path), exist_ok=True)
        command = [
            "convert",
            image_path,
            "-resize",
            "300x300",
            "-quality",
            "80",
            preview_path,
        ]
        p = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
        )
        out, err = p.communicate()
        if p.returncode != 0:
            print("PREVIEW ERROR")
            print(out)
            print(err)

    def analyze_video(self, path, info):
        ff_info = self._call_json_process(
            [
                "ffprobe",
                "-print_format",
                "json",
                "-show_error",
                "-show_format",
                "-show_programs",
                "-show_streams",
                "-show_chapters",
                # "-count_frames", "-count_packets",
                path,
            ]
        )

        video_streams = []
        audio_streams = []
        subtitle_streams = []

        for stream in ff_info["streams"]:
            if stream["codec_type"] == "video":
                if (
                    "avg_frame_rate" in stream
                    and stream["avg_frame_rate"] in ("0/0")
                    or stream["codec_name"] == "mjpeg"
                ):
                    continue
                video_streams.append(stream)
            elif stream["codec_type"] == "audio":
                audio_streams.append(stream)
            elif stream["codec_type"] == "subtitle":
                subtitle_streams.append(stream)
            elif stream["codec_type"] == "data":
                pass
            else:
                raise MediaFileError(
                    "UNKNOWN STREAM CODEC TYPE {}".format(stream["codec_type"])
                )
        if len(video_streams) != 1:
            raise MediaFileError("UNEXPECTED NUMBER OF VIDEO STREAMS", video_streams)

        b = self.bindings
        info[b.fileType] = [
            b.VideoFile,
        ]
        video_info = video_streams[0]
        info[b.widthInPixels] = video_info["width"]
        info[b.heightInPixels] = video_info["height"]
        if "duration" in ff_info["format"]:
            info[b.durationInSeconds] = Decimal(ff_info["format"]["duration"])

        return info

    def analyze_audio(self, path, info):
        ff_info = self._call_json_process(
            [
                "ffprobe",
                "-print_format",
                "json",
                "-show_error",
                "-show_format",
                "-show_programs",
                "-show_streams",
                "-show_chapters",
                path,
            ]
        )

        video_streams = []
        audio_streams = []
        subtitle_streams = []

        for stream in ff_info["streams"]:
            if stream["codec_type"] != "audio":
                raise MediaFileError("UNEXPECTED NON-AUDIO STREAM")
            else:
                audio_streams.append(stream)
        if len(audio_streams) != 1:
            raise MediaFileError("UNEXPECTED NUMBER OF AUDIO STREAMS")

        b = self.bindings
        info[b.fileType] = [
            b.AudioFile,
        ]
        audio_info = audio_streams[0]
        if "duration" in ff_info["format"]:
            info[b.durationInSeconds] = Decimal(ff_info["format"]["duration"])

        return info

    def determine_filetype(self, path):
        main, sub = self._get_mime_type(path)

        mimemap = MIME_TYPE_MAPPING
        if (main, sub) in mimemap:
            filetype = mimemap[(main, sub)]
        elif (main,) in mimemap:
            filetype = mimemap[(main,)]
        else:
            raise MediaFileError("Unknown mime type: {}/{}".format(main, sub))

        more = None
        if filetype == "more":
            more = self._get_more_type(path)
            for regex, filetype_candidate in MORE_MAPPING.items():
                if re.match(regex, more):
                    filetype = filetype_candidate
                    break
            else:
                print("MORE", more)

        print("FILETYPE", filetype, main, sub, more)
        return filetype

    def analyze(self, path, preview_path=None):
        b = self.bindings
        info = {}
        info[b.fileSize] = path.stat().st_size
        info[b.type] = [b.ComputerFile]
        info[b.label] = safe_string(path.name)

        filetype = self.determine_filetype(path)

        if filetype == "compressed":
            cmain, csub = self._get_compressed_mime_type(path)
            if (cmain, csub) in mimemap:
                filetype = mimemap[(cmain, csub)]
            elif (cmain,) in mimemap:
                filetype = mimemap[(cmain,)]
            else:
                raise MediaFileError(
                    "Unknown compressed mime type: {}/{}".format(cmain, csub)
                )
            info[b.fileType].append(b.CompressedFile)

        if filetype == "image" or filetype == "imageorvideo":
            info = self.analyze_image(path, info, preview_path)
        elif filetype == "video":
            info = self.analyze_video(path, info)
        elif filetype == "audio":
            info = self.analyze_audio(path, info)
        elif filetype == "document":
            info[b.fileType].append(b.DocumentFile)
        elif filetype == "archive":
            info[b.fileType].append(b.ArchiveFile)
        elif filetype == "program":
            info[b.fileType].append(b.ProgramFile)
        elif filetype == "metadata":
            info[b.fileType].append(b.MetadataFile)
        elif filetype == "ignore":
            pass
        else:
            raise MediaFileError("Unknown file type: {}".format(filetype))

        return info
