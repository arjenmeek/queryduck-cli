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

    @staticmethod
    def check_requirements():
        ffprobe_available = os.path.exists("/usr/bin/ffprobe")
        convert_available = os.path.exists("/usr/bin/convert")
        rsvg_available = os.path.exists("/usr/bin/rsvg-convert")
        return ffprobe_available and convert_available and rsvg_available

    def _get_mime_type(self, path, uncompress=False):
        mime = magic.Magic(mime=True, uncompress=uncompress)
        file_type = mime.from_file(str(path))
        return file_type.split("/")[0:2]

    def _get_compressed_mime_type(self, path):
        mime = magic.Magic(mime=True, uncompress=True)
        file_type = mime.from_file(str(path))
        return file_type.split("/")[0:2]

    def _get_more_type(self, path, uncompress=False):
        m = magic.Magic(mime=False, uncompress=uncompress)
        file_type = m.from_file(str(path))
        return file_type

    def _get_compressed_more_type(self, path):
        m = magic.Magic(mime=False, uncompress=True)
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
            try:
                info = json.loads(safe_bytes(out))
            except json.decoder.JSONDecodeError:
                raise MediaFileError("Command returned invalid json")
        else:
            # info = {}
            raise MediaFileError(
                "Command returned error status, stderr output: {}".format(err)
            )
        return info

    def process_blob(self, blob, path, context):
        b, c = context.get_bc()
        need_analysis = False
        #print(blob)
        resources = c.subjects_for(blob, b.fileContent)
        if len(resources) > 1:
            print(f"Multiple resources for single Blob! ({len(resources)})")
            print(resources)
            return
        elif len(resources) == 1:
            r = resources[0]
        else:
            need_analysis = True
            r = context.transaction.add(None, b.type, b.Resource)

        context.ensure(r, b.type, b.ComputerFile)
        context.ensure(r, b.fileContent, blob)
        context.ensure(r, b.fileSize, path.stat().st_size)
        context.ensure(r, b.label, safe_string(path.name))

        filetypes = c.objects_for(r, b.fileType)
        if len(filetypes) == 0:
#            print("No filetypes")
            need_analysis = True
        else:
            check_predicates = []
            if b.ImageFile in filetypes:
                check_predicates += [b.widthInPixels, b.heightInPixels]
            if b.VideoFile in filetypes:
                check_predicates += [
                    b.widthInPixels,
                    b.heightInPixels,
                    b.durationInSeconds,
#                        b.numberOfFrames,
                ]
            check_predicates = set(check_predicates)
            for pred in check_predicates:
                if not c.object_for(r, pred):
                    pass
                    need_analysis = True

        if need_analysis:
            print("Analyze", safe_string(str(path)))
            try:
                self.analyze(r, path, context)
            except MediaFileError:
                print("There was an error, ignoring file")
                return

    def analyze(self, r, path, context, preview_path=None):
        b = self.bindings
        filetype = self.determine_filetype(path, context)

        if filetype == "image" or filetype == "imageorvideo":
            #types.append(b.ImageFile)
            self.analyze_image(r, path, context, preview_path)
        elif filetype == "video":
            #types.append(b.VideoFile)
            self.analyze_video(r, path, context)
        elif filetype == "audio":
            context.ensure(r, b.fileType, b.AudioFile)
            #info = self.analyze_audio(path, info)
        elif filetype == "document":
            context.ensure(r, b.fileType, b.DocumentFile)
        elif filetype == "archive":
            context.ensure(r, b.fileType, b.ArchiveFile)
        elif filetype == "program":
            context.ensure(r, b.fileType, b.ProgramFile)
        elif filetype == "metadata":
            context.ensure(r, b.fileType, b.MetadataFile)
        elif filetype == "ignore":
            pass
        else:
            raise MediaFileError("Unknown file type: {} for {}".format(filetype, path))

    def analyze_image(self, r, path, context, preview_path=None):
        b = self.bindings
        im_info = self._call_json_process(["convert", path, "json:-"])
        image_info = im_info[0]["image"]

        if "scenes" in image_info and image_info["scenes"] > 1:
            # animated "picture" (GIF etc.), treat it as a video
            return self.analyze_video(r, path, context)

        context.ensure(r, b.fileType, b.ImageFile)
        context.ensure(r, b.widthInPixels, image_info["geometry"]["width"])
        context.ensure(r, b.heightInPixels, image_info["geometry"]["height"])

        if preview_path:
            self.make_preview(path, preview_path)

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

    def analyze_video(self, r, path, context):
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
        context.ensure(r, b.fileType, b.VideoFile)
        video_info = video_streams[0]
        context.ensure(r, b.widthInPixels, video_info["width"])
        context.ensure(r, b.heightInPixels, video_info["height"])
        if "duration" in ff_info["format"]:
            context.ensure(r, b.durationInSeconds, Decimal(ff_info["format"]["duration"]))

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

    def determine_filetype(self, path, context):
        b = context.bindings
        main, sub = self._get_mime_type(path)
        types = []

        mimemap = MIME_TYPE_MAPPING
        if (main, sub) in mimemap:
            filetype = mimemap[(main, sub)]
        elif (main,) in mimemap:
            filetype = mimemap[(main,)]
        else:
            raise MediaFileError("Unknown mime type: {}/{} for {}".format(main, sub, path))

        uncompress = False
        if filetype == "compressed":
            uncompress = True
            cmain, csub = self._get_mime_type(path, uncompress=uncompress)
            if (cmain, csub) in mimemap:
                filetype = mimemap[(cmain, csub)]
            elif (cmain,) in mimemap:
                filetype = mimemap[(cmain,)]
            else:
                raise MediaFileError(
                    "Unknown compressed mime type: {}/{}".format(cmain, csub)
                )
            types.append(b.CompressedFile)

        more = False
        if filetype == "more":
            more = self._get_more_type(path, uncompress=True)
            for regex, filetype_candidate in MORE_MAPPING.items():
                if re.match(regex, more):
                    filetype = filetype_candidate
                    #print("MORETYPE", filetype)
                    break
#            else:
#                print("MORE", more)

#        print("    FILETYPE", filetype, main, sub, more)
        return filetype
