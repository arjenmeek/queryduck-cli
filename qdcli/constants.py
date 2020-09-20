MIME_TYPE_MAPPING = {
    ("image", "gif"): "imageorvideo",
    ("image", "x-tga"): "ignore",
    ("image",): "image",

    ("audio",): "audio",

    ("video",): "video",

    ("application", "gzip"): "compressed",
    ("application", "x-bzip2"): "compressed",
    ("application", "x-xz"): "compressed",

    ("application", "x-rar"): "archive",
    ("application", "x-tar"): "archive",
    ("application", "zip"): "archive",

    ("text",): "document",
    ("application", "pdf"): "document",
    ("application", "epub+zip"): "document",
    ("application", "vnd.oasis.opendocument.text"): "document",
    ("application", "msword"): "document",
    ("application", "vnd.openxmlformats-officedocument.wordprocessingml.document"): "document",

    ("application", "x-dosexec"): "program",
    ("application", "x-shockwave-flash"): "program",

    ("application", "octet-stream"): "more",
    ("application", "vnd.debian.binary-package"): "ignore",
    ("application", "vnd.ms-excel"): "ignore",
    ("application", "x-bittorrent"): "metadata",
    ("application", "x-gnucash"): "ignore",
    ("application", "CDFV2"): "ignore",
}

MORE_MAPPING = {
    "^AportisDoc/PalmDOC E-book$": "document",
    "^AppleDouble encoded Macintosh file$": "metadata",
    "^Mobipocket E-book .*$": "document",
    "^PARity archive data .*$": "metadata",
    "^Parity Archive Volume Set": "metadata",
}
