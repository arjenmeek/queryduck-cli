MIME_TYPE_MAPPING = {
    ('image', 'gif'): 'imageorvideo',
    ('image',): 'image',

    ('audio',): 'audio',

    ('video',): 'video',

    ('application', 'gzip'): 'compressed',
    ('application', 'x-bzip2'): 'compressed',
    ('application', 'x-xz'): 'compressed',

    ('application', 'x-tar'): 'archive',
    ('application', 'zip'): 'archive',

    ('text',): 'document',
    ('application', 'pdf'): 'document',
    ('application', 'epub+zip'): 'document',
    ('application', 'vnd.oasis.opendocument.text'): 'document',
    ('application', 'msword'): 'document',
    ('application', 'vnd.openxmlformats-officedocument.wordprocessingml.document'): 'document',

    ('application', 'octet-stream'): 'ignore',
    ('application', 'vnd.debian.binary-package'): 'ignore',
    ('application', 'vnd.ms-excel'): 'ignore',
    ('application', 'x-bittorrent'): 'ignore',
    ('application', 'x-gnucash'): 'ignore',
}
