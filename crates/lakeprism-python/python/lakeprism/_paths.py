"""Canonical file URIs, including Windows verbatim filesystem paths."""

def file_uri(path):
    if path.drive.upper().startswith("\\\\?\\UNC\\"):
        path = type(path)("\\\\" + str(path)[8:])
    elif path.drive.startswith("\\\\?\\"):
        path = type(path)(str(path)[4:])
    return path.as_uri()
