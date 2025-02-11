import mimetypes
from pathlib import Path


def get_file_type(file_path: Path) -> str:
    """Determines the MIME type of a given file.

    Args:
        file_path (Path): The path to the file whose type is to be determined.

    Returns:
        str: The MIME type of the file.

    Raises:
        ValueError: If the MIME type cannot be determined.
    """
    mime_type, _ = mimetypes.guess_type(file_path)
    if mime_type is None:
        raise ValueError("Cannot determine the file type")

    return mime_type
