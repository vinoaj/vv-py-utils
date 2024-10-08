from urllib.parse import urlparse

from vvpyutils.config.settings import VALID_IMAGE_EXTENSIONS


def is_image_url(url: str) -> bool:
    """
    Checks if a given URL is both valid and points to an image file.

    Args:
        url (str): The URL to be checked.

    Returns:
        bool: True if the URL is valid and points to an image file, False otherwise.
    """
    if not url or not isinstance(url, str):
        return False

    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            return False

        if not parsed.netloc:
            return False

        return any(parsed.path.lower().endswith(ext) for ext in VALID_IMAGE_EXTENSIONS)

    except Exception:
        return False
