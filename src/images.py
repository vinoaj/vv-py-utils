import base64
from pathlib import Path

from .files import get_file_type
from .config.logger import logger


def encode_image_base64(image_path: Path) -> str:
    """
    Encodes an image file to a base64 string.

    Args:
        image_path (Path): The path to the image file to be encoded.

    Returns:
        str: The base64 encoded string of the image content.
    """
    logger.info(f"Encoding image: {image_path}")
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")


def get_image_base64_encoded_url(image_path: Path) -> str:
    """
    Generates a data URL for the given image file.

    Args:
        image_path (Path): The path to the image file.

    Returns:
        str: The data URL containing the base64-encoded image and its MIME type.

    Logs:
        Logs the generated data URL for the given image path.
    """
    encoded_image = encode_image_base64(image_path)
    mime_type = get_file_type(image_path)
    data_url = f"data:{mime_type};base64,{encoded_image}"
    logger.info(f"Generated data URL for {image_path}: {data_url}")
    return data_url
