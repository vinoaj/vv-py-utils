import base64
import io
from pathlib import Path
from typing import Union

from .config.logger import logger
from .files import get_file_type


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


def get_image_base64_encoded_url(
    image_source: Union[Path, io.BytesIO], mime_type: str = None
) -> str:
    """
    Generates a data URL for the given image file or image buffer.

    Args:
        image_source (Union[Path, io.BytesIO]): The path to the image file or an in-memory image buffer.
        mime_type (str, optional): The MIME type of the image. If None, it will be inferred.

    Returns:
        str: The data URL containing the base64-encoded image and its MIME type.
    """
    if isinstance(image_source, Path):
        encoded_image = encode_image_base64(image_source)
        mime_type = mime_type or get_file_type(image_source)
    elif isinstance(image_source, io.BytesIO):
        encoded_image = base64.b64encode(image_source.getvalue()).decode("utf-8")
        mime_type = mime_type or "image/png"  # Defaulting to PNG if not provided

    data_url = f"data:{mime_type};base64,{encoded_image}"
    logger.info(f"Generated data URL for {image_source}: {data_url[:100]}...")
    return data_url
