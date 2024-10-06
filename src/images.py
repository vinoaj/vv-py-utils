import base64
from pathlib import Path

from .files import get_file_type
from .config.logger import logger


def encode_image(image_path: Path) -> str:
    logger.info(f"Encoding image: {image_path}")
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")


def get_image_encoded_url(image_path: Path) -> str:
    encoded_image = encode_image(image_path)
    mime_type = get_file_type(image_path)
    data_url = f"data:{mime_type};base64,{encoded_image}"
    logger.info(f"Generated data URL for {image_path}: {data_url}")
    return data_url
