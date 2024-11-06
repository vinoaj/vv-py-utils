import base64
import io
import string
from pathlib import Path
from typing import Union

from PIL import Image
from PIL.Image import Image as ImageType

from .config.logger import logger
from .files import get_file_type


def open_image(image_source: Union[Path, bytes, io.BytesIO]) -> Image.Image:
    """
    Opens an image from various input types (Path, bytes, io.BytesIO).

    Args:
        image_source (Union[Path, bytes, io.BytesIO]): The image source as a file path, bytes, or io.BytesIO.

    Returns:
        Image.Image: An opened PIL image.
    """
    if isinstance(image_source, Path):
        image_source = io.BytesIO(image_source.read_bytes())
    elif isinstance(image_source, bytes):
        image_source = io.BytesIO(image_source)

    return Image.open(image_source)


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
    elif isinstance(image_source, bytes):
        encoded_image = base64.b64encode(image_source).decode("utf-8")
        mime_type = mime_type or "image/png"
    else:
        raise TypeError("Invalid image source type")

    data_url = f"data:{mime_type};base64,{encoded_image}"
    logger.info(f"Generated data URL for {image_source}: {data_url[:100]}...")
    return data_url


def decode_image_data_url(data_url: string) -> Image:
    # Strip off the metadata prefix and decode the base64 data
    _, encoded = data_url.split(",", 1)
    image_data = base64.b64decode(encoded)
    return Image.open(io.BytesIO(image_data))


def combine_images_vertically(
    images: list[Path | bytes | str], return_data_url: bool = False
) -> ImageType | str:
    pil_images = []
    for image in images:
        if isinstance(image, bytes):
            pil_images.append(Image.open(io.BytesIO(image)))
        elif isinstance(image, str):
            pil_images.append(decode_image_data_url(image))
        elif isinstance(image, Path):
            pil_images.append(Image.open(image))
        else:
            raise TypeError("Invalid image type")

    widths, heights = zip(*(i.size for i in pil_images))

    total_width = max(widths)
    total_height = sum(heights)

    new_image = Image.new("RGB", (total_width, total_height))

    y_offset = 0
    for image in pil_images:
        new_image.paste(image, (0, y_offset))
        y_offset += image.size[1]

    if return_data_url:
        buffer = io.BytesIO()
        new_image.save(buffer, format="PNG")
        return get_image_base64_encoded_url(buffer, "image/png")
    else:
        return new_image


def resize_image(
    image_source: Union[Path, bytes, io.BytesIO],
    max_width: int = 512,
    max_height: int = 512,
    image_format: str = "webp",
) -> bytes:
    """
    Resizes the image to fit within max_width and max_height, preserving aspect ratio.

    Args:
        image_source (Union[Path, bytes, io.BytesIO]): The path to the image file, bytes, or an in-memory image buffer.
        max_width (int): Maximum width of the resized image.
        max_height (int): Maximum height of the resized image.

    Returns:
        bytes: Resized image in bytes.
    """

    with open_image(image_source) as img:
        img = open_image(image_source)
        img.thumbnail((max_width, max_height))

        buffer = io.BytesIO()
        img.save(buffer, format=image_format)
        return buffer.getvalue()


def convert_to_webp(
    image_source: Union[Path, bytes, io.BytesIO], quality: int = 80
) -> bytes:
    """
    Converts an image to WebP format and returns it as bytes.

    Args:
        image_source (Union[Path, bytes, io.BytesIO]): The image source as a file path, bytes, or io.BytesIO.
        quality (int): The quality of the WebP image (0-100).

    Returns:
        bytes: The image in WebP format as bytes.
    """

    with open_image(image_source) as img:
        img = open_image(image_source)

        # Convert the image to WebP format
        buffer = io.BytesIO()
        img.save(buffer, format="WEBP", quality=quality)  # Adjust quality as needed
        return buffer.getvalue()


def convert_to_grayscale(
    image_source: Union[Path, bytes, io.BytesIO], image_format: str = "webp"
) -> bytes:
    """
    Converts the image to grayscale and resizes it to fit within max_width and max_height, preserving aspect ratio.

    Args:
        image_source (Union[Path, bytes, io.BytesIO]): The path to the image file, bytes, or an in-memory image buffer.

    Returns:
        bytes: Grayscale image in bytes.
    """

    with open_image(image_source) as img:
        img = open_image(image_source)
        img = img.convert("L")  # Convert the image to grayscale

        buffer = io.BytesIO()
        img.save(buffer, format=image_format)
        return buffer.getvalue()
