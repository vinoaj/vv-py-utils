import base64
import io
from pathlib import Path
from typing import List, Union

import pdf2image

from .config.logger import logger
from .images import get_image_base64_encoded_url


def base64_encode_pdf(pdf_path: Path) -> str:
    """
    Encodes a PDF file to a base64 string.

    Args:
        pdf_path (Path): The path to the PDF file to be encoded.

    Returns:
        str: The base64 encoded string of the PDF file.
    """
    logger.info(f"Encoding PDF: {pdf_path}")
    with open(pdf_path, "rb") as pdf_file:
        return base64.b64encode(pdf_file.read()).decode("utf-8")


def get_pdf_base64_encoded_url(pdf_path: str) -> str:
    """
    Converts a PDF file to a base64 encoded data URL.

    Args:
        pdf_path (str): The file path to the PDF document.

    Returns:
        str: A base64 encoded data URL representing the PDF document.
    """
    encoded_pdf = base64_encode_pdf(pdf_path)
    return f"data:application/pdf;base64,{encoded_pdf}"


def pdf_pages_to_images(
    pdf_file: Union[Path, bytes],
    output_path: Path = None,
    return_as_data_url: bool = False,
) -> Union[List[Path], List[str]]:
    """
    Converts a PDF file (path or in-memory bytes) to a series of images and optionally returns them as data URLs.

    Args:
        pdf_file (Union[Path, bytes]): The path to the PDF file to be converted or the bytes content of a PDF.
        output_path (Path, optional): The directory where the resulting images will be saved if not returning as data URLs.
        return_as_data_url (bool): If True, return the images as base64-encoded data URLs.

    Returns:
        Union[List[Path], List[str]]: A list of paths to the saved image files or a list of data URLs.

    Raises:
        ValueError: If output_path is not specified when return_as_data_url is False.
    """
    IMAGE_FORMAT = "PNG"
    MIME_TYPE = "image/png"

    # Determine the type of input and use the appropriate pdf2image function
    if isinstance(pdf_file, Path):
        images = pdf2image.convert_from_path(pdf_file)
    elif isinstance(pdf_file, bytes):
        images = pdf2image.convert_from_bytes(pdf_file)
    else:
        raise TypeError("pdf_file must be either a Path or bytes")

    if return_as_data_url:
        data_urls = []
        for image in images:
            buffered = io.BytesIO()
            image.save(buffered, format=IMAGE_FORMAT)
            data_url = get_image_base64_encoded_url(buffered, mime_type=MIME_TYPE)
            data_urls.append(data_url)
        return data_urls

    if output_path is None:
        raise ValueError("output_path must be specified if not returning as data URLs")

    file_base = Path(pdf_file).stem if isinstance(pdf_file, Path) else "pdf_image"
    image_paths = []
    for i, image in enumerate(images):
        image_path = output_path / f"{file_base}-{i}.{IMAGE_FORMAT.lower()}"
        image.save(image_path, IMAGE_FORMAT)
        image_paths.append(image_path)

    return image_paths
