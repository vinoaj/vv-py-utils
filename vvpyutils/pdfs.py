import base64
import io
from pathlib import Path
from typing import List, Optional, Union

import pdf2image
from pypdf import PdfReader, PdfWriter

from .config.logger import logger
from .images import get_image_base64_encoded_url


def base64_encode_pdf(
    pdf_file: Path | bytes,
    return_as_data_url: bool = False,
    pages: Optional[list[int]] = None,
) -> str:
    """
    Encodes specified pages of a PDF file (path or in-memory bytes) to a base64 string or data URL.

    Args:
        pdf_file (Union[Path, bytes]): The path to the PDF file or the bytes content of a PDF.
        return_as_data_url (bool, optional): If True, returns the base64 encoded PDF as a data URL.
        pages (List[int], optional): A list of page numbers (0-indexed) to include in the encoding.
                                     If empty, all pages are encoded.

    Returns:
        str: The base64 encoded string or data URL of the selected PDF pages.

    Raises:
        TypeError: If pdf_file is not a Path or bytes.
    """
    logger.info(
        f"base64 encoding PDF: {type(pdf_file)} | {return_as_data_url=} | {pages=}"
    )

    # Load the PDF file as bytes
    if isinstance(pdf_file, Path):
        pdf_data = pdf_file.read_bytes()
    elif isinstance(pdf_file, bytes):
        pdf_data = pdf_file
    else:
        raise TypeError("pdf_file must be either a Path or bytes")

    # If specific pages are specified, create a new PDF with only those pages
    if pages:
        pdf_reader = PdfReader(io.BytesIO(pdf_data))
        pdf_writer = PdfWriter()

        for page_number in pages:
            if 0 <= page_number < len(pdf_reader.pages):
                pdf_writer.add_page(pdf_reader.pages[page_number])

        # Write the selected pages to a bytes buffer
        buffer = io.BytesIO()
        pdf_writer.write(buffer)
        buffer.seek(0)
        pdf_data = buffer.read()  # Update pdf_data to only include selected pages

    # Encode the PDF data to base64
    encoded_pdf = base64.b64encode(pdf_data).decode("utf-8")

    # Return as data URL if specified
    if return_as_data_url:
        return f"data:application/pdf;base64,{encoded_pdf}"

    return encoded_pdf


def get_pdf_base64_encoded_url(pdf_path: str) -> str:
    """
    Converts a PDF file to a base64 encoded data URL.
    Retaining this function for backward compatibility purposes.

    Args:
        pdf_path (str): The file path to the PDF document.

    Returns:
        str: A base64 encoded data URL representing the PDF document.
    """
    return base64_encode_pdf(Path(pdf_path), return_as_data_url=True)


def pdf_pages_to_images(
    pdf_file: Union[Path, bytes],
    output_path: Path = None,
    return_as_data_url: bool = False,
    size: int | tuple[int, int] = None,
    use_cairo: bool = True,
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

    if isinstance(pdf_file, Path):
        images = pdf2image.convert_from_path(
            pdf_file, size=size, use_pdftocairo=use_cairo
        )
    elif isinstance(pdf_file, bytes):
        images = pdf2image.convert_from_bytes(
            pdf_file, size=size, use_pdftocairo=use_cairo
        )
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
