import base64
import pdf2image
from pathlib import Path
from typing import List
from .config.logger import logger


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


def pdf_to_image(pdf_file: Path, output_path: Path) -> List[Path]:
    """
    Converts a PDF file to a series of images and saves them to the specified output path.

    Args:
        pdf_file (Path): The path to the PDF file to be converted.
        output_path (Path): The directory where the resulting images will be saved.

    Returns:
        List[Path]: A list of paths to the saved image files.
    """
    file_base = pdf_file.stem
    images = pdf2image.convert_from_path(pdf_file)

    image_paths = []
    for i, image in enumerate(images):
        image_path = output_path / f"{file_base}-{i}.png"
        image.save(image_path, "PNG")
        image_paths.append(image_path)

    return image_paths
