import base64
import io
import subprocess
from pathlib import Path
from typing import List, Optional, Union

import pdf2image
import pytesseract
from PIL import Image
from pydantic import BaseModel
from pypdf import PdfReader, PdfWriter

from .config.logger import logger
from .images import get_image_base64_encoded_url


def get_page_texts(
    pdf_file: Path | bytes,
    pages: Optional[list[int]] = None,
    extraction_mode: str = "layout",  # "layout" or "plain"
) -> list[str]:
    """
    Extracts text content from specified pages of a PDF file (path or in-memory bytes).

    Args:
        pdf_file (Union[Path, bytes]): The path to the PDF file or the bytes content of a PDF.
        pages (List[int], optional): A list of page numbers (0-indexed) to extract text from.
                                     If empty, text from all pages is extracted.

    Returns:
        List[str]: A list of text content from the selected PDF pages.

    Raises:
        TypeError: If pdf_file is not a Path or bytes.
    """
    logger.info(f"extracting text from PDF: {type(pdf_file)} | {pages=}")

    # Load the PDF file as bytes
    if isinstance(pdf_file, Path):
        pdf_data = pdf_file.read_bytes()
    elif isinstance(pdf_file, bytes):
        pdf_data = pdf_file
    else:
        raise TypeError("pdf_file must be either a Path or bytes")

    # If specific pages are specified, create a new PDF with only those pages
    if pages and len(pages) > 0:
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

    # Extract text content from the PDF data
    pdf_reader = PdfReader(io.BytesIO(pdf_data))
    return [
        page.extract_text(extraction_mode=extraction_mode) for page in pdf_reader.pages
    ]


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
    if pages and len(pages) > 0:
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
    return_as_pil_images: bool = False,
    size: int | tuple[int, int] = None,
    use_cairo: bool = True,
) -> Union[List[Path], List[str], List[Image.Image]]:
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

    if return_as_pil_images:
        return images

    if output_path is None:
        raise ValueError(
            "output_path must be specified if not returning as data URLs or bytes"
        )

    file_base = Path(pdf_file).stem if isinstance(pdf_file, Path) else "pdf_image"
    image_paths = []
    for i, image in enumerate(images):
        image_path = output_path / f"{file_base}-{i}.{IMAGE_FORMAT.lower()}"
        image.save(image_path, IMAGE_FORMAT)
        image_paths.append(image_path)

    return image_paths


def combine_pdfs(pdf_files: List[Path], output_file: Path) -> Path:
    """Merge PDFs in order. First file will be front of file."""

    merged_pdf = PdfWriter()
    for pdf in pdf_files:
        merged_pdf.append(pdf)

    merged_pdf.write(output_file)
    return output_file


def convert_pdf_to_pdfa(input_path: Path, output_path: Path) -> Path:
    """Converts non-compliant PDF files to PDF/A-1b compliant files using
    Ghostscript."""
    input_path, output_path = str(input_path), str(output_path)

    try:
        # Ghostscript command for PDF/A-1b
        command = [
            "gs",
            "-dPDFA=1",  # PDF/A level
            "-dBATCH",
            "-dNOPAUSE",
            "-sDEVICE=pdfwrite",
            "-sOutputFile=" + output_path,
            "-dPDFACompatibilityPolicy=1",
            input_path,
        ]

        subprocess.run(command, check=True)
        print(f"PDF successfully converted to PDF/A: {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error during PDF/A conversion: {e}")
    except FileNotFoundError:
        print("Ghostscript is not installed or not found in PATH.")

    return output_path


class OCRResult(BaseModel):
    """Result of OCR processing for a page"""

    page_num: int
    text: str
    confidence: float


class PDFOCRProcessor(BaseModel):
    pdf_path: Path
    # output_dir: Optional[Path] = None
    language: str = "eng"
    dpi: int = 300

    _ocr_results: List[OCRResult] = []

    def _process_page(self, image, page_num: int) -> OCRResult:
        ocr_data = pytesseract.image_to_data(
            image, lang=self.language, output_type=pytesseract.Output.DICT
        )

        text_parts = []
        confidences = []

        for i, text in enumerate(ocr_data["text"]):
            if text and not text.isspace():
                conf = float(ocr_data["conf"][i])
                if conf > 0:  # Filter out negative confidence values
                    text_parts.append(text)
                    confidences.append(conf)

        # Join text parts
        full_text = " ".join(text_parts)

        # Calculate average confidence
        avg_confidence = sum(confidences) / len(confidences) if confidences else 0

        return OCRResult(page_num=page_num, text=full_text, confidence=avg_confidence)

    def process_pdf(self) -> List[OCRResult]:
        logger.info(f"Performing OCR on PDF: {self.pdf_path}")

        images = pdf_pages_to_images(pdf_file=self.pdf_path, return_as_pil_images=True)

        logger.info(f"Found {len(images)} pages")
        results = []

        for page_num, image in enumerate(images):
            page_result = self._process_page(image, page_num)
            results.append(page_result)

        self._ocr_results = results
        return self._ocr_results

    def ocr_results_to_str(self) -> str:
        """Convert OCR results to a single string."""
        return "\n".join([result.text for result in self._ocr_results])
