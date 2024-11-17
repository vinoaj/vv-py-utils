from pathlib import Path
import base64
import io
import pytest
from pypdf import PdfReader
from vvpyutils.pdfs import base64_encode_pdf, get_page_texts

SAMPLE_PDFS_PATH = Path(__file__).parent / "pdf_samples"
SAMPLE_PDFS = ["sample.pdf"]


@pytest.fixture
def sample_pdf_path(tmp_path):
    pdf_path = tmp_path / SAMPLE_PDFS[0]
    pdf_data = (SAMPLE_PDFS_PATH / SAMPLE_PDFS[0]).read_bytes()
    pdf_path.write_bytes(pdf_data)
    return pdf_path


@pytest.fixture
def sample_pdf_bytes():
    return (SAMPLE_PDFS_PATH / SAMPLE_PDFS[0]).read_bytes()


def test_base64_encode_pdf_from_path(sample_pdf_path):
    encoded_pdf = base64_encode_pdf(sample_pdf_path)
    assert isinstance(encoded_pdf, str)
    assert encoded_pdf.startswith(
        base64.b64encode(sample_pdf_path.read_bytes()).decode("utf-8")[:10]
    )


def test_base64_encode_pdf_from_bytes(sample_pdf_bytes):
    encoded_pdf = base64_encode_pdf(sample_pdf_bytes)
    assert isinstance(encoded_pdf, str)
    assert encoded_pdf.startswith(
        base64.b64encode(sample_pdf_bytes).decode("utf-8")[:10]
    )


def test_base64_encode_pdf_with_pages(sample_pdf_bytes):
    pdf_reader = PdfReader(io.BytesIO(sample_pdf_bytes))
    total_pages = len(pdf_reader.pages)
    pages_to_encode = [0, total_pages - 1] if total_pages > 1 else [0]

    encoded_pdf = base64_encode_pdf(sample_pdf_bytes, pages=pages_to_encode)
    assert isinstance(encoded_pdf, str)
    # Decode the base64 string and check the number of pages
    decoded_pdf = base64.b64decode(encoded_pdf)
    pdf_reader = PdfReader(io.BytesIO(decoded_pdf))
    assert len(pdf_reader.pages) == len(pages_to_encode)


def test_base64_encode_pdf_as_data_url(sample_pdf_bytes):
    encoded_pdf = base64_encode_pdf(sample_pdf_bytes, return_as_data_url=True)
    assert isinstance(encoded_pdf, str)
    assert encoded_pdf.startswith("data:application/pdf;base64,")


def test_base64_encode_pdf_invalid_type():
    with pytest.raises(TypeError):
        base64_encode_pdf(12345)


def test_base64_encode_pdf_all_pages(sample_pdf_bytes):
    encoded_pdf = base64_encode_pdf(sample_pdf_bytes)
    assert isinstance(encoded_pdf, str)
    # Decode the base64 string and check the number of pages
    decoded_pdf = base64.b64decode(encoded_pdf)
    pdf_reader = PdfReader(io.BytesIO(decoded_pdf))
    original_pdf_reader = PdfReader(io.BytesIO(sample_pdf_bytes))
    assert len(pdf_reader.pages) == len(original_pdf_reader.pages)


def test_base64_encode_pdf_empty_pages_list(sample_pdf_bytes):
    encoded_pdf = base64_encode_pdf(sample_pdf_bytes, pages=[])
    assert isinstance(encoded_pdf, str)
    # Decode the base64 string and check the number of pages
    decoded_pdf = base64.b64decode(encoded_pdf)
    pdf_reader = PdfReader(io.BytesIO(decoded_pdf))
    original_pdf_reader = PdfReader(io.BytesIO(sample_pdf_bytes))
    assert len(pdf_reader.pages) == len(original_pdf_reader.pages)


def test_base64_encode_pdf_out_of_bounds(sample_pdf_bytes):
    pdf_reader = PdfReader(io.BytesIO(sample_pdf_bytes))
    total_pages = len(pdf_reader.pages)
    out_of_bounds_page = total_pages + 3  # Page number that is out of bounds

    # Encode with an out-of-bounds page number
    encoded_pdf = base64_encode_pdf(sample_pdf_bytes, pages=[0, out_of_bounds_page])
    assert isinstance(encoded_pdf, str)

    # Decode the base64 string and check the number of pages
    decoded_pdf = base64.b64decode(encoded_pdf)
    pdf_reader = PdfReader(io.BytesIO(decoded_pdf))
    assert len(pdf_reader.pages) == 1


def test_read_pages(sample_pdf_bytes):
    page_texts = get_page_texts(sample_pdf_bytes)
    print(page_texts)
    assert isinstance(page_texts, list)
    assert len(page_texts) == 1
    assert isinstance(page_texts[0], str)
    assert "Sample PDF" in page_texts[0]
