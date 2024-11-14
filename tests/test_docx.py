import pytest
from pathlib import Path
from vvpyutils.docx import convert_docx_to_pdf, batch_convert


@pytest.fixture
def sample_docx() -> Path:
    return Path(__file__).parent / "docx_samples" / "demo.docx"


def test_convert_docx_to_pdf(sample_docx: Path, tmp_path: Path = None) -> None:
    if tmp_path is None:
        tmp_path = Path(__file__).parent / "output"

    output_pdf = tmp_path / "output.pdf"
    result = convert_docx_to_pdf(sample_docx, output_pdf)
    assert result.exists()
    assert result.suffix == ".pdf"


# def test_batch_convert(tmp_path: Path) -> None:
#     input_dir = tmp_path / "input"
#     input_dir.mkdir()
#     output_dir = tmp_path / "output"
#     output_dir.mkdir()

#     # Copy sample DOCX file to input directory
#     sample_docx = Path("docx_samples/demo.docx")
#     for i in range(3):
#         docx_file = input_dir / f"demo_{i}.docx"
#         docx_file.write_bytes(sample_docx.read_bytes())

#     successful, failed = batch_convert(input_dir, output_dir)
#     assert len(successful) == 3
#     assert len(failed) == 0

#     for pdf_file in successful:
#         assert pdf_file.exists()
#         assert pdf_file.suffix == ".pdf"
