from docx2pdf import convert

# import argparse
from typing import Optional, List
from pathlib import Path
from .config.logger import logger


def convert_docx_to_pdf(input_path: Path, output_path: Optional[Path] = None) -> Path:
    """
    Convert a single DOCX file to PDF format.

    Args:
        input_path (Path): Path to the input DOCX file.
        output_path (Path, optional): Path for the output PDF file. If not provided,
                                      will use the same name as input with .pdf extension.

    Returns:
        Path: Path to the output PDF file.

    Raises:
        FileNotFoundError: If the input DOCX file does not exist.
        ValueError: If the input file is not a DOCX file.
        Exception: If there is an error during the conversion process.
    """
    try:
        if not input_path.exists():
            logger.error(f"Error: Input file '{input_path}' does not exist.")
            raise FileNotFoundError(f"Input file '{input_path}' does not exist.")

        if input_path.suffix != ".docx":
            logger.error(f"Error: Input file '{input_path}' is not a DOCX file.")
            raise ValueError(f"Input file '{input_path}' is not a DOCX file.")

        if output_path is None:
            output_path = Path(input_path).with_suffix(".pdf")

        logger.info(f"Converting '{input_path}' to '{output_path}'")
        convert(input_path, output_path)
        logger.info(f"Successfully converted '{input_path}' to '{output_path}'")
        return output_path

    except Exception as e:
        logger.error(f"Error converting '{input_path}': {str(e)}")
        raise e


def batch_convert(
    input_dir: Path, output_dir: Optional[Path] = None
) -> tuple[list[Path], list[Path]]:
    """
    Convert all DOCX files in a directory to PDF format.

    Args:
        input_dir (Path): Directory containing DOCX files
        output_dir (Path, optional): Directory for output PDF files. If not provided,
                                  PDFs will be created in the same directory as DOCXs

    Returns:
        Tuple[List[Path], List[Path]]: Tuple containing (successful_conversions, failed_conversions)
    """
    try:
        if not input_dir.exists():
            logger.error(f"Error: Input directory '{input_dir}' does not exist.")
            raise FileNotFoundError(f"Input directory '{input_dir}' does not exist.")

        if not input_dir.is_dir():
            logger.error(f"Error: '{input_dir}' is not a directory.")
            raise NotADirectoryError(f"'{input_dir}' is not a directory.")

        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)

        successful_conversions: List[Path] = []
        failed_conversions: List[Path] = []

        for docx_file in input_dir.glob("*.docx"):
            try:
                if output_dir:
                    output_path = output_dir / docx_file.with_suffix(".pdf").name
                else:
                    output_path = None

                converted_path = convert_docx_to_pdf(docx_file, output_path)
                successful_conversions.append(converted_path)
                logger.info(f"Successfully converted {docx_file}")

            except Exception as e:
                failed_conversions.append(docx_file)
                logger.error(f"Failed to convert {docx_file}: {str(e)}")

        logger.info(
            f"Conversion complete: {len(successful_conversions)} successful, {len(failed_conversions)} failed"
        )
        return successful_conversions, failed_conversions

    except Exception as e:
        logger.error(f"Batch conversion failed: {str(e)}")
        raise e


# if __name__ == "__main__":
#     import argparse

#     parser = argparse.ArgumentParser(description="Convert DOCX files to PDF format")
#     parser.add_argument("input", type=Path, help="Input DOCX file or directory")
#     parser.add_argument(
#         "-o", "--output", type=Path, help="Output PDF file or directory (optional)"
#     )
#     args = parser.parse_args()

#     try:
#         if args.input.is_file():
#             convert_docx_to_pdf(args.input, args.output)
#         else:
#             successful, failed = batch_convert(args.input, args.output)
#             if failed:
#                 logger.warning(
#                     f"Failed conversions: {', '.join(str(p) for p in failed)}"
#                 )
#     except Exception as e:
#         logger.error(f"Conversion failed: {str(e)}")
#         exit(1)
