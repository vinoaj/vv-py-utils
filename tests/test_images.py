import base64
import io
from pathlib import Path
from PIL import Image
import pytest
from vvpyutils.images import combine_images_vertically, get_image_base64_encoded_url

SAMPLE_IMAGES_PATH = Path(__file__).parent / "image_samples"
SAMPLE_IMAGES = [
    "1706.03762v7-0.png",
    "1706.03762v7-1.png",
    "1706.03762v7-2.png",
    "1706.03762v7-3.png",
]


@pytest.fixture
def sample_image_paths(tmp_path):
    image_paths = []
    for image_name in SAMPLE_IMAGES[:4]:  # Only the first 4 images
        image_path = tmp_path / image_name
        image_data = (SAMPLE_IMAGES_PATH / image_name).read_bytes()
        image_path.write_bytes(image_data)
        image_paths.append(image_path)
    return image_paths


@pytest.fixture
def sample_image_bytes():
    return [
        (SAMPLE_IMAGES_PATH / image_name).read_bytes()
        for image_name in SAMPLE_IMAGES[:4]
    ]


@pytest.fixture
def sample_image_data_urls(sample_image_bytes):
    return [
        get_image_base64_encoded_url(io.BytesIO(image_bytes))
        for image_bytes in sample_image_bytes
    ]


# Update tests to use the modified fixtures
def test_combine_images_vertically_from_paths(sample_image_paths):
    combined_image = combine_images_vertically(sample_image_paths)
    assert isinstance(combined_image, Image.Image)
    assert (
        combined_image.height
        == len(sample_image_paths) * Image.open(sample_image_paths[0]).height
    )


def test_combine_images_vertically_from_bytes(sample_image_bytes):
    combined_image = combine_images_vertically(sample_image_bytes)
    assert isinstance(combined_image, Image.Image)
    assert (
        combined_image.height
        == len(sample_image_bytes)
        * Image.open(io.BytesIO(sample_image_bytes[0])).height
    )


def test_combine_images_vertically_from_data_urls(sample_image_data_urls):
    combined_image = combine_images_vertically(sample_image_data_urls)
    assert isinstance(combined_image, Image.Image)
    assert (
        combined_image.height
        == len(sample_image_data_urls)
        * Image.open(
            io.BytesIO(base64.b64decode(sample_image_data_urls[0].split(",")[1]))
        ).height
    )


def test_combine_images_vertically_return_data_url(sample_image_bytes):
    combined_image_data_url = combine_images_vertically(
        sample_image_bytes, return_data_url=True
    )
    assert isinstance(combined_image_data_url, str)
    assert combined_image_data_url.startswith("data:image/png;base64,")


def test_combine_images_vertically_invalid_type():
    with pytest.raises(TypeError):
        combine_images_vertically([12345])
