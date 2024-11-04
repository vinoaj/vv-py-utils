import json
from json_repair import repair_json


def json_str_to_dict(json_str: str) -> dict:
    """
    Converts a JSON string to a dictionary.

    Args:
        json_str (str): The JSON string to be converted.

    Returns:
        dict: The dictionary representation of the JSON string.

    Raises:
        ValueError: If the JSON string is invalid.
    """

    json_str = json_str.replace("\n", "").replace(" ", "")

    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON string: {e}") from None


def extract_json_from_string(content: str) -> dict[str, any]:
    try:
        return json.loads(repair_json(content))
    except json.JSONDecodeError:
        raise ValueError("The JSON part of the message content is invalid JSON")
