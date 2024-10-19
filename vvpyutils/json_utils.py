import json


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
