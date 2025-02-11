from datetime import datetime
from typing import Optional

from dateparser.search import search_dates
from dateutil.relativedelta import relativedelta
from dateutil import parser

dateparser_settings = {
    "RELATIVE_BASE": datetime.now(),  # Use current date as reference
    "DEFAULT_LANGUAGES": ["en"],  # Use English as default language
    "PREFER_DATES_FROM": "future",  # Prefer future dates for relative expressions
    "PREFER_DAY_OF_MONTH": "first",  # Prefer the first day of the month for relative expressions
}


def convert_date_str_to_YYYYMMDD(
    date_str: str, settings: Optional[dict[str, str]] = dateparser_settings
) -> str | None:
    """Convert a date string to YYYY-MM-DD format.

    Args:
        date_str (str): Input date string to convert
        settings (Optional[dict[str, str]], optional): Custom dateparser settings.
            Defaults to dateparser_settings.

    Returns:
        str | None: Formatted date string in YYYY-MM-DD format if successful, None if conversion fails

    Example:
        >>> convert_date_str_to_YYYYMMDD("January 1st 2023")
        '2023-01-01'
    """
    if date_str is None:
        return None
    try:
        date_obj: list[tuple[str, datetime]] = search_dates(date_str, settings=settings)
        formatted_date = date_obj[0][1].strftime("%Y-%m-%d")
        return formatted_date
    except (ValueError, AttributeError, TypeError) as e:
        print(f"Exception with date string: {date_str}: {str(e)}")
        return None


def convert_YYYY_MM_to_str(y: int, m: int, long_format: bool = False) -> str:
    """Convert year and month integers to a formatted date string.

    This function takes a year and month as integers and returns a formatted string representation
    of the date in either long or short month format.

    Args:
        y (int): The year (e.g., 2023)
        m (int): The month as a number (1-12)
        long_format (bool, optional): If True, uses full month name (e.g., "January 2023").
            If False, uses abbreviated month name (e.g., "Jan 2023"). Defaults to False.

    Returns:
        str: A formatted string representing the year and month.
            Example outputs: "Jan 2023" or "January 2023"

    Example:
        >>> convert_YYYY_MM_to_str(2023, 1)
        'Jan 2023'
        >>> convert_YYYY_MM_to_str(2023, 1, long_format=True)
        'January 2023'
    """
    date = datetime(year=y, month=1, day=1) + relativedelta(months=m - 1)
    date_str = date.strftime("%B %Y") if long_format else date.strftime("%b %Y")
    return date_str


def convert_time_str_to_hhmm(time_str: str) -> str:
    """Convert a time string to HHMM format.

    Args:
        time_str (str): Input time string in any parseable format

    Returns:
        str: Time in HHMM format (e.g. '1430' for 2:30 PM), or None if parsing fails
    """
    try:
        time_obj = parser.parse(time_str)
        formatted_time = time_obj.strftime("%H%M")
        return formatted_time
    except ValueError:
        print(f"Exception with time string: {time_str}")
        return None
