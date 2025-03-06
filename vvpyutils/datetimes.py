import re
from datetime import datetime
from typing import Optional

import pytz
from dateparser.search import search_dates
from dateutil import parser
from dateutil.relativedelta import relativedelta

from vvpyutils.config.logger import logger
from vvpyutils.config.settings import (
    DEFAULT_TIMEZONE,
    LOCALE_DATE_ORDERS,
    LOCALE_DEFAULT,
)

dateparser_settings = {
    "RELATIVE_BASE": datetime.now(pytz.timezone(DEFAULT_TIMEZONE)),
    "DEFAULT_LANGUAGES": ["en"],
    "PREFER_DATES_FROM": "future",
    "PREFER_DAY_OF_MONTH": "first",
}


def is_iso_format(date_str: str) -> bool:
    """Check if the date string is in ISO-like format (YYYY-MM-DD)."""
    return re.match(r"\d{4}-\d{1,2}-\d{1,2}", date_str) is not None


def convert_date_str_to_YYYYMMDD(
    date_str: str, locale: str = LOCALE_DEFAULT
) -> Optional[str]:
    """Convert a date string to YYYY-MM-DD format based on the given locale.

    Args:
        date_str (str): Input date string to convert.
        locale (str, optional): Locale determining date order for ambiguous formats, e.g., 'en-AU' for Australia, 'en-US' for the United States.Defaults to LOCALE_DEFAULT (typically 'en-AU').

    Returns:
        Optional[str]: Formatted date string in YYYY-MM-DD format, or None if conversion fails.

    Examples:
        >>> convert_date_str_to_YYYYMMDD("07/02/2025", locale="en-AU")
        '2025-02-07'
        >>> convert_date_str_to_YYYYMMDD("07/02/2025", locale="en-US")
        '2025-07-02'
    """
    if not date_str:
        return None

    try:
        language = locale.split("-")[0]
        settings = dateparser_settings.copy()
        settings["DEFAULT_LANGUAGES"] = [language]

        if not is_iso_format(date_str):
            date_order = LOCALE_DATE_ORDERS.get(locale, "DMY")
            settings["DATE_ORDER"] = date_order

        if date_obj := search_dates(date_str, settings=settings):
            parsed_date = date_obj[0][1].astimezone(pytz.timezone(DEFAULT_TIMEZONE))

            current_year = datetime.now().year
            if parsed_date.year >= current_year + 99:
                parsed_date = parsed_date.replace(year=parsed_date.year - 100)

            return parsed_date.strftime("%Y-%m-%d")
        else:
            logger.error(f"Failed to parse date string: {date_str}")
            return None
    except Exception as e:
        logger.error(f"Exception while parsing date string: {date_str}: {str(e)}")
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
