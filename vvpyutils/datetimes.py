from datetime import datetime
from typing import Optional

from dateparser.search import search_dates

dateparser_settings = {
    "RELATIVE_BASE": datetime.now(),  # Use current date as reference
    "DEFAULT_LANGUAGES": ["en"],  # Use English as default language
    "PREFER_DATES_FROM": "future",  # Prefer future dates for relative expressions
    "PREFER_DAY_OF_MONTH": "first",  # Prefer the first day of the month for relative expressions
}


def convert_date_str_to_YYYYMMDD(
    date_str: str, settings: Optional[dict[str, str]] = dateparser_settings
) -> str | None:
    if date_str is None:
        return None
    try:
        date_obj: list[tuple[str, datetime]] = search_dates(date_str, settings=settings)
        formatted_date = date_obj[0][1].strftime("%Y-%m-%d")
        return formatted_date
    except (ValueError, AttributeError, TypeError) as e:
        print(f"Exception with date string: {date_str}: {str(e)}")
        return None
