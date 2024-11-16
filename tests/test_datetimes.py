from vvpyutils.datetimes import convert_date_str_to_YYYYMMDD
import datetime


def test_valid_date():
    assert convert_date_str_to_YYYYMMDD("2023-10-05") == "2023-10-05"


def test_valid_date_with_time():
    assert convert_date_str_to_YYYYMMDD("2023-10-05 14:30:00") == "2023-10-05"


def test_invalid_date():
    assert convert_date_str_to_YYYYMMDD("invalid-date") is None


def test_empty_string():
    assert convert_date_str_to_YYYYMMDD("") is None


def test_none_input():
    assert convert_date_str_to_YYYYMMDD(None) is None


def test_next_friday():
    next_friday = (
        datetime.datetime.now()
        + datetime.timedelta((4 - datetime.datetime.now().weekday()) % 7)
    ).strftime("%Y-%m-%d")
    assert convert_date_str_to_YYYYMMDD("next friday") == next_friday


# def test_two_fridays_from_today():
#     # two_fridays = (
#     #     datetime.datetime.now()
#     #     + datetime.timedelta((4 - datetime.datetime.now().weekday()) % 7 + 7)
#     # ).strftime("%Y-%m-%d")
#     assert convert_date_str_to_YYYYMMDD("two fridays from today") is None


def test_full_date_with_comma():
    assert convert_date_str_to_YYYYMMDD("3 July, 1979") == "1979-07-03"


def test_abbreviated_date():
    assert convert_date_str_to_YYYYMMDD("3 Jul 1979") == "1979-07-03"
