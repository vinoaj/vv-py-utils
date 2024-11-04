from vvpyutils.json_utils import extract_json_from_string


def test_extract_json_from_string():
    content = (
        "**Answer:**\n"
        "```\n"
        "{\n"
        '  "status": "success",\n'
        '  "error_message": null,\n'
        '  "with_gst_charge": true,\n'
        '  "receipt_date": "2024-10-08",\n'
        '  "receipt_time": "10:05",\n'
        '  "invoice_number": "503-0759680-5380644",\n'
        '  "expense_code": 0,\n'
        '  "expense_description": "Other",\n'
        '  "vendor_name": "Amazon",\n'
        '  "line_items": [\n'
        "    {\n"
        '      "description": "Amazon Basics Reusable Vacuum Compression Storage Bags with Free Hand Pump '
        'Medium, 5-Pack Best for Travel Packing",\n'
        '      "quantity": 1,\n'
        '      "unit_cost": 18.76,\n'
        '      "inclusive_of_gst": true\n'
        "    }\n"
        "  ],\n"
        '  "meeting_info": null,\n'
        '  "currency": "AUD",\n'
        '  "final_amount": 0.0,\n'
        '  "final_gst": 0.0,\n'
        '  "new_filename": "2024-10-08-1005-Amazon-Other.pdf"\n'
        "}\n"
        "```"
    )

    expected_output = {
        "status": "success",
        "error_message": None,
        "with_gst_charge": True,
        "receipt_date": "2024-10-08",
        "receipt_time": "10:05",
        "invoice_number": "503-0759680-5380644",
        "expense_code": 0,
        "expense_description": "Other",
        "vendor_name": "Amazon",
        "line_items": [
            {
                "description": "Amazon Basics Reusable Vacuum Compression Storage Bags with Free Hand Pump "
                "Medium, 5-Pack Best for Travel Packing",
                "quantity": 1,
                "unit_cost": 18.76,
                "inclusive_of_gst": True,
            }
        ],
        "meeting_info": None,
        "currency": "AUD",
        "final_amount": 0.0,
        "final_gst": 0.0,
        "new_filename": "2024-10-08-1005-Amazon-Other.pdf",
    }

    result = extract_json_from_string(content)
    assert (
        result == expected_output
    ), "The extracted JSON does not match the expected output."
