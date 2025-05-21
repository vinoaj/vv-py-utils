# vv-py-utils

Collection of common utility functions I use across a variety of my projects

## Installation

```sh
uv add git+https://github.com/vinoaj/vv-py-utils
poetry add git+https://github.com/vinoaj/vv-py-utils
```

## Usage

```python
from vvpyutils.pdfs import ...
...
```

### Google Services

```python
from vvpyutils.google.auth import GoogleAuthManager, Scopes
from vvpyutils.google.services.calendar import GoogleCalendar

auth_manager = GoogleAuthManager(
    # credentials_file=Path("client_secret.json"),
    # token_file=Path("token.pickle"),
    scopes=[
        Scopes.Calendar.EVENTS,
        Scopes.Gmail.MODIFY,
    ]
)

calendar = GoogleCalendar(auth_manager=auth_manager)
upcoming_events = calendar.get_events(max_results=5)
for event in upcoming_events:
    print(
        f"{event['summary']} - {event['start'].get('dateTime', event['start'].get('date'))}"
    )
```

## Utilities

### docx

- Utilises the [docx2pdf](https://github.com/AlJohri/docx2pdf) package
  - For this to work, you need Microsoft Word present on your device
