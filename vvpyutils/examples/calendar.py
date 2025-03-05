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

calendar = GoogleCalendar(auth_manager=auth_manager, timezone="Australia/Sydney")

# Get upcoming events
upcoming_events = calendar.get_events(max_results=5)
for event in upcoming_events:
    print(
        f"{event['summary']} - {event['start'].get('dateTime', event['start'].get('date'))}"
    )

# Clean up - delete the event we just created
# if created_event:
#     calendar.delete_event(created_event["id"])
