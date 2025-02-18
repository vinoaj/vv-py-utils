from datetime import datetime, timezone
from typing import ClassVar, Optional, Union

from pydantic import BaseModel, Field

from ..auth import GoogleAuthManager, Scopes
from .service import GoogleService


class CalendarEvent(BaseModel):
    """Model representing a Google Calendar event"""

    summary: str
    start_datetime: datetime
    end_datetime: datetime
    description: Optional[str] = None
    location: Optional[str] = None
    timezone: str = Field(
        default="Australia/Sydney", description="Timezone for the event"
    )


class GoogleCalendar(GoogleService):
    """Manager for Google Calendar operations"""

    timezone: str = Field(default="UTC", description="Default timezone for events")

    service_name: ClassVar[str] = "calendar"
    service_version: ClassVar[str] = "v3"

    def create_event(self, event: Union[CalendarEvent, dict]) -> Optional[dict]:
        """
        Create a calendar event

        Args:
            event: Either a CalendarEvent instance or a dictionary with event details

        Returns:
            The created event or None if creation failed
        """
        if isinstance(event, dict):
            event = CalendarEvent(**event)

        event_body = {
            "summary": event.summary,
            "start": {
                "dateTime": event.start_datetime.isoformat(),
                "timeZone": event.timezone,
            },
            "end": {
                "dateTime": event.end_datetime.isoformat(),
                "timeZone": event.timezone,
            },
        }

        if event.description:
            event_body["description"] = event.description
        if event.location:
            event_body["location"] = event.location

        try:
            created_event = (
                self.service.events()
                .insert(calendarId="primary", body=event_body)
                .execute()
            )
            print(f"Event created: {created_event.get('htmlLink')}")
            return created_event
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def get_events(
        self,
        calendar_id: str = "primary",
        max_results: int = 10,
        time_min: Optional[datetime] = None,
    ) -> list:
        """
        Get upcoming events from the calendar

        Args:
            max_results: Maximum number of events to return
            time_min: Datetime to start fetching events from (defaults to now)

        Returns:
            List of calendar events
        """
        if time_min is None:
            time_min = datetime.now(timezone.utc).isoformat()

        events_result = (
            self.service.events()
            .list(
                calendarId=calendar_id,
                timeMin=time_min,
                maxResults=max_results,
                singleEvents=True,
                orderBy="startTime",
            )
            .execute()
        )

        return events_result.get("items", [])

    def delete_event(self, event_id: str) -> bool:
        """
        Delete a calendar event

        Args:
            event_id: ID of the event to delete

        Returns:
            True if deletion was successful, False otherwise
        """

        try:
            self.service.events().delete(
                calendarId="primary", eventId=event_id
            ).execute()
            return True
        except Exception as e:
            print(f"An error occurred while deleting event: {e}")
            return False


if __name__ == "__main__":
    auth_manager = GoogleAuthManager(
        # credentials_file=Path("client_secret.json"),
        # token_file=Path("token.pickle"),
        scopes=[
            Scopes.Calendar.EVENTS,
            Scopes.Gmail.MODIFY,
        ]
    )

    calendar = GoogleCalendar(auth_manager=auth_manager, timezone="Australia/Sydney")

    # Example: Create an event for tomorrow
    # tomorrow = datetime.now() + timedelta(days=1)
    # event = CalendarEvent(
    #     summary="Team Meeting",
    #     start_datetime=tomorrow.replace(hour=10, minute=0),
    #     end_datetime=tomorrow.replace(hour=11, minute=0),
    #     description="Weekly team sync",
    #     location="Conference Room A",
    #     timezone="America/Los_Angeles",
    # )

    # created_event = calendar.create_event(event)

    # Get upcoming events
    upcoming_events = calendar.get_events(max_results=5)
    for event in upcoming_events:
        print(
            f"{event['summary']} - {event['start'].get('dateTime', event['start'].get('date'))}"
        )

    # Clean up - delete the event we just created
    # if created_event:
    #     calendar.delete_event(created_event["id"])
