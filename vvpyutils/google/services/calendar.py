from datetime import datetime, timezone
from typing import Any, ClassVar, Optional, Union

from googleapiclient.errors import HttpError
from pydantic import BaseModel, Field

from ...config.logger import logger
from ..auth import GoogleAuthManager, Scopes
from .service import GoogleService


class CalendarAttendee(BaseModel):
    # Currently this schema only reflects the minimum required fields to create an event
    email: str
    optional: bool = False
    responseStatus: str = Field(
        default="needsAction",
        description="Attendee's response status: needsAction, declined, accepted, or tentative",
    )


class CalendarEvent(BaseModel):
    """Model representing a Google Calendar event.
    Schema at: https://developers.google.com/calendar/api/v3/reference/events
    """

    # Currently this schema only reflects the minimum required fields to create an event

    status: str = Field(
        default="confirmed",
        description="Event status: confirmed, tentative, or cancelled",
    )
    summary: str = Field(..., description="Event title")
    description: Optional[str] = None
    location: Optional[str] = None
    colorId: Optional[str] = None
    start_datetime: datetime
    end_datetime: datetime
    timezone: str = Field(
        default="Australia/Sydney", description="Timezone for the event"
    )
    recurrence: Optional[list[str]] = None
    attendees: Optional[list[CalendarAttendee]] = None
    guestsCanInviteOthers: bool = True
    guestsCanModify: bool = True
    visibility: str = Field(
        default="default",
        description="Visibility of the event: default, public, or private",
    )


class GoogleCalendar(GoogleService):
    """Manager for Google Calendar operations"""

    timezone: str = Field(default="UTC", description="Default timezone for events")

    service_name: ClassVar[str] = "calendar"
    service_version: ClassVar[str] = "v3"

    def create_event(
        self, event: Union[CalendarEvent, dict[str, Any]], calendar_id: str = "primary"
    ) -> Optional[dict[str, Any]]:
        """
        Create a calendar event

        Args:
            event: Either a CalendarEvent instance or a dictionary with event details
            calendar_id: The calendar ID to create the event in (defaults to "primary")

        Returns:
            The created event dictionary or None if creation failed

        Raises:
            ValueError: If event data is invalid
        """

        # Convert dict to CalendarEvent and validate
        try:
            if isinstance(event, dict):
                event = CalendarEvent(**event)
        except ValueError as e:
            logger.error(f"Invalid event data: {e}")
            raise ValueError(f"Invalid event data: {e}")

        # Use class timezone if event timezone not specified
        event_timezone = event.timezone if event.timezone else self.timezone

        # Build event body using dictionary comprehension for optional fields
        event_body = {
            "status": event.status,
            "summary": event.summary,
            "start": {
                "dateTime": event.start_datetime.isoformat(),
                "timeZone": event_timezone,
            },
            "end": {
                "dateTime": event.end_datetime.isoformat(),
                "timeZone": event_timezone,
            },
            "guestsCanInviteOthers": event.guestsCanInviteOthers,
            "guestsCanModify": event.guestsCanModify,
            "visibility": event.visibility,
            **{
                k: v
                for k, v in {
                    "description": event.description,
                    "location": event.location,
                    "colorId": event.colorId,
                    "recurrence": event.recurrence,
                }.items()
                if v is not None
            },
        }

        # Handle attendees separately due to different structure
        if event.attendees:
            event_body["attendees"] = [
                {"email": attendee.email}
                for attendee in event.attendees
                if attendee.email  # Ensure email is not None
            ]

        try:
            created_event = (
                self.service.events()
                .insert(calendarId=calendar_id, body=event_body)
                .execute()
            )
            logger.info(f"Event created successfully: {created_event.get('htmlLink')}")
            return created_event

        except HttpError as e:
            logger.error(f"Failed to create event: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error creating event: {e}")
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
