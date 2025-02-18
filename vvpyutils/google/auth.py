import pickle
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pydantic import BaseModel, ConfigDict, Field, field_validator


class Calendar(str, Enum):
    """Google Calendar API Scopes"""

    EVENTS = "https://www.googleapis.com/auth/calendar.events"
    READ_ONLY = "https://www.googleapis.com/auth/calendar.readonly"
    SETTINGS = "https://www.googleapis.com/auth/calendar.settings.readonly"
    FULL = "https://www.googleapis.com/auth/calendar"


class Gmail(str, Enum):
    """Gmail API Scopes"""

    MODIFY = "https://www.googleapis.com/auth/gmail.modify"
    COMPOSE = "https://www.googleapis.com/auth/gmail.compose"
    SEND = "https://www.googleapis.com/auth/gmail.send"
    READ_ONLY = "https://www.googleapis.com/auth/gmail.readonly"
    FULL = "https://www.googleapis.com/auth/gmail.full"


class Drive(str, Enum):
    """Google Drive API Scopes"""

    FILE = "https://www.googleapis.com/auth/drive.file"
    READ_ONLY = "https://www.googleapis.com/auth/drive.readonly"
    METADATA = "https://www.googleapis.com/auth/drive.metadata.readonly"
    FULL = "https://www.googleapis.com/auth/drive"


class Scopes:
    """Google API Scopes namespace"""

    Calendar = Calendar
    Gmail = Gmail
    Drive = Drive


class GoogleAuthState(BaseModel):
    """Internal state of the auth manager"""

    last_refresh: Optional[datetime] = Field(
        default=None, description="Last time the credentials were refreshed"
    )
    active_services: dict[str, datetime] = Field(
        default_factory=dict,
        description="Dictionary of active services and their initialization times",
    )

    # model_config = dict(arbitrary_types_allowed=True)


class GoogleAuthManager(BaseModel):
    """Pydantic model for Google Authentication Manager"""

    # From GoogleAuthConfig
    credentials_file: Path = Field(
        default=Path("credentials.json"),
        description="Path to the credentials.json file",
    )
    token_file: Path = Field(
        default=Path("token.pickle"),
        description="Path to save/load the token pickle file",
    )
    scopes: list[str] = Field(
        description="List of Google API scopes needed",
        default_factory=lambda: ["https://www.googleapis.com/auth/calendar"],
    )

    # State tracking
    state: GoogleAuthState = Field(default_factory=GoogleAuthState)
    creds: Optional[Credentials] = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @field_validator("credentials_file")
    def validate_credentials_file(cls, v: Path) -> Path:
        if not v.exists():
            raise ValueError(f"Credentials file not found at: {v}")
        return v

    @field_validator("token_file")
    def validate_token_file_path(cls, v: Path) -> Path:
        v.parent.mkdir(parents=True, exist_ok=True)
        return v

    def authenticate(self) -> Credentials:
        """Handles the authentication flow and returns valid credentials"""
        if self.token_file.exists():
            self.creds = pickle.loads(self.token_file.read_bytes())

        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(self.credentials_file), self.scopes
                )
                self.creds = flow.run_local_server(port=0)

            self.state.last_refresh = datetime.now()
            self.token_file.write_bytes(pickle.dumps(self.creds))

        return self.creds

    def get_service(self, api_name: str, api_version: str):
        """Get a Google service client"""
        if not self.creds:
            self.authenticate()

        service = build(api_name, api_version, credentials=self.creds)
        self.state.active_services[api_name] = datetime.now()
        return service

    def get_auth_status(self) -> dict:
        """Get the current authentication status"""
        return {
            "is_authenticated": bool(self.creds and self.creds.valid),
            "last_refresh": self.state.last_refresh,
            "active_services": self.state.active_services,
            "scopes": self.scopes,
        }


if __name__ == "__main__":
    auth_manager = auth_manager = GoogleAuthManager(
        # credentials_file=Path("client_secret.json"),
        # token_file=Path("token.pickle"),
        scopes=[
            Scopes.Calendar.EVENTS,
            Scopes.Gmail.MODIFY,
        ]
    )
    auth_manager.authenticate()

    status = auth_manager.get_auth_status()
    print("Authentication Status:", status)
