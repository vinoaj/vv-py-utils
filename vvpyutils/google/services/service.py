from functools import cached_property
from typing import Any, ClassVar

from pydantic import BaseModel

from vvpyutils.google.auth import GoogleAuthManager


class GoogleService(BaseModel):
    auth_manager: GoogleAuthManager

    service_name: ClassVar[str] = "calendar"
    service_version: ClassVar[str] = "v3"

    @cached_property
    def service(self) -> Any:
        return self.auth_manager.get_service(self.service_name, self.service_version)
