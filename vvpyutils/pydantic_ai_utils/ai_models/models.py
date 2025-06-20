from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import ClassVar

from agentsynthpanel.utils.google.auth import GoogleAuthHelper
from pydantic import BaseModel, ConfigDict
from pydantic_ai.agent import ModelSettings
from pydantic_ai.models.google import GoogleModel, GoogleModelSettings
from pydantic_ai.providers import Provider
from pydantic_ai.providers.google import GoogleProvider
from pydantic_ai.providers.openai import OpenAIProvider

# TODO: move model loading logic to a separate module


class ModelProvider(Enum):
    """Model provider IDs aligned with Pydantic AI's model identifiers."""

    GOOGLE_GLA: str = "google-gla"  # Google Generative Language API
    GOOGLE_VERTEX: str = "google-vertex"  # Google Vertex AI
    OLLAMA: str = "ollama"
    OPENAI: str = "openai"


class LLMModelConfig(BaseModel):
    provider: ClassVar[ModelProvider]
    llm_model_id: ClassVar[str]
    temperature: float = 0.7
    extra_headers: dict[str, str] = {}
    pydantic_provider: Provider | None = OpenAIProvider

    # Settings for thinking models
    is_thinking_model: bool = False
    thinking_budget: int = 1024
    include_thoughts: bool = True

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @cached_property
    def llm_model_id_str(self) -> str:
        """Returns the model ID as a string."""
        return f"{self.provider.value}:{self.llm_model_id}"

    def get_model_settings(self) -> ModelSettings:
        settings_kwargs = {"temperature": self.temperature}
        if self.extra_headers and len(self.extra_headers) > 0:
            settings_kwargs["extra_headers"] = self.extra_headers

        settings = ModelSettings(**settings_kwargs)

        return settings

    def get_model(self):
        """
        - For standard models, Pydantic AI can use a model string identifer.
        - For models that require custom authentication settings (e.g. accessing models via Vertex AI), we need to return the relevant pydantic_ai.providers.xxx instance."""

        return self.llm_model_id_str


class OpenAIGPT41Nano(LLMModelConfig):
    provider: ClassVar[ModelProvider] = ModelProvider.OPENAI
    llm_model_id: ClassVar[str] = "gpt-4.1-nano"


class OpenAIGPT41Mini(LLMModelConfig):
    provider: ClassVar[ModelProvider] = ModelProvider.OPENAI
    llm_model_id: ClassVar[str] = "gpt-4.1-mini"


class OpenAIGPT4o(LLMModelConfig):
    provider: ClassVar[ModelProvider] = ModelProvider.OPENAI
    llm_model_id: ClassVar[str] = "gpt-4o"


class OpenAIGPT4oMini(LLMModelConfig):
    provider: ClassVar[ModelProvider] = ModelProvider.OPENAI
    llm_model_id: ClassVar[str] = "gpt-4o-mini"


# https://ai.pydantic.dev/models/google/#vertex-ai-enterprisecloud
class GoogleModelConfig(LLMModelConfig):
    provider: ClassVar[ModelProvider] = ModelProvider.GOOGLE_VERTEX
    pydantic_provider: GoogleProvider | None = None

    # TODO: Add support for ADC authentication
    def _get_pydantic_provider(
        self,
        google_api_key: str | None = None,
        service_account_file_path: Path | None = None,
        project_id: str | None = None,
        location: str | None = None,
    ) -> GoogleProvider:
        """Returns a GoogleProvider instance based on the provided input parameters.
        Automatically determines if it should return a Google Vertex AI or GLA provider."""

        if not self.pydantic_provider:
            if self.provider == ModelProvider.GOOGLE_VERTEX:
                self.pydantic_provider = GoogleProvider(
                    vertexai=True,
                    credentials=GoogleAuthHelper.load_credentials(
                        service_account_file_path,
                    ),
                    project=project_id,
                    location=location,
                )
            elif self.provider == ModelProvider.GOOGLE_GLA:
                self.pydantic_provider = GoogleProvider(api_key=google_api_key)

        return self.pydantic_provider

    def get_model_settings(self) -> GoogleModelSettings:
        """Returns the model settings for Google Vertex AI."""
        parent_settings: ModelSettings = super().get_model_settings()

        settings = GoogleModelSettings(
            **parent_settings,  # Unpack all parent settings
            google_thinking_config={
                # Currently can't set thinking budget for Gemini 2.5 Pro.
                # "thinking_budget": self.thinking_budget,
                "include_thoughts": self.include_thoughts,
            },
        )

        return settings

    def get_model(
        self,
        google_api_key: str | None = None,
        service_account_file_path: Path | None = None,
        project_id: str | None = None,
        location: str | None = None,
    ) -> GoogleModel:
        """Returns a GoogleModel instance configured with the provider."""
        return GoogleModel(
            model_name=self.llm_model_id,
            provider=self._get_pydantic_provider(
                service_account_file_path=service_account_file_path,
                project_id=project_id,
                location=location,
                google_api_key=google_api_key,
            ),
        )


# https://ai.pydantic.dev/models/google/#vertex-ai-enterprisecloud
class Gemini25ProVertex(GoogleModelConfig):
    provider: ClassVar[ModelProvider] = ModelProvider.GOOGLE_VERTEX
    llm_model_id: ClassVar[str] = "gemini-2.5-pro-preview-05-06"
    is_thinking_model: bool = True


class Gemini25ProGLA(GoogleModelConfig):
    provider: ClassVar[ModelProvider] = ModelProvider.GOOGLE_GLA
    llm_model_id: ClassVar[str] = "gemini-2.5-pro-preview-05-06"
    is_thinking_model: bool = True
