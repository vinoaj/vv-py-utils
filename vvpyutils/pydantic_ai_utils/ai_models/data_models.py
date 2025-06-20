"""Pydantic models to represent common Pydantic AI data structures such as messages, usage details, etc."""

from typing import Literal

from pydantic import BaseModel, Field


# All possible roles: https://platform.openai.com/docs/api-reference/chat/create#chat-create-messages
class Message(BaseModel):
    role: Literal["developer", "user", "assistant"] = Field(
        ..., description="Role of the message sender."
    )
    type: str = Field(
        default="message",
        description="Type of the message content, e.g., 'message', 'web_search_call'",
    )
    text: str = Field(..., description="Content of the message")


class UsageDetails(BaseModel):
    """Token usage details for a request."""

    accepted_prediction_tokens: int = Field(
        ..., description="Number of accepted prediction tokens"
    )
    rejected_prediction_tokens: int = Field(
        ..., description="Number of rejected prediction tokens"
    )
    audio_tokens: int = Field(..., description="Number of audio tokens used")
    reasoning_tokens: int = Field(..., description="Number of reasoning tokens used")
    cached_tokens: int = Field(..., description="Number of cached tokens used")


class Usage(BaseModel):
    """Token usage details for entire agent flow."""

    requests: int = Field(..., description="Number of requests made")
    request_tokens: int = Field(..., description="Number of tokens used in requests")
    response_tokens: int = Field(..., description="Number of tokens used in responses")
    total_tokens: int = Field(
        ..., description="Total number of tokens used (requests + responses)"
    )
    details: dict[str, int] | None = None
