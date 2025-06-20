import datetime

from agentsynthpanel.models.ai import Message
from pydantic import BaseModel, ConfigDict
from pydantic_ai.agent import AgentRunResult


class AgentRunResultMetadata(BaseModel):
    messages: list[Message] = []
    created_at: datetime
    llm_model_name: str = "unknown"

    model_config = ConfigDict(arbitrary_types_allowed=True)


class PydanticAIUtils(BaseModel):
    agent_run_result: AgentRunResult | None = None

    def extract_result_metadata(
        self, agent_run_result: AgentRunResult | None = None
    ) -> AgentRunResultMetadata:
        """Extracts metadata from the AgentRunResult."""
        if not agent_run_result:
            agent_run_result = self.agent_run_result

        if not agent_run_result:
            raise ValueError("AgentRunResult is not set.")

        messages = []
        created_at = None
        llm_model_name = None

        # Use the all_messages() helper method from AgentRunResult
        model_messages = agent_run_result.all_messages()

        # Process all messages from the run
        for message in model_messages:
            # Check if it's a model request (containing system prompts and user messages)
            if message.kind == "request" and hasattr(message, "parts"):
                for part in message.parts:
                    # Extract system prompt
                    if hasattr(part, "part_kind") and part.part_kind == "system-prompt":
                        messages.append(
                            Message(
                                role="developer",
                                type="system_prompt",
                                text=part.content,
                            )
                        )

                    # Extract user message
                    elif hasattr(part, "part_kind") and part.part_kind == "user-prompt":
                        messages.append(
                            Message(role="user", type="message", text=part.content)
                        )

            # Check if it's a model response (containing assistant's reply)
            elif message.kind == "response" and hasattr(message, "parts"):
                for part in message.parts:
                    if hasattr(part, "part_kind") and part.part_kind == "text":
                        messages.append(
                            Message(role="assistant", type="message", text=part.content)
                        )

                # Get the timestamp from the response for created_at
                if hasattr(message, "timestamp"):
                    created_at = message.timestamp

                if hasattr(message, "model_name"):
                    llm_model_name = message.model_name

        # If no timestamp was found, use current datetime
        if not created_at:
            created_at = datetime.datetime.now(datetime.timezone.utc)

        return AgentRunResultMetadata(
            messages=messages,
            created_at=created_at,
            llm_model_name=llm_model_name,
        )
