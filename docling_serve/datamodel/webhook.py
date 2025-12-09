from typing import Mapping, Sequence
from urllib.parse import urlparse

from pydantic import AnyHttpUrl, BaseModel, Field
from typing_extensions import Literal


def validate_webhook_url(
    url: AnyHttpUrl | str,
    allowed_hosts: Sequence[str],
    allowed_schemes: Sequence[str],
) -> None:
    parsed = urlparse(str(url))
    if allowed_schemes and parsed.scheme not in allowed_schemes:
        raise ValueError(
            f"Webhook URL scheme '{parsed.scheme}' is not allowed."
            f" Allowed schemes: {', '.join(allowed_schemes)}."
        )

    if allowed_hosts and parsed.hostname not in allowed_hosts:
        raise ValueError(
            f"Webhook host '{parsed.hostname}' is not in the allowed host list."
        )


class WebhookConfig(BaseModel):
    url: AnyHttpUrl | None = Field(
        default=None,
        description="Webhook callback endpoint. If unset, webhooks are disabled.",
    )
    method: Literal["POST", "PUT", "PATCH"] = Field(
        default="POST",
        description="HTTP method to use when invoking the webhook.",
    )
    secret: str | None = Field(
        default=None,
        description=(
            "Shared secret attached to the webhook payload for downstream validation."
        ),
    )
    headers: Mapping[str, str] = Field(
        default_factory=dict,
        description="Custom headers to include with the webhook payload.",
    )
    max_retries: int = Field(
        default=0,
        ge=0,
        description="Maximum number of retry attempts for failed webhook deliveries.",
    )
    backoff_factor: float = Field(
        default=1.0,
        ge=0.0,
        description="Backoff multiplier (in seconds) between webhook retry attempts.",
    )


class WebhookOverride(BaseModel):
    url: AnyHttpUrl = Field(
        description="Override callback endpoint for this task only.",
    )
    secret: str | None = Field(
        default=None,
        description="Optional secret that overrides the default when set.",
    )

    def validate_against(
        self, allowed_hosts: Sequence[str], allowed_schemes: Sequence[str]
    ) -> "WebhookOverride":
        validate_webhook_url(
            url=self.url,
            allowed_hosts=allowed_hosts,
            allowed_schemes=allowed_schemes,
        )
        return self
