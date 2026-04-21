"""
Legacy Apify actor integration.

The optional ``apify-client`` package is no longer required. Actor runs are
no-ops that return an empty dataset, so miners can run without Apify billing.
Validators that still need live Apify validation should install ``apify-client``
and restore a full implementation if required.
"""

from typing import List, Optional

import bittensor as bt
from pydantic import Field, PositiveInt

from common.data import StrictBaseModel


class RunConfig(StrictBaseModel):
    """Parameters for a (stubbed) actor run — kept for API compatibility."""

    api_key: Optional[str] = Field(
        default=None,
        description="Unused when Apify is disabled.",
    )
    actor_id: str = Field(
        description="The ID of the actor to run.",
        default="",
    )
    timeout_secs: PositiveInt = Field(
        description="The timeout for the actor run.",
        default=180,
    )
    max_data_entities: PositiveInt = Field(
        description="Max items hint (unused in stub).",
        default=100,
    )
    debug_info: str = Field(
        description="Optional debug info for logs.",
        default="",
    )
    memory_mb: Optional[int] = Field(
        description="Memory hint (unused in stub).", default=None
    )


class ActorRunError(Exception):
    """Exception raised when an actor run fails."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


class ActorRunner:
    def __init__(self):
        pass

    async def run(self, config: RunConfig, run_input: dict) -> List[dict]:
        """
        Stub: does not call Apify. Returns an empty dataset so scrapers exit cleanly.
        """
        bt.logging.trace(
            "Apify disabled (stub ActorRunner): returning empty dataset. "
            f"{config.debug_info or config.actor_id}"
        )
        return []
