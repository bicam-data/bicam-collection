"""
Dynamic Key Pool for Streamlined Processing

This module provides a dynamic API key pool that manages API keys
with rate limiting and automatic rotation.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class KeyState(Enum):
    """API key states."""

    AVAILABLE = "available"
    IN_USE = "in_use"
    RATE_LIMITED = "rate_limited"
    COOLING_DOWN = "cooling_down"


@dataclass
class PooledAPIKey:
    """Represents a single API key in the pool with usage tracking."""

    key: str
    state: KeyState = KeyState.AVAILABLE
    request_count: int = 0
    checked_out_by: str | None = None
    checked_out_at: datetime | None = None
    rate_limited_at: datetime | None = None
    rate_limit_duration: timedelta = field(
        default_factory=lambda: timedelta(minutes=30)
    )
    last_request_time: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def requests_remaining(self) -> int:
        """Calculate remaining requests before rate limit."""
        return max(0, 5000 - self.request_count)

    @property
    def is_available(self) -> bool:
        """Check if key is available for checkout."""
        if self.state == KeyState.AVAILABLE:
            return True

        # Check if rate limit has expired
        if (
            self.state == KeyState.RATE_LIMITED
            and self.rate_limited_at
            and datetime.now(UTC) - self.rate_limited_at > self.rate_limit_duration
        ):
            self.state = KeyState.AVAILABLE
            self.request_count = 0
            return True

        return False

    def mark_rate_limited(self):
        """Mark key as rate limited and start cooldown."""
        self.state = KeyState.RATE_LIMITED
        self.rate_limited_at = datetime.now(UTC)
        logger.info(
            f"Key {self.key[:8]}... rate limited. Will be available at "
            f"{self.rate_limited_at + self.rate_limit_duration}"
        )


class DynamicKeyPool:
    """Dynamic API key pool with rate limiting and rotation."""

    def __init__(self, api_keys: list[str], rate_limit_threshold: int = 4995):
        self.rate_limit_threshold = rate_limit_threshold
        self._keys: dict[str, PooledAPIKey] = {}
        self._lock = asyncio.Lock()

        # Initialize keys
        for key in api_keys:
            self._keys[key] = PooledAPIKey(key=key)

        # Stagger initial request counts to prevent synchronized rate limiting
        self._stagger_initial_keys()

        logger.info(f"Initialized dynamic key pool with {len(api_keys)} keys")

    def _stagger_initial_keys(self):
        """Stagger initial request counts to prevent synchronized rate limiting."""
        for i, key_obj in enumerate(self._keys.values()):
            key_obj.request_count = i * 100  # Spread out by 100 requests each

    async def checkout_key(
        self, worker_id: str, preferred_requests: int = 250
    ) -> PooledAPIKey | None:
        """Check out a key for use by a worker."""
        async with self._lock:
            return await self._checkout_key_internal(worker_id, preferred_requests)

    async def _checkout_key_internal(
        self, worker_id: str, preferred_requests: int
    ) -> PooledAPIKey | None:
        """Internal key checkout logic."""
        # Reset expired rate limits first
        await self.reset_expired_limits()

        # Find best available key
        best_key = self._find_best_key(preferred_requests)

        if best_key is None:
            logger.warning(f"No available keys for worker {worker_id}")
            return None

        # Check out the key
        return self._complete_checkout(best_key, worker_id)

    def _complete_checkout(self, key_obj: PooledAPIKey, worker_id: str) -> PooledAPIKey:
        """Complete the key checkout process."""
        key_obj.state = KeyState.IN_USE
        key_obj.checked_out_by = worker_id
        key_obj.checked_out_at = datetime.now(UTC)

        logger.debug(
            f"Checked out key {key_obj.key[:8]}... to {worker_id} "
            f"({key_obj.requests_remaining} requests remaining)"
        )

        return key_obj

    def _find_best_key(self, preferred_requests: int) -> PooledAPIKey | None:
        """Find the best available key for the requested number of requests."""
        available_keys = [k for k in self._keys.values() if k.is_available]

        if not available_keys:
            return None

        # Score keys based on availability and request capacity
        def key_score(k: PooledAPIKey):
            requests_remaining = k.requests_remaining
            if requests_remaining < preferred_requests:
                return -1  # Not suitable
            return requests_remaining  # Prefer keys with more requests remaining

        # Sort by score (highest first)
        available_keys.sort(key=key_score, reverse=True)
        return available_keys[0] if available_keys else None

    async def checkin_key(
        self, key: str, requests_made: int = 0, rate_limited: bool = False
    ):
        """Check in a key after use."""
        async with self._lock:
            if key not in self._keys:
                logger.warning(f"Unknown key checked in: {key[:8]}...")
                return

            key_obj = self._keys[key]

            # Update usage statistics
            key_obj.request_count += requests_made
            key_obj.last_request_time = datetime.now(UTC)

            if rate_limited or key_obj.request_count >= self.rate_limit_threshold:
                key_obj.mark_rate_limited()
            else:
                key_obj.state = KeyState.AVAILABLE

            # Clear checkout info
            worker_id = key_obj.checked_out_by
            key_obj.checked_out_by = None
            key_obj.checked_out_at = None

            logger.debug(
                f"Checked in key {key[:8]}... from {worker_id} "
                f"({requests_made} requests, {key_obj.requests_remaining} remaining)"
            )

    async def get_pool_status(self) -> dict[str, Any]:
        """Get current pool status."""
        async with self._lock:
            await self.reset_expired_limits()

            status_by_state = {}
            for state in KeyState:
                status_by_state[state.value] = len(
                    [k for k in self._keys.values() if k.state == state]
                )

            total_requests_remaining = sum(
                k.requests_remaining for k in self._keys.values()
            )

            return {
                "total_keys": len(self._keys),
                "status_by_state": status_by_state,
                "total_requests_remaining": total_requests_remaining,
                "average_requests_remaining": (
                    total_requests_remaining / len(self._keys) if self._keys else 0
                ),
                "keys_needing_reset": len(
                    [k for k in self._keys.values() if k.state == KeyState.RATE_LIMITED]
                ),
            }

    async def reset_expired_limits(self):
        """Reset rate limits that have expired."""
        now = datetime.now(UTC)
        reset_count = 0

        for key_obj in self._keys.values():
            if (
                key_obj.state == KeyState.RATE_LIMITED
                and key_obj.rate_limited_at
                and now - key_obj.rate_limited_at > key_obj.rate_limit_duration
            ):
                key_obj.state = KeyState.AVAILABLE
                key_obj.request_count = 0
                key_obj.rate_limited_at = None
                reset_count += 1

        if reset_count > 0:
            logger.info(f"Reset {reset_count} expired rate limits")

        return reset_count
