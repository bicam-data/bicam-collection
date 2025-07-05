"""
Dynamic API Key Pool Manager for optimal concurrent processing.

This module implements a dynamic key pool that allows workers to check out
and return keys as needed, maximizing API utilization.
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from threading import Lock

logger = logging.getLogger(__name__)


class KeyState(Enum):
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
            f"Key {self.key[:8]}... rate limited. Will be available at {self.rate_limited_at + self.rate_limit_duration}"
        )


class DynamicKeyPool:
    """
    Dynamic API key pool for optimal distribution across workers.

    Features:
    - Dynamic checkout/checkin of keys
    - Automatic rate limit detection and cooldown
    - Request counting with preemptive key rotation
    - Staggered key usage to avoid simultaneous rate limits
    """

    def __init__(self, api_keys: list[str], rate_limit_threshold: int = 4995):
        self.keys = {key: PooledAPIKey(key=key) for key in api_keys}
        self.rate_limit_threshold = rate_limit_threshold
        self._lock = Lock()
        self._checkout_condition = asyncio.Condition()

        # Stagger initial usage to avoid all keys hitting limits simultaneously
        self._stagger_initial_keys()

        logger.info("==================================================")
        logger.info("=      DynamicKeyPool INITIALIZED                =")
        logger.info(f"=      Mode: DYNAMIC, Keys: {len(api_keys)}               =")
        logger.info("==================================================")

    def _stagger_initial_keys(self):
        """Stagger initial key usage to create a rolling wave of availability."""
        stagger_amount = 100  # Requests between each key's initial count
        for i, key_obj in enumerate(self.keys.values()):
            if i < len(self.keys) // 2:  # Only stagger first half
                key_obj.request_count = i * stagger_amount

    async def checkout_key(
        self, worker_id: str, preferred_requests: int = 250
    ) -> PooledAPIKey | None:
        """
        Check out an API key for a worker.

        Args:
            worker_id: Unique identifier for the worker
            preferred_requests: Preferred number of requests the worker plans to make

        Returns:
            PooledAPIKey object or None if no keys available
        """
        async with self._checkout_condition:
            while True:
                with self._lock:
                    # Find best available key
                    best_key = self._find_best_key(preferred_requests)

                    if best_key:
                        best_key.state = KeyState.IN_USE
                        best_key.checked_out_by = worker_id
                        best_key.checked_out_at = datetime.now(UTC)

                        logger.debug(
                            f"Worker {worker_id} checked out key {best_key.key[:8]}... "
                            f"(requests remaining: {best_key.requests_remaining})"
                        )
                        return best_key

                # No keys available, wait for one to be returned
                logger.debug(f"Worker {worker_id} waiting for available key...")
                await self._checkout_condition.wait()

    def _find_best_key(self, preferred_requests: int) -> PooledAPIKey | None:
        """Find the best available key for the requested number of operations."""
        available_keys = [k for k in self.keys.values() if k.is_available]

        if not available_keys:
            return None

        # Sort by:
        # 1. Keys with enough remaining requests for the full batch
        # 2. Keys with most remaining requests
        # 3. Keys that were least recently used
        def key_score(k: PooledAPIKey):
            has_enough = k.requests_remaining >= preferred_requests
            return (
                has_enough,  # Prefer keys that can handle full batch
                k.requests_remaining,  # Then most remaining requests
                -k.last_request_time.timestamp(),  # Then least recently used
            )

        available_keys.sort(key=key_score, reverse=True)
        return available_keys[0]

    async def checkin_key(
        self, key: str, requests_made: int = 0, rate_limited: bool = False
    ):
        """
        Return a key to the pool.

        Args:
            key: The API key to return
            requests_made: Number of requests made with this key
            rate_limited: Whether the key was rate limited
        """
        async with self._checkout_condition:
            with self._lock:
                if key not in self.keys:
                    logger.error(f"Attempted to check in unknown key: {key[:8]}...")
                    return

                key_obj = self.keys[key]
                key_obj.request_count += requests_made
                key_obj.last_request_time = datetime.now(UTC)

                if rate_limited:
                    key_obj.mark_rate_limited()
                elif key_obj.request_count >= self.rate_limit_threshold:
                    # Preemptively mark as rate limited
                    logger.info(
                        f"Key {key[:8]}... approaching limit "
                        f"({key_obj.request_count}/{self.rate_limit_threshold}), marking as rate limited"
                    )
                    key_obj.mark_rate_limited()
                else:
                    key_obj.state = KeyState.AVAILABLE

                key_obj.checked_out_by = None
                key_obj.checked_out_at = None

                logger.debug(
                    f"Key {key[:8]}... checked in. "
                    f"Requests: {key_obj.request_count}, State: {key_obj.state.value}"
                )

            # Notify waiting workers
            self._checkout_condition.notify_all()

    def get_pool_status(self) -> dict:
        """Get current status of all keys in the pool."""
        with self._lock:
            total_keys = len(self.keys)
            available = sum(
                1 for k in self.keys.values() if k.state == KeyState.AVAILABLE
            )
            in_use = sum(1 for k in self.keys.values() if k.state == KeyState.IN_USE)
            rate_limited = sum(
                1 for k in self.keys.values() if k.state == KeyState.RATE_LIMITED
            )

            total_requests = sum(k.request_count for k in self.keys.values())
            total_capacity = total_keys * 5000

            return {
                "total_keys": total_keys,
                "available": available,
                "in_use": in_use,
                "rate_limited": rate_limited,
                "total_requests_made": total_requests,
                "total_capacity": total_capacity,
                "capacity_used_percent": (total_requests / total_capacity) * 100,
                "keys_detail": [
                    {
                        "key": k.key[:8] + "...",
                        "state": k.state.value,
                        "requests": k.request_count,
                        "remaining": k.requests_remaining,
                        "checked_out_by": k.checked_out_by,
                    }
                    for k in self.keys.values()
                ],
            }

    async def reset_expired_limits(self):
        """Reset any keys whose rate limit period has expired."""
        async with self._checkout_condition:
            with self._lock:
                reset_count = 0
                for key_obj in self.keys.values():
                    if (
                        key_obj.state == KeyState.RATE_LIMITED
                        and key_obj.rate_limited_at
                        and datetime.now(UTC) - key_obj.rate_limited_at
                        > key_obj.rate_limit_duration
                    ):
                        key_obj.state = KeyState.AVAILABLE
                        key_obj.request_count = 0
                        key_obj.rate_limited_at = None
                        reset_count += 1
                        logger.info(
                            f"Key {key_obj.key[:8]}... rate limit expired, now available"
                        )

                if reset_count > 0:
                    self._checkout_condition.notify_all()

                return reset_count
