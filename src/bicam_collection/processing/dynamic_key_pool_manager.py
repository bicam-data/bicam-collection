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

        # Use separate locks for better performance
        self._keys_lock = asyncio.Lock()  # For key state modifications
        self._checkout_condition = asyncio.Condition()  # For waiters
        self._checkout_semaphore = asyncio.Semaphore(
            len(api_keys)
        )  # Limit concurrent checkouts

        # Performance optimization: track available keys count
        self._available_keys_count = len(api_keys)

        # Stagger initial usage to avoid all keys hitting limits simultaneously
        self._stagger_initial_keys()

        logger.info("==================================================")
        logger.info("=      DynamicKeyPool INITIALIZED (OPTIMIZED)    =")
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
        Check out an API key for a worker with optimized performance.

        Args:
            worker_id: Unique identifier for the worker
            preferred_requests: Preferred number of requests the worker plans to make

        Returns:
            PooledAPIKey object or None if no keys available
        """
        # Fast path: check if any keys are available without heavy locking
        if self._available_keys_count <= 0:
            return None

        # Use semaphore to limit concurrent checkout operations
        async with self._checkout_semaphore:
            # Quick lock-free check with timeout to prevent hanging
            try:
                return await asyncio.wait_for(
                    self._checkout_key_internal(worker_id, preferred_requests),
                    timeout=15.0,
                )
            except asyncio.TimeoutError:
                logger.warning(f"Worker {worker_id} timed out during key checkout")
                return None

    async def _checkout_key_internal(
        self, worker_id: str, preferred_requests: int
    ) -> PooledAPIKey | None:
        """Internal optimized checkout implementation."""
        # Try immediate checkout first
        async with self._keys_lock:
            best_key = self._find_best_key(preferred_requests)
            if best_key:
                return self._complete_checkout(best_key, worker_id)

        # If no key available, wait for notification
        max_wait_attempts = 3
        for attempt in range(max_wait_attempts):
            async with self._checkout_condition:
                # Double-check after acquiring condition lock
                async with self._keys_lock:
                    best_key = self._find_best_key(preferred_requests)
                    if best_key:
                        return self._complete_checkout(best_key, worker_id)

                # Wait for a key to become available
                logger.debug(
                    f"Worker {worker_id} waiting for available key (attempt {attempt + 1}/{max_wait_attempts})"
                )
                try:
                    await asyncio.wait_for(self._checkout_condition.wait(), timeout=5.0)
                except asyncio.TimeoutError:
                    if attempt == max_wait_attempts - 1:
                        logger.warning(
                            f"Worker {worker_id} exhausted all wait attempts"
                        )
                        return None
                    continue

        return None

    def _complete_checkout(self, key_obj: PooledAPIKey, worker_id: str) -> PooledAPIKey:
        """Complete the checkout process for a key."""
        key_obj.state = KeyState.IN_USE
        key_obj.checked_out_by = worker_id
        key_obj.checked_out_at = datetime.now(UTC)

        # Update available count
        self._available_keys_count -= 1

        logger.debug(
            f"Worker {worker_id} checked out key {key_obj.key[:8]}... "
            f"(requests remaining: {key_obj.requests_remaining})"
        )
        return key_obj

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
        Return a key to the pool with optimized performance.

        Args:
            key: The API key to return
            requests_made: Number of requests made with this key
            rate_limited: Whether the key was rate limited
        """
        key_became_available = False

        async with self._keys_lock:
            if key not in self.keys:
                logger.error(f"Attempted to check in unknown key: {key[:8]}...")
                return

            key_obj = self.keys[key]
            old_state = key_obj.state
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
                # Key became available if it wasn't before
                key_became_available = old_state != KeyState.AVAILABLE
                if key_became_available:
                    self._available_keys_count += 1

            key_obj.checked_out_by = None
            key_obj.checked_out_at = None

            logger.debug(
                f"Key {key[:8]}... checked in. "
                f"Requests: {key_obj.request_count}, State: {key_obj.state.value}"
            )

        # Notify waiting workers if a key became available (outside of keys lock)
        if key_became_available:
            async with self._checkout_condition:
                self._checkout_condition.notify_all()

    async def get_pool_status(self) -> dict:
        """Get current status of all keys in the pool."""
        async with self._keys_lock:
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
                "available_count_cached": self._available_keys_count,
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
        reset_count = 0

        async with self._keys_lock:
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
                    self._available_keys_count += 1
                    logger.info(
                        f"Key {key_obj.key[:8]}... rate limit expired, now available"
                    )

        # Notify waiting workers if any keys were reset (outside of keys lock)
        if reset_count > 0:
            async with self._checkout_condition:
                self._checkout_condition.notify_all()

        return reset_count
