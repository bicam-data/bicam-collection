"""
System-Level API Key Manager for Congressional Data Processing

This module provides coordinated API key management across multiple data types
and processing clients. It ensures optimal key distribution, prevents conflicts,
and provides system-wide rate limiting coordination.

Features:
- Round-robin key assignment across data types
- Key pool management with rebalancing
- Rate limit tracking per key
- Automatic key redistribution when clients finish
- Thread-safe operations for concurrent Dagster assets
- Parallelization with API key pairs for concurrent sessions
"""

import logging
from datetime import UTC, datetime, timedelta
from threading import Lock
from typing import Any

from ..processing.key_pool import DynamicKeyPool

logger = logging.getLogger(__name__)


class APIKeySession:
    """Represents a parallel session with paired API keys for backup/failover."""

    def __init__(self, session_id: str, api_keys: list[str]):
        self.session_id = session_id
        self.api_keys = api_keys  # Usually 2 keys for backup
        self.current_key_index = 0
        self.assigned_to: str | None = None
        self.active = False
        self.last_used = datetime.now(UTC)

    def get_primary_key(self) -> str:
        """Get the primary API key for this session."""
        return self.api_keys[0] if self.api_keys else None

    def get_backup_key(self) -> str:
        """Get the backup API key for this session."""
        return self.api_keys[1] if len(self.api_keys) > 1 else self.api_keys[0]

    def rotate_key(self):
        """Rotate to the next key in the session (for rate limit handling)."""
        if len(self.api_keys) > 1:
            self.current_key_index = (self.current_key_index + 1) % len(self.api_keys)
            logger.info(
                f"Session {self.session_id} rotated to key index {self.current_key_index}"
            )

    def get_current_key(self) -> str:
        """Get the currently active key for this session."""
        return self.api_keys[self.current_key_index] if self.api_keys else None


class APIKeyStatus:
    """Track status and usage of a single API key."""

    def __init__(self, key: str):
        self.key = key
        self.assigned_to: str | None = None  # data_type name or session_id
        self.assigned_session: str | None = (
            None  # session_id if part of parallel session
        )
        self.last_used: datetime = datetime.now(UTC)
        self.request_count: int = 0
        self.is_rate_limited: bool = False
        self.rate_limit_until: datetime | None = None
        self.consecutive_errors: int = 0


class SystemAPIKeyManager:
    """
    System-wide API key manager for coordinated distribution across data types.

    This manager ensures that API keys are optimally distributed across different
    data type processors (bills, amendments, members, etc.) to maximize throughput
    while respecting rate limits.

    New Features:
    - Parallel sessions with API key pairs for backup/failover
    - Session-based key management for concurrent processing
    """

    def __init__(
        self,
        api_keys: list[str],
        default_keys_per_client: int = 2,
        enable_parallelization: bool = True,
        use_dynamic_pool: bool = True,
    ):
        if not api_keys:
            raise ValueError("No API keys provided to SystemAPIKeyManager")

        self.api_keys = list(set(api_keys))  # Remove duplicates
        self.default_keys_per_client = min(default_keys_per_client, len(self.api_keys))
        self.enable_parallelization = enable_parallelization
        self.use_dynamic_pool = use_dynamic_pool
        self.dynamic_pool: DynamicKeyPool | None = None

        if self.use_dynamic_pool:
            self.dynamic_pool = DynamicKeyPool(self.api_keys)
            logger.info("Initialized SystemAPIKeyManager with dynamic key pool.")

        # Track key status
        self.key_status: dict[str, APIKeyStatus] = {
            key: APIKeyStatus(key) for key in self.api_keys
        }

        # Track assignments (traditional single-client assignment)
        self.data_type_assignments: dict[str, list[str]] = {}  # data_type -> [keys]
        self.active_data_types: set[str] = set()

        # Track parallel sessions
        self.parallel_sessions: dict[
            str, APIKeySession
        ] = {}  # session_id -> APIKeySession
        self.data_type_sessions: dict[str, list[str]] = {}  # data_type -> [session_ids]

        # Thread safety for concurrent Dagster assets
        self._lock = Lock()

        logger.info(
            f"Initialized SystemAPIKeyManager with {len(self.api_keys)} keys (parallelization: {enable_parallelization})"
        )

    def get_dynamic_pool(self) -> DynamicKeyPool | None:
        """Get the dynamic pool if enabled."""
        return self.dynamic_pool if self.use_dynamic_pool else None

    def assign_parallel_sessions_for_data_type(
        self, data_type: str, num_sessions: int | None = None, keys_per_session: int = 2
    ) -> list[APIKeySession]:
        """
        Assign parallel sessions with API key pairs to a specific data type.

        Args:
            data_type: Name of the data type (e.g., 'bills', 'amendments')
            num_sessions: Number of parallel sessions to create (calculated from available keys if None)
            keys_per_session: Number of keys per session (default 2 for backup)

        Returns:
            List of APIKeySession objects for parallel processing
        """
        with self._lock:
            if not self.enable_parallelization:
                # Fallback to traditional assignment
                keys = self.assign_keys_for_data_type(data_type)
                if keys:
                    session = APIKeySession(f"{data_type}_session_0", keys)
                    return [session]
                return []

            # If data type already has sessions, return them
            if data_type in self.data_type_sessions:
                logger.info(
                    f"Data type {data_type} already has parallel sessions assigned"
                )
                return [
                    self.parallel_sessions[sid]
                    for sid in self.data_type_sessions[data_type]
                ]

            # Calculate optimal number of sessions
            if num_sessions is None:
                available_keys = len(
                    [k for k, s in self.key_status.items() if s.assigned_to is None]
                )
                num_sessions = available_keys // keys_per_session
                if num_sessions == 0 and available_keys > 0:
                    num_sessions = 1
                    keys_per_session = available_keys

            if num_sessions == 0:
                logger.warning(
                    f"No available keys for parallel sessions for {data_type}"
                )
                return []

            logger.info(
                f"Creating {num_sessions} parallel sessions for {data_type} with {keys_per_session} keys each"
            )

            # Create sessions
            sessions = []
            used_keys = []

            for i in range(num_sessions):
                session_id = f"{data_type}_session_{i}"

                # Get keys for this session
                session_keys = self._get_keys_for_session(keys_per_session, used_keys)
                if not session_keys:
                    logger.warning(f"Could not get keys for session {session_id}")
                    break

                # Create session
                session = APIKeySession(session_id, session_keys)
                session.assigned_to = data_type
                session.active = True

                # Update key status
                for key in session_keys:
                    self.key_status[key].assigned_to = data_type
                    self.key_status[key].assigned_session = session_id
                    used_keys.append(key)

                # Store session
                self.parallel_sessions[session_id] = session
                sessions.append(session)

            # Track data type sessions
            if sessions:
                self.data_type_sessions[data_type] = [s.session_id for s in sessions]
                self.active_data_types.add(data_type)

                logger.info(
                    f"Created {len(sessions)} parallel sessions for {data_type}"
                )
                for session in sessions:
                    masked_keys = [k[:8] + "..." for k in session.api_keys]
                    logger.info(f"  Session {session.session_id}: {masked_keys}")

            return sessions

    def _get_keys_for_session(
        self, num_keys: int, exclude_keys: list[str]
    ) -> list[str]:
        """Get available keys for a session, excluding already used keys."""
        available = [
            key
            for key, status in self.key_status.items()
            if status.assigned_to is None
            and key not in exclude_keys
            and not status.is_rate_limited
        ]

        # Sort by last used time (prefer least recently used)
        available.sort(key=lambda k: self.key_status[k].last_used)

        return available[:num_keys]

    def release_parallel_sessions_for_data_type(
        self, data_type: str
    ) -> list[APIKeySession]:
        """
        Release all parallel sessions assigned to a data type.

        Args:
            data_type: Name of the data type

        Returns:
            List of released sessions
        """
        with self._lock:
            if data_type not in self.data_type_sessions:
                logger.warning(f"No parallel sessions assigned to {data_type}")
                return []

            session_ids = self.data_type_sessions[data_type]
            released_sessions = []

            # Release each session
            for session_id in session_ids:
                if session_id in self.parallel_sessions:
                    session = self.parallel_sessions[session_id]

                    # Release keys
                    for key in session.api_keys:
                        self.key_status[key].assigned_to = None
                        self.key_status[key].assigned_session = None

                    session.active = False
                    released_sessions.append(session)

                    # Remove session
                    del self.parallel_sessions[session_id]

            # Remove from assignments
            del self.data_type_sessions[data_type]
            self.active_data_types.discard(data_type)

            logger.info(
                f"Released {len(released_sessions)} parallel sessions from {data_type}"
            )

            # Trigger rebalancing if other data types are still active
            if self.active_data_types:
                self._rebalance_keys()

            return released_sessions

    def rotate_session_key(
        self, session_id: str, reason: str = "rate_limit"
    ) -> str | None:
        """
        Rotate to the backup key in a session when the primary key hits rate limits.

        Args:
            session_id: ID of the session to rotate
            reason: Reason for rotation (for logging)

        Returns:
            New current key for the session, or None if session not found
        """
        with self._lock:
            if session_id not in self.parallel_sessions:
                logger.warning(f"Session {session_id} not found for key rotation")
                return None

            session = self.parallel_sessions[session_id]
            old_key = session.get_current_key()
            session.rotate_key()
            new_key = session.get_current_key()

            if old_key != new_key:
                logger.info(
                    f"Session {session_id} rotated from {old_key[:8]}... to {new_key[:8]}... (reason: {reason})"
                )

                # Mark old key as rate limited temporarily
                if reason == "rate_limit":
                    self.mark_key_rate_limited(old_key, duration_seconds=120)

                return new_key
            else:
                logger.warning(
                    f"Session {session_id} could not rotate key (only one key available)"
                )
                return new_key

    def get_session_for_data_type(
        self, data_type: str, session_index: int = 0
    ) -> APIKeySession | None:
        """Get a specific session for a data type by index."""
        with self._lock:
            if data_type not in self.data_type_sessions:
                return None

            session_ids = self.data_type_sessions[data_type]
            if session_index >= len(session_ids):
                return None

            session_id = session_ids[session_index]
            return self.parallel_sessions.get(session_id)

    def assign_keys_for_data_type(
        self, data_type: str, num_keys: int | None = None
    ) -> list[str]:
        """
        Assign API keys to a specific data type (traditional single-client mode).

        Args:
            data_type: Name of the data type (e.g., 'bills', 'amendments')
            num_keys: Number of keys to assign (defaults to default_keys_per_client)

        Returns:
            List of assigned API keys
        """
        with self._lock:
            if num_keys is None:
                num_keys = self.default_keys_per_client

            # Don't assign more keys than available
            num_keys = min(num_keys, len(self.api_keys))

            # If data type already has keys, return them
            if data_type in self.data_type_assignments:
                logger.info(f"Data type {data_type} already has keys assigned")
                return self.data_type_assignments[data_type]

            # Find best available keys
            available_keys = self._get_best_available_keys(num_keys)

            if len(available_keys) < num_keys:
                logger.warning(
                    f"Only {len(available_keys)} keys available for {data_type}, "
                    f"requested {num_keys}"
                )

            # Assign keys
            self.data_type_assignments[data_type] = available_keys
            self.active_data_types.add(data_type)

            # Update key status
            for key in available_keys:
                self.key_status[key].assigned_to = data_type
                self.key_status[key].last_used = datetime.now(UTC)

            logger.info(
                f"Assigned {len(available_keys)} keys to {data_type}: {available_keys}"
            )
            return available_keys

    def release_keys_for_data_type(self, data_type: str) -> list[str]:
        """
        Release all keys assigned to a data type.

        Args:
            data_type: Name of the data type

        Returns:
            List of released keys
        """
        with self._lock:
            if data_type not in self.data_type_assignments:
                logger.warning(f"No keys assigned to {data_type}")
                return []

            released_keys = self.data_type_assignments[data_type]

            # Release keys
            for key in released_keys:
                self.key_status[key].assigned_to = None

            # Remove from assignments
            del self.data_type_assignments[data_type]
            self.active_data_types.discard(data_type)

            logger.info(f"Released {len(released_keys)} keys from {data_type}")

            # Trigger rebalancing if other data types are still active
            if self.active_data_types:
                self._rebalance_keys()

            return released_keys

    def _get_best_available_keys(self, num_keys: int) -> list[str]:
        """Get the best available keys based on usage and status."""
        # Prefer unassigned keys first
        unassigned = [
            key
            for key, status in self.key_status.items()
            if status.assigned_to is None and not status.is_rate_limited
        ]

        if len(unassigned) >= num_keys:
            # Sort by last used time (prefer least recently used)
            unassigned.sort(key=lambda k: self.key_status[k].last_used)
            return unassigned[:num_keys]

        # If not enough unassigned keys, we need to rebalance
        logger.info("Not enough unassigned keys, triggering rebalance")
        self._rebalance_keys()

        # Try again with rebalanced keys
        unassigned = [
            key
            for key, status in self.key_status.items()
            if status.assigned_to is None and not status.is_rate_limited
        ]

        return unassigned[:num_keys]

    def _rebalance_keys(self):
        """Rebalance keys across active data types."""
        if not self.active_data_types:
            return

        logger.info(
            f"Rebalancing keys across {len(self.active_data_types)} active data types"
        )

        # Calculate fair distribution
        available_keys = [
            key for key, status in self.key_status.items() if not status.is_rate_limited
        ]

        keys_per_type = max(1, len(available_keys) // len(self.active_data_types))

        # Redistribute keys
        key_index = 0
        for data_type in sorted(self.active_data_types):  # Sort for consistency
            # Release current keys
            for key in self.data_type_assignments.get(data_type, []):
                self.key_status[key].assigned_to = None

            # Assign new keys
            new_keys = available_keys[key_index : key_index + keys_per_type]
            self.data_type_assignments[data_type] = new_keys

            for key in new_keys:
                self.key_status[key].assigned_to = data_type

            key_index += keys_per_type

            logger.info(f"Rebalanced {data_type}: {len(new_keys)} keys")

    def mark_key_rate_limited(self, key: str, duration_seconds: int = 60):
        """Mark a key as rate limited for a specific duration."""
        with self._lock:
            if key in self.key_status:
                self.key_status[key].is_rate_limited = True
                self.key_status[key].rate_limit_until = datetime.now(UTC) + timedelta(
                    seconds=duration_seconds
                )
                logger.warning(
                    f"Key {key[:8]}... marked as rate limited for {duration_seconds}s"
                )

    def mark_key_error(self, key: str):
        """Mark a key as having encountered an error."""
        with self._lock:
            if key in self.key_status:
                self.key_status[key].consecutive_errors += 1
                logger.warning(
                    f"Key {key[:8]}... error count: {self.key_status[key].consecutive_errors}"
                )

    def update_key_usage(self, key: str):
        """Update last used time for a key."""
        with self._lock:
            if key in self.key_status:
                self.key_status[key].last_used = datetime.now(UTC)
                self.key_status[key].request_count += 1
                self.key_status[
                    key
                ].consecutive_errors = 0  # Reset error count on success

    def get_status_summary(self) -> dict[str, Any]:
        """Get a summary of current key assignments and status."""
        with self._lock:
            return {
                "total_keys": len(self.api_keys),
                "active_data_types": len(self.active_data_types),
                "assignments": {
                    data_type: len(keys)
                    for data_type, keys in self.data_type_assignments.items()
                },
                "rate_limited_keys": sum(
                    1 for status in self.key_status.values() if status.is_rate_limited
                ),
                "unassigned_keys": sum(
                    1
                    for status in self.key_status.values()
                    if status.assigned_to is None
                ),
            }

    def cleanup_expired_rate_limits(self):
        """Clean up expired rate limits."""
        with self._lock:
            now = datetime.now(UTC)
            for status in self.key_status.values():
                if (
                    status.is_rate_limited
                    and status.rate_limit_until
                    and now > status.rate_limit_until
                ):
                    status.is_rate_limited = False
                    status.rate_limit_until = None
