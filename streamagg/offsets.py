"""Consumer offsets with monotonic commit validation and durable persistence.

An offset is the ordinal of the last finalized window a consumer has consumed
for ``(tenant, stream, consumer)``. Commit rules (all explicit, never silent):

* committing the same offset again is idempotent -> returns the current value;
* committing backwards (``offset < current``) is ``409 offset_rolled_back``;
* with ``expected`` supplied (optimistic locking), a mismatch is
  ``409 offset_conflict``;
* committing past the highest finalized ordinal is
  ``409 offset_ahead_of_data``.

Offsets live in memory and are persisted as part of the durable snapshot, so
after a restart they continue from exactly where they were committed.
"""

from __future__ import annotations

import logging

from streamagg.errors import ConflictError, ValidationError

logger = logging.getLogger("streamagg.offsets")


class OffsetStore:
    def __init__(self) -> None:
        # (tenant, stream, consumer) -> committed ordinal
        self._off: dict[tuple[str, str, str], int] = {}

    @staticmethod
    def _k(tenant: str, stream: str, consumer: str) -> tuple[str, str, str]:
        return (tenant, stream, consumer)

    def get(self, tenant: str, stream: str, consumer: str) -> int:
        return self._off.get(self._k(tenant, stream, consumer), 0)

    def commit(self, tenant: str, stream: str, consumer: str,
               offset: int, expected: int | None = None,
               max_ordinal: int | None = None) -> int:
        if not isinstance(offset, int) or offset < 0:
            raise ValidationError("offset must be a non-negative integer",
                                  details={"reason": "invalid_offset"})
        k = self._k(tenant, stream, consumer)
        current = self._off.get(k, 0)

        if expected is not None and expected != current:
            raise ConflictError(
                "offset commit conflicts with current committed offset",
                details={"reason": "offset_conflict",
                         "current": current, "expected": expected,
                         "submitted": offset},
            )
        if offset == current:
            logger.info("idempotent offset commit tenant=%s stream=%s consumer=%s off=%s",
                        tenant, stream, consumer, current)
            return current
        if offset < current:
            raise ConflictError(
                "offset rollback is not allowed",
                details={"reason": "offset_rolled_back",
                         "current": current, "submitted": offset},
            )
        if max_ordinal is not None and offset > max_ordinal:
            raise ConflictError(
                "offset is ahead of any finalized window",
                details={"reason": "offset_ahead_of_data",
                         "max_ordinal": max_ordinal, "submitted": offset},
            )
        self._off[k] = offset
        logger.info("offset committed tenant=%s stream=%s consumer=%s %s->%s",
                    tenant, stream, consumer, current, offset)
        return offset

    def snapshot(self) -> dict:
        return {"|".join(k): v for k, v in self._off.items()}

    def restore(self, state: dict) -> None:
        self._off = {tuple(k.split("|", 2)): int(v) for k, v in (state or {}).items()}
