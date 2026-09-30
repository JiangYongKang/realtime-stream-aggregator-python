"""Static bearer-token tenant isolation (local-only, no external services).

A token maps to exactly one tenant. A request without a known token is
``401 unauthorized``; a request that tries to touch another tenant's resource
is ``403 tenant_mismatch``. Tokens are configured locally (config/env), never
fetched from an external identity service.
"""

from __future__ import annotations

from streamagg.errors import AuthError, ForbiddenError


class TenantAuth:
    def __init__(self, token_tenant: dict[str, str] | None) -> None:
        self._map = dict(token_tenant or {})

    def tenant_for_token(self, token: str | None) -> str:
        if not token or token not in self._map:
            raise AuthError("missing or invalid bearer token",
                            details={"reason": "invalid_token"})
        return self._map[token]

    def authorize(self, token: str | None, tenant: str) -> str:
        """Return the authenticated tenant if it matches the target tenant."""
        actor = self.tenant_for_token(token)
        if actor != tenant:
            raise ForbiddenError(
                "tenant can only access its own streams",
                details={"reason": "tenant_mismatch",
                         "authenticated_tenant": actor,
                         "requested_tenant": tenant},
            )
        return actor

    @staticmethod
    def parse_bearer(authorization: str | None) -> str | None:
        if not authorization:
            return None
        parts = authorization.split(" ", 1)
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return None
        token = parts[1].strip()
        return token or None
