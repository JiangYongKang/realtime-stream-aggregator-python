from __future__ import annotations


class AuthError(Exception):
    """Raised when a principal is unknown or not allowed for a tenant."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


class Authz:
    """Static principal -> tenant binding.

    A principal belongs to exactly one tenant and can only read/write that
    tenant's streams. There is intentionally no external IAM dependency.
    """

    __slots__ = ("_map",)

    def __init__(self, principal_tenants: dict[str, str] | None = None) -> None:
        # copy so later mutation of caller's dict cannot change permissions
        self._map: dict[str, str] = dict(principal_tenants or {})

    def resolve(self, principal: str | None) -> str:
        """Return tenant bound to the principal or raise AuthError."""
        if not principal:
            raise AuthError("MISSING_PRINCIPAL", "X-Principal header is required")
        tenant = self._map.get(principal)
        if tenant is None:
            raise AuthError("UNKNOWN_PRINCIPAL", f"principal '{principal}' is not registered")
        return tenant

    def check(self, principal: str | None, tenant: str) -> str:
        """Ensure principal exists and owns tenant; return tenant or raise."""
        owned = self.resolve(principal)
        if owned != tenant:
            raise AuthError(
                "TENANT_FORBIDDEN",
                f"principal's tenant '{owned}' cannot access tenant '{tenant}'",
            )
        return tenant
