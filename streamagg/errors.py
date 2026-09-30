"""Typed errors carrying a stable machine-readable code and HTTP status."""

from __future__ import annotations


class AppError(Exception):
    code = "internal_error"
    http_status = 500

    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class AuthError(AppError):
    code = "unauthorized"
    http_status = 401


class ForbiddenError(AppError):
    code = "forbidden"
    http_status = 403


class ValidationError(AppError):
    code = "invalid_request"
    http_status = 400


class OverloadError(AppError):
    code = "overloaded"
    http_status = 503


class ConflictError(AppError):
    code = "conflict"
    http_status = 409


class NotFoundError(AppError):
    code = "not_found"
    http_status = 404
