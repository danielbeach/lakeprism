"""Application-owned credential callback protocols.

These protocols intentionally describe callables rather than credential
objects. LakePrism stores a callback only; each returned secret is consumed for
one request and is never serialized, logged, attached to a MediaRef, or
retained in a catalog/session.
"""

from typing import Optional, Protocol


class OAuthTokenSupplier(Protocol):
    """Supply a fresh OAuth bearer token for one Unity REST request."""

    def __call__(
        self, query_id: str, principal: str, catalog_identity: Optional[str]
    ) -> str: ...


class FlightAuthSupplier(Protocol):
    """Supply a fresh Flight bearer token for one ``FlightClient.execute`` call."""

    def __call__(self) -> str: ...
