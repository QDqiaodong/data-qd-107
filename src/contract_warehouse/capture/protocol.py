"""Strict loopback portal protocol primitives (no SQLite, no workflow).

This module implements only the *wire* rules shared by the capture client and
its tests:

* endpoint parsing restricted to ``http`` on an explicit localhost host with
  an explicit port, no credentials, no query and no fragment;
* percent-encoding of caller-provided identifiers so they can never escape a
  URL path segment or introduce query parameters;
* strict UTF-8 JSON decoding that rejects duplicate object keys, extra fields,
  non-integer booleans and schema versions other than the integer ``1``;
* bounded HTTP response reading (1 MiB JSON, 2 MiB CSV) that enforces the
  limits on the bytes actually read and validates ``Content-Length`` against
  the complete body;
* frozen-manifest validation including same-origin absolute path URLs.

Everything here uses the Python standard library only.
"""

from __future__ import annotations

import http.client
import json
from urllib.parse import quote, urlsplit

from .errors import EndpointError, ProtocolError

#: Only schema_version == integer 1 is accepted; True/False are not integers.
SUPPORTED_SCHEMA_VERSION = 1
MODE_INHERITED = "inherited_contracts_v2"

#: Wire-level byte limits, enforced while reading rather than trusted headers.
MAX_JSON_BYTES = 1 * 1024 * 1024
MAX_CSV_BYTES = 2 * 1024 * 1024

#: Per-request socket timeout in seconds.  Kept as a module attribute so tests
#: can exercise real timeout behavior without waiting five seconds.
REQUEST_TIMEOUT_SECONDS = 5.0

ROLES = ("customers", "hierarchy", "contracts", "tickets")

# Hosts that may appear in an endpoint.  The portal is a loopback fixture.
_LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
_IDENTITY_FIELDS = {"schema_version", "request_id", "job_id"}
_JOB_FIELDS = {
    "schema_version", "request_id", "job_id", "state",
    "snapshot_id", "files", "error",
}
_FILE_FIELDS = {"role", "url", "sha256", "byte_length"}
_STATES = {"pending", "ready", "failed"}
_HEX64 = set("0123456789abcdef")


# ---------------------------------------------------------------------------
# Endpoint and identifier handling
# ---------------------------------------------------------------------------


def validate_endpoint(endpoint: str) -> str:
    """Return the normalized endpoint base or raise :class:`EndpointError`.

    The endpoint must use ``http`` (no TLS portal in this loopback fixture),
    name an explicit ``localhost``/``127.0.0.1``/``::1`` host with an explicit
    port, carry no userinfo, query or fragment, and must not itself contain a
    backslash.  A trailing slash (or run of them) is stripped; any other path
    prefix is preserved verbatim and used as the API base path.
    """
    if not isinstance(endpoint, str) or not endpoint:
        raise EndpointError("endpoint must be a non-empty string")
    if "\\" in endpoint:
        raise EndpointError("endpoint must not contain backslashes")
    try:
        parts = urlsplit(endpoint)
    except ValueError as exc:
        raise EndpointError(f"endpoint is not a valid URL: {endpoint!r}") from exc
    if parts.scheme != "http":
        raise EndpointError("endpoint must use the http scheme")
    if parts.query != "":
        raise EndpointError("endpoint must not contain a query string")
    if parts.fragment != "":
        raise EndpointError("endpoint must not contain a URL fragment")
    if parts.username is not None or parts.password is not None:
        raise EndpointError("endpoint must not contain user credentials")
    hostname = parts.hostname
    port = parts.port
    if hostname not in _LOCAL_HOSTS:
        raise EndpointError(
            "endpoint host must be one of localhost/127.0.0.1/::1, "
            f"got {hostname!r}"
        )
    if port is None:
        raise EndpointError("endpoint must include an explicit port")
    if not 1 <= port <= 65535:
        raise EndpointError("endpoint port must be between 1 and 65535")
    # urlsplit() accepts some percent-encoded userinfo/host oddities; the raw
    # netloc is checked directly as well.
    netloc = parts.netloc
    if "@" in netloc:
        raise EndpointError("endpoint must not contain user credentials")
    base_path = parts.path.rstrip("/")
    if "\\" in base_path:
        raise EndpointError("endpoint path must not contain backslashes")
    normalized = f"http://{netloc}{base_path}"
    # Re-parse the normalized form to guarantee it round-trips cleanly.
    again = urlsplit(normalized)
    if again.scheme != "http" or again.hostname not in _LOCAL_HOSTS or again.port != port:
        raise EndpointError(f"endpoint normalized to an unsupported URL: {normalized!r}")
    return normalized


def origin_of(endpoint: str) -> tuple[str, str, int]:
    """Return ``(scheme, hostname, port)`` for composing same-origin URLs."""
    parts = urlsplit(endpoint)
    return parts.scheme, parts.hostname, parts.port


def host_header(endpoint: str) -> str:
    """The literal Host header value (preserves an IPv6 bracket form)."""
    return urlsplit(endpoint).netloc


def encode_identifier(identifier: str, what: str = "request_id") -> str:
    """Percent-encode a caller identifier for use inside one URL path segment.

    ``quote(..., safe="")`` encodes every reserved character, including ``/``,
    space, ``?`` and ``#``, and percent-encodes non-ASCII bytes, so the value
    can never break out of its path segment, inject a query string or produce
    a non-ASCII request target.  The original (unencoded) string stays the
    identity used in JSON bodies and local state.
    """
    if not isinstance(identifier, str) or not identifier or identifier != identifier.strip():
        raise ProtocolError(
            f"{what} must be a non-empty string without leading or trailing whitespace"
        )
    encoded = quote(identifier, safe="", encoding="utf-8", errors="strict")
    try:
        encoded.encode("ascii")
    except UnicodeEncodeError as exc:  # pragma: no cover - quote always ASCII
        raise ProtocolError(f"{what} could not be encoded into an ASCII URL") from exc
    return encoded


def by_request_url(endpoint: str, request_id: str) -> str:
    return f"{endpoint}/exports/by-request/{encode_identifier(request_id)}"


def job_url(endpoint: str, job_id: str) -> str:
    return f"{endpoint}/exports/{encode_identifier(job_id, 'job_id')}"


def create_url(endpoint: str) -> str:
    return f"{endpoint}/exports"


def path_of(endpoint: str, absolute_url: str) -> str:
    """Return the request-target path (with query) for an endpoint-relative URL."""
    prefix = endpoint
    if not absolute_url.startswith(prefix):
        raise ProtocolError(
            f"URL {absolute_url!r} is not under the endpoint {endpoint!r}"
        )
    target = absolute_url[len(prefix):]
    if not target.startswith("/"):
        raise ProtocolError(f"malformed endpoint-relative URL {absolute_url!r}")
    return target


def compose_same_origin_url(endpoint: str, path: str) -> str:
    """Join the endpoint origin with a manifest path validated same-origin."""
    scheme, hostname, port = origin_of(endpoint)
    if hostname == "::1":
        netloc = f"[{hostname}]:{port}"
    else:
        netloc = f"{hostname}:{port}"
    return f"{scheme}://{netloc}{path}"


# ---------------------------------------------------------------------------
# Strict JSON
# ---------------------------------------------------------------------------


def _reject_duplicates(pairs):
    """``object_pairs_hook``: duplicate keys are a protocol violation."""
    seen = set()
    result = {}
    for key, value in pairs:
        if key in seen:
            raise ProtocolError(f"JSON object contains duplicate key {key!r}")
        seen.add(key)
        result[key] = value
    return result


def parse_strict_json(raw: bytes, expected_fields: set[str], what: str) -> dict:
    """Decode ``raw`` as strict UTF-8 JSON with an exact top-level shape."""
    if not isinstance(raw, (bytes, bytearray)):
        raise ProtocolError(f"{what}: response body must be bytes")
    try:
        text = bytes(raw).decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ProtocolError(f"{what}: response body is not strict UTF-8") from exc
    try:
        payload = json.loads(text, object_pairs_hook=_reject_duplicates)
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"{what}: response is not valid JSON: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise ProtocolError(f"{what}: top-level JSON value must be an object")
    if set(payload) != expected_fields:
        raise ProtocolError(
            f"{what}: fields {sorted(payload)!r} do not match expected "
            f"{sorted(expected_fields)!r}"
        )
    # bool is a subclass of int; the protocol never accepts a boolean here.
    if type(payload.get("schema_version")) is not int:
        raise ProtocolError(
            f"{what}: schema_version must be an integer 1, "
            f"got {payload.get('schema_version')!r}"
        )
    if payload["schema_version"] != SUPPORTED_SCHEMA_VERSION:
        raise ProtocolError(
            f"{what}: unsupported schema_version {payload['schema_version']!r}"
        )
    return payload


def _non_empty_text(value, what: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ProtocolError(f"{what} must be a non-empty string without surrounding whitespace")
    return value


def parse_identity(raw: bytes, what: str) -> dict:
    """Validate a 200/201 identity body: schema_version, request_id, job_id."""
    payload = parse_strict_json(raw, _IDENTITY_FIELDS, what)
    payload["request_id"] = _non_empty_text(payload["request_id"], f"{what}.request_id")
    payload["job_id"] = _non_empty_text(payload["job_id"], f"{what}.job_id")
    return payload


def parse_job(raw: bytes) -> dict:
    """Validate a full job-status body including the frozen ready manifest."""
    payload = parse_strict_json(raw, _JOB_FIELDS, "job status")
    payload["request_id"] = _non_empty_text(payload["request_id"], "job.request_id")
    payload["job_id"] = _non_empty_text(payload["job_id"], "job.job_id")
    state = payload["state"]
    if type(state) is not str or state not in _STATES:
        raise ProtocolError(f"job.state must be one of {sorted(_STATES)}, got {state!r}")
    files = payload["files"]
    if type(files) is not list:
        raise ProtocolError("job.files must be an array")
    error = payload["error"]
    snapshot_id = payload["snapshot_id"]
    if state == "pending":
        if snapshot_id is not None or error is not None or files != []:
            raise ProtocolError("pending job must have null snapshot_id/error and empty files")
    elif state == "failed":
        if snapshot_id is not None or files != []:
            raise ProtocolError("failed job must have null snapshot_id and empty files")
        _non_empty_text(error, "job.error")
    else:  # ready
        if error is not None:
            raise ProtocolError("ready job must have error=null")
        payload["snapshot_id"] = _non_empty_text(snapshot_id, "job.snapshot_id")
        payload["files"] = validate_manifest(files)
    return payload


def validate_manifest(files: list) -> list[dict]:
    """Validate the four-role frozen manifest and return it sorted by role."""
    roles = []
    seen_urls = set()
    for index, item in enumerate(files):
        what = f"job.files[{index}]"
        if type(item) is not dict or set(item) != _FILE_FIELDS:
            raise ProtocolError(f"{what} must have exactly fields {sorted(_FILE_FIELDS)!r}")
        role = item["role"]
        if type(role) is not str or role not in ROLES:
            raise ProtocolError(f"{what}.role must be one of {ROLES}, got {role!r}")
        digest = item["sha256"]
        if type(digest) is not str or len(digest) != 64 or set(digest) - _HEX64:
            raise ProtocolError(f"{what}.sha256 must be 64 lowercase hex characters")
        length = item["byte_length"]
        if type(length) is not int or type(length) is bool or not 0 <= length <= MAX_CSV_BYTES:
            raise ProtocolError(
                f"{what}.byte_length must be an integer in [0, {MAX_CSV_BYTES}]"
            )
        url = item["url"]
        validate_manifest_path(url, what)
        if url in seen_urls:
            raise ProtocolError(f"{what}.url duplicates another manifest entry")
        seen_urls.add(url)
        roles.append(role)
    if sorted(roles) != sorted(ROLES) or len(roles) != len(set(roles)):
        raise ProtocolError(
            "ready manifest must cover exactly the four distinct roles "
            f"{ROLES}, got {sorted(roles)!r}"
        )
    return sorted(files, key=lambda item: item["role"])


def validate_manifest_path(url: str, what: str) -> None:
    """A manifest URL is a same-origin absolute path, never another origin."""
    if type(url) is not str or not url:
        raise ProtocolError(f"{what}.url must be a non-empty string")
    if url.startswith("//"):
        raise ProtocolError(f"{what}.url must not be scheme-relative (//)")
    if "\\" in url:
        raise ProtocolError(f"{what}.url must not contain backslashes")
    try:
        parts = urlsplit(url)
    except ValueError as exc:
        raise ProtocolError(f"{what}.url is not a valid URL path") from exc
    if parts.scheme != "" or parts.netloc != "":
        raise ProtocolError(f"{what}.url must stay same-origin and contain no host")
    if parts.fragment != "":
        raise ProtocolError(f"{what}.url must not contain a fragment")
    if not parts.path.startswith("/"):
        raise ProtocolError(f"{what}.url must be an absolute path starting with a single /")
    if "@" in parts.path:
        raise ProtocolError(f"{what}.url must not contain credentials")
    try:
        url.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ProtocolError(f"{what}.url must be an ASCII request target") from exc


# ---------------------------------------------------------------------------
# Bounded HTTP exchange
# ---------------------------------------------------------------------------


def _connect(endpoint: str) -> http.client.HTTPConnection:
    scheme, hostname, port = origin_of(endpoint)
    if scheme != "http":  # validated already, but keep the client explicit
        raise EndpointError("only http loopback endpoints are supported")
    return http.client.HTTPConnection(hostname, port, timeout=REQUEST_TIMEOUT_SECONDS)


def read_bounded(response: http.client.HTTPResponse, limit: int, what: str) -> bytes:
    """Read at most ``limit`` bytes and check the complete length.

    The limit is enforced on the bytes actually read: one byte beyond
    ``limit`` fails the exchange instead of being silently truncated.  When a
    ``Content-Length`` header is present it must match the complete body
    exactly; short reads and framing mismatches raise :class:`ProtocolError`.
    """
    declared = response.getheader("Content-Length")
    declared_length = None
    if declared is not None:
        if not declared.isascii() or not declared.isdigit():
            raise ProtocolError(f"{what}: invalid Content-Length header {declared!r}")
        declared_length = int(declared)
        if declared_length > limit:
            raise ProtocolError(
                f"{what}: Content-Length {declared_length} exceeds the {limit} byte limit"
            )
    chunks = []
    total = 0
    while True:
        chunk = response.read(65536)
        if not chunk:
            break
        total += len(chunk)
        if total > limit:
            raise ProtocolError(f"{what}: body exceeds the {limit} byte read limit")
        chunks.append(chunk)
    raw = b"".join(chunks)
    if declared_length is not None and declared_length != len(raw):
        raise ProtocolError(
            f"{what}: Content-Length declared {declared_length} but received {len(raw)} bytes"
        )
    return raw


def _check_encoding(response: http.client.HTTPResponse, what: str) -> None:
    encoding = response.getheader("Content-Encoding")
    if encoding is not None and encoding.lower() != "identity":
        raise ProtocolError(
            f"{what}: only explicit identity Content-Encoding is accepted, "
            f"got {encoding!r}"
        )


def request(endpoint: str, method: str, url_path: str, body: bytes | None = None,
            limit: int = MAX_JSON_BYTES, on_sent=None) -> tuple[int, bytes]:
    """Perform one bounded, no-redirect HTTP exchange.

    Returns ``(status, raw_body)``.  Redirects are refused explicitly rather
    than followed, so a manifest or job URL can never lead to another origin.
    Network errors propagate as :class:`ProtocolError`; the caller records
    them and decides what local state transition is legal.
    """
    what = f"{method} {url_path}"
    headers = {"Accept": "application/json", "Host": host_header(endpoint)}
    if body is not None:
        headers["Content-Type"] = "application/json; charset=utf-8"
        headers["Content-Length"] = str(len(body))
        headers["Content-Encoding"] = "identity"
    conn = _connect(endpoint)
    try:
        try:
            conn.request(method, url_path, body=body, headers=headers)
            if on_sent is not None:
                on_sent()
            response = conn.getresponse()
        except (OSError, http.client.HTTPException) as exc:
            raise ProtocolError(f"{what}: network error: {exc}") from exc
        if 300 <= response.status < 400:
            raise ProtocolError(f"{what}: redirects are not allowed (status {response.status})")
        _check_encoding(response, what)
        try:
            raw = read_bounded(response, limit, what)
        except ProtocolError:
            raise
        except (OSError, http.client.HTTPException) as exc:
            raise ProtocolError(f"{what}: network error while reading body: {exc}") from exc
        return response.status, raw
    finally:
        conn.close()


def create_body(request_id: str) -> bytes:
    """Serialize the exact creation body with stable key order."""
    payload = {
        "schema_version": SUPPORTED_SCHEMA_VERSION,
        "request_id": request_id,
        "mode": MODE_INHERITED,
    }
    return json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
