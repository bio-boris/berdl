"""hub_client.py – Python client for the KBase MCP Hub API.

This module is auto-generated at runtime from the OpenAPI spec served by the
Hub API itself.  It supports two calling conventions:

Nested namespace access (mirrors the API URL path)::

    b = hub_client(token="MY_TOKEN", url="https://hub.berdl.kbase.us/apis/mcp")

    b.delta.databases.list()
    b.delta.databases.tables.list(database="mydb")
    b.delta.databases.tables.schema(database="mydb", table="mytable")
    b.delta.tables.query.async_.submit(query="SELECT * FROM mydb.mytable LIMIT 10")
    b.delta.tables.query.async_.status(job_id="abc123")
    b.delta.tables.query.async_.results(job_id="abc123")
    b.delta.tables.query.async_.jobs()

Flat method access (all path segments joined with underscores)::

    b.delta_databases_list()
    b.delta_tables_query_async_submit(query="SELECT 1")
    b.delta_tables_query_async_status(job_id="abc123")

Call ``b.help()`` to print every available method and the underlying
HTTP verb + path it maps to.
"""

from __future__ import annotations

import json
import re

import requests

# Python keywords / builtins that clash with attribute names
_RESERVED: frozenset[str] = frozenset(
    {
        "async", "await", "class", "def", "del", "elif", "else",
        "except", "finally", "for", "from", "global", "if", "import",
        "in", "is", "lambda", "nonlocal", "not", "or", "and", "pass",
        "raise", "return", "try", "while", "with", "yield",
    }
)


def _sanitize(name: str) -> str:
    """Make a path segment safe to use as a Python attribute name.

    Appends an underscore when the segment is a reserved keyword so that it
    can be accessed as ``b.async_`` rather than the invalid ``b.async``.
    """
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", name)
    if safe in _RESERVED:
        safe = safe + "_"
    return safe


def _flat_seg(name: str) -> str:
    """Sanitize a path segment for use in a flat (joined) method name.

    Unlike ``_sanitize``, this does *not* append an underscore for reserved
    keywords because the word only forms part of a longer identifier
    (e.g. ``delta_tables_query_async_jobs``), where it is not a keyword.
    """
    return re.sub(r"[^a-zA-Z0-9_]", "_", name)


# ---------------------------------------------------------------------------
# _Endpoint
# ---------------------------------------------------------------------------

class _Endpoint:
    """A single callable API endpoint produced from one OpenAPI operation."""

    def __init__(
        self,
        client: "hub_client",
        http_method: str,
        path_template: str,
        operation: dict,
    ) -> None:
        self._client = client
        self._method = http_method.lower()
        self._path = path_template
        self._op = operation
        # path-parameter names, e.g. {'job_id'} for /async/{job_id}/status
        self._path_params: frozenset[str] = frozenset(
            seg[1:-1]
            for seg in path_template.split("/")
            if seg.startswith("{") and seg.endswith("}")
        )
        self.__doc__ = self._make_doc()

    # ------------------------------------------------------------------

    def _make_doc(self) -> str:
        op = self._op
        parts: list[str] = []
        if op.get("summary"):
            parts.append(op["summary"])
        if op.get("description"):
            # Use only the first paragraph to keep docstrings short
            first_para = op["description"].split("\n\n")[0].strip()
            if first_para != op.get("summary", "").strip():
                parts.append("\n" + first_para)
        spec_params = op.get("parameters", [])
        if spec_params or self._path_params:
            parts.append("\nParameters")
            parts.append("----------")
        for p in spec_params:
            req = " (required)" if p.get("required") else ""
            desc = p.get("description", "")
            parts.append(f"  {p['name']}{req}: {desc}")
        for pp in sorted(self._path_params):
            if pp not in {p["name"] for p in spec_params}:
                parts.append(f"  {pp} (required, path parameter)")
        parts.append(f"\nHTTP: {self._method.upper()} {self._path}")
        return "\n".join(parts)

    # ------------------------------------------------------------------

    def __call__(self, **kwargs):  # noqa: ANN204
        """Call this endpoint.  Supply path params and body fields as kwargs."""
        path = self._path

        # 1. Extract and substitute path parameters
        for pp in self._path_params:
            if pp not in kwargs:
                raise TypeError(
                    f"Missing required path parameter '{pp}' for {self._path!r}"
                )
            path = path.replace("{" + pp + "}", str(kwargs.pop(pp)))

        url = self._client.base_url.rstrip("/") + path
        headers = self._client._headers()

        # 2. Identify named query parameters from the OpenAPI spec
        query_param_names: set[str] = {
            p["name"]
            for p in self._op.get("parameters", [])
            if p.get("in") == "query"
        }

        # 3. Route remaining kwargs to query string or request body
        explicit_body = kwargs.pop("body", None)
        if explicit_body is not None and not isinstance(explicit_body, dict):
            raise TypeError(
                f"'body' must be a dict, got {type(explicit_body).__name__}"
            )
        query: dict = {}
        body_fields: dict = {}

        for k, v in kwargs.items():
            if k in query_param_names:
                query[k] = v
            else:
                body_fields[k] = v

        if self._method in ("post", "put", "patch"):
            # Merge body_fields into the request body
            if explicit_body is None:
                body: object = body_fields if body_fields else {}
            else:
                body = dict(explicit_body, **body_fields)
        else:
            # GET / DELETE: extra kwargs become query params
            body = None
            query.update(body_fields)

        req_kwargs: dict = dict(
            headers=headers,
            params=query or None,
            timeout=30,
        )
        if body is not None:
            req_kwargs["json"] = body

        fn = getattr(requests, self._method)
        resp = fn(url, **req_kwargs)
        resp.raise_for_status()
        try:
            return resp.json()
        except (json.JSONDecodeError, ValueError):
            return resp.text

    def __repr__(self) -> str:
        return f"<Endpoint {self._method.upper()} {self._path}>"


# ---------------------------------------------------------------------------
# _Namespace
# ---------------------------------------------------------------------------

class _Namespace:
    """A namespace node that can have child namespaces AND optionally be callable.

    This dual role allows a single node to represent both a callable endpoint
    (e.g. ``b.delta.tables.query()``) and a parent namespace
    (e.g. ``b.delta.tables.query.async_.submit()``).
    """

    def __init__(self, name: str = "") -> None:
        object.__setattr__(self, "_name", name)
        object.__setattr__(self, "_children", {})
        object.__setattr__(self, "_endpoint", None)

    # --- internal helpers ---------------------------------------------------

    def _get_or_add_child(self, name: str) -> "_Namespace":
        children = object.__getattribute__(self, "_children")
        if name not in children:
            children[name] = _Namespace(name)
        return children[name]

    def _set_endpoint(self, endpoint: _Endpoint) -> None:
        object.__setattr__(self, "_endpoint", endpoint)

    # --- Python protocol ----------------------------------------------------

    def __call__(self, **kwargs):  # noqa: ANN204
        ep = object.__getattribute__(self, "_endpoint")
        if ep is None:
            name = object.__getattribute__(self, "_name")
            raise TypeError(
                f"Namespace '{name}' is not callable (no endpoint registered here)"
            )
        return ep(**kwargs)

    def __getattr__(self, name: str):  # noqa: ANN204
        if name.startswith("_"):
            raise AttributeError(name)
        children = object.__getattribute__(self, "_children")
        if name in children:
            return children[name]
        ns_name = object.__getattribute__(self, "_name")
        raise AttributeError(f"Namespace '{ns_name}' has no attribute '{name}'")

    def __dir__(self) -> list[str]:
        children = object.__getattribute__(self, "_children")
        ep = object.__getattribute__(self, "_endpoint")
        base = list(children.keys())
        if ep is not None:
            base.append("__call__")
        return base

    def __repr__(self) -> str:
        children = object.__getattribute__(self, "_children")
        ep = object.__getattribute__(self, "_endpoint")
        name = object.__getattribute__(self, "_name")
        callable_tag = " (callable)" if ep is not None else ""
        return f"<Namespace '{name}'{callable_tag} children={list(children.keys())}>"


# ---------------------------------------------------------------------------
# hub_client
# ---------------------------------------------------------------------------

class hub_client:
    """Python client for the KBase MCP Hub API.

    The client fetches the live OpenAPI spec from the server on construction
    and auto-generates methods for every operation.

    Parameters
    ----------
    token:
        KBase authentication token.
    url:
        Base URL of the MCP API.  Defaults to the public Hub endpoint.

    Examples
    --------
    ::

        from hub_client import hub_client

        b = hub_client(token="MY_TOKEN")

        # --- Nested namespace access (mirrors the API path) ---
        b.delta.databases.list()
        b.delta.databases.tables.list(database="mydb")
        b.delta.databases.tables.schema(database="mydb", table="mytable")
        b.delta.databases.structure(with_schema=True)
        b.delta.tables.count(database="mydb", table="mytable")
        b.delta.tables.sample(database="mydb", table="mytable", n=5)
        b.delta.tables.query.async_.submit(query="SELECT * FROM mydb.mytable LIMIT 10")
        b.delta.tables.query.async_.status(job_id="abc123")
        b.delta.tables.query.async_.results(job_id="abc123")
        b.delta.tables.query.async_.jobs()
        b.health()

        # --- Flat method access ---
        b.delta_databases_list()
        b.delta_tables_query_async_submit(query="SELECT 1")
        b.delta_tables_query_async_status(job_id="abc123")

        # --- Discovery ---
        b.help()
    """

    DEFAULT_URL: str = "https://hub.berdl.kbase.us/apis/mcp"

    def __init__(self, token: str, url: str = DEFAULT_URL) -> None:
        self.token = token
        self.base_url = url.rstrip("/")
        # These must be set before _load_and_build so __getattr__ doesn't recurse
        self.__dict__["_root"] = _Namespace("root")
        self.__dict__["_flat"] = {}
        self._load_and_build()

    # --- internal -----------------------------------------------------------

    def _headers(self) -> dict:
        h: dict = {"Accept": "application/json", "Content-Type": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def _load_spec(self) -> dict:
        import os
        local_path = "openapi.json"
        if os.path.exists(local_path):
            with open(local_path) as fh:
                return json.load(fh)
        resp = requests.get(
            self.base_url + "/openapi.json",
            headers={"Accept": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def _load_and_build(self) -> None:
        try:
            spec = self._load_spec()
        except Exception as exc:
            print(f"Warning: could not load OpenAPI spec ({type(exc).__name__}: {exc})")
            print("Client created without auto-generated methods. Check the URL and your network connection.")
            return
        self.__dict__["_spec"] = spec
        for path_template, path_item in spec.get("paths", {}).items():
            for http_method, operation in path_item.items():
                if http_method.startswith("x-") or not isinstance(operation, dict):
                    continue
                ep = _Endpoint(self, http_method, path_template, operation)
                self._register(path_template, ep)

    def _register(self, path_template: str, ep: _Endpoint) -> None:
        """Register one endpoint in both the namespace tree and the flat dict."""
        raw_segs = [s for s in path_template.strip("/").split("/") if s]
        # Build Python-safe names; drop {param} segments from namespace path
        py_segs = [
            _sanitize(s)
            for s in raw_segs
            if not (s.startswith("{") and s.endswith("}"))
        ]
        if not py_segs:
            return

        # --- namespace tree: navigate to the last segment node ---
        node: _Namespace = self.__dict__["_root"]
        for seg in py_segs:
            node = node._get_or_add_child(seg)
        node._set_endpoint(ep)

        # --- flat method: join all segments with underscores (no keyword suffix) ---
        flat_segs = [
            _flat_seg(s)
            for s in raw_segs
            if not (s.startswith("{") and s.endswith("}"))
        ]
        flat_name = "_".join(flat_segs)
        self.__dict__["_flat"][flat_name] = ep

    # --- Python protocol ----------------------------------------------------

    def __getattr__(self, name: str):  # noqa: ANN204
        if name.startswith("_"):
            raise AttributeError(name)
        flat: dict = self.__dict__.get("_flat", {})
        if name in flat:
            return flat[name]
        root: _Namespace = self.__dict__.get("_root", _Namespace())
        children: dict = object.__getattribute__(root, "_children")
        if name in children:
            return children[name]
        raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")

    def __dir__(self) -> list[str]:
        flat: dict = self.__dict__.get("_flat", {})
        root: _Namespace = self.__dict__.get("_root", _Namespace())
        children: dict = object.__getattribute__(root, "_children")
        return sorted(
            set(list(flat.keys()) + list(children.keys()) + list(self.__dict__.keys()))
        )

    def __repr__(self) -> str:
        return f"<hub_client url='{self.base_url}'>"

    # --- public helpers -----------------------------------------------------

    def help(self) -> None:
        """Print a summary of every available method."""
        flat: dict = self.__dict__.get("_flat", {})
        root: _Namespace = self.__dict__.get("_root", _Namespace())
        print(f"hub_client  →  {self.base_url}\n")
        print("Namespace access  (b.<path.to.endpoint>(**kwargs)):")
        self._print_tree(root, prefix="b")
        print(f"\nFlat methods  ({len(flat)}):")
        for name in sorted(flat.keys()):
            ep = flat[name]
            print(f"  b.{name}(**kwargs)    #  {ep._method.upper()} {ep._path}")

    def _print_tree(self, node: _Namespace, prefix: str) -> None:
        ep = object.__getattribute__(node, "_endpoint")
        children: dict = object.__getattribute__(node, "_children")
        if ep is not None:
            print(f"  {prefix}(**kwargs)    #  {ep._method.upper()} {ep._path}")
        for child_name, child in sorted(children.items()):
            self._print_tree(child, f"{prefix}.{child_name}")
