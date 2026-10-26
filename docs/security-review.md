# Security and bug review: swpt_debtors

| | |
|---|---|
| Reviewed commit | `a1edf43` (2026-10-15) |
| Review date | 2026-10-26 |
| Scope | `swpt_debtors/` (all modules), `docker/` configs, `pyproject.toml` |
| Method | Manual code review; every finding reproduced against PostgreSQL 16 with the project's test configuration |

This document covers the issues **fixed in this pull request**, plus
general hardening notes. Additional findings that are not fixed here
have been reported to the maintainers privately.

## Summary

| ID | Severity | Finding | Status |
|---|---|---|---|
| SEC-1 | High | Stored XSS through public documents served as HTML | Fixed |
| BUG-1 | Medium | Document size limit silently capped at 20 KB | Fixed |
| BUG-2 | Medium | Oversized chunked upload is truncated and stored | Fixed |
| BUG-3 | Medium | Chunked upload stores an empty document | Fixed |
| BUG-4 | Low | Upload without `Content-Length` crashes with a 500 | Fixed |
| DEP-1 | Low | Dependency advisories; `pytest` shipped as a runtime dependency | Not fixed (hardening note) |

## SEC-1: Stored XSS through public documents (High, fixed)

**Where:** `swpt_debtors/routes.py`, `SaveDocumentEndpoint.post` and
`DocumentEndpoint.get`.

**Problem.** Any active debtor can save a document. The uploader
chooses the `Content-Type`, and if none is sent the server uses
`text/html; charset=utf-8`. The document is then served on the public,
unauthenticated URL `/debtors/{id}/documents/{docId}/public`, with
that content type, a one-year cache lifetime, and no
`X-Content-Type-Options`, `Content-Security-Policy` or
`Content-Disposition` header.

A browser that opens the link renders the HTML and runs any scripts in
it as code from the API's origin. In the reference deployment
(`docker/nginx/templates`), `/debtors/`, `/debtors-webapp/`,
`/debtors-login/` and `/debtors-consent/` share one origin, so the
script runs same-origin with the issuer web app and the OAuth login and
consent pages.

**Reproduction.**

```
POST /debtors/4294967307/documents/      (no Content-Type)
body: <script>alert(document.domain)</script>     → 201
GET  /debtors/4294967307/documents/0/public       → 200
  Content-Type: text/html; charset=utf-8
  Cache-Control: max-age=31536000
  (no nosniff, no CSP, no Content-Disposition)
```

**Fix.** Every document response (the public `GET` and the `POST`
echo) now carries:

```
X-Content-Type-Options: nosniff
Content-Security-Policy: sandbox; default-src 'none'
```

- `sandbox` puts the document in a unique opaque origin with scripts,
  forms and popups disabled.
- `default-src 'none'` blocks every resource load, so the page cannot
  leak data through image requests and cannot be styled to imitate the
  real site.
- `nosniff` stops browsers from rendering a document as a type other
  than the declared one.

HTML, SVG, `text/xml`, `application/xml`, `text/xsl` and every `*+xml`
type are also sent with `Content-Disposition: attachment`, so browsers
download them instead of displaying them. Other types (PDF, images,
plain text) still display, inside the sandbox. Storage of arbitrary
documents is unchanged.

**Residual risk.** A sandboxed page can still show plain text and
ordinary links on the trusted domain. Serving user documents from a
separate, cookieless origin removes this completely.

**Deployment note.** Documents already cached by browsers or proxies
keep their old headers until the one-year cache lifetime expires.
Purge CDN or proxy caches after deploying.

## Document upload bugs (fixed)

All four are in `SaveDocumentEndpoint.post`.

### BUG-1: Document size limit silently capped at 20 KB (Medium)

Commit `73454a3` (2026-01-19, "Limit Flask content lengths") set the
app-wide `MAX_CONTENT_LENGTH = 20480`. `APP_DOCUMENT_MAX_CONTENT_LENGTH`
defaults to 50000, but Flask rejects the body before that check runs,
so every document between 20 KB and 50 KB gets a 413. This affects every
deployment.

### BUG-2: Oversized chunked upload is truncated and stored (Medium)

Gunicorn sets `wsgi.input_terminated`. For a chunked body, Werkzeug then
caps the read at `max_content_length` but raises only on a further read,
which `get_data()` never makes. The first N bytes were stored as a
corrupted document with a 201, using up the yearly quota.

```
chunked, 101 bytes, limit 100, input_terminated → 201, stored 100 bytes
```

### BUG-3: Chunked upload stores an empty document (Medium)

When the WSGI server does not terminate the input stream and there is
no `Content-Length`, Werkzeug safely returns an empty body. A zero-byte
document was stored with a 201, using up quota.

```
chunked, 101 bytes, no input_terminated → 201, stored 0 bytes
```

### BUG-4: Upload without Content-Length crashes with a 500 (Low)

`request.content_length > limit` compared `None` with `int`:

```
TypeError: '>' not supported between instances of 'NoneType' and 'int'
```

### Fix

The endpoint now:

1. returns 411 when there is neither a `Content-Length` nor a terminated
   input stream;
2. sets `request.max_content_length` to the document limit plus one
   byte, so the app-wide limit no longer applies and oversized streams
   can be detected;
3. checks the length of the body actually read, returning 413 when it
   exceeds the limit.

nginx's default request buffering converts chunked uploads to requests
with a `Content-Length`, which hides BUG-2 to BUG-4 in the reference
deployment.

## DEP-1: Dependency hardening notes (Low, not fixed)

`pip-audit` on the locked environment reports advisories in:

- runtime: `werkzeug 3.1.6` (Windows-only `safe_join` issue, fixed in
  3.1.9) and `mako 1.3.10` (Windows path traversal, fixed in 1.3.12,
  pulled in by alembic). Neither code path is used on the Linux image;
- development only: `black`, `pytest`, `python-dotenv`, `pygments`, and
  `ujson` (through `python-lsp-server`).

`pytest` and `pytest-mock` are main dependencies, so they ship in the
production image. Suggested: bump `werkzeug` and `mako`, move `pytest`
and `pytest-mock` to the dev group, and run `pip-audit` in CI.
`swpt_pythonlib` is installed from a git tag and was not audited.

## Checked and ruled out

- **Duplicate-UUID race in `initiate_running_transfer`.** The
  check-then-insert is wrapped in `retry_on_integrity_error`, which
  reruns the whole transaction.
- **Empty `APP_SUPERUSER_SUBJECT_REGEX` default.** `create_app` replaces
  it with an anchored, escaped `^debtors-superuser$`.
- **`GET /debtors/{id}/` returning 403 for a missing debtor.**
  Intentional (asserted by tests); prevents debtor-ID enumeration.
- **Deactivated debtors are never garbage-collected.** Intentional since
  `67020d4` ("Do not delete dead debtors").
- **Transfer state machine.** Prepared, rejected and finalized signal
  matching, cancel-after-settle, idempotent finalization, and
  account-update ordering by `(creation_date, last_change_ts, seqnum)`
  are correct.
- **Recipient check.** `parse_account_uri` requires the recipient's
  debtor ID to match the path.

## Tests

- `test_document_security_headers` (9 content types, upload reply and
  public `GET`)
- `test_save_document_without_content_length`
- `test_save_document_bigger_than_flask_limit`

Each new test fails without its fix. Full suite: 67 passed, 3 skipped;
`flake8` clean.
