# @0k/kal-sse-client

Plain TypeScript client for the `kal-sse` snapshot + server-owned cursor
contract. The package owns EventSource lifecycle, bounded reconnect backoff,
snapshot registration, and gap-free re-snapshot recovery. Hosts own endpoint
paths, URL resolution, payload parsing, and application state.

## Rules

- Raw `new EventSource(...)` is allowed only in this package.
- Never expose or send a client-owned cursor index; streams carry only `sub`.
- Re-snapshot before every reconnect and preserve the last applied snapshot
  while recovery is pending.
- Keep the package framework-agnostic and compatible with Chrome 91.
- Write specification tests first and run `npm run check` before delivery.
- Deliver consumers with `npm run vendor`; never publish.
