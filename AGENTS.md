# @0k/kal-sse-client

Plain TypeScript client for the `kal-sse` snapshot + server-owned cursor
contract. The package owns EventSource lifecycle, bounded reconnect backoff,
snapshot registration, and resume-first recovery. Hosts own endpoint
paths, URL resolution, payload parsing, and application state.

## Rules

- Raw `new EventSource(...)` is allowed only in this package.
- On reconnect, resume with the `last_event_id` of the last applied event;
  re-snapshot only when no event was applied since the last snapshot or the
  resume attempt fails before opening. Preserve the last applied snapshot
  while recovery is pending.
- Keep the package framework-agnostic and compatible with Chrome 91.
- Write specification tests first and run `npm run check` before delivery.
- Released to npm with `pkg` (the version comes from the git tag;
  `package.json` keeps `%%version%%`). Consumers depend on the published
  version (`"@0k/kal-sse-client": "^X.Y"`) and develop against this checkout
  with `npm link`, never committed. Never deliver it as a tarball.
- `README.md` is generated from `README.org` at release; never commit it.
