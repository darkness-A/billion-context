# OpenCode V2 Responses WebSocket integration (#1844)

## Approved scope

The user authorized implementing the previously discussed two-ended WebSocket
ACP integration on 2026-10-01. OpenCode V2 connects to bili over WebSocket;
bili connects to the selected Responses upstream over WebSocket. This includes
the OpenAI API and ChatGPT OAuth/Codex endpoint. It does not implement Realtime,
generic opaque WebSocket passthrough, or change the user's OpenCode configuration.

## Decision points

- The V2 handshake hook routes supported Responses sockets through the existing
  explicit-protocol tunnel, with plugin identity and model metadata. Other
  protocols, disabled plugins, and older hosts retain their existing behavior.
- Only loopback, cooperative OpenCode Responses upgrades are admitted initially.
  Destination admission reuses the existing tunnel guard. Unclaimed upgrades
  still receive the existing immediate 426, independently of PR #1472.
- Responses frames enter the existing request pipeline through an in-process
  HTTP-shaped envelope, not a network HTTP hop. An async-scoped fetch transport
  executes model requests, retries, and preflight requests over upstream
  WebSocket. Existing ACP policy, tool execution, usage accounting, and prose
  filtering remain the single authority. Tool argument strings are never cleaned.
- Client and upstream continuation checkpoints are separate: the client sees
  its original history and visible output; the upstream sees the ACP-processed
  history. Incoming deltas are expanded before ACP. Outgoing deltas are used only
  when the processed input extends the upstream checkpoint exactly. A fold or
  other history change sends full processed input without previous_response_id.
  This starts a new response chain without requiring a new socket.
  Request-size logs marked `view=ws-expanded` describe reconstructed pipeline
  envelopes, not incremental WebSocket wire bytes.
- Only completed responses establish continuation checkpoints. Disconnect,
  cancellation, failed/incomplete responses, and unknown response IDs do not
  fabricate successful history. Checkpoints and queued payloads are bounded.
- No configuration field, environment variable, package version, persistence
  format, or acp-kernel version is added or changed. Socket checkpoints are
  connection-local; ACP persistence remains unchanged. The ws implementation is
  a bundled build-time dependency, not an external runtime requirement.

## Verification

Use random loopback ports and fake Responses WebSocket upstreams, with no real
credentials or model usage. Prove both directions stay on WebSocket, normal
continuation is incremental, a real ACP fold resets the upstream history,
tool argument payloads survive unchanged, usage reaches the existing session,
and unknown upgrades retain 426. Verify upstream proxy routing, errors,
cancellation, reconnect, checkpoint misses, and session isolation. Exercise a
real OpenCode V2 binary with an isolated configuration and successful fake
WebSocket upstream; an unavailable socket that merely falls back to HTTP is
not acceptable evidence. Run typecheck, the full unit suite, build, and the
required Responses client E2E before submitting a human-reviewed PR.
