# IU-WS3-01: explicit lifecycle audio producer

The shared types and validators come from
`github.com/Kome-Lab/Autostream-Contracts/v3lifecycle` at
`d32ea3514b1c1010391caae0348b0ff9e751f447`. This slice adds an explicit
`audioforward.NewLifecycleClient`; ordinary v2 construction remains unchanged.
There is no environment switch, production qualification, permit issuance, or
automatic conversion to an older Evidence envelope.

The client implements Discord's existing `AudioForwarder` port. Explicit
injection through `SetAudioForwarder` connects its `ForwardOpus` method to the
existing bounded queue. A trusted `LifecycleFrameSource` provides the original
CC02 attempt. Both profiles, all limits, complete consumer identity, exact raw
and queue resources, queue admission reference, resource-bound job and
connection axis keys, and a trusted `QueueWorkIDs` mapping from the original
correlation to the expected child and parent IDs are required. Missing
configuration is refused. The mapping has no default and must not use the
response as its oracle; it is not an independently invented shared wire rule.

The internal synthetic transport is POST
`/v3/streams/{id}/audio/opus`, with the direct shared CC02 Record encoded as
base64url in `X-Autostream-Lifecycle` (32768 encoded bytes maximum). The body
uses the existing Opus packet representation. Raw payload, token, and user
payload are not placed in a shared Record, witness, or diagnostic.

A 202 response must contain a direct shared CC08 Record. The client compares
the original correlation, binding, scope, record reference, consumer process,
and each boundary's owner/resource. Known raw, durability, and queue admission
claims need valid typed details and matching observed source frames/revisions.
Only a full raw write, `SYNC_OBSERVED`, and the exact queue admission can set
`QueueAdmitted`. Queue admission does not assert queue terminal, codec, archive,
or provider completion. `ResponseValidation` retains the validator's assessment;
an invalid/unbound response after dispatch has an UNKNOWN operation result.

Each frame/attempt is sent at most once by a client instance. The retained
attempt set is bounded by the explicit profile limit; exhaustion refuses new
dispatches. No retry, reissue, expiry release, or HELD release is implemented.
ACK loss, malformed 2xx, and unbound error results require explicit `Inspect`
using the original CC02 Record. Inspection sends no Opus and prepares no new
attempt. Restarted client memory cannot prove remote non-execution: the ER
participant must retain and inspect the original tuple.

The transport is an opt-in conformance lane, not an adopted production signed
wire or a qualified Workload Identity/Provider. Default startup does not enable
it. Real Discord/VC/Opus, codec, remote sink, host independence, provider
qualification, and release acceptance remain outside this synthetic evidence.

Required roots are recorded separately by test-only CI. The source tests cover
typed local admission, incorrect 2xx bindings/source frames, ACK loss and same
attempt inspection, generation refusal without dispatch, explicit limits, and
duplicate dispatch refusal. The fixed `fixture-token`, synthetic packet bytes,
and fixture speaker strings are test values and have no real credential/user.
