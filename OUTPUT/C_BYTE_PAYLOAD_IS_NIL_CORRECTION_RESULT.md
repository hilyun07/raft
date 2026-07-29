# C Byte Payload `is_nil` Correction Result

The intermediate byte-wrapper flags were removed. Both byte types now use
`bool is_nil` solely to preserve Go nil-versus-empty shape:

```c
typedef struct raft_byte_view {
    const uint8_t *data;
    size_t len;
    bool is_nil;
} raft_byte_view_t;

typedef struct raft_bytes {
    uint8_t *data;
    size_t len;
    bool is_nil;
} raft_bytes_t;
```

Canonical nil is `{NULL, 0, true}`, canonical present-empty is
`{NULL, 0, false}`, and non-empty is `{non-NULL, positive length, false}`.
Dummy zero-length allocations and `is_nil` non-empty payloads are invalid.

`is_nil` is a presence bit, not an ownership tag. Ownership comes entirely
from the type:

- `raft_byte_view_t` is borrowed call-scoped input and is never freed or
  retained;
- `raft_bytes_t` is C-owned retained/output data and is always freed by
  `raft_bytes_free`.

The removed `RAFT_BYTES_NIL` and `RAFT_BYTES_OWNED` macros have no compatibility
aliases. The full aggregate view/owned design, recursive copies, cgo shim
rules, and test results are in `C_TYPE_SEPARATED_PAYLOAD_API_RESULT.md`.

