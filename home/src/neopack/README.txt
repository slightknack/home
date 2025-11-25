NEOPACK - Binary Serialization Format

A zero-panic, streaming binary serialization format with bounded memory usage.
Designed for untrusted input parsing without risk of memory exhaustion.

WIRE FORMAT

All multi-byte integers use little-endian encoding.
Sizes are u16 (max 65535 bytes) to prevent memory exhaustion attacks.

SCALAR TYPES

  Bool:   [0x01][value:u8]              value: 0=false, 1=true
  S8:     [0x02][value:i8]
  U8:     [0x03][value:u8]
  S16:    [0x04][value:i16]
  U16:    [0x05][value:u16]
  S32:    [0x06][value:i32]
  U32:    [0x07][value:u32]
  S64:    [0x08][value:i64]
  U64:    [0x09][value:u64]
  F32:    [0x0A][value:f32]
  F64:    [0x0B][value:f64]

BLOB TYPES

  String: [0x10][len:u16][utf8_bytes]
  Bytes:  [0x11][len:u16][raw_bytes]
  Struct: [0x12][len:u16][raw_bytes]   opaque structured data

CONTAINER TYPES

  List:   [0x20][count:u16][value]*
          Heterogeneous list. Each value is a full encoded value with tag.

  Map:    [0x21][count:u16][entry]*
          Each entry is: [0x10][key_len:u16][key_utf8][value]
          Keys must be strings. Values can be any type.

  Array:  [0x23][item_tag:u8][stride:u16][count:u16][raw_bytes]
          Homogeneous array of fixed-stride items.
          Raw bytes contains (stride * count) bytes.
          No per-item tags - all items have same type.

STREAMING DECODER

The decoder returns Error::Pending(n) when it needs n more bytes.
This allows incremental parsing of data streams without buffering entire messages.

Example:
  let mut reader = Reader::new(partial_data);
  match reader.u32() {
      Ok(val) => { /* got value */ }
      Err(Error::Pending(n)) => { /* need n more bytes */ }
      Err(e) => { /* handle error */ }
  }

ERROR TYPES

  Pending(usize)   - Need more bytes (streaming)
  InvalidTag(u8)   - Unknown type tag
  InvalidUtf8      - String contains invalid UTF-8
  TypeMismatch     - Expected different type
  Malformed        - Invalid structure (e.g. array size overflow)

ENCODER USAGE

  let mut enc = Encoder::new();

  // Scalars chain
  enc.bool(true).u32(42).str("hello");

  // Lists use scopes
  {
      let mut list = enc.list();
      list.u32(1).u32(2).u32(3);
  }  // Count patched on drop

  // Maps use key-value API
  {
      let mut map = enc.map();
      map.key("name").str("Alice");
      map.key("age").u32(30);
  }

  // Arrays for fixed-stride data
  {
      let mut arr = enc.array(Tag::U32, 4);
      arr.push(&[1, 0, 0, 0]);  // Raw bytes for u32
      arr.push(&[2, 0, 0, 0]);
  }

  let bytes = enc.into_bytes();

DECODER USAGE

  let mut r = Reader::new(&bytes);

  // Scalars
  let b = r.bool()?;
  let n = r.u32()?;
  let s = r.str()?;

  // Generic value reader
  let val = r.value()?;
  match val {
      ValueReader::U32(n) => { /* ... */ }
      ValueReader::String(s) => { /* ... */ }
      _ => { /* ... */ }
  }

  // Lists
  let mut list = r.list()?;
  while let Some(val) = list.next()? {
      // Process val
  }

  // Maps
  let mut map = r.map()?;
  while let Some((key, val)) = map.next()? {
      // Process key, val
  }

  // Arrays
  let mut arr = r.array()?;
  assert_eq!(arr.item_tag(), Tag::U32);
  while let Some(chunk) = arr.next()? {
      // chunk is &[u8] with stride bytes
  }

DESIGN PROPERTIES

- Zero-panic: All errors returned as Result types
- Streaming: Decoder can handle partial data
- Bounded memory: u16 sizes prevent unbounded allocations
- Type-safe: State machines enforce correct encoding patterns
- Zero-copy: Decoder returns slices into input buffer
- Efficient: Minimal allocations, direct byte manipulation

CONSTRAINTS

- Max blob/container size: 65535 bytes
- Max array size: stride * count <= 65535
- Keys in maps must be valid UTF-8 strings
- Array items must exactly match declared stride

RUNTIME INVARIANT CHECKS

The encoder enforces these invariants with panics:
- Blob sizes (strings, bytes, structs) must be <= 65535 bytes
- Array stride must be > 0 and <= 65535
- Array items must match declared stride exactly (checked per push)
- Container counts (list, map, array) must not exceed 65535 items
- MapValueEncoder must be consumed (enforced by borrow checker + #[must_use])

The decoder validates:
- Array total size (stride * count) must not overflow
- All tag bytes must be valid
- String data must be valid UTF-8
- Type tags must match expected types

The format is designed for scenarios where you need to parse untrusted input
incrementally without risk of OOM, DoS, or panics.
