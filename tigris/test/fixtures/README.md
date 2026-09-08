# Nippy compatibility fixtures

These files were frozen once with Nippy **3.9.0** using
`simplemono.event-store.tigris.codec/encode` (regular header, automatic
compression, metadata enabled, no outer gzip).

| File | Expected value in `codec_test.clj` | Compression |
| --- | --- | --- |
| `nippy-3.9.0-small.bin` | `fixture-event` | None selected by auto |
| `nippy-3.9.0-large.bin` | `large-fixture-event` | LZ4 selected by auto |

The values exercise plain collections, metadata, numeric types, UUIDs, Date and
Instant timestamps, Unicode, and byte arrays. The larger fixture adds a repetitive
payload to exercise automatic decompression.

**Do not regenerate these files when upgrading Nippy.** Tests must demonstrate
that the new reader can read the old bytes. Add fixtures for new versions or
newly supported values instead. Tests compare decoded values, not the output of
a new freeze against these bytes: Nippy does not promise stable byte output.

Run from the module directory:

```sh
cd tigris && clojure -M:test
```
