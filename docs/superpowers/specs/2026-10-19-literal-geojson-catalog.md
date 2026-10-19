# Explicit literal GeoJSON catalog draft

The existing Fiona reader can raise `JSONDecodeError` on a genuine field containing both numbers and a literal empty string. This additive path catalogs complete pinned GeoJSON directly without converting its properties through Fiona, Pandas or Arrow. Source, CRS interpretation, schema, baseline, rights and release decisions remain external.

An authored source manifest explicitly opts in:

```json
{"literal_geojson_catalog":{"schema_version":1,"path":"geojson/native.geojson","bytes":123,"sha256":"<64 lowercase hexadecimal characters>"}}
```

The object must be the sole file under canonical `geojson/`, with no higher-precedence canonical source. Malformed/null/empty profiles, conflicting inherited profiles, unpinned extra objects, byte drift, metadata drift and aliases fail closed. The catalog resolves actual authored manifest pins rather than trusting a supplied replacement profile. Existing resolved metadata without the opt-in retains its ordinary reader. The bounded layer-read option is additive; legacy manifest reads retain their default behavior.

The parser accepts a strict RFC7946 FeatureCollection with `type`, `features` and optional 2D `bbox`; Features support `type`, `properties`, `geometry`, optional `id` and 2D `bbox`. Explicit legacy CRS and other unsupported root/Feature members are rejected. Omitted CRS is reported as the normative GeoJSON CRS84 interpretation, never producer authority or acceptance. No reprojection, geometry repair, source mapping or new terminal format is performed.

All rows contribute property presence, missing/null and exact JSON type counts; complete geometry/null/empty/invalid counts and bounds include rows beyond the 5,000-feature example window. Ordinary property names `geometry`, `wkb_geometry` and `ID` remain ordinary properties. Top-level `Feature.id` has a separate typed descriptor and full-population identity hash; no substitute IDs are generated. Null `properties` and an empty property object have distinct container evidence. Arrays/objects preserve nested JSON types. Integer IDs never pass through floating point. Fractional/exponent examples expose decoded binary64 plus recursively retained original numeric lexemes; the population hashes include these typed lexemes. Raw JSON bytes remain pinned and unchanged, so binary64 examples are not claimed to preserve every mathematical decimal value.

Dictionary fields use `type: json` with full native JSON type counts rather than an invented homogeneous Arrow/SQL contract. Examples retain native values and type tags, limited to five distinct examples per property in the first 5,000 features, 64KiB per example and a 4MiB aggregate encoded budget. Presence/type statistics still cover the entire population when no example fits. At most 4,096 distinct property names are supported. Malformed JSON, duplicate members, nonfinite values/float overflow and incomplete bodies are rejected. Invalid geometry structure or topology produces blocking full-population quality, without coercion/closure repair.

Input bodies have a 128MiB encoded-byte ceiling; authored manifest layers have a 2MiB bounded range-read ceiling. The standard-library decoded JSON tree may consume substantially more memory: this is not a hard RSS quota. Local and GCS-shaped transports validate exactly one selected data object and its authored manifests; raw captures/other version objects are not downloaded for catalog inspection. Registered synthetic graph checks and independent exact-head review remain required before this draft is handed to source adapters. No real release is approved by this profile.
