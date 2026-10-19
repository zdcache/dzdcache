# Runtime schema contracts

POST a `SchemaDefinition` to `/cache/schemas`. The node validates the definition, generates Java source with safe internal field/class names, compiles it in memory with the JDK compiler, and builds one reusable ZDCache `SearchSpecification`. Peers receive the same definition and compile their own isolated classes. Java class names in responses are diagnostic; clients identify types by specification id and version.

The publishing service supplies this definition before publishing roots. Definitions may be authored directly or produced by tooling chosen by the publisher; inspecting OpenAPI-generated client code is not required. Consumers use the agreed specification/version and dimensions, which are also available through the schema inspection endpoints. The definition describes cached data and searches, and contains no original-service URL or fallback policy.

## Supported subset

- Root: a non-null `object`.
- Explicit types: `object`, `array`, `string`, `integer`, `number`, `boolean`.
- Nullable properties: `["string", "null"]`, or an equivalent pair for another supported type.
- Objects: declared `properties`, `required`, boolean `additionalProperties`.
- Arrays: one `items` schema; nested arrays/objects are supported.
- Scalars: `enum` (up to 256 values).
- Metadata accepted without validation semantics: `$schema`, `$id`, `$comment`, `title`, `description`, `default`, `examples`. In particular, `default` does not populate missing fields.

Integers must fit signed Java `long`. Numbers use normalized `BigDecimal`, so numeric indexes do not depend on decimal scale. Absent optional properties remain absent; explicit nulls remain null when allowed. Unknown properties are preserved by default as ordinary JSON graph data, and rejected when `additionalProperties` is false. Unknown properties cannot be indexed without being declared in the schema.

Unsupported keywords, including `$ref`, `oneOf`/`anyOf`/`allOf`, `format`, `pattern`, and min/max constraints, are rejected rather than silently ignored. Recursive references, polymorphic schemas, tuples, and schema-valued additional properties are not implemented. Resolve/normalize an OpenAPI schema into the supported subset before publishing; direct arbitrary OpenAPI Schema Objects are not yet a supported input contract.

## Identity and search dimensions

Paths use JSON Pointer escaping (`~0` means `~`, `~1` means `/`). An array traversal is written `*`, for example `/courses/*/code`.

- Identity must resolve to exactly one non-null scalar for every inserted root, without array traversal. String or integer identities are recommended.
- `exact`: one scalar query in `value`; can index many values across nested arrays.
- `prefix`: one string prefix in `value`; can index many strings across nested arrays.
- `range`: inclusive scalar `from` and `to`; supports numbers or strings and cannot traverse arrays.
- `compound`: 2 to 8 `parts`, each with a `path` and an `index`; only the last part can be `prefix` or `range`. Query with `value` as an array of the part values, and `from`/`to` for a last range part. Parts under the same array element, such as `/courses/*/code` and `/courses/*/year`, match within one element.
- Missing or null optional indexed fields contribute no index value.

Only declared dimensions can be queried. The server decodes queries to the same runtime scalar type used during indexing. It returns matching full root graphs supplied by ZDCache; a course match returns the whole Student, including other courses.

## Registration without a schema

Omit `schema` and give every dimension and every compound part a `type` instead. No Java classes are compiled; each root is kept as compact JSON with its index values extracted once on write. See [student-json.json](../examples/student-json.json). These definitions are listed and inspected through the same endpoints.

| `type` | Indexed and queried as |
| --- | --- |
| `string` | JSON string. |
| `integer` | JSON number that fits a signed Java `long` exactly. |
| `number` | JSON number as normalized `BigDecimal`. |
| `boolean` | JSON boolean. |
| `timestamp` | ISO-8601 instant string, such as `2024-05-01T12:00:00Z`. |

- A value of another JSON type is rejected with 400. Missing or null values contribute no index value.
- The identity is a JSON Pointer without `*` to a string or an integer.
- `prefix` needs `string`; `range` cannot traverse arrays or use `boolean`. A compound part outside a shared array element that traverses arrays must be `exact`.
- Paths, compound rules and limits are otherwise the same as with a schema. A dimension `type` is rejected when a schema is given, because the schema decides the type.
- Properties that are not indexed are stored as sent and are not validated.

## Schema evolution and limits

The `(id, version)` contract is immutable. Reposting the same definition is idempotent. Changed definitions require a new positive version, which starts a distinct empty bucket; there is no implicit migration or cross-version query. Property ordering in JSON objects is not significant. Preserve dimension array ordering when republishing an existing definition.

Compilation bounds: 256 KiB schema JSON, 24 levels of schema nesting, 512 declared properties, 128 generated object classes, 128 dimensions. Graph decoding allows at most 100,000 nodes and 64 levels. HTTP byte limits apply independently.

Uploaded Java source and class names are never compiled. Generated code contains only generated identifiers and JDK field types; annotation processing is disabled. Each compilation uses an isolated classloader with the platform classloader as parent. A full JDK is required even when running the packaged executable JAR. Schema versions are retained for the process lifetime, bounded by `max-schemas`.
