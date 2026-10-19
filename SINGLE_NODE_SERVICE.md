# DZDCache service and generated integration proposal

> Historical proposal. The current [implementation](README.md) uses runtime-compiled Java types from published schemas and generic `/cache/*` endpoints. Its wire contract is in [openapi.yaml](openapi.yaml); the examples and JSON-tree storage proposal below are superseded.

> Latest scope decision: services publish and query through the generic HTTP API. Publishers own population and updates; consumers own data-source selection and fallback. The client inspection and generation proposals below are optional external tooling, not required DZDCache functionality. See the current [responsibility split](README.md#responsibilities).

This proposal follows the discussion through 14 October 2026. Distributed ZDCache (DZDCache) is the service and integration project; ZDCache is its underlying in-process cache library. This proposal focuses on one Spring Boot service holding ZDCache. It is not an implementation of the HTTP service or generator. The accompanying Java experiment verifies the central type-handling mechanism against ZDCache source.

## Proposed solution

Keep Java domain types in the applications that already use them. Generate a declarative description of identities, search dimensions, and operation mappings from the OpenAPI-generated Java client code. The cache service loads the approved descriptions and stores root graphs as JSON trees, building real ZDCache search specifications with extraction functions that read those trees.

This allows the same server image to support Student, Customer, or another root type by loading generated configuration. A Java model JAR does not have to be installed on the server for every domain. The server still knows the logical type, identity, value formats, and allowed searches through its registry.

```text
OpenAPI-generated Java webclient sources and model classes
                          |
                          v
                   inspection tool
                          |
                          v
                 human-reviewed manifest
                          |
             +------------+----------------+
             v            v                v
       server registry   consumer         owner
       configuration     subclasses       integration helpers
             |            |                |
             v            +-------+--------+
       generic Spring Boot cache service
             |
       registry resolves logical root type/version
             |
       ZDCache stores JSON root graphs
```

## Verified ZDCache extension points

The inspected revision is [`5b2e759b948af905fabf2ab9827f5d765119651d`](https://github.com/zdcache/zdcache/commit/5b2e759b948af905fabf2ab9827f5d765119651d).

- `IdentityDimension.of(Class, name, Function)` accepts an identity extractor.
- `SearchSpecification` accepts functions for exact, prefix, range, nested collection, and compound dimensions. They can read JSON properties rather than invoke domain getters.
- `ZDCache.register(specification)` makes an empty root type searchable before its first insertion.
- `put(graph, specification)` stores a graph and updates its indexes.
- `search(Class, dimensionName, values...)` accepts a dimension name that can arrive over HTTP.
- Registrations are indexed by Java `Class`. All generic JSON roots share a Java class, so the proposed registry allocates **one ZDCache instance per namespace, logical root type, and contract version**. This prevents Student and Customer identities and specifications from colliding.
- Specification instances must be reused. Runtime replacement must use the explicit specification-update API; a new equivalent specification cannot simply be passed on every write.

Sources: [ZDCache.java](https://github.com/zdcache/zdcache/blob/5b2e759b948af905fabf2ab9827f5d765119651d/src/main/java/io/github/zdcache/ZDCache.java), [SearchSpecification.java](https://github.com/zdcache/zdcache/blob/5b2e759b948af905fabf2ab9827f5d765119651d/src/main/java/io/github/zdcache/SearchSpecification.java), [IdentityDimension.java](https://github.com/zdcache/zdcache/blob/5b2e759b948af905fabf2ab9827f5d765119651d/src/main/java/io/github/zdcache/IdentityDimension.java).

## Generated contract

The following is an illustrative format, not a finalized schema:

```yaml
formatVersion: 1
namespace: school
rootTypes:
  Student:
    contractVersion: 1
    sourceModel: com.example.school.client.model.Student
    identity:
      pointer: /id
      scalar: int64
    capacity: 10000
    ttl: infinite
    dimensions:
      sex:
        pointer: /sex
        scalar: string
        index: exact
      name:
        pointer: /name
        scalar: string
        index: prefix
      age:
        pointer: /age
        scalar: int64
        index: range
operations:
  "com.example.school.client.api.StudentsApi.findBySex(java.lang.String)":
    enabled: true
    rootType: Student
    dimension: sex
    arguments: [sex]
    result: collection
    consumerBackfill: false
```

The source model name is useful to the generator; the generic server does not attempt to load that class. The generator emits the server's subset of this contract, including scalar formats, identity and search paths, and a fingerprint. Consumers and owners reference the same namespace, logical type, and version.

For the first version, load the generated registry from a deployment file at startup. Repeating the build with updated generated configuration can add types without editing server Java code. Runtime registration and schema replacement can be added later. A contract version identifies identity, projection, index, and scalar semantics; incompatible definitions must not silently share a bucket.

## HTTP boundary

Example paths below use namespace `school` and root contract `Student/v1`.

| Request | Behavior |
| --- | --- |
| `GET /v1/types/school/Student/v1` | Describes its identity, available dimensions, and supported query shapes. |
| `PUT /v1/roots/school/Student/v1/42` with a JSON object | Validates the root identity against the URL, extracts index values, and replaces root 42. |
| `POST /v1/roots/school/Student/v1/batch` with an array | Stores each root separately. Initial implementation can define partial batch behavior or reject the batch before applying it; this must be explicit. |
| `GET /v1/roots/school/Student/v1/42` | Returns the root JSON, or a distinct not-found response. |
| `POST /v1/search/school/Student/v1` | Runs the declared dimension search and returns the matching root graphs as an array. |
| `DELETE /v1/roots/school/Student/v1/42` | Removes that root and its index entries. |

An exact search request could be `{"dimension":"sex","value":"female"}`. A range search could be `{"dimension":"age","from":18,"to":25}`. The server knows the index kind and converts both stored values and query arguments to the same scalar type. The current range index uses inclusive endpoints. A public API must not advertise operations the installed index cannot execute.

The server accepts finite JSON object graphs, owns their parsed representation, and does not mutate an indexed tree in place. Updates replace the graph through ZDCache. Serialization emits the payload graphs returned by ZDCache, including nested fields that were not indexed. A nested search finds matching roots; it does not prune children out of those roots.

## Agreed response semantics

- A successful collection search is `200` with an array, including `[]` for zero matches.
- The consumer accepts that array as the answer. There is no collection-completeness check or query-result manifest.
- Independent LRU eviction can change the roots returned by subsequent searches. This is part of the accepted contract.
- A single-root lookup can return a distinct miss status if the root is absent.
- Unregistered types, invalid dimensions, or incompatible query values are errors, not successful empty searches.
- A cache timeout or availability failure allows the consumer to call the original service. Contract errors need an explicit client policy so a broken generated mapping is observable.

Because `200 []` is authoritative, an empty but healthy cache does **not** cause consumers to load collections from the original service. Owner initialization or explicit population must load roots if useful collection results are required immediately after startup. The owner can push initial roots and subsequent changes through the same write API.

## Generator responsibilities

Use a Java source parser and type resolution to inspect the actual OpenAPI Generator output, including request construction, public method signatures, generic return types, and model serialization annotations. Reflection alone would not recover all endpoint parameter bindings. Pin and test the supported generator output shape.

The inspection stage can discover API classes, candidate operations, returned root types, nested model structures, JSON property names, and candidate mappings between arguments and properties. The human-reviewed manifest confirms identity, allowed index kinds, TTL, backfill, and the mapping from each API operation to a cache search. An arbitrary business filter cannot be inferred solely from an argument name or return type; unresolved mappings must remain unenabled.

The generation stage emits:

1. The cache registry configuration.
2. Consumer subclasses that preserve the generated Java interface and map approved calls to cache requests. Responses deserialize into the original generated model types.
3. Owner helpers that publish root graphs and invalidate identities. The owning service places these helpers at its actual transaction boundaries; client source inspection cannot discover database commits.

The OpenAPI `webclient` template has separate public methods for the ordinary result, HTTP information, and `ResponseSpec`. Overriding the ordinary result method alone does not intercept the other entry points because each uses a private request-creation method. The generator must cover the advertised entry points or explicitly constrain its supported surface.

For a collection returning `Flux<Student>`, decide fallback from the **HTTP result before flattening the body into a Flux**. Applying `switchIfEmpty(originalCall)` after decoding `200 []` would incorrectly trigger fallback. Keep an explicit success/miss/failure result around the decoded array until the decision is made. Handle cache errors separately from origin errors so an origin failure is not accidentally retried as a cache fallback.

Source: [OpenAPI Generator webclient template](https://github.com/OpenAPITools/openapi-generator/blob/master/modules/openapi-generator/src/main/resources/Java/libraries/webclient/api.mustache).

## Boundaries still worth specifying

- **Root representation:** if two operations return different projections of Student 42, a small projection must not erase fields needed by another operation. Require one canonical representation per logical type or assign separate logical view types.
- **Scalar values:** IDs, enums, numbers, dates, missing properties, and nulls need declared conversions. ZDCache's Java equality and range comparison require compatible values on insert and query.
- **Nested dimensions:** preserve array traversal and same-child compound relationships through the existing `each` and compound builders. Arbitrary Java lambdas cannot be shipped as JSON; custom computations require a supported declarative operator or an explicit extension.
- **Expiry:** enforce finite TTL outside ZDCache, both on identity reads and searches. Delayed cleanup must not expose expired roots. Define a root policy when several methods can populate the same root with different TTL settings. Infinite TTL leaves invalidation and LRU eviction active.
- **Access scope:** authenticate owners and consumers, and make namespace/tenant scope part of registry selection where data is not shared. Index names alone do not enforce authorization.
- **LRU use semantics:** the inspected ZDCache documentation treats searches differently from identity `get` for recency. Preserve and document that behavior unless search promotion is deliberately added.

Distribution can later wrap the same registry and write operations with membership, versioning, and broadcast acknowledgments. Nothing in this proposal claims those guarantees for the initial single-node service.

## Feasibility result

[`JsonRootAdapterProof.java`](experiments/json-root-adapter/JsonRootAdapterProof.java) was compiled with Java 21 against the inspected ZDCache source and run with Jackson. All 11 checks passed: exact/prefix/range/nested searches, whole-graph JSON serialization, empty results, isolation from caller mutation, reindexing on replacement, logical type isolation, removal, and LRU eviction.

This verifies the type adapter foundation. It does not test Spring endpoints, source inspection, generated subclasses, owner transaction integration, or distribution.
