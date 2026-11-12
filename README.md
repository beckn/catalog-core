# catalog-core

Library for Beckn Catalog publishing and discovery/crawling.

`catalog-core` gives you the building blocks for two sides of the same protocol:

- **Publishing**: turn a catalog document into a signed, versioned set of index/baseline/change files ready to be served.
- **Crawling / discovery**: fetch those files from a remote node, verify their signatures and integrity, and fold them into an up-to-date local copy of a catalog.

It is a library, not a service: it has no `cmd/`, no HTTP server, and no database. Every side effect (HTTP calls, blob storage, persistence of crawl cursors) is expressed as a small interface that the calling application implements.

## Module

```
module github.com/beckn/catalog-core
go 1.26.1
```

## Package map

```
pkg/
├── catalog/                 Catalog-file spec: index/entry schema, change-file apply/diff,
│                             fetch+verify orchestration (Fetcher), resolve (baseline+changes fold)
│   ├── crawler/              Content-agnostic HTTP fetch/verify primitives (client, SSRF guard,
│   │                          digest check, signature verification, fault taxonomy)
│   │   └── decode/            Codec registry (gzip/json) for decoding fetched artifact bytes
│   ├── crawlmanager/         Crawl-side orchestration: PollIndexes + SyncNext over caller-supplied
│   │                          Source / Sink / Store ports
│   ├── publisher/            Publish-side orchestration: diff + sign submissions into
│   │                          baseline/change/tombstone entries
│   └── store/                Backend-agnostic index/file assembler over a pluggable BlobStore
└── security/
    ├── artifactsigner/        Ed25519 signing helpers (detached JWS, self-signed JSON, file tuples)
    └── artifactverifier/      Matching verification counterparts + public-key/signature parsing
```

### How the layers fit together

```
                    pkg/security/artifactsigner  ◄──────┐
                              │                          │ (signs)
                              ▼                          │
                    pkg/catalog/publisher ─────► pkg/catalog/store
                              │
                              ▼
                        pkg/catalog  (Fetcher, Resolve, index/entry types, change detection)
                        │        │
                        ▼        ▼
     pkg/catalog/crawler   pkg/catalog/crawler/decode
                │
                ▼
    pkg/security/artifactverifier  (verifies)

                    pkg/catalog/crawlmanager
             (orchestrates catalog.Fetcher + caller's Source/Sink/Store)
```

- `pkg/catalog/crawler` (+ `decode`) is the generic, catalog-agnostic HTTP fetch/verify/decode layer.
- `pkg/catalog` is the catalog-shaped layer built on top of it (index/entry schema, `Fetcher`, `Resolve`, change detection, scoping, identity stamping).
- `pkg/catalog/crawlmanager` is the top-level crawl orchestration layer, built on `pkg/catalog` and driven by three ports you implement: `Source`, `Sink`, `Store`.
- `pkg/catalog/publisher` + `pkg/catalog/store` are the publish-side counterpart: `publisher.Publish` diffs and signs submissions, `store.Store` assembles the resulting files against your `BlobStore`.
- `pkg/security/artifactsigner` / `artifactverifier` are shared low-level signing/verification primitives used by both sides.

## Package reference

### `pkg/catalog`

Catalog-file spec primitives — the index/entry schema, change-file apply/diff, fetch+verify orchestration, and baseline+changes resolution.

**Index & entry types** (`index.go`)

```go
type FileEntry struct {
    Version, FromVersion, ToVersion int64
    URL, Digest, Encoding           string
    Size                            int64
}
func (f FileEntry) EffectiveVersion() int64

type MasterDependency struct {
    CatalogID string
    Version   int64
    IndexURL  string
}

type CatalogEntry struct {
    CatalogID    string
    EntryVersion int64
    CatalogType  string
    Dependencies *Dependencies
    SchemaTypes  []string
    NetworkIDs   []string
    IsActive     *bool
    Baseline     FileEntry
    Changes      []FileEntry
    Latest       *FileEntry
    RetiredAt    string
    CrawlHint    string
    Signature    EntrySignature
}
func (e CatalogEntry) IsRetired() bool
func (e CatalogEntry) IsPaused() bool
func (e CatalogEntry) IsPublic() bool
func (e CatalogEntry) LatestVersion() int64

type Index struct {
    NodeID     string
    NextUpdate string
    Catalogs   []CatalogEntry
}
func FindCatalog(idx Index, catalogID string) (CatalogEntry, bool)

type IndexConditions struct { ETag, LastModified string }
type IndexResult struct {
    Index                     Index
    NotModified               bool
    ETag, LastModified        string
    Dropped                   []DroppedEntry // {CatalogID, Reason}
}
```

**Fetching** (`fetch.go`) — wraps `crawler.Client` with catalog-aware semantics:

```go
func NewFetcher(client *crawler.Client, keys crawler.KeySource, maxDecompressed int64) *Fetcher
func (f *Fetcher) FetchIndex(ctx context.Context, url string, cond IndexConditions) (IndexResult, error)
func (f *Fetcher) FetchFile(ctx context.Context, nodeID, catalogID string, entry FileEntry) ([]byte, error)
```

**Resolving baseline + changes into current content** (`resolve.go`):

```go
const FaultGap crawler.FaultClass = "gap"

func Resolve(
    baseline []byte, baselineVersion int64, changes []FileEntry,
    fetch func(FileEntry) ([]byte, error),
    onApplied func(FileEntry, []byte),
) ([]byte, error)
```

**Change detection & scoping** (`change.go`, `scope.go`):

```go
type Action string
const (
    ActionSync          Action = "sync"
    ActionSkipUnchanged Action = "skip_unchanged"
    ActionRetire        Action = "retire"
    ActionRollback      Action = "rollback"
)
type Decision struct { Action Action; ToVersion, EntryVersion int64 }
func DetectChange(entry CatalogEntry, entryCursor, contentCursor int64, seen bool) Decision

func ResolveScope(entry CatalogEntry, callerNetworks []string) (take bool, visibleTo []string)
```

**Change-file (diff) documents** (`catalogfile.go`, `diff.go`):

```go
type DiffBlock struct {
    Upserts  []json.RawMessage `json:"upserts,omitempty"`
    Removals []string          `json:"removals,omitempty"`
}
func (b DiffBlock) IsEmpty() bool

type ChangeFileDoc struct {
    CatalogID              string
    FromVersion, ToVersion int64
    NextUpdate             time.Time
    Resources, Offers      DiffBlock
    Catalog                json.RawMessage
    Signature              FileSignature
}

func Apply(catalog []byte, changeRaw []byte) ([]byte, error)
func ItemID(raw json.RawMessage) (string, error)

type CatalogDiff struct { Resources, Offers DiffBlock }
func Diff(prior, next json.RawMessage) (CatalogDiff, json.RawMessage, error)
```

**Catalog file document** (`filedoc.go`):

```go
type FileSignature struct { KeyID, Canonicalization, Value string }

type CatalogFileDoc struct {
    CatalogID  string
    Version    int64
    NextUpdate time.Time
    Catalog    json.RawMessage
    RetiredAt  *time.Time
    Signature  FileSignature
}
```

**Envelope & identity** (`envelope.go`, `identity.go`):

```go
func ExtractEnvelope(doc []byte) (descriptor, provider json.RawMessage, err error)
func BuildRetireDoc(catalogID string, descriptor, provider json.RawMessage) ([]byte, error)
func StampIdentity(doc []byte, bppID, bppURI string) ([]byte, error)
```

### `pkg/catalog/crawler`

Content-agnostic fetch/verify primitives — no catalog knowledge, just HTTP + integrity + signatures.

```go
// HTTP client with SSRF guard, size limits, and conditional GET support
func NewClient(timeout time.Duration, maxBytes int64, allowPrivate bool, opts ...Option) *Client
func (c *Client) Get(ctx context.Context, url string) ([]byte, error)
func (c *Client) GetConditional(ctx context.Context, url string, cond Conditions) (Result, error)

type Conditions struct { ETag, LastModified string }
type Result struct { Body []byte; NotModified bool; ETag, LastModified string }

// Integrity
func DigestMatches(body []byte, expected string) bool

// Signature verification
type KeySource func(ctx context.Context, nodeID, keyID string) (ed25519.PublicKey, error)
func StaticKeys(keys map[string]ed25519.PublicKey) KeySource // test helper
func ResolveSigningKey(ctx context.Context, keys KeySource, nodeID, keyID, what string) (ed25519.PublicKey, error)
func VerifySignature(ctx context.Context, keys KeySource, nodeID, keyID, sigValue string, raw []byte, sigField string) error

// Fault taxonomy — classifies errors as permanent (don't retry) vs transient
type FaultClass string
const (
    FaultSSRF           FaultClass = "ssrf"
    FaultOversize       FaultClass = "oversize"
    FaultDigestMismatch FaultClass = "digest_mismatch"
    FaultSignature      FaultClass = "signature"
    FaultDecode         FaultClass = "decode"
    FaultContentInvalid FaultClass = "content_invalid"
    FaultTransient      FaultClass = "transient"
)
func (f FaultClass) Permanent() bool
func ClassifyFault(httpStatus int, err error) FaultClass
func Permanentf(format string, a ...any) error
func PermanentFaultf(class FaultClass, format string, a ...any) error
func IsPermanent(err error) bool
func PermanentClass(err error) FaultClass
```

### `pkg/catalog/crawler/decode`

```go
func Decode(encoding string, b []byte, maxDecompressed int64) ([]byte, error)
func EncodingFor(entryEncoding, url string) string
```

### `pkg/catalog/crawlmanager`

Crawl-side orchestration. You provide three ports; `Params.PollIndexes` and `Params.SyncNext` drive the crawl loop.

```go
type Params struct {
    Fetcher       *catalog.Fetcher
    Source        Source
    Sink          Sink
    Store         Store
    Networks      []string
    RetryDelay    time.Duration
    MaxAttempts   int
    ResolveBppURI func(ctx context.Context, nodeID, keyID string) (string, error)
    Now           func() time.Time
    Log           *slog.Logger
}
func (p Params) PollIndexes(ctx context.Context) error       // discover indexes, enqueue sync work
func (p Params) SyncNext(ctx context.Context) (claimed bool, err error) // process one queued item
```

**Ports you implement:**

```go
type Source interface {
    Discover(ctx context.Context) ([]IndexRef, error) // {ParticipantID, IndexURL}
}

type Sink interface {
    Send(ctx context.Context, entry catalog.CatalogEntry, content []byte) (SinkOutcome, error)
}
type SinkOutcome struct { Accepted bool; Reason string }

type Store interface {
    GetCatalogCursor(ctx context.Context, catalogID string) (cursor CatalogCursor, seen bool, err error)
    RecordFailure(ctx context.Context, report PassReport) error
    GetCatalogEnvelope(ctx context.Context, catalogID string) (descriptor, provider json.RawMessage, catalogType, participantID string, ok bool, err error)
    GetIndexCursor(ctx context.Context, indexURL string) (*IndexCursor, error)
    UpsertIndexCursor(ctx context.Context, cursor IndexCursor) error
    Enqueue(ctx context.Context, item QueueItem) error
    ClaimNext(ctx context.Context) (*ClaimedItem, error)
    Complete(ctx context.Context, id, claimID string, cursor CatalogCursor) error
    Reschedule(ctx context.Context, id, claimID string, nextAttemptAt time.Time) error
    Park(ctx context.Context, id, claimID string) error
}
```

Supporting types: `CatalogCursor`, `IndexCursor`, `PassReport`, `QueueItem` (`Op` is `"sync"` or `"retire"`), `ClaimedItem`.

### `pkg/catalog/publisher`

Publish-side orchestration: diffs new catalog content against prior state, signs the result, and produces a `store.PublishRequest`.

```go
type Submission struct {
    CatalogID    string
    CatalogType  string
    SchemaTypes  []string
    NetworkIds   []string
    Dependencies []catalog.MasterDependency
    CrawlHint    string
    Catalog      json.RawMessage
}

type Params struct {
    Catalogs                       []Submission
    PriorState                     map[string]store.CatalogState
    Retire                         []string
    ForceBaseline                  bool
    CompactionChangeCountThreshold int
    CompactionSizeRatioThreshold   float64
    Gzip                           bool
    PublishLatest                  bool
    NextUpdateIn                   time.Duration
    PublicBaseURL                  string
    SigningKey                     ed25519.PrivateKey
    KeyID                          string
    Domain                         string
    Logger                         *slog.Logger
}

type Outcome struct {
    CatalogID                    string
    Version, EntryVersion        int64
    Changed                      bool
    Digest                       string
    Mode                         string // "baseline" | "change" | "metadata" | "unchanged"
    Content, LatestContent       json.RawMessage
    LatestDigest                 string
}

type PublishError struct {
    CatalogID string
    Stage     string // "validate" | "diff" | "retire"
    Reason    string
    Fatal     bool
}

type Result struct {
    PublishedAt time.Time
    Publish     store.PublishRequest
    Reports     []Outcome
    Errors      []PublishError
}

func IndexURL(publicBaseURL string) string
func Publish(ctx context.Context, p Params) (Result, error)
```

### `pkg/catalog/store`

Backend-agnostic index/file assembler. You supply a `BlobStore`; `Store` handles path layout and file assembly.

```go
type BlobStore interface {
    Get(ctx context.Context, path string) ([]byte, error)
    Put(ctx context.Context, path string, content []byte) error
}
var ErrBlobNotFound = errors.New("catalogstore: blob not found")

const IndexFilename = "becknCatalogs.index.json"
func IndexPath() string
func LocalName(catalogID string) string
func CatalogFilePath(catalogID string, version int64, suffix string, compressed bool) string
func LatestFilePath(catalogID string, compressed bool) string

type CatalogState struct {
    Catalog         json.RawMessage
    BaselineFile    *catalog.FileEntry
    ChangeFiles     []catalog.FileEntry
    EntryVersion    int64
    CatalogType     string
    NetworkIds      []string
    SchemaTypes     []string
    IsActive        bool
    Dependencies    []catalog.MasterDependency
    CrawlHint       string
    LatestPublished bool
    Retired         bool // entry is already a tombstone; only EntryVersion/CatalogType/NetworkIds/SchemaTypes are set
}

type FileWrite struct {
    Version       int64
    Content       json.RawMessage
    ServedContent []byte
    Compressed    bool
}

type CatalogUpdate struct {
    CatalogID   string
    SignedEntry json.RawMessage
    Baseline, Change, Latest *FileWrite
}

type PublishRequest struct {
    NodeID      string
    NextUpdate  *time.Time
    Updates     []CatalogUpdate
    Retirements []CatalogUpdate
}

func New(blobs BlobStore) *Store
func (s *Store) WithLogger(logger *slog.Logger) *Store
func (s *Store) LoadCatalogs(ctx context.Context, catalogIDs []string) (map[string]CatalogState, error)
func (s *Store) Publish(ctx context.Context, req PublishRequest) error
```

### `pkg/security/artifactsigner`

Ed25519 signing helpers.

```go
func SignDetachedJWS(doc []byte, priv ed25519.PrivateKey) (string, error)
func SignJSON(doc []byte, excludeField string, priv ed25519.PrivateKey) (string, error)
func SignFileTuple(catalogID string, version int, url, digest string, validUntil time.Time, priv ed25519.PrivateKey) (string, error)
```

### `pkg/security/artifactverifier`

Matching verification counterparts, plus generic public-key/signature parsing.

```go
func VerifyDetachedArtifact(content, signaturePayload, publicKeyPayload []byte) error
func ParseSignature(body []byte) ([]byte, error)
func ParsePublicKeyResponse(body []byte) (any, error)
func VerifyDetached(content, signature []byte, key any) error

func CanonicalizeJCS(doc []byte) ([]byte, error)
func CanonicalizeJCSExcluding(doc []byte, excludeField string) ([]byte, error)
func VerifyDetachedJWS(doc []byte, jws string, pub ed25519.PublicKey) error
func VerifyJSON(doc []byte, excludeField, sigValueB64 string, pub ed25519.PublicKey) error
func VerifyFileTuple(catalogID string, version int, url, digest string, validUntil time.Time, sigValueB64 string, pub ed25519.PublicKey) error
```

## Typical integration shapes

**Building a crawler**: construct a `crawler.Client` and `crawler.KeySource`, wrap them in a `catalog.NewFetcher`, implement `crawlmanager.Source` (index discovery), `crawlmanager.Sink` (where resolved catalog content goes), and `crawlmanager.Store` (crawl-cursor persistence), then drive the loop with `crawlmanager.Params.PollIndexes` / `SyncNext`.

**Building a publisher**: implement `store.BlobStore` over your storage backend, load prior state with `store.New(blobs).LoadCatalogs`, call `publisher.Publish` with your `Submission`s and signing key, then hand the resulting `Result.Publish` to `store.Store.Publish`.
