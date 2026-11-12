package catalog

// fetch.go — Fetcher: the catalog-shaped caller layered on pkg/crawler's
// content-agnostic fetch/verify/decode primitives. It is the only place
// that knows an index entry's self-signature covers catalogId/catalogType/
// status/networkIds/schemaTypes/baseline/changes/latest together, that a
// baseline wraps its content as CatalogFileDoc's {catalog, signature}
// envelope, and that a fetched file's own declared catalogId/version must
// match what the index entry said to expect (CON-TBD-12). pkg/crawler
// never needs to know any of that.
//
// Like pkg/crawler itself, this is plain Go, not an onix plugin: the file
// spec mandates exactly one correct way to verify and unwrap these
// documents, so there is nothing here for a deployment to legitimately swap.

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/beckn/catalog-core/pkg/catalog/crawler"
	"github.com/beckn/catalog-core/pkg/catalog/crawler/decode"
)

// supportedCanonicalization is the one FileSignature.Canonicalization value
// Fetcher knows how to verify. The field exists on the wire for future
// extensibility (unlike EntrySignature, which declares none), but nothing
// producing catalog/change files today signs with anything else -- see
// catalog/publisher's signCatalogFile/signChangeFile. A file declaring any
// other value is rejected outright rather than verified against an
// assumption of what it probably meant.
const supportedCanonicalization = "JCS"

// Fetcher fetches, verifies, and decodes a publishing node's catalog index
// and the files it references.
type Fetcher struct {
	client          *crawler.Client
	keys            crawler.KeySource
	maxDecompressed int64
}

// NewFetcher builds a Fetcher over client, verifying every self-signature
// against keys. A nil keys fails closed: FetchIndex drops every entry and
// FetchFile rejects every file, per crawler.VerifySignature.
//
// maxDecompressed caps decode.Decode's output for both the index and each
// fetched file -- the decompression-bomb guard, distinct from client's own
// at-rest (compressed) size cap.
func NewFetcher(client *crawler.Client, keys crawler.KeySource, maxDecompressed int64) *Fetcher {
	return &Fetcher{client: client, keys: keys, maxDecompressed: maxDecompressed}
}

// rawIndex is the index document's shape parsed generically first, so each
// entry's self-signature is verified against its OWN raw JSON bytes as
// received -- never a Go-struct re-marshal, which can silently drop/reorder
// fields (e.g. omitempty) and break canonicalization.
type rawIndex struct {
	NodeID     string            `json:"nodeId"`
	NextUpdate string            `json:"next_update"`
	Catalogs   []json.RawMessage `json:"catalogs"`
}

// FetchIndex GETs, decodes (the index itself may be compressed -- encoding
// is inferred from the URL suffix), and parses a publishing node's catalog
// index. cond's validators are sent as If-None-Match / If-Modified-Since; a
// host that honours them can answer 304 (IndexResult.NotModified, no body).
//
// Every entry's own self-signature (file spec: "each catalog entry signs
// itself") is verified before it is trusted; an entry that doesn't parse or
// doesn't verify is dropped and reported in IndexResult.Dropped rather than
// failing the whole fetch -- one forged or malformed entry must not hide
// every healthy one behind it.
func (f *Fetcher) FetchIndex(ctx context.Context, url string, cond IndexConditions) (IndexResult, error) {
	res, err := f.client.GetConditional(ctx, url, crawler.Conditions{ETag: cond.ETag, LastModified: cond.LastModified})
	if err != nil {
		return IndexResult{}, err
	}
	if res.NotModified {
		return IndexResult{NotModified: true, ETag: cond.ETag, LastModified: cond.LastModified}, nil
	}
	decoded, err := decode.Decode(decode.EncodingFor("", url), res.Body, f.maxDecompressed)
	if err != nil {
		return IndexResult{}, err
	}

	var raw rawIndex
	if err := json.Unmarshal(decoded, &raw); err != nil {
		return IndexResult{}, fmt.Errorf("catalog: parsing index %s: %w", url, err)
	}
	idx := Index{NodeID: raw.NodeID, NextUpdate: raw.NextUpdate}
	var dropped []DroppedEntry
	for _, entryRaw := range raw.Catalogs {
		var entry CatalogEntry
		if err := json.Unmarshal(entryRaw, &entry); err != nil {
			// Malformed entry: not trustworthy, drop it (fail closed). Reported so
			// a caller doesn't see a clean poll with silently fewer catalogs.
			dropped = append(dropped, DroppedEntry{Reason: fmt.Sprintf("parsing entry: %v", err)})
			continue
		}
		// EntrySignature declares no canonicalization (unlike a file's
		// FileSignature) -- it is always JCS, so there is nothing to check
		// before verifying, unlike verifyFileSignature below.
		if err := crawler.VerifySignature(ctx, f.keys, raw.NodeID, entry.Signature.KeyID, entry.Signature.Value, entryRaw, "signature"); err != nil {
			// Unverifiable entry: drop it rather than trust an unsigned/forged one.
			dropped = append(dropped, DroppedEntry{CatalogID: entry.CatalogID, Reason: err.Error(), Transient: !crawler.IsPermanent(err)})
			continue
		}
		idx.Catalogs = append(idx.Catalogs, entry)
	}
	return IndexResult{Index: idx, ETag: res.ETag, LastModified: res.LastModified, Dropped: dropped}, nil
}

// FetchFile GETs one file and verifies it end to end: the fetched bytes
// against the declared digest, then the file's OWN embedded self-signature
// (file spec: baseline and change files alike sign their own content). Both
// are mandatory and both fail closed -- the digest says these are the bytes
// the index named, the signature says the publisher genuinely produced them.
// A digest alone would let anyone who can serve the URL swap in their own
// signed-by-nobody content.
//
// nodeID is the publishing node's identity (the enclosing index's nodeId),
// used to resolve the file's signing key. catalogID is the enclosing index
// entry's own catalogId, cross-checked (CON-TBD-12) against the file's
// internal catalogId/version once its signature verifies.
//
// The returned bytes are what downstream code should use: for a baseline
// (CatalogFileDoc's {catalog, signature} envelope) that is the unwrapped
// .catalog object; for a change file (ChangeFileDoc, flat) that is the
// decoded bytes unchanged.
func (f *Fetcher) FetchFile(ctx context.Context, nodeID, catalogID string, entry FileEntry) ([]byte, error) {
	if entry.Digest == "" {
		return nil, crawler.Permanentf("catalog: %s has no digest (integrity check required)", entry.URL)
	}
	body, err := f.client.Get(ctx, entry.URL)
	if err != nil {
		return nil, err
	}
	// Decode BEFORE the digest check: the digest (and signature) cover the
	// canonical DECOMPRESSED content, never the compressed bytes at rest --
	// gzip's own output isn't guaranteed byte-stable across tool versions for
	// identical input, so digesting the compressed form would make a
	// legitimate re-publish indistinguishable from tampering. decode.Decode's
	// decompression-bomb guard still runs first regardless, so this ordering
	// does not weaken that protection.
	decoded, err := decode.Decode(decode.EncodingFor(entry.Encoding, entry.URL), body, f.maxDecompressed)
	if err != nil {
		return nil, err
	}
	if !crawler.DigestMatches(decoded, entry.Digest) {
		// Permanent, not transient: re-fetching the same URL yields the same bad
		// bytes -- treat as tampering, don't retry it on a loop.
		return nil, crawler.PermanentFaultf(crawler.FaultDigestMismatch, "catalog: digest mismatch for %s", entry.URL)
	}
	return f.verifyAndUnwrap(ctx, nodeID, catalogID, entry.URL, decoded, entry.EffectiveVersion())
}

// fileDiscriminator is peeked at first to tell a CatalogFileDoc (baseline)
// from a ChangeFileDoc (change file) apart: FromVersion is required on a
// change file and absent (zero) on a baseline -- a reliable discriminator,
// unlike Catalog being non-empty, which a change file's optional
// catalog-attribute patch can also produce.
type fileDiscriminator struct {
	FromVersion int64 `json:"fromVersion"`
}

// verifyAndUnwrap verifies decoded's embedded self-signature -- rejecting an
// unsupported Canonicalization outright -- cross-checks its own internal
// catalogId/version against what the index entry declared (CON-TBD-12: a
// mismatch here is treated exactly like a digest mismatch -- discard, don't
// index, log; neither side is authoritative), then returns the bytes
// downstream code should use.
func (f *Fetcher) verifyAndUnwrap(ctx context.Context, nodeID, wantCatalogID, url string, decoded []byte, wantVersion int64) ([]byte, error) {
	var disc fileDiscriminator
	if err := json.Unmarshal(decoded, &disc); err != nil {
		return nil, crawler.PermanentFaultf(crawler.FaultContentInvalid, "catalog: %s: not a JSON object: %v", url, err)
	}

	var (
		sig          FileSignature
		gotCatalogID string
		gotVersion   int64
		unwrapped    json.RawMessage // non-nil only for a baseline
	)
	if disc.FromVersion != 0 {
		var doc ChangeFileDoc
		if err := json.Unmarshal(decoded, &doc); err != nil {
			return nil, crawler.PermanentFaultf(crawler.FaultContentInvalid, "catalog: %s: not a valid change file: %v", url, err)
		}
		sig, gotCatalogID, gotVersion = doc.Signature, doc.CatalogID, doc.ToVersion
	} else {
		var doc CatalogFileDoc
		if err := json.Unmarshal(decoded, &doc); err != nil {
			return nil, crawler.PermanentFaultf(crawler.FaultContentInvalid, "catalog: %s: not a valid catalog file: %v", url, err)
		}
		sig, gotCatalogID, gotVersion = doc.Signature, doc.CatalogID, doc.Version
		unwrapped = doc.Catalog
	}

	if sig.Canonicalization != supportedCanonicalization {
		// Permanent: a scheme this Fetcher doesn't implement will never verify
		// no matter how many times it's retried, and silently falling back to
		// "assume JCS anyway" would let a declared-but-unchecked field become a
		// downgrade vector.
		return nil, crawler.PermanentFaultf(crawler.FaultSignature,
			"catalog: %s: unsupported signature canonicalization %q", url, sig.Canonicalization)
	}
	if err := crawler.VerifySignature(ctx, f.keys, nodeID, sig.KeyID, sig.Value, decoded, "signature"); err != nil {
		return nil, err
	}
	// CON-TBD-12: the file's own internal catalogId/version must agree with
	// what the index entry declared for it. Checked AFTER signature
	// verification (so this is the file's genuine, authored identity, not
	// something an attacker without the signing key could forge) but treated
	// exactly like a digest mismatch, not a signature failure -- the content
	// is authentic, just not the content this reference claims it is.
	if gotCatalogID != wantCatalogID || gotVersion != wantVersion {
		return nil, crawler.PermanentFaultf(crawler.FaultDigestMismatch,
			"catalog: %s declares catalogId=%q version=%d, index entry expected catalogId=%q version=%d",
			url, gotCatalogID, gotVersion, wantCatalogID, wantVersion)
	}
	if unwrapped == nil {
		return decoded, nil // change file: signature is a sibling field, nothing to unwrap
	}
	if len(unwrapped) == 0 {
		return nil, crawler.PermanentFaultf(crawler.FaultContentInvalid, "catalog: %s: baseline has no catalog content", url)
	}
	return unwrapped, nil // baseline envelope: unwrap to the bare catalog document
}
