package artifactverifier_test

// jcs_facts_test.go records, as executable facts, how the canonical form
// this module signs and verifies ("canonicalization": "JCS" in every file
// signature) relates to RFC 8785. It changes no behaviour. Each test pins
// what happens today; a decision to move to strict RFC 8785 would flip the
// ones marked DIFFERS.
//
// What is signed: CanonicalizeJCSExcluding decodes the document into
// map[string]any and re-encodes it with encoding/json. That gives sorted
// keys and no white space, like RFC 8785, with these differences:
//
//   1. encoding/json escapes "&", "<", ">" as \u0026, \u003c, \u003e and
//      U+2028, U+2029 as \u2028, \u2029. RFC 8785 writes all five literally.
//   2. Keys are sorted by UTF-8 bytes. RFC 8785 sorts by UTF-16 code units.
//      The orders differ only between a key starting in U+E000..U+FFFF and
//      one starting with a character outside the Basic Multilingual Plane.
//   3. Every number goes through float64, so an integer above 2^53 is
//      rounded before it is signed.
//
// Signer and verifier here share the function, so they always agree with
// each other. The question is only what a verifier written from RFC 8785
// computes for the same document.

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/beckn/catalog-core/pkg/security/artifactsigner"
	"github.com/beckn/catalog-core/pkg/security/artifactverifier"
)

// strictJCS is RFC 8785 for the documents these tests use: objects, arrays,
// strings, booleans, null, and integers small enough to be exact.
func strictJCS(t *testing.T, doc []byte, exclude string) []byte {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(doc))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("strictJCS: %v", err)
	}
	if m, ok := v.(map[string]any); ok {
		delete(m, exclude)
	}
	var b strings.Builder
	writeStrict(&b, v)
	return []byte(b.String())
}

func writeStrict(b *strings.Builder, v any) {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		fmt.Fprintf(b, "%t", x)
	case json.Number:
		b.WriteString(x.String())
	case string:
		writeStrictString(b, x)
	case []any:
		b.WriteByte('[')
		for i, e := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			writeStrict(b, e)
		}
		b.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { // UTF-16 code unit order
			a, c := utf16.Encode([]rune(keys[i])), utf16.Encode([]rune(keys[j]))
			for n := 0; n < len(a) && n < len(c); n++ {
				if a[n] != c[n] {
					return a[n] < c[n]
				}
			}
			return len(a) < len(c)
		})
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			writeStrictString(b, k)
			b.WriteByte(':')
			writeStrict(b, x[k])
		}
		b.WriteByte('}')
	}
}

func writeStrictString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch {
		case r == '"':
			b.WriteString(`\"`)
		case r == '\\':
			b.WriteString(`\\`)
		case r == '\b':
			b.WriteString(`\b`)
		case r == '\f':
			b.WriteString(`\f`)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case r < 0x20:
			fmt.Fprintf(b, `\u%04x`, r)
		default:
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
}

func ours(t *testing.T, doc string) []byte {
	t.Helper()
	c, err := artifactverifier.CanonicalizeJCSExcluding([]byte(doc), "signature")
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestJCSFacts_SignedBytesAgainstRFC8785(t *testing.T) {
	for _, tc := range []struct {
		name, doc string
		same      bool
		oursWant  string // what this module signs, when it differs
	}{
		{"SAME plain ASCII, nested, unsorted keys", `{"b":{"z":1,"a":[true,null,"x"]},"a":"y","signature":{"value":"s"}}`, true, ""},
		{"SAME non-ASCII text", `{"name":"चावल Ünïcode 米"}`, true, ""},
		{"SAME quotes, backslash, control characters", `{"t":"a\"b\\c\n\u0001"}`, true, ""},
		{"DIFFERS ampersand", `{"name":"Fish & Chips"}`, false, `{"name":"Fish \u0026 Chips"}`},
		{"DIFFERS angle brackets", `{"html":"<b>hot</b>"}`, false, `{"html":"\u003cb\u003ehot\u003c/b\u003e"}`},
		{"DIFFERS U+2028 line separator", "{\"t\":\"a\u2028b\"}", false, `{"t":"a\u2028b"}`},
		{"DIFFERS key order beyond the BMP", "{\"\U0001F600\":1,\"\uFF5E\":2}", false, "{\"\uFF5E\":2,\"\U0001F600\":1}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, strict := ours(t, tc.doc), strictJCS(t, []byte(tc.doc), "signature")
			if same := bytes.Equal(got, strict); same != tc.same {
				t.Fatalf("signed bytes equal RFC 8785 bytes: %v, recorded as %v\n ours   %s\n strict %s", same, tc.same, got, strict)
			}
			if !tc.same && string(got) != tc.oursWant {
				t.Errorf("this module signs\n %s\nrecorded as\n %s", got, tc.oursWant)
			}
		})
	}
}

// Each side rejects the other's signature once the content has one of the
// characters above: a strict RFC 8785 consumer cannot verify what this
// module publishes, and this module's Fetcher (which calls VerifyJSON)
// cannot verify a file from a publisher that signs strict RFC 8785.
func TestJCSFacts_CrossVerification(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	strictVerify := func(doc []byte, sigB64 string) bool {
		sig, _ := base64.StdEncoding.DecodeString(sigB64)
		return ed25519.Verify(pub, strictJCS(t, doc, "signature"), sig)
	}
	strictSign := func(doc []byte) string {
		return base64.StdEncoding.EncodeToString(ed25519.Sign(priv, strictJCS(t, doc, "signature")))
	}
	for _, tc := range []struct {
		name, doc string
		interwork bool
	}{
		{"plain content", `{"catalogId":"a.example/CAT-1","catalog":{"descriptor":{"name":"Rice 5kg"}}}`, true},
		{"content with & < >", `{"catalogId":"a.example/CAT-1","catalog":{"descriptor":{"name":"Fish & Chips <large>"}}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := []byte(tc.doc)
			oursSig, err := artifactsigner.SignJSON(doc, "signature", priv)
			if err != nil {
				t.Fatal(err)
			}
			if err := artifactverifier.VerifyJSON(doc, "signature", oursSig, pub); err != nil {
				t.Fatalf("this module does not verify its own signature: %v", err)
			}
			if got := strictVerify(doc, oursSig); got != tc.interwork {
				t.Errorf("a strict RFC 8785 verifier accepts this module's signature: %v, recorded as %v", got, tc.interwork)
			}
			if got := artifactverifier.VerifyJSON(doc, "signature", strictSign(doc), pub) == nil; got != tc.interwork {
				t.Errorf("this module accepts a strict RFC 8785 signature: %v, recorded as %v", got, tc.interwork)
			}
		})
	}
}

// An integer above 2^53 is rounded before signing, so two documents that
// differ only in such a number have the same signature. No field of an
// index entry or file envelope gets that large (versions, sizes); catalog
// content could carry one. A file's digest still covers its exact bytes.
func TestJCSFacts_LargeIntegersAreRoundedBeforeSigning(t *testing.T) {
	a, b := ours(t, `{"n":9007199254740993}`), ours(t, `{"n":9007199254740992}`)
	if !bytes.Equal(a, b) {
		t.Errorf("recorded as equal; got %s and %s", a, b)
	}
}
