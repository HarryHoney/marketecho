package dedup

import (
	"crypto/sha256"
	"encoding/hex"
	"hash/fnv"
	"math/bits"
	"strings"
	"unicode"
)

var stopWords = map[string]struct{}{
	"the": {}, "a": {}, "an": {}, "and": {}, "or": {}, "but": {}, "in": {},
	"on": {}, "at": {}, "to": {}, "for": {}, "of": {}, "with": {}, "by": {},
	"from": {}, "as": {}, "is": {}, "was": {}, "are": {}, "were": {}, "be": {},
	"been": {}, "has": {}, "have": {}, "had": {}, "that": {}, "this": {}, "it": {},
	"reuters": {}, "cnbc": {}, "yahoo": {}, "bloomberg": {}, "associated": {}, "press": {},
	"reported": {}, "reports": {}, "according": {}, "said": {}, "says": {},
}

// ComputeSHA256Key returns a hex-encoded SHA-256 digest of normalized Title + URL.
func ComputeSHA256Key(title, url string) string {
	normTitle := strings.ToLower(strings.TrimSpace(title))
	normURL := strings.ToLower(strings.TrimSpace(url))
	sum := sha256.Sum256([]byte(normTitle + "|" + normURL))
	return hex.EncodeToString(sum[:])
}

// SimHash computes a 64-bit locality-sensitive SimHash fingerprint from the given text.
// It uses normalized word tokens and 2-word shingles hashed via 64-bit FNV-1a.
func SimHash(text string) uint64 {
	tokens := tokenize(text)
	if len(tokens) == 0 {
		return 0
	}

	var weights [64]int

	// Weight individual content words (weight = 2) and 2-word shingles (weight = 1)
	for i, tok := range tokens {
		addFeatureHash(&weights, tok, 2)
		if i+1 < len(tokens) {
			shingle := tok + " " + tokens[i+1]
			addFeatureHash(&weights, shingle, 1)
		}
	}

	var fingerprint uint64
	for bit := 0; bit < 64; bit++ {
		if weights[bit] > 0 {
			fingerprint |= (1 << uint(bit))
		}
	}
	return fingerprint
}

// HammingDistance returns the number of differing bits between two 64-bit SimHash values.
func HammingDistance(a, b uint64) int {
	return bits.OnesCount64(a ^ b)
}

func addFeatureHash(weights *[64]int, feature string, weight int) {
	h := fnv.New64a()
	_, _ = h.Write([]byte(feature))
	val := h.Sum64()
	for bit := 0; bit < 64; bit++ {
		if (val & (1 << uint(bit))) != 0 {
			weights[bit] += weight
		} else {
			weights[bit] -= weight
		}
	}
}

func tokenize(text string) []string {
	lower := strings.ToLower(text)
	fields := strings.FieldsFunc(lower, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	tokens := make([]string, 0, len(fields))
	for _, f := range fields {
		if len(f) <= 1 {
			continue
		}
		if _, isStop := stopWords[f]; isStop {
			continue
		}
		tokens = append(tokens, f)
	}
	return tokens
}
