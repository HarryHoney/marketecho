package dedup

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"news-fetcher/internal/model"
	"strconv"
	"strings"
	"time"
)

// RedisDeduplicator implements Deduplicator backed by Redis Stack using
// SETNX + TTL for SHA-256 exact keys and a sorted set (ZSET) scored by expiration
// unix timestamp for active 64-bit SimHash fingerprints.
type RedisDeduplicator struct {
	addr        string
	ttl         time.Duration
	maxDistance int
	dialTimeout time.Duration
}

const simhashZSetKey = "dedup:l1:simhash_zset"

func NewRedisDeduplicator(host string, port string, ttl time.Duration) *RedisDeduplicator {
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "6379"
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &RedisDeduplicator{
		addr:        net.JoinHostPort(host, port),
		ttl:         ttl,
		maxDistance: DefaultSimHashMaxHammingDistance,
		dialTimeout: 3 * time.Second,
	}
}

func (r *RedisDeduplicator) Ping(ctx context.Context) error {
	resp, err := r.execCommand(ctx, "PING")
	if err != nil {
		return err
	}
	if resp != "PONG" {
		return fmt.Errorf("unexpected redis PING response: %q", resp)
	}
	return nil
}

func (r *RedisDeduplicator) IsDuplicate(ctx context.Context, key string) (bool, error) {
	redisKey := "dedup:l1:sha256:" + key
	ttlSeconds := strconv.Itoa(int(r.ttl.Seconds()))

	// SET key 1 NX EX <ttlSeconds>
	// Returns "OK" if set (new key), or "" (nil bulk string) if key already existed.
	resp, err := r.execCommand(ctx, "SET", redisKey, "1", "NX", "EX", ttlSeconds)
	if err != nil {
		return false, err
	}
	return resp == "", nil
}

func (r *RedisDeduplicator) IsArticleDuplicate(ctx context.Context, article *model.Article) (bool, string, error) {
	shaKey := ComputeSHA256Key(article.GetTitle(), article.GetUrl())

	// 1. Check exact SHA-256 via Redis SET NX EX
	isExactDup, err := r.IsDuplicate(ctx, shaKey)
	if err != nil {
		return false, "", err
	}
	if isExactDup {
		return true, "sha256", nil
	}

	// 2. Check 64-bit SimHash near-duplicate against active fingerprints in Redis ZSET
	sim := SimHash(article.GetTitle() + " " + article.GetDescription() + " " + article.GetContent())
	if sim == 0 {
		return false, "", nil
	}

	nowUnix := time.Now().Unix()
	expUnix := time.Now().Add(r.ttl).Unix()
	ttlSeconds := strconv.Itoa(int(r.ttl.Seconds()))

	// Prune expired SimHash entries: ZREMRANGEBYSCORE dedup:l1:simhash_zset -inf <nowUnix>
	_, _ = r.execCommand(ctx, "ZREMRANGEBYSCORE", simhashZSetKey, "-inf", strconv.FormatInt(nowUnix, 10))

	// Fetch active SimHash fingerprints: ZRANGE dedup:l1:simhash_zset 0 -1
	existingHexList, err := r.execArrayCommand(ctx, "ZRANGE", simhashZSetKey, "0", "-1")
	if err != nil {
		return false, "", err
	}

	for _, hexVal := range existingHexList {
		existingHash, parseErr := strconv.ParseUint(hexVal, 16, 64)
		if parseErr != nil {
			continue
		}
		if HammingDistance(sim, existingHash) <= r.maxDistance {
			return true, "simhash", nil
		}
	}

	// Record this new SimHash fingerprint with score = expiration timestamp & refresh ZSET key TTL
	simHex := fmt.Sprintf("%016x", sim)
	_, _ = r.execCommand(ctx, "ZADD", simhashZSetKey, strconv.FormatInt(expUnix, 10), simHex)
	_, _ = r.execCommand(ctx, "EXPIRE", simhashZSetKey, ttlSeconds)

	return false, "", nil
}

func (r *RedisDeduplicator) dial(ctx context.Context) (net.Conn, error) {
	d := net.Dialer{Timeout: r.dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", r.addr)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(r.dialTimeout))
	}
	return conn, nil
}

func writeRESPCommand(w io.Writer, args ...string) error {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	_, err := io.WriteString(w, sb.String())
	return err
}

func (r *RedisDeduplicator) execCommand(ctx context.Context, args ...string) (string, error) {
	conn, err := r.dial(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	if err := writeRESPCommand(conn, args...); err != nil {
		return "", err
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if len(line) == 0 {
		return "", fmt.Errorf("empty RESP response")
	}

	switch line[0] {
	case '+': // Simple string
		return line[1:], nil
	case ':': // Integer
		return line[1:], nil
	case '-': // Error
		return "", fmt.Errorf("redis error: %s", line[1:])
	case '$': // Bulk string
		length, err := strconv.Atoi(line[1:])
		if err != nil {
			return "", err
		}
		if length < 0 {
			return "", nil // Nil bulk string
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return "", err
		}
		return string(buf[:length]), nil
	default:
		return "", fmt.Errorf("unexpected RESP prefix: %q", line)
	}
}

func (r *RedisDeduplicator) execArrayCommand(ctx context.Context, args ...string) ([]string, error) {
	conn, err := r.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := writeRESPCommand(conn, args...); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if len(line) == 0 {
		return nil, fmt.Errorf("empty RESP array response")
	}
	if line[0] == '-' {
		return nil, fmt.Errorf("redis error: %s", line[1:])
	}
	if line[0] != '*' {
		return nil, fmt.Errorf("expected RESP array '*', got %q", line)
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil || count <= 0 {
		return nil, err
	}

	result := make([]string, 0, count)
	for i := 0; i < count; i++ {
		hdr, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimSuffix(strings.TrimSuffix(hdr, "\n"), "\r")
		if len(hdr) == 0 || hdr[0] != '$' {
			return nil, fmt.Errorf("expected bulk string '$', got %q", hdr)
		}
		length, err := strconv.Atoi(hdr[1:])
		if err != nil {
			return nil, err
		}
		if length < 0 {
			continue
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return nil, err
		}
		result = append(result, string(buf[:length]))
	}
	return result, nil
}
