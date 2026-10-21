package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Noy-Meir/buyer-throttling/internal/evaluator"
)

// Key layout:
//
//	stats:{buyer}:{window}  hash  ok, failed, timeout counters for one window
//	state:{buyer}           hash  rate, reason, last_window, carry_* fields
//	buyers                  set   every buyer that has reported stats
const (
	buyersKey    = "buyers"
	heartbeatKey = "evaluator:heartbeat"

	fieldOK      = "ok"
	fieldFailed  = "failed"
	fieldTimeout = "timeout"

	fieldRate         = "rate"
	fieldReason       = "reason"
	fieldLastWindow   = "last_window"
	fieldCarryTotal   = "carry_total"
	fieldCarryFailed  = "carry_failed"
	fieldCarryTimeout = "carry_timeout"
	fieldCarryWindows = "carry_windows"
)

// casScript writes a buyer's state only if its last evaluated window is earlier
// than the window being written. Redis runs a script atomically, so the check
// and the write cannot interleave with another evaluator's write.
//
// Each successful write also sets the state key's expiry, so an evaluated buyer
// never expires while it is still being written.
//
// KEYS[1] = state key
// ARGV    = window, rate, reason, carry_total, carry_failed, carry_timeout,
//
//	carry_windows, state TTL in milliseconds
var casScript = redis.NewScript(`
local last = tonumber(redis.call('HGET', KEYS[1], 'last_window') or '0')
local window = tonumber(ARGV[1])
if last >= window then
  return 0
end
redis.call('HSET', KEYS[1],
  'last_window', ARGV[1],
  'rate', ARGV[2],
  'reason', ARGV[3],
  'carry_total', ARGV[4],
  'carry_failed', ARGV[5],
  'carry_timeout', ARGV[6],
  'carry_windows', ARGV[7])
redis.call('PEXPIRE', KEYS[1], ARGV[8])
return 1
`)

// RedisStore implements Store on Redis 7 with go-redis/v9.
type RedisStore struct {
	client   redis.UniversalClient
	statsTTL time.Duration
	stateTTL time.Duration
}

// NewRedisStore returns a Store backed by client. Stats keys expire statsTTL
// after their last write, long enough for the evaluator to read a closed window.
// The state key and the buyers set expire stateTTL after their last write.
func NewRedisStore(client redis.UniversalClient, statsTTL, stateTTL time.Duration) *RedisStore {
	return &RedisStore{client: client, statsTTL: statsTTL, stateTTL: stateTTL}
}

var _ Store = (*RedisStore)(nil)

func statsKey(buyerID string, window int64) string {
	return fmt.Sprintf("stats:%s:%d", buyerID, window)
}

func stateKey(buyerID string) string {
	return "state:" + buyerID
}

// IncrStats applies the batch in one MULTI/EXEC transaction, so a batch lands
// entirely or not at all.
func (s *RedisStore) IncrStats(ctx context.Context, batch []Increment) error {
	if len(batch) == 0 {
		return nil
	}
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, inc := range batch {
			key := statsKey(inc.BuyerID, inc.Window)
			if inc.OK != 0 {
				pipe.HIncrBy(ctx, key, fieldOK, inc.OK)
			}
			if inc.Failed != 0 {
				pipe.HIncrBy(ctx, key, fieldFailed, inc.Failed)
			}
			if inc.Timeout != 0 {
				pipe.HIncrBy(ctx, key, fieldTimeout, inc.Timeout)
			}
			pipe.Expire(ctx, key, s.statsTTL)
			pipe.SAdd(ctx, buyersKey, inc.BuyerID)
		}
		// Refreshed on every batch. The set expires only when no buyer has sent
		// stats for stateTTL.
		pipe.Expire(ctx, buyersKey, s.stateTTL)
		return nil
	})
	if err != nil {
		return fmt.Errorf("store: incr stats: %w", err)
	}
	return nil
}

// WindowStats reads the three counters and derives Total from them.
func (s *RedisStore) WindowStats(ctx context.Context, buyerID string, window int64) (evaluator.WindowStats, error) {
	vals, err := s.client.HMGet(ctx, statsKey(buyerID, window), fieldOK, fieldFailed, fieldTimeout).Result()
	if err != nil {
		return evaluator.WindowStats{}, fmt.Errorf("store: window stats: %w", err)
	}
	var counts [3]int64
	for i, v := range vals {
		counts[i], err = parseCount(v)
		if err != nil {
			return evaluator.WindowStats{}, fmt.Errorf("store: window stats %s: %w", buyerID, err)
		}
	}
	ok, failed, timeout := counts[0], counts[1], counts[2]
	return evaluator.WindowStats{
		Total:   ok + failed + timeout,
		Failed:  failed,
		Timeout: timeout,
	}, nil
}

// Buyers returns the known buyers in sorted order, so output is stable.
func (s *RedisStore) Buyers(ctx context.Context) ([]string, error) {
	ids, err := s.client.SMembers(ctx, buyersKey).Result()
	if err != nil {
		return nil, fmt.Errorf("store: buyers: %w", err)
	}
	sort.Strings(ids)
	return ids, nil
}

// State returns one buyer's snapshot, or the zero Snapshot if none is stored.
func (s *RedisStore) State(ctx context.Context, buyerID string) (Snapshot, error) {
	fields, err := s.client.HGetAll(ctx, stateKey(buyerID)).Result()
	if err != nil {
		return Snapshot{}, fmt.Errorf("store: state %s: %w", buyerID, err)
	}
	snap, err := decodeSnapshot(buyerID, fields)
	if err != nil {
		return Snapshot{}, fmt.Errorf("store: state %s: %w", buyerID, err)
	}
	return snap, nil
}

// AllStates reads every known buyer's state in one pipelined round trip.
func (s *RedisStore) AllStates(ctx context.Context) ([]Snapshot, error) {
	ids, err := s.Buyers(ctx)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	_, err = s.client.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, id := range ids {
			cmds[i] = pipe.HGetAll(ctx, stateKey(id))
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("store: all states: %w", err)
	}
	snaps := make([]Snapshot, 0, len(ids))
	for i, id := range ids {
		snap, err := decodeSnapshot(id, cmds[i].Val())
		if err != nil {
			return nil, fmt.Errorf("store: all states, buyer %s: %w", id, err)
		}
		snaps = append(snaps, snap)
	}
	return snaps, nil
}

// CompareAndSetState runs casScript. It returns false, with no error, when the
// window was already evaluated.
// Beat increments the heartbeat counter. Each completed evaluator tick calls it.
func (s *RedisStore) Beat(ctx context.Context) (int64, error) {
	v, err := s.client.Incr(ctx, heartbeatKey).Result()
	if err != nil {
		return 0, fmt.Errorf("store: beat: %w", err)
	}
	return v, nil
}

// Heartbeat reads the heartbeat counter. A missing key is ok=false, not an error.
func (s *RedisStore) Heartbeat(ctx context.Context) (int64, bool, error) {
	v, err := s.client.Get(ctx, heartbeatKey).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("store: heartbeat: %w", err)
	}
	return v, true, nil
}

func (s *RedisStore) CompareAndSetState(ctx context.Context, buyerID string, window int64, next evaluator.State) (bool, error) {
	n, err := casScript.Run(ctx, s.client, []string{stateKey(buyerID)},
		window,
		next.ThrottleRateBP,
		string(next.Reason),
		next.CarriedStats.Total,
		next.CarriedStats.Failed,
		next.CarriedStats.Timeout,
		next.CarriedWindows,
		s.stateTTL.Milliseconds(),
	).Int()
	if err != nil {
		return false, fmt.Errorf("store: cas state %s: %w", buyerID, err)
	}
	return n == 1, nil
}

// parseCount converts an HMGET value to a count. A missing field is nil and
// counts as zero.
func parseCount(v any) (int64, error) {
	if v == nil {
		return 0, nil
	}
	str, ok := v.(string)
	if !ok {
		return 0, fmt.Errorf("unexpected reply type %T", v)
	}
	return strconv.ParseInt(str, 10, 64)
}

// decodeSnapshot turns a state hash into a Snapshot. An empty hash is the
// never-evaluated zero state.
func decodeSnapshot(buyerID string, f map[string]string) (Snapshot, error) {
	if len(f) == 0 {
		return Snapshot{BuyerID: buyerID}, nil
	}
	var (
		snap Snapshot
		err  error
		rate int64
	)
	snap.BuyerID = buyerID
	parse := func(field string, dst *int64) {
		if err != nil {
			return
		}
		*dst, err = strconv.ParseInt(f[field], 10, 64)
		if err != nil {
			err = fmt.Errorf("field %s: %w", field, err)
		}
	}
	parse(fieldRate, &rate)
	parse(fieldLastWindow, &snap.LastEvaluatedWindow)
	parse(fieldCarryTotal, &snap.State.CarriedStats.Total)
	parse(fieldCarryFailed, &snap.State.CarriedStats.Failed)
	parse(fieldCarryTimeout, &snap.State.CarriedStats.Timeout)

	var windows int64
	parse(fieldCarryWindows, &windows)
	if err != nil {
		return Snapshot{}, err
	}
	snap.State.ThrottleRateBP = int(rate)
	snap.State.CarriedWindows = int(windows)
	snap.State.Reason = evaluator.Reason(f[fieldReason])
	return snap, nil
}
