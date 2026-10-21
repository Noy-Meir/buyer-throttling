package buyersim

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scriptedRandom returns fixed draws in order.
type scriptedRandom struct {
	values []int
	i      int
}

func (r *scriptedRandom) IntN(n int) int {
	v := r.values[r.i%len(r.values)]
	r.i++
	return v
}

func TestParseSpec(t *testing.T) {
	got, err := ParseSpec("buyer_good:failure=0,timeout=0,participation=1; buyer_bad:failure=0.8 ;buyer_quiet:participation=0.05")
	require.NoError(t, err)
	assert.Equal(t, map[string]Behavior{
		"buyer_good":  {FailureRate: 0, TimeoutRate: 0, Participation: 1},
		"buyer_bad":   {FailureRate: 0.8, Participation: 1}, // participation defaults to 1
		"buyer_quiet": {Participation: 0.05},
	}, got)
}

func TestParseSpecRejectsBadInput(t *testing.T) {
	tests := map[string]string{
		"empty":            "",
		"no colon":         "buyer_good",
		"duplicate":        "a:failure=0;a:failure=0.1",
		"not key=value":    "a:failure",
		"bad number":       "a:failure=high",
		"unknown key":      "a:latency=3",
		"out of range":     "a:failure=1.5",
		"sum above one":    "a:failure=0.7,timeout=0.5",
		"negative partic.": "a:participation=-1",
	}
	for name, spec := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := ParseSpec(spec)
			assert.Error(t, err)
		})
	}
}

func TestRegistryUpdateIsPartialAndAllOrNothing(t *testing.T) {
	reg, err := NewRegistry(map[string]Behavior{"b": {Participation: 1}})
	require.NoError(t, err)

	fail := 0.8
	got, err := reg.Update("b", Patch{FailureRate: &fail})
	require.NoError(t, err)
	assert.Equal(t, Behavior{FailureRate: 0.8, Participation: 1}, got, "untouched fields keep their value")

	bad := 0.9
	timeout := 0.5 // 0.9 + 0.5 > 1 together with failure, so this must be rejected
	_, err = reg.Update("b", Patch{FailureRate: &bad, TimeoutRate: &timeout})
	require.Error(t, err)
	cur, _ := reg.Get("b")
	assert.Equal(t, 0.8, cur.FailureRate, "a rejected update changes nothing")
	assert.Equal(t, 0.0, cur.TimeoutRate)
}

func TestRegistryUnknownBuyer(t *testing.T) {
	reg, err := NewRegistry(map[string]Behavior{"b": {}})
	require.NoError(t, err)
	_, err = reg.Update("nope", Patch{})
	assert.ErrorIs(t, err, ErrUnknownBuyer)
}

func newTestServer(t *testing.T, draws []int, behaviors map[string]Behavior) (*httptest.Server, *Registry) {
	t.Helper()
	reg, err := NewRegistry(behaviors)
	require.NoError(t, err)
	srv := NewServer(reg, &scriptedRandom{values: draws}, time.Hour, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, reg
}

func bidStatus(t *testing.T, base, id string) (int, string) {
	t.Helper()
	resp, err := http.Post(base+"/bid/"+id, "application/json", strings.NewReader(`{"auction_id":"a"}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestBidPriceIsReturnedOnSuccess(t *testing.T) {
	// Draws: 0 for the outcome (no timeout, no failure at rate 0), then 250 for the price.
	ts, _ := newTestServer(t, []int{5000, 250}, map[string]Behavior{"b": {Participation: 1}})
	status, body := bidStatus(t, ts.URL, "b")
	require.Equal(t, http.StatusOK, status)
	var resp bidResponse
	require.NoError(t, json.Unmarshal([]byte(body), &resp))
	assert.Equal(t, 3.5, resp.Price)
}

func TestBidFailureRange(t *testing.T) {
	ts, _ := newTestServer(t, []int{0, 2999, 3000, 3000}, map[string]Behavior{
		"b": {FailureRate: 0.3, Participation: 1},
	})
	// Draws 0 and 2999 are below 3000, so they fail. Draw 3000 bids.
	// The price draw is the fourth value, so it is not consumed here.
	status, _ := bidStatus(t, ts.URL, "b")
	assert.Equal(t, http.StatusInternalServerError, status)
	status, _ = bidStatus(t, ts.URL, "b")
	assert.Equal(t, http.StatusInternalServerError, status)
	status, _ = bidStatus(t, ts.URL, "b")
	assert.Equal(t, http.StatusOK, status)
}

func TestBidUnknownBuyerIs404(t *testing.T) {
	ts, _ := newTestServer(t, []int{0}, map[string]Behavior{"b": {}})
	status, _ := bidStatus(t, ts.URL, "missing")
	assert.Equal(t, http.StatusNotFound, status)
}

func TestTimeoutOutcomeHonoursClientCancel(t *testing.T) {
	// timeout rate 1 with a one-hour delay: the handler must return as soon as
	// the client gives up, not after the delay.
	reg, err := NewRegistry(map[string]Behavior{"b": {TimeoutRate: 1, Participation: 1}})
	require.NoError(t, err)
	srv := NewServer(reg, &scriptedRandom{values: []int{0}}, time.Hour, nil)

	req := httptest.NewRequest(http.MethodPost, "/bid/b", nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	assert.Empty(t, rec.Body.String())
}

func TestAdminListAndPatch(t *testing.T) {
	ts, _ := newTestServer(t, []int{0}, map[string]Behavior{
		"buyer_bad": {Participation: 1},
	})

	resp, err := http.Post(ts.URL+"/admin/buyers/buyer_bad", "application/json",
		strings.NewReader(`{"failure_rate":0.8}`))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	list, err := http.Get(ts.URL + "/admin/buyers")
	require.NoError(t, err)
	defer list.Body.Close()
	var out struct {
		Buyers []Entry `json:"buyers"`
	}
	require.NoError(t, json.NewDecoder(list.Body).Decode(&out))
	require.Len(t, out.Buyers, 1)
	assert.Equal(t, 0.8, out.Buyers[0].FailureRate)
	assert.Equal(t, 1.0, out.Buyers[0].Participation)
}

func TestAdminRejectsBadRequests(t *testing.T) {
	ts, _ := newTestServer(t, []int{0}, map[string]Behavior{"b": {}})
	tests := []struct {
		name   string
		path   string
		body   string
		status int
	}{
		{"unknown buyer", "/admin/buyers/nope", `{"failure_rate":0.1}`, http.StatusNotFound},
		{"unknown field", "/admin/buyers/b", `{"latency":3}`, http.StatusBadRequest},
		{"malformed JSON", "/admin/buyers/b", `{`, http.StatusBadRequest},
		{"out of range", "/admin/buyers/b", `{"failure_rate":2}`, http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Post(ts.URL+tc.path, "application/json", strings.NewReader(tc.body))
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Equal(t, tc.status, resp.StatusCode)
		})
	}
}
