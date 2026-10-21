package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func get(t *testing.T, h http.Handler) (int, map[string]any, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/state", nil))
	body, _ := io.ReadAll(rec.Body)
	var out map[string]any
	if rec.Code == http.StatusOK {
		require.NoError(t, json.Unmarshal(body, &out))
	}
	return rec.Code, out, string(body)
}

func TestStateFormatMatchesAssignment(t *testing.T) {
	h := StateHandler(func(context.Context) ([]Entry, error) {
		return []Entry{{BuyerID: "buyer_123", RateBP: 9500, Reason: "failure_rate"}}, nil
	}, nil)

	code, _, body := get(t, h)
	assert.Equal(t, http.StatusOK, code)
	assert.JSONEq(t, `{"buyers":[{"buyer_id":"buyer_123","throttle_rate":0.95,"reason":"failure_rate"}]}`, body)
}

func TestStateSortsBuyersByID(t *testing.T) {
	h := StateHandler(func(context.Context) ([]Entry, error) {
		return []Entry{
			{BuyerID: "buyer_789"},
			{BuyerID: "buyer_123", RateBP: 9500, Reason: "failure_rate"},
			{BuyerID: "buyer_456", RateBP: 3500, Reason: "recovering"},
		}, nil
	}, nil)

	_, _, body := get(t, h)
	assert.JSONEq(t, `{"buyers":[
		{"buyer_id":"buyer_123","throttle_rate":0.95,"reason":"failure_rate"},
		{"buyer_id":"buyer_456","throttle_rate":0.35,"reason":"recovering"},
		{"buyer_id":"buyer_789","throttle_rate":0,"reason":""}
	]}`, body)
}

func TestStateReasonAndRateMapping(t *testing.T) {
	tests := []struct {
		name  string
		entry Entry
		want  string
	}{
		{"failure rate block", Entry{BuyerID: "b", RateBP: 9500, Reason: "failure_rate"}, `{"buyer_id":"b","throttle_rate":0.95,"reason":"failure_rate"}`},
		{"timeout rate block", Entry{BuyerID: "b", RateBP: 9500, Reason: "timeout_rate"}, `{"buyer_id":"b","throttle_rate":0.95,"reason":"timeout_rate"}`},
		{"recovering", Entry{BuyerID: "b", RateBP: 500, Reason: "recovering"}, `{"buyer_id":"b","throttle_rate":0.05,"reason":"recovering"}`},
		{"open", Entry{BuyerID: "b"}, `{"buyer_id":"b","throttle_rate":0,"reason":""}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := StateHandler(func(context.Context) ([]Entry, error) { return []Entry{tc.entry}, nil }, nil)
			_, _, body := get(t, h)
			assert.JSONEq(t, `{"buyers":[`+tc.want+`]}`, body)
		})
	}
}

func TestStateEmptyIsEmptyArrayNotNull(t *testing.T) {
	h := StateHandler(func(context.Context) ([]Entry, error) { return nil, nil }, nil)
	_, _, body := get(t, h)
	assert.JSONEq(t, `{"buyers":[]}`, body)
}

func TestStateReturns503WhenListerFails(t *testing.T) {
	h := StateHandler(func(context.Context) ([]Entry, error) { return nil, errors.New("redis down") }, nil)
	code, _, body := get(t, h)
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.NotContains(t, body, "redis down", "internal errors are logged, not returned to the caller")
}
