package auction

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Noy-Meir/buyer-throttling/internal/throttler"
)

func TestHTTPCallerClassifiesOutcomes(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/bid/ok":
			_, _ = w.Write([]byte(`{"price": 4.5}`))
		case "/bid/error":
			http.Error(w, "boom", http.StatusInternalServerError)
		case "/bid/garbage":
			_, _ = w.Write([]byte(`not json`))
		case "/bid/slow":
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	caller := NewHTTPCaller(srv.URL+"/", srv.Client()) // trailing slash is trimmed
	tests := []struct {
		name      string
		buyer     string
		timeout   time.Duration
		wantPrice float64
		wantRes   throttler.Result
	}{
		{"success returns the price", "ok", time.Second, 4.5, throttler.ResultOK},
		{"5xx is a failure", "error", time.Second, 0, throttler.ResultFailed},
		{"unreadable body is a failure", "garbage", time.Second, 0, throttler.ResultFailed},
		{"deadline passing is a timeout", "slow", 20 * time.Millisecond, 0, throttler.ResultTimeout},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), tc.timeout)
			defer cancel()
			price, res := caller.Call(ctx, tc.buyer, "auction-1")
			assert.Equal(t, tc.wantPrice, price)
			assert.Equal(t, tc.wantRes, res)
		})
	}
}

func TestHTTPCallerConnectionErrorIsFailure(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // nothing is listening any more

	_, res := NewHTTPCaller(url, http.DefaultClient).Call(context.Background(), "b", "a")
	assert.Equal(t, throttler.ResultFailed, res)
}
