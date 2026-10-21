package auction

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/Noy-Meir/buyer-throttling/internal/throttler"
)

// HTTPCaller calls buyers over HTTP at baseURL/bid/{buyerID}. The deadline comes
// from the context the Service passes in, so the caller sets no timeout of its own.
type HTTPCaller struct {
	baseURL string
	client  *http.Client
}

// NewHTTPCaller returns a caller that uses client.
func NewHTTPCaller(baseURL string, client *http.Client) *HTTPCaller {
	return &HTTPCaller{baseURL: strings.TrimRight(baseURL, "/"), client: client}
}

// Call posts the auction to the buyer and classifies the outcome. A context
// deadline is a timeout. A transport error, a non-2xx status or an unreadable
// body is a failure.
func (c *HTTPCaller) Call(ctx context.Context, buyerID, auctionID string) (float64, throttler.Result) {
	body, _ := json.Marshal(map[string]string{"auction_id": auctionID})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/bid/"+buyerID, bytes.NewReader(body))
	if err != nil {
		return 0, throttler.ResultFailed
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, classify(ctx, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return 0, throttler.ResultFailed
	}
	var out struct {
		Price float64 `json:"price"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return 0, classify(ctx, err)
	}
	return out.Price, throttler.ResultOK
}

// classify separates a timeout from other failures. A response can arrive
// after the deadline has already fired, so it checks the context first.
func classify(ctx context.Context, err error) throttler.Result {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return throttler.ResultTimeout
	}
	return throttler.ResultFailed
}
