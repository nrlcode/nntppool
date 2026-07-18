package nntppool_test

import (
	"context"
	"reflect"
	"testing"

	nntppool "github.com/javi11/nntppool/v4"
)

func TestFNCORERequestDispatchControlsRemainPrivate(t *testing.T) {
	requestType := reflect.TypeOf(nntppool.Request{})
	for _, field := range []string{"ValidateBody", "FreshTransport", "Priority"} {
		if _, exists := requestType.FieldByName(field); exists {
			t.Errorf("unsupported transport control Request.%s remains exported", field)
		}
	}

	// Preserve the supported keyed Request surface used by the existing external
	// compatibility fixture while internal dispatch controls move private.
	responseChannel := make(chan nntppool.Response, 1)
	request := nntppool.Request{
		Ctx:     context.Background(),
		Payload: []byte("DATE\r\n"),
		RespCh:  responseChannel,
	}
	if request.Ctx == nil || string(request.Payload) != "DATE\r\n" || request.RespCh != responseChannel {
		t.Fatalf("supported keyed Request fields changed: ctx-nil=%v payload=%q response-channel-match=%v",
			request.Ctx == nil, request.Payload, request.RespCh == responseChannel)
	}
}
