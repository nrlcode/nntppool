package nntppool

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestF451CMappedRetryPreservesDifferentFinalOutcome(t *testing.T) {
	server := &regressionProvider{
		host: "f451c-mixed-retry.invalid:119",
		respond: func(connection int, _ string) []byte {
			if connection == 1 {
				return []byte("451 provider-mapped article absence\r\n")
			}
			return []byte("499 different retry outcome\r\n")
		},
	}
	provider, _ := f451cProvider(server, "f451c-mixed-retry", f451cPolicyAbsentAfterRetry)
	client := f451cClient(t, provider)

	_, err := client.Stat(context.Background(), "mixed-retry@example.invalid")
	transportErr := f451cRequireTransportError(t, err, OutcomeInconclusive)
	if errors.Is(err, ErrArticleNotFound) {
		t.Fatalf("mixed retry error = %v, must not collapse to article absence", err)
	}
	if got := server.commandCount("STAT"); got != 2 {
		t.Fatalf("STAT attempts = %d, want mapped response plus fresh retry", got)
	}
	if transportErr == nil || len(transportErr.Attempts) != 2 {
		t.Fatalf("mixed retry evidence = %+v, want two ordered attempts", transportErr)
	}
	first, second := transportErr.Attempts[0], transportErr.Attempts[1]
	if first.Operation != OperationStat || first.Outcome != OutcomeHardArticleAbsence || first.ResponseCode != 451 {
		t.Errorf("first attempt = %+v, want mapped hard-absence STAT 451", first)
	}
	if second.Operation != OperationStat || second.Outcome != OutcomeInconclusive || second.ResponseCode != 499 {
		t.Errorf("second attempt = %+v, want distinct inconclusive STAT 499", second)
	}
}

func TestF451CCancellationAfterMappedRetryDispatch(t *testing.T) {
	retryDispatched := make(chan struct{})
	releaseRetry := make(chan struct{})
	var dispatchOnce sync.Once
	primary := &regressionProvider{
		host: "f451c-cancel-primary.invalid:119",
		respond: func(connection int, command string) []byte {
			if connection == 1 {
				return []byte("451 provider-mapped article absence\r\n")
			}
			dispatchOnce.Do(func() { close(retryDispatched) })
			<-releaseRetry
			return []byte("451 late response after cancellation\r\n")
		},
	}
	backup := &regressionProvider{
		host: "f451c-cancel-backup.invalid:119",
		respond: func(_ int, _ string) []byte {
			return yencSinglePart([]byte("backup must remain untouched"), "backup.bin")
		},
	}
	primaryProvider, _ := f451cProvider(primary, "f451c-cancel-primary", f451cPolicyAbsentAfterRetry)
	backupProvider := backup.provider(true)
	backupProvider.ID = "f451c-cancel-backup"
	client := f451cClient(t, primaryProvider, backupProvider)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := client.Body(ctx, "cancel-retry@example.invalid")
		result <- err
	}()
	<-retryDispatched
	cancel()
	err := <-result
	close(releaseRetry)

	transportErr := f451cRequireTransportError(t, err, OutcomeCancellation)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Body() error = %v, want caller cancellation", err)
	}
	if got := primary.commandCount("BODY"); got != 2 {
		t.Errorf("primary BODY attempts = %d, want cancellation after retry dispatch", got)
	}
	if got := backup.commandCount("BODY"); got != 0 {
		t.Errorf("backup BODY attempts = %d, want untouched", got)
	}
	if transportErr == nil || len(transportErr.Attempts) != 2 {
		t.Fatalf("cancellation evidence = %+v, want two ordered attempts", transportErr)
	}
	first, second := transportErr.Attempts[0], transportErr.Attempts[1]
	if first.ProviderID != "f451c-cancel-primary" || first.Operation != OperationBody ||
		first.Outcome != OutcomeHardArticleAbsence || first.ResponseCode != 451 {
		t.Errorf("first attempt = %+v, want mapped hard-absence BODY 451", first)
	}
	if second.ProviderID != "f451c-cancel-primary" || second.Operation != OperationBody ||
		second.Outcome != OutcomeCancellation || !errors.Is(second.Cause, context.Canceled) {
		t.Errorf("second attempt = %+v, want cancellation on dispatched fresh retry", second)
	}
}
