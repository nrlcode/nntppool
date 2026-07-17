package nntppool

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// OutcomeKind is the transport-owned classification of one provider attempt
// or a final pool result.
type OutcomeKind string

const (
	OutcomeSuccess             OutcomeKind = "success"
	OutcomeHardArticleAbsence  OutcomeKind = "hard_article_absence"
	OutcomeTemporaryFailure    OutcomeKind = "temporary_failure"
	OutcomeProviderUnavailable OutcomeKind = "provider_unavailable"
	OutcomeCorruptBody         OutcomeKind = "corrupt_body"
	OutcomeCancellation        OutcomeKind = "cancellation"
	OutcomeTransportFailure    OutcomeKind = "transport_failure"
	OutcomeInconclusive        OutcomeKind = "inconclusive"
)

const outcomeLocalFailure OutcomeKind = "local_failure"

// Operation identifies the NNTP command represented by attempt evidence.
type Operation string

const (
	OperationUnknown Operation = "UNKNOWN"
	OperationBody    Operation = "BODY"
	OperationStat    Operation = "STAT"
	OperationHead    Operation = "HEAD"
	OperationPost    Operation = "POST"
)

const operationArticle Operation = "ARTICLE"

// BodyValidationStatus records whether a BODY attempt crossed the complete
// transport validation boundary.
type BodyValidationStatus string

const (
	BodyValidationNotApplicable BodyValidationStatus = "not_applicable"
	BodyValidationNotRequested  BodyValidationStatus = "not_requested"
	BodyValidationValid         BodyValidationStatus = "valid"
	BodyValidationInvalid       BodyValidationStatus = "invalid"
	BodyValidationIncomplete    BodyValidationStatus = "incomplete"
)

// AttemptEvidence is transport-owned evidence for one bounded provider
// attempt. The three durations intentionally remain separate.
type AttemptEvidence struct {
	ProviderID string
	Operation  Operation
	Outcome    OutcomeKind

	ResponseCode   int
	BodyValidation BodyValidationStatus
	Cause          error
	// ProviderResponseTimeout is true only when the response-head service
	// deadline expired. Caller cancellation and local queue expiry leave it false.
	ProviderResponseTimeout bool

	PoolQueueDuration        time.Duration
	PipelineHeadWaitDuration time.Duration
	ResponseServiceDuration  time.Duration
}

// TransportError is the structured final error returned after cancellation or
// provider exhaustion. Kind classifies the complete pool result. ProviderID,
// ResponseCode, and Cause describe one coherent representative attempt when
// all attempts agree; mixed outcomes intentionally leave provider/code empty
// and retain complete attribution in Attempts. Cause remains wrapped for
// errors.Is/errors.As callers.
type TransportError struct {
	Kind         OutcomeKind
	ProviderID   string
	ResponseCode int
	Attempts     []AttemptEvidence
	Cause        error
}

// classifiedError preserves an existing error string and underlying cause
// while attaching a stable semantic category for errors.Is/errors.As callers.
// It is used when connection bootstrap discovers authentication or local
// provider configuration errors before an NNTP command can be issued.
type classifiedError struct {
	cause    error
	category error
}

func (e *classifiedError) Error() string {
	if e == nil || e.cause == nil {
		return "<nil>"
	}
	return e.cause.Error()
}

func (e *classifiedError) Unwrap() []error {
	if e == nil {
		return nil
	}
	return []error{e.cause, e.category}
}

func withErrorClassification(cause, category error) error {
	if cause == nil || category == nil || errors.Is(cause, category) {
		return cause
	}
	return &classifiedError{cause: cause, category: category}
}

// safeIdentityText preserves ordinary identity text while rendering control
// characters and delimiters visibly at error/log text boundaries.
func safeIdentityText(identity string) string {
	quoted := strconv.QuoteToGraphic(identity)
	return quoted[1 : len(quoted)-1]
}

func (e *TransportError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.ProviderID != "" {
		return fmt.Sprintf("nntp: %s from %s: %v", e.Kind, safeIdentityText(e.ProviderID), e.Cause)
	}
	return fmt.Sprintf("nntp: %s: %v", e.Kind, e.Cause)
}

func (e *TransportError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func operationFromPayload(payload []byte) Operation {
	end := 0
	for end < len(payload) {
		switch payload[end] {
		case ' ', '\t', '\r', '\n':
			goto parsed
		}
		end++
	}
parsed:
	if end == 0 {
		return OperationUnknown
	}
	switch {
	case equalASCIIFold(payload[:end], "BODY"):
		return OperationBody
	case equalASCIIFold(payload[:end], "ARTICLE"):
		return operationArticle
	case equalASCIIFold(payload[:end], "STAT"):
		return OperationStat
	case equalASCIIFold(payload[:end], "HEAD"):
		return OperationHead
	case equalASCIIFold(payload[:end], "POST"):
		return OperationPost
	}
	return OperationUnknown
}

func equalASCIIFold(value []byte, expected string) bool {
	if len(value) != len(expected) {
		return false
	}
	for i, char := range value {
		if char >= 'a' && char <= 'z' {
			char -= 'a' - 'A'
		}
		if char != expected[i] {
			return false
		}
	}
	return true
}

func classifyOutcome(code int, err error) OutcomeKind {
	switch {
	case err == nil && code >= 200 && code < 400:
		return OutcomeSuccess
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return OutcomeCancellation
	case errors.Is(err, ErrBodyCorrupt), errors.Is(err, ErrCRCMismatch):
		return OutcomeCorruptBody
	case errors.Is(err, ErrCircuitBreakerOpen):
		return OutcomeTemporaryFailure
	case errors.Is(err, ErrServiceUnavailable), errors.Is(err, ErrAuthRequired),
		errors.Is(err, ErrAuthRejected), errors.Is(err, ErrQuotaExceeded),
		errors.Is(err, ErrInvalidProviderConfiguration), errors.Is(err, ErrMaxConnections), code == 502:
		return OutcomeProviderUnavailable
	case code == 423 || code == 430 || errors.Is(err, ErrArticleNotFound):
		return OutcomeHardArticleAbsence
	case code == 451:
		return OutcomeTemporaryFailure
	case func() bool {
		var protocolErr *Error
		return errors.As(err, &protocolErr)
	}():
		return OutcomeInconclusive
	case err != nil:
		return OutcomeTransportFailure
	default:
		return OutcomeInconclusive
	}
}

func classifyAttemptOutcome(req *Request, code int, err error) OutcomeKind {
	var writerError *callerWriterError
	if errors.As(err, &writerError) {
		return outcomeLocalFailure
	}
	if errors.Is(err, context.DeadlineExceeded) {
		if req != nil && req.deadlineCauseIsCaller() {
			return OutcomeCancellation
		}
		return OutcomeTransportFailure
	}
	return classifyOutcome(code, err)
}

func cloneAttempts(attempts []AttemptEvidence) []AttemptEvidence {
	if len(attempts) == 0 {
		return nil
	}
	return append([]AttemptEvidence(nil), attempts...)
}

func newTransportError(attempts []AttemptEvidence, cause error) *TransportError {
	kind := OutcomeInconclusive
	haveKind := false
	mixed := false
	for _, attempt := range attempts {
		if attempt.Outcome == OutcomeSuccess {
			continue
		}
		if attempt.Outcome == OutcomeCancellation {
			kind = OutcomeCancellation
			haveKind = true
			mixed = false
			continue
		}
		if kind == OutcomeCancellation {
			continue
		}
		if !haveKind {
			kind = attempt.Outcome
			haveKind = true
		} else if kind != attempt.Outcome {
			// A final result is conclusive only when every provider attempt
			// agrees on its outcome class. Per-provider detail remains in Attempts.
			kind = OutcomeInconclusive
			mixed = true
		}
	}

	var providerID string
	responseCode := 0
	if mixed {
		// Never let a hard-absence cause make errors.Is report global absence
		// for a mixed result. Choose the last non-absence cause, while leaving
		// provider/code unattributed because Kind describes the aggregate.
		for i := len(attempts) - 1; i >= 0; i-- {
			attempt := attempts[i]
			if attempt.Outcome != OutcomeSuccess && attempt.Outcome != OutcomeHardArticleAbsence && attempt.Cause != nil {
				cause = attempt.Cause
				break
			}
		}
	} else {
		// Uniform (or single) outcomes select all summary fields from the same
		// attempt so ProviderID/ResponseCode can never wrap another provider's
		// cause.
		for i := len(attempts) - 1; i >= 0; i-- {
			attempt := attempts[i]
			if attempt.Outcome != kind {
				continue
			}
			providerID = attempt.ProviderID
			responseCode = attempt.ResponseCode
			if attempt.Cause != nil {
				cause = attempt.Cause
			}
			break
		}
	}
	return &TransportError{
		Kind:         kind,
		ProviderID:   providerID,
		ResponseCode: responseCode,
		Attempts:     cloneAttempts(attempts),
		Cause:        cause,
	}
}

func responseError(resp Response) error {
	cause := resp.Err
	if cause == nil {
		cause = toError(resp.StatusCode, resp.Status)
	}
	if cause == nil {
		return nil
	}
	if _, ok := cause.(*TransportError); ok {
		return cause
	}
	if len(resp.Attempts) == 0 {
		return cause
	}
	return newTransportError(resp.Attempts, cause)
}

func cancellationResponse(attempts []AttemptEvidence, cause error) Response {
	providerID := ""
	if len(attempts) > 0 {
		providerID = attempts[len(attempts)-1].ProviderID
	}
	err := &TransportError{
		Kind:       OutcomeCancellation,
		ProviderID: providerID,
		Attempts:   cloneAttempts(attempts),
		Cause:      cause,
	}
	return Response{Err: err, Attempts: cloneAttempts(attempts)}
}

// Error represents an NNTP protocol-level error with a status code.
type Error struct {
	Code    int
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("nntp: %d %s", e.Code, e.Message)
}

// Is matches by semantic category so that, for example, both 430 ("no such article")
// and 423 ("no article with that number") match ErrArticleNotFound.
func (e *Error) Is(target error) bool {
	var t *Error
	if !errors.As(target, &t) {
		return false
	}
	return errorCategory(e.Code) == errorCategory(t.Code)
}

func errorCategory(code int) int {
	switch code {
	case 423, 430:
		return 430 // article not found
	default:
		return code
	}
}

var (
	ErrArticleNotFound     = &Error{Code: 430, Message: "no such article"}
	ErrPostingNotPermitted = &Error{Code: 440, Message: "posting not permitted"}
	ErrPostingFailed       = &Error{Code: 441, Message: "posting failed"}
	ErrAuthRequired        = &Error{Code: 480, Message: "authentication required"}
	ErrAuthRejected        = &Error{Code: 481, Message: "authentication rejected"}
	ErrServiceUnavailable  = &Error{Code: 502, Message: "service unavailable"}
	ErrCRCMismatch         = errors.New("nntp: yEnc CRC mismatch")
	ErrBodyCorrupt         = errors.New("nntp: corrupt article body")
	ErrProtocolDesync      = errors.New("nntp: protocol desync: expected status line, got binary data")
	ErrQuotaExceeded       = errors.New("nntp: download quota exceeded")
	// ErrInvalidProviderConfiguration identifies a local provider address,
	// authentication setup, TLS policy, or caller-supplied factory error that
	// must remain distinct from temporary provider transport health. Custom
	// ConnFactory implementations may wrap this sentinel when appropriate.
	ErrInvalidProviderConfiguration = errors.New("nntp: invalid provider configuration")
)

// toError maps an NNTP status code to a sentinel error, or returns nil for success codes.
func toError(code int, status string) error {
	switch {
	case code >= 200 && code < 400:
		return nil
	case code == 423 || code == 430:
		return ErrArticleNotFound
	case code == 440:
		return ErrPostingNotPermitted
	case code == 441:
		return ErrPostingFailed
	case code == 480:
		return ErrAuthRequired
	case code == 481:
		return ErrAuthRejected
	case code == 502:
		return ErrServiceUnavailable
	default:
		return &Error{Code: code, Message: status}
	}
}
