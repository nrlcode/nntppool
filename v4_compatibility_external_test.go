package nntppool_test

import (
	"context"
	"io"

	nntppool "github.com/javi11/nntppool/v4"
)

// Compile-time fixtures define the supported v4 source-compatibility surface:
// existing methods remain available, zero values remain constructible, and
// external keyed literals remain valid as additive fields are introduced.
var (
	_ = nntppool.Provider{Host: "example.invalid:119", Connections: 1}
	_ = nntppool.Provider{
		Host:              "mapped.example.invalid:119",
		Connections:       1,
		Response451Policy: nntppool.Response451AbsentAfterRetry,
	}
	_ = nntppool.Request{Ctx: context.Background(), Payload: []byte("DATE\r\n"), RespCh: make(chan nntppool.Response, 1)}
	_ = nntppool.Response{StatusCode: 200, Status: "200 ready"}
	_ = nntppool.ArticleBody{MessageID: "fixture@example.invalid", Bytes: []byte("payload")}
	_ = nntppool.ArticleHead{MessageID: "fixture@example.invalid"}
	_ = nntppool.ArticleHead{
		MessageID:  "mapped@example.invalid",
		ProviderID: "provider",
		Attempts:   []nntppool.AttemptEvidence{},
	}
	_ = nntppool.StatResult{MessageID: "fixture@example.invalid", Number: 1}
	_ = nntppool.ProviderStats{Name: "provider"}
	_ = nntppool.ClientStats{}

	_ nntppool.Response451Policy = nntppool.Response451Temporary
	_ nntppool.Operation         = nntppool.OperationArticle
)

type v4ClientMethods interface {
	Send(context.Context, []byte, io.Writer, ...func(nntppool.YEncMeta)) <-chan nntppool.Response
	SendPriority(context.Context, []byte, io.Writer, ...func(nntppool.YEncMeta)) <-chan nntppool.Response
	Body(context.Context, string, ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error)
	BodyPriority(context.Context, string, ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error)
	BodyStream(context.Context, string, io.Writer, ...func(nntppool.YEncMeta)) (*nntppool.ArticleBody, error)
	BodyAsync(context.Context, string, io.Writer, ...func(nntppool.YEncMeta)) <-chan nntppool.BodyResult
	Stat(context.Context, string) (*nntppool.StatResult, error)
	Close() error
}

var _ v4ClientMethods = (*nntppool.Client)(nil)
