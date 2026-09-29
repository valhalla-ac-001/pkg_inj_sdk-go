package exchange

import (
	"context"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/InjectiveLabs/sdk-go/client/common"
	derivativePB "github.com/InjectiveLabs/sdk-go/exchange/derivative_exchange_rpc/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

type diagnosticTestHandler struct{ begins atomic.Int32 }

func (h *diagnosticTestHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}
func (h *diagnosticTestHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (h *diagnosticTestHandler) HandleConn(context.Context, stats.ConnStats) {}
func (h *diagnosticTestHandler) HandleRPC(_ context.Context, event stats.RPCStats) {
	if _, ok := event.(*stats.Begin); ok {
		h.begins.Add(1)
	}
}

type diagnosticTestServer struct {
	derivativePB.UnimplementedInjectiveDerivativeExchangeRPCServer
}

func (diagnosticTestServer) PositionsV2(_ context.Context, request *derivativePB.PositionsV2Request) (*derivativePB.PositionsV2Response, error) {
	if request.SubaccountId == "fail" {
		return nil, status.Error(codes.Unavailable, "test unavailable")
	}
	return &derivativePB.PositionsV2Response{}, nil
}

func TestExchangeOptionalStatsHandlerAttached(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	derivativePB.RegisterInjectiveDerivativeExchangeRPCServer(server, diagnosticTestServer{})
	go server.Serve(listener)
	defer server.Stop()
	defer listener.Close()
	network := common.Network{ExchangeGrpcEndpoint: listener.Addr().String(), ExchangeCookieAssistant: &common.DisabledCookieAssistant{}}
	handler := &diagnosticTestHandler{}
	client, err := NewExchangeClient(network, common.OptionGRPCStatsHandler(handler))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.GetDerivativePositionsV2(ctx, &derivativePB.PositionsV2Request{}); err != nil {
		t.Fatal(err)
	}
	if handler.begins.Load() != 1 {
		t.Fatalf("stats option not attached: %d", handler.begins.Load())
	}
	plain, err := NewExchangeClient(network)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	if _, err := plain.GetDerivativePositionsV2(ctx, &derivativePB.PositionsV2Request{}); err != nil {
		t.Fatal(err)
	}
	if handler.begins.Load() != 1 {
		t.Fatal("handler leaked to default client")
	}
}

func TestExchangeDiagnosticErrorLogging(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	derivativePB.RegisterInjectiveDerivativeExchangeRPCServer(server, diagnosticTestServer{})
	go server.Serve(listener)
	defer server.Stop()
	defer listener.Close()
	network := common.Network{ExchangeGrpcEndpoint: listener.Addr().String(), ExchangeCookieAssistant: &common.DisabledCookieAssistant{}}
	for _, enabled := range []bool{false, true} {
		var options []common.ClientOption
		if enabled {
			options = append(options, common.OptionGRPCStatsHandler(&diagnosticTestHandler{}))
		}
		client, err := NewExchangeClient(network, options...)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		original := os.Stdout
		os.Stdout = writer
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		response, callErr := client.GetDerivativePositionsV2(ctx, &derivativePB.PositionsV2Request{SubaccountId: "fail"})
		cancel()
		os.Stdout = original
		writer.Close()
		output, readErr := io.ReadAll(reader)
		reader.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if response == nil || status.Code(callErr) != codes.Unavailable {
			t.Fatalf("error behavior changed: %v %v", response, callErr)
		}
		if enabled && len(output) != 0 {
			t.Fatalf("instrumented duplicate output: %s", output)
		}
		if !enabled && !strings.Contains(string(output), "test unavailable") {
			t.Fatal("default logging changed")
		}
	}
}
