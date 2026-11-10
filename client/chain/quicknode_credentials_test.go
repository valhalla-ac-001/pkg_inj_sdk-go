package chain

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/InjectiveLabs/sdk-go/client/common"
	"github.com/cosmos/cosmos-sdk/client/grpc/cmtservice"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type chainTestAuth struct{}

func (chainTestAuth) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"x-token": "test-chain-token"}, nil
}
func (chainTestAuth) RequireTransportSecurity() bool { return true }

type credentialBlockServer struct {
	cmtservice.UnimplementedServiceServer
}

func (credentialBlockServer) GetLatestBlock(context.Context, *cmtservice.GetLatestBlockRequest) (*cmtservice.GetLatestBlockResponse, error) {
	return &cmtservice.GetLatestBlockResponse{}, nil
}

func TestChainGRPCCredentialsStayOffChainStream(t *testing.T) {
	var observed []string
	var mu sync.Mutex
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		mu.Lock()
		observed = append(observed, strings.Join(md.Get("x-token"), ""))
		mu.Unlock()
		return handler(ctx, req)
	}))
	cmtservice.RegisterServiceServer(server, &credentialBlockServer{})
	httpServer := httptest.NewUnstartedServer(server)
	httpServer.EnableHTTP2 = true
	httpServer.StartTLS()
	defer httpServer.Close()
	defer server.Stop()
	pool := x509.NewCertPool()
	pool.AddCert(httpServer.Certificate())
	network := common.LoadNetwork("local", "")
	network.ChainGrpcEndpoint = strings.TrimPrefix(httpServer.URL, "https://")
	network.ChainStreamGrpcEndpoint = network.ChainGrpcEndpoint
	network.ChainTLSCert = credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})
	dir := t.TempDir()
	oldPath, oldFile := OfacListPath, OfacListFilename
	OfacListPath, OfacListFilename = dir, "ofac.json"
	t.Cleanup(func() { OfacListPath, OfacListFilename = oldPath, oldFile })
	if err := os.WriteFile(filepath.Join(dir, "ofac.json"), []byte("[]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	clientCtx, err := NewClientContext(network.ChainId, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := NewChainClientV2(clientCtx, network, common.OptionGRPCOnly(ctx), common.OptionChainGRPCCredentials(chainTestAuth{}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := cmtservice.NewServiceClient(client.QueryClient()).GetLatestBlock(ctx, &cmtservice.GetLatestBlockRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cmtservice.NewServiceClient(client.(*chainClientV2).chainStreamConn).GetLatestBlock(ctx, &cmtservice.GetLatestBlockRequest{}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(observed) != 2 || observed[0] != "test-chain-token" || observed[1] != "" {
		t.Fatalf("credentials escaped primary chain connection: %v", observed)
	}
	network.ChainTLSCert = nil
	if client, err := NewChainClientV2(clientCtx, network, common.OptionGRPCOnly(ctx), common.OptionChainGRPCCredentials(chainTestAuth{})); err == nil {
		client.Close()
		t.Fatal("accepted chain credentials without TLS")
	}
}
