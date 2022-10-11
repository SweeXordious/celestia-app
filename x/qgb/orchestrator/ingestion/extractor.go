package ingestion

import (
	"context"
	"github.com/tendermint/tendermint/rpc/client/http"
	coretypes "github.com/tendermint/tendermint/rpc/core/types"
)

var _ ExtractorI = &RPCExtractor{}

type ExtractorI interface {
	ExtractBlock(ctx context.Context, height *int64) (*coretypes.ResultBlock, error)
	QueryHeight(ctx context.Context) (int64, error)
	SubscribeNewBlocks(ctx context.Context, subscriptionName string) (<-chan coretypes.ResultEvent, error)
	//ExtractTransaction(block *coretypes.ResultBlock) (tmtypes.Txs, error)
	Stop() error
}

type RPCExtractor struct {
	tendermintRPC *http.HTTP
}

func NewRPCExtractor(tendermintRPC string) (*RPCExtractor, error) {
	trpc, err := http.New(tendermintRPC, "/websocket")
	if err != nil {
		return nil, err
	}
	err = trpc.Start()
	if err != nil {
		return nil, err
	}
	return &RPCExtractor{tendermintRPC: trpc}, nil
}

func (extractor RPCExtractor) Stop() error {
	return extractor.tendermintRPC.Stop()
}

func (extractor RPCExtractor) ExtractBlock(ctx context.Context, height *int64) (*coretypes.ResultBlock, error) {
	block, err := extractor.tendermintRPC.Block(ctx, height)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (extractor RPCExtractor) QueryHeight(ctx context.Context) (int64, error) {
	resp, err := extractor.tendermintRPC.Status(ctx)
	if err != nil {
		return 0, err
	}

	return resp.SyncInfo.LatestBlockHeight, nil
}

func (extractor RPCExtractor) SubscribeNewBlocks(ctx context.Context, subscriptionName string) (<-chan coretypes.ResultEvent, error) {
	// This doesn't seem to complain when the node is down
	results, err := extractor.tendermintRPC.Subscribe(
		ctx,
		subscriptionName,
		"tm.event = 'NewBlock'",
		200,
	)
	if err != nil {
		return nil, err
	}
	return results, err
}
