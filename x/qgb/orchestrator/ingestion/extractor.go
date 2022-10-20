package ingestion

import (
	"context"
	"fmt"

	"github.com/celestiaorg/celestia-app/app/encoding"
	"github.com/celestiaorg/celestia-app/x/qgb/types"
	cdctypes "github.com/cosmos/cosmos-sdk/codec/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/pkg/errors"
	"github.com/tendermint/tendermint/rpc/client/http"
	coretypes "github.com/tendermint/tendermint/rpc/core/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var _ TmExtractorI = &TmRPCExtractor{}

type TmExtractorI interface {
	ExtractBlock(ctx context.Context, height *int64) (*coretypes.ResultBlock, error)
	QueryHeight(ctx context.Context) (int64, error)
	SubscribeNewBlocks(ctx context.Context, subscriptionName string) (<-chan coretypes.ResultEvent, error)
	SubscribeQGBEvents(ctx context.Context, subscriptionName string, eventName string) (<-chan coretypes.ResultEvent, error)
	// ExtractTransaction(block *coretypes.ResultBlock) (tmtypes.Txs, error)
	Stop() error
}

type TmRPCExtractor struct {
	tendermintRPC *http.HTTP
}

func NewTmRPCExtractor(tendermintRPC string) (*TmRPCExtractor, error) {
	trpc, err := http.New(tendermintRPC, "/websocket")
	if err != nil {
		return nil, err
	}
	err = trpc.Start()
	if err != nil {
		return nil, err
	}
	return &TmRPCExtractor{tendermintRPC: trpc}, nil
}

func (extractor TmRPCExtractor) Stop() error {
	return extractor.tendermintRPC.Stop()
}

func (extractor TmRPCExtractor) ExtractBlock(ctx context.Context, height *int64) (*coretypes.ResultBlock, error) {
	block, err := extractor.tendermintRPC.Block(ctx, height)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func (extractor TmRPCExtractor) QueryHeight(ctx context.Context) (int64, error) {
	resp, err := extractor.tendermintRPC.Status(ctx)
	if err != nil {
		return 0, err
	}

	return resp.SyncInfo.LatestBlockHeight, nil
}

func (extractor TmRPCExtractor) SubscribeNewBlocks(ctx context.Context, subscriptionName string) (<-chan coretypes.ResultEvent, error) {
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

func (extractor TmRPCExtractor) SubscribeQGBEvents(
	ctx context.Context,
	subscriptionName string,
	eventName string,
) (<-chan coretypes.ResultEvent, error) {
	// This doesn't seem to complain when the node is down
	results, err := extractor.tendermintRPC.Subscribe(
		ctx,
		subscriptionName,
		fmt.Sprintf("%s.%s='%s'", types.EventTypeAttestationRequest, sdk.AttributeKeyModule, types.ModuleName),
	)
	if err != nil {
		return nil, err
	}
	return results, err
}

type QGBExtractorI interface {
	QueryLastUnbondingHeight(ctx context.Context) (uint64, error)           // TODO subscribe
	QueryLastUnbondingAttestationNonce(ctx context.Context) (uint64, error) // TODO subscribe
	ExtractAttestationByNonce(
		ctx context.Context,
		nonce uint64,
	) (types.AttestationRequestI, error)
	QueryLatestAttestationNonce(ctx context.Context) (uint64, error) // TODO subscribe
	QueryLastValsetBeforeNonce(ctx context.Context, nonce uint64) (*types.Valset, error)
	QueryLatestValset(ctx context.Context) (*types.Valset, error)
	Stop() error
}

type QGBRPCExtractor struct {
	qgbRPC *grpc.ClientConn
	encCfg encoding.Config
}

func NewQGBRPCExtractor(qgbRPCAddr string, enc encoding.Config) (*QGBRPCExtractor, error) {
	qgbGRPC, err := grpc.Dial(qgbRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &QGBRPCExtractor{
		qgbRPC: qgbGRPC,
		encCfg: enc,
	}, nil
}

func (e QGBRPCExtractor) Stop() error {
	return e.qgbRPC.Close()
}

func (e QGBRPCExtractor) QueryLastUnbondingHeight(ctx context.Context) (uint64, error) {
	queryClient := types.NewQueryClient(e.qgbRPC)
	resp, err := queryClient.LastUnbondingHeight(ctx, &types.QueryLastUnbondingHeightRequest{})
	if err != nil {
		return 0, err
	}

	return resp.Height, nil
}

func (e QGBRPCExtractor) QueryLastUnbondingAttestationNonce(ctx context.Context) (uint64, error) {
	queryClient := types.NewQueryClient(e.qgbRPC)
	resp, err := queryClient.LastUnbondingAttestationNonce(ctx, &types.QueryLastUnbondingAttestationNonceRequest{})
	if err != nil {
		return 0, err
	}

	return resp.Nonce, nil
}

func (e QGBRPCExtractor) ExtractAttestationByNonce(
	ctx context.Context,
	nonce uint64,
) (types.AttestationRequestI, error) { // FIXME is it alright to return interface?
	queryClient := types.NewQueryClient(e.qgbRPC)
	atResp, err := queryClient.AttestationRequestByNonce(
		ctx,
		&types.QueryAttestationRequestByNonceRequest{Nonce: nonce},
	)
	if err != nil {
		return nil, err
	}
	if atResp.Attestation == nil {
		return nil, nil
	}

	unmarshalledAttestation, err := e.unmarshallAttestation(atResp.Attestation)
	if err != nil {
		return nil, err
	}

	return unmarshalledAttestation, nil
}

func (e QGBRPCExtractor) QueryLatestAttestationNonce(ctx context.Context) (uint64, error) {
	queryClient := types.NewQueryClient(e.qgbRPC)

	resp, err := queryClient.LatestAttestationNonce(
		ctx,
		&types.QueryLatestAttestationNonceRequest{},
	)
	if err != nil {
		return 0, err
	}

	return resp.Nonce, nil
}

func (e QGBRPCExtractor) unmarshallAttestation(attestation *cdctypes.Any) (types.AttestationRequestI, error) {
	var unmarshalledAttestation types.AttestationRequestI
	err := e.encCfg.InterfaceRegistry.UnpackAny(attestation, &unmarshalledAttestation)
	if err != nil {
		return nil, err
	}
	return unmarshalledAttestation, nil
}

func (e QGBRPCExtractor) QueryLatestValset(ctx context.Context) (*types.Valset, error) {
	latestNonce, err := e.QueryLatestAttestationNonce(ctx)
	if err != nil {
		return nil, err
	}

	att, err := e.ExtractAttestationByNonce(ctx, latestNonce)
	if err != nil {
		return nil, err
	}

	switch vs := att.(type) {
	case *types.Valset:
		return vs, nil
	default:
		return e.QueryLastValsetBeforeNonce(ctx, latestNonce)
	}
}

// QueryLastValsetBeforeNonce returns the previous valset before the provided `nonce`.
// the `nonce` can be a valset, but this method will return the valset before it.
// If the provided nonce is 1. It will return an error. Because, there is no valset before nonce 1.
func (e QGBRPCExtractor) QueryLastValsetBeforeNonce(ctx context.Context, nonce uint64) (*types.Valset, error) {
	if nonce == 1 { // TODO investigate if we want nonces to start at 1
		return nil, types.ErrNoValsetBeforeNonceOne
	}
	latestAttestationNonce, err := e.QueryLatestAttestationNonce(ctx)
	if err != nil {
		return nil, err
	}
	if nonce > latestAttestationNonce {
		return nil, types.ErrNonceHigherThanLatestAttestationNonce
	}
	// starting at 1 because the current nonce can be a valset
	// and we need the previous one.
	i := uint64(1)
	for {
		if i >= nonce {
			return nil, fmt.Errorf("couldn't find valset before nonce %d", nonce)
		}
		at, err := e.ExtractAttestationByNonce(ctx, nonce-i)
		if err != nil {
			return nil, err
		}
		if at == nil {
			return nil, fmt.Errorf("nil attestation queried. nonce: %d", nonce-i)
		}
		if at.Type() == types.ValsetRequestType {
			valset, ok := at.(*types.Valset)
			if !ok {
				return nil, errors.Wrap(types.ErrAttestationNotValsetRequest, "couldn't cast attestation to valset")
			}
			return valset, nil
		}
		i++
	}
}
