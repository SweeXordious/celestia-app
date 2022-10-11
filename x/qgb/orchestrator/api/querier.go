package api

import (
	"context"
	"fmt"

	"github.com/celestiaorg/celestia-app/app/encoding"
	"github.com/celestiaorg/celestia-app/x/qgb/types"
	cdctypes "github.com/cosmos/cosmos-sdk/codec/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/pkg/errors"
	"github.com/tendermint/tendermint/libs/bytes"
	tmlog "github.com/tendermint/tendermint/libs/log"
	"github.com/tendermint/tendermint/rpc/client/http"
	coretypes "github.com/tendermint/tendermint/rpc/core/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var _ RPCStateQuerierI = &RPCStateQuerier{}

type RPCStateQuerierI interface {
	// attestation queries

	QueryAttestationByNonce(ctx context.Context, nonce uint64) (types.AttestationRequestI, error)
	QueryLatestAttestationNonce(ctx context.Context) (uint64, error)

	// data commitment queries

	QueryDataCommitmentByNonce(ctx context.Context, nonce uint64) (*types.DataCommitment, error)

	// valset queries

	QueryValsetByNonce(ctx context.Context, nonce uint64) (*types.Valset, error)
	QueryLatestValset(ctx context.Context) (*types.Valset, error)
	QueryLastValsetBeforeNonce(
		ctx context.Context,
		nonce uint64,
	) (*types.Valset, error)

	// misc queries

	QueryLastUnbondingHeight(ctx context.Context) (uint64, error)

	// tendermint

	QueryCommitment(ctx context.Context, beginBlock uint64, endBlock uint64) (bytes.HexBytes, error)
	SubscribeEvents(ctx context.Context, subscriptionName string, eventName string) (<-chan coretypes.ResultEvent, error)
}

type RPCStateQuerier struct {
	qgbRPC        *grpc.ClientConn
	logger        tmlog.Logger
	tendermintRPC *http.HTTP
	encCfg        encoding.Config
}

func NewRPCStateQuerier(
	qgbRPCAddr, tendermintRPC string,
	logger tmlog.Logger,
	encCft encoding.Config,
) (*RPCStateQuerier, error) {
	qgbGRPC, err := grpc.Dial(qgbRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	trpc, err := http.New(tendermintRPC, "/websocket")
	if err != nil {
		return nil, err
	}
	err = trpc.Start()
	if err != nil {
		return nil, err
	}

	return &RPCStateQuerier{
		qgbRPC:        qgbGRPC,
		logger:        logger,
		tendermintRPC: trpc,
		encCfg:        encCft,
	}, nil
}

// TODO add the other stop methods for other clients.
func (q RPCStateQuerier) Stop() {
	err := q.qgbRPC.Close()
	if err != nil {
		q.logger.Error(err.Error())
	}
	err = q.tendermintRPC.Stop()
	if err != nil {
		q.logger.Error(err.Error())
	}
}

func (q RPCStateQuerier) QueryLastUnbondingHeight(ctx context.Context) (uint64, error) {
	queryClient := types.NewQueryClient(q.qgbRPC)
	resp, err := queryClient.LastUnbondingHeight(ctx, &types.QueryLastUnbondingHeightRequest{})
	if err != nil {
		return 0, err
	}

	return resp.Height, nil
}

func (q RPCStateQuerier) QueryLatestValset(ctx context.Context) (*types.Valset, error) {
	latestNonce, err := q.QueryLatestAttestationNonce(ctx)
	if err != nil {
		return nil, err
	}

	var latestValset *types.Valset
	if vs, err := q.QueryValsetByNonce(ctx, latestNonce); err == nil {
		latestValset = vs
	} else {
		latestValset, err = q.QueryLastValsetBeforeNonce(ctx, latestNonce)
		if err != nil {
			return nil, err
		}
	}
	return latestValset, nil
}

// QueryLastValsetBeforeNonce returns the previous valset before the provided `nonce`.
// the `nonce` can be a valset, but this method will return the valset before it.
// If the provided nonce is 1. It will return an error. Because, there is no valset before nonce 1.
func (q RPCStateQuerier) QueryLastValsetBeforeNonce(ctx context.Context, nonce uint64) (*types.Valset, error) {
	if nonce == 1 {
		return nil, types.ErrNoValsetBeforeNonceOne
	}
	latestAttestationNonce, err := q.QueryLatestAttestationNonce(ctx)
	if err != nil {
		return nil, err
	}
	if nonce > latestAttestationNonce {
		return nil, types.ErrNonceHigherThanLatestAttestationNonce
	}
	// starting at 1 because the current nonce can be a valset
	// and we need the previous one.
	for i := uint64(1); i < nonce; i++ {
		at, err := q.QueryAttestationByNonce(ctx, nonce-i)
		if err != nil {
			return nil, err
		}
		if at.Type() == types.ValsetRequestType {
			valset, ok := at.(*types.Valset)
			if !ok {
				return nil, errors.Wrap(types.ErrAttestationNotValsetRequest, "couldn't cast attestation to valset")
			}
			return valset, nil
		}
	}
	return nil, errors.Wrap(
		ErrValsetNotFound,
		fmt.Sprintf("couldn't find valset before nonce %d", nonce),
	)
}

func (q RPCStateQuerier) QueryDataCommitmentByNonce(ctx context.Context, nonce uint64) (*types.DataCommitment, error) {
	attestation, err := q.QueryAttestationByNonce(ctx, nonce)
	if err != nil {
		return nil, err
	}
	if attestation == nil {
		return nil, types.ErrAttestationNotFound
	}

	if attestation.Type() != types.DataCommitmentRequestType {
		return nil, types.ErrAttestationNotDataCommitmentRequest
	}

	dcc, ok := attestation.(*types.DataCommitment)
	if !ok {
		return nil, types.ErrAttestationNotDataCommitmentRequest
	}

	return dcc, nil
}

// QueryAttestationByNonce Queries the attestation with nonce `nonce.
// Returns nil if not found.
func (q RPCStateQuerier) QueryAttestationByNonce(
	ctx context.Context,
	nonce uint64,
) (types.AttestationRequestI, error) { // FIXME is it alright to return interface?
	queryClient := types.NewQueryClient(q.qgbRPC)
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

	unmarshalledAttestation, err := q.unmarshallAttestation(atResp.Attestation)
	if err != nil {
		return nil, err
	}

	return unmarshalledAttestation, nil
}

func (q RPCStateQuerier) QueryValsetByNonce(ctx context.Context, nonce uint64) (*types.Valset, error) {
	attestation, err := q.QueryAttestationByNonce(ctx, nonce)
	if err != nil {
		return nil, err
	}
	if attestation == nil {
		return nil, types.ErrAttestationNotFound
	}

	if attestation.Type() != types.ValsetRequestType {
		return nil, types.ErrAttestationNotValsetRequest
	}

	value, ok := attestation.(*types.Valset)
	if !ok {
		return nil, ErrUnmarshallValset
	}

	return value, nil
}

func (q RPCStateQuerier) QueryLatestAttestationNonce(ctx context.Context) (uint64, error) {
	queryClient := types.NewQueryClient(q.qgbRPC)

	resp, err := queryClient.LatestAttestationNonce(
		ctx,
		&types.QueryLatestAttestationNonceRequest{},
	)
	if err != nil {
		return 0, err
	}

	return resp.Nonce, nil
}

// QueryCommitment queries the commitment over a set of blocks defined in the query.
func (q RPCStateQuerier) QueryCommitment(ctx context.Context, beginBlock uint64, endBlock uint64) (bytes.HexBytes, error) {
	dcResp, err := q.tendermintRPC.DataCommitment(ctx, beginBlock, endBlock)
	if err != nil {
		return nil, err
	}
	return dcResp.DataCommitment, nil
}

func (q RPCStateQuerier) SubscribeEvents(ctx context.Context, subscriptionName string, eventName string) (<-chan coretypes.ResultEvent, error) {
	// This doesn't seem to complain when the node is down
	results, err := q.tendermintRPC.Subscribe(
		ctx,
		subscriptionName,
		fmt.Sprintf("%s.%s='%s'", types.EventTypeAttestationRequest, sdk.AttributeKeyModule, types.ModuleName),
	)
	if err != nil {
		return nil, err
	}
	return results, err
}

func (q RPCStateQuerier) unmarshallAttestation(attestation *cdctypes.Any) (types.AttestationRequestI, error) {
	var unmarshalledAttestation types.AttestationRequestI
	err := q.encCfg.InterfaceRegistry.UnpackAny(attestation, &unmarshalledAttestation)
	if err != nil {
		return nil, err
	}
	return unmarshalledAttestation, nil
}
