package api

import (
	"context"
	"fmt"
	"math/big"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/tendermint/tendermint/libs/bytes"
	"github.com/tendermint/tendermint/rpc/client/http"
	coretypes "github.com/tendermint/tendermint/rpc/core/types"

	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/evm"
	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/utils"
	"github.com/celestiaorg/celestia-app/x/qgb/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	tmlog "github.com/tendermint/tendermint/libs/log"
)

type QGBQuerierI interface {
	// TODO add docs same as qgb_store
	QueryAttestationByNonce(ctx context.Context, nonce uint64) (types.AttestationRequestI, error)
	QueryLatestAttestationNonce(ctx context.Context) (uint64, error)
	QueryDataCommitmentByNonce(ctx context.Context, nonce uint64) (*types.DataCommitment, error)
	QueryValsetByNonce(ctx context.Context, nonce uint64) (*types.Valset, error)
	QueryLatestValset(ctx context.Context) (*types.Valset, error)
	QueryLastValsetBeforeNonce(
		ctx context.Context,
		nonce uint64,
	) (*types.Valset, error)
	QueryLastUnbondingHeight(ctx context.Context) (int64, error)
	QueryLastUnbondingAttestationNonce(ctx context.Context) (uint64, error)
	QueryTwoThirdsDataCommitmentConfirms(
		ctx context.Context,
		timeout time.Duration,
		dc types.DataCommitment,
	) ([]types.MsgDataCommitmentConfirm, error)
	QueryTwoThirdsValsetConfirms(
		ctx context.Context,
		timeout time.Duration,
		valset types.Valset,
	) ([]types.MsgValsetConfirm, error)
	QueryValsetConfirmByOrchestratorAddress(
		ctx context.Context,
		nonce uint64,
		address string,
	) (types.MsgValsetConfirm, error)
	QueryDataCommitmentConfirmByOrchestratorAddress(
		ctx context.Context,
		nonce uint64,
		address string,
	) (types.MsgDataCommitmentConfirm, error)
	QueryDataCommitmentConfirms(
		ctx context.Context,
		nonce uint64,
	) ([]types.MsgDataCommitmentConfirm, error)
	QueryValsetConfirms(
		ctx context.Context,
		nonce uint64,
	) ([]types.MsgValsetConfirm, error)
	GetStorageHeightsMilestone() int64
}

var _ QGBQuerierI = &QGBQuerier{}

type QGBQuerier struct {
	logger      tmlog.Logger
	StoreLoader QGBLoaderI
	TmQuerier   TmQuerierI
}

func NewQGBQuerier(logger tmlog.Logger, storeLoader QGBLoaderI, tmQuerier TmQuerierI) *QGBQuerier {
	return &QGBQuerier{
		logger:      logger,
		StoreLoader: storeLoader,
		TmQuerier:   tmQuerier,
	}
}

func (q *QGBQuerier) QueryTwoThirdsDataCommitmentConfirms(
	ctx context.Context,
	timeout time.Duration,
	dc types.DataCommitment,
) ([]types.MsgDataCommitmentConfirm, error) {
	valset, err := q.QueryLastValsetBeforeNonce(ctx, dc.Nonce)
	if err != nil {
		return nil, err
	}

	// create a map to easily search for power
	vals := make(map[string]types.BridgeValidator)
	for _, val := range valset.Members {
		vals[val.GetEthereumAddress()] = val
	}

	majThreshHold := valset.TwoThirdsThreshold()

	for {
		select {
		case <-ctx.Done():
			return nil, nil //nolint:nilnil
		case <-time.After(timeout):
			return nil, errors.Wrap(
				ErrNotEnoughDataCommitmentConfirms,
				fmt.Sprintf("failure to query for majority validator set confirms: timout %s", timeout),
			)
		default:
			currThreshold := uint64(0)
			confirms, err := q.QueryDataCommitmentConfirms(ctx, dc.Nonce)
			if err != nil {
				return nil, err
			}

			// used to be tested against when checking if all confirms have the correct commitment.
			// this can be extended to slashing the validators who submitted commitments that
			// have wrong commitments or signatures.
			// https://github.com/celestiaorg/celestia-app/pull/613/files#r947992851
			commitment, err := q.TmQuerier.QueryCommitment(ctx, dc.BeginBlock, dc.EndBlock)
			if err != nil {
				return nil, err
			}

			correctConfirms := make([]types.MsgDataCommitmentConfirm, 0)
			for _, dataCommitmentConfirm := range confirms {
				val, has := vals[dataCommitmentConfirm.EthAddress]
				if !has {
					q.logger.Debug(fmt.Sprintf(
						"dataCommitmentConfirm signer not found in stored validator set: address %s nonce %d",
						val.EthereumAddress,
						valset.Nonce,
					))
					continue
				}
				if err := validateDCConfirm(commitment.String(), dataCommitmentConfirm); err != nil {
					q.logger.Error("found an invalid data commitment confirm",
						"nonce",
						dataCommitmentConfirm.Nonce,
						"signer_eth_address",
						dataCommitmentConfirm.EthAddress,
						"err",
						err.Error(),
					)
					continue
				}
				currThreshold += val.Power
				correctConfirms = append(correctConfirms, dataCommitmentConfirm)
			}

			if currThreshold >= majThreshHold {
				q.logger.Debug("found enough data commitment confirms to be relayed",
					"majThreshHold",
					majThreshHold,
					"currThreshold",
					currThreshold,
				)
				return correctConfirms, nil
			}
			q.logger.Debug(
				"found DataCommitmentConfirms",
				"begin_block",
				dc.BeginBlock,
				"end_block",
				dc.EndBlock,
				"total_power",
				currThreshold,
				"number_of_confirms",
				len(confirms),
				"missing_confirms",
				len(valset.Members)-len(confirms),
			)
		}
		// TODO: make the timeout configurable
		time.Sleep(1 * time.Second)
	}
}

// validateDCConfirm runs validation on the data commitment confirm to make sure it was well created.
// it tests if the commitment it carries is the correct commitment. Then, checks whether the signature
// is valid.
func validateDCConfirm(commitment string, confirm types.MsgDataCommitmentConfirm) error {
	// TODO also check for validators orchestrator address if matches (can be used later for slashing)
	// TODO make the checks more exhaustive
	if confirm.Commitment != commitment {
		return ErrInvalidCommitmentInConfirm
	}
	bCommitment := common.Hex2Bytes(commitment)
	dataRootHash := utils.DataCommitmentTupleRootSignBytes(types.BridgeID, big.NewInt(int64(confirm.Nonce)), bCommitment)
	err := evm.ValidateEthereumSignature(dataRootHash.Bytes(), common.Hex2Bytes(confirm.Signature), common.HexToAddress(confirm.EthAddress))
	if err != nil {
		return err
	}
	return nil
}

func (q *QGBQuerier) QueryTwoThirdsValsetConfirms(
	ctx context.Context,
	timeout time.Duration,
	valset types.Valset,
) ([]types.MsgValsetConfirm, error) {
	var currentValset types.Valset
	if valset.Nonce == 1 {
		// In fact, the first nonce should never be signed. Because, the first attestation, in the case
		// where the `earliest` flag is specified when deploying the contract, will be relayed as part of
		// the deployment of the QGB contract.
		// It will be signed temporarily for now.
		currentValset = valset
	} else {
		vs, err := q.QueryLastValsetBeforeNonce(ctx, valset.Nonce)
		if err != nil {
			return nil, err
		}
		currentValset = *vs
	}
	// create a map to easily search for power
	vals := make(map[string]types.BridgeValidator)
	for _, val := range currentValset.Members {
		vals[val.GetEthereumAddress()] = val
	}

	majThreshHold := valset.TwoThirdsThreshold()

	for {
		select {
		case <-ctx.Done():
			return nil, nil //nolint:nilnil
		// TODO: remove this extra case, and we can instead rely on the caller to pass a context with a timeout
		case <-time.After(timeout):
			return nil, errors.Wrap(
				ErrNotEnoughValsetConfirms,
				fmt.Sprintf("failure to query for majority validator set confirms: timout %s", timeout),
			)
		default:
			currThreshold := uint64(0)
			confirms, err := q.QueryValsetConfirms(ctx, valset.Nonce)
			if err != nil {
				return nil, err
			}

			validConfirms := make([]types.MsgValsetConfirm, 0)
			for _, valsetConfirm := range confirms {
				val, has := vals[valsetConfirm.EthAddress]
				if !has {
					q.logger.Debug(
						fmt.Sprintf(
							"valSetConfirm signer not found in stored validator set: address %s nonce %d",
							val.EthereumAddress,
							valset.Nonce,
						))
					continue
				}
				if err := validateValsetConfirm(valset, valsetConfirm); err != nil {
					q.logger.Error("found an invalid valset confirm",
						"nonce",
						valsetConfirm.Nonce,
						"signer_eth_address",
						valsetConfirm.EthAddress,
						"err",
						err.Error(),
					)
					continue
				}
				currThreshold += val.Power
				validConfirms = append(validConfirms, valsetConfirm)
			}

			if currThreshold >= majThreshHold {
				q.logger.Debug("found enough valset confirms to be relayed",
					"majThreshHold",
					majThreshHold,
					"currThreshold",
					currThreshold,
				)
				return validConfirms, nil
			}
			q.logger.Debug(
				"found ValsetConfirms",
				"nonce",
				valset.Nonce,
				"total_power",
				currThreshold,
				"number_of_confirms",
				len(validConfirms),
				"missing_confirms",
				len(currentValset.Members)-len(validConfirms),
			)
		}
		// TODO: make the timeout configurable
		time.Sleep(1 * time.Second)
	}
}

// validateValsetConfirm runs validation on the valset confirm to make sure it was well created.
// For now, it only checks if the signature is correct. Can be improved afterwards.
func validateValsetConfirm(vs types.Valset, confirm types.MsgValsetConfirm) error {
	// TODO make the checks  more exhaustive
	signBytes, err := vs.SignBytes(types.BridgeID)
	if err != nil {
		return err
	}
	err = evm.ValidateEthereumSignature(signBytes.Bytes(), common.Hex2Bytes(confirm.Signature), common.HexToAddress(confirm.EthAddress))
	if err != nil {
		return err
	}
	return nil
}

func (q *QGBQuerier) QueryValsetConfirmByOrchestratorAddress(
	ctx context.Context,
	nonce uint64,
	address string,
) (types.MsgValsetConfirm, error) {
	return q.StoreLoader.GetValsetConfirmByOrchestratorAddress(nonce, address)
}

func (q *QGBQuerier) QueryDataCommitmentConfirmByOrchestratorAddress(
	ctx context.Context,
	nonce uint64,
	address string,
) (types.MsgDataCommitmentConfirm, error) {
	return q.StoreLoader.GetDataCommitmentConfirmByOrchestratorAddress(nonce, address)
}

func (q *QGBQuerier) QueryDataCommitmentConfirms(
	ctx context.Context,
	nonce uint64,
) ([]types.MsgDataCommitmentConfirm, error) {
	return q.StoreLoader.GetDataCommitmentConfirms(nonce)
}

func (q *QGBQuerier) QueryValsetConfirms(
	ctx context.Context,
	nonce uint64,
) ([]types.MsgValsetConfirm, error) {
	return q.StoreLoader.GetValsetConfirms(nonce)
}

func (q *QGBQuerier) QueryLastUnbondingHeight(ctx context.Context) (int64, error) {
	return q.StoreLoader.GetLastUnbondingHeight()
}

func (q *QGBQuerier) QueryLastUnbondingAttestationNonce(ctx context.Context) (uint64, error) {
	return q.StoreLoader.GetLastUnbondingHeightAttestationNonce()
}

func (q *QGBQuerier) QueryLatestValset(ctx context.Context) (*types.Valset, error) {
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
func (q *QGBQuerier) QueryLastValsetBeforeNonce(ctx context.Context, nonce uint64) (*types.Valset, error) {
	if nonce == 1 { // TODO investigate if we want nonces to start at 1
		return nil, types.ErrNoValsetBeforeNonceOne
	}
	latestAttestationNonce, err := q.QueryLatestAttestationNonce(ctx)
	if err != nil {
		return nil, err
	}
	if nonce > latestAttestationNonce {
		return nil, types.ErrNonceHigherThanLatestAttestationNonce
	}
	timeout := 60 * time.Second
	// starting at 1 because the current nonce can be a valset
	// and we need the previous one.
	i := uint64(1)
	for {
		select {
		case <-time.After(timeout):
			return nil, fmt.Errorf("timeout while querying last valset before nonce")
		default:
			if i >= nonce {
				return nil, errors.Wrap(
					ErrValsetNotFound,
					fmt.Sprintf("couldn't find valset before nonce %d", nonce),
				)
			}
			at, err := q.QueryAttestationByNonce(ctx, nonce-i)
			if err != nil {
				return nil, err
			}
			if at == nil {
				timer := time.After(1 * time.Second)
				q.logger.Debug("waiting for attestation to be ingested", "nonce", nonce-i)
				select {
				case <-timer:
					continue
				case <-ctx.Done():
					return nil, nil
				}
			}
			if at.Type() == types.ValsetRequestType {
				valset, ok := at.(*types.Valset)
				if !ok {
					return nil, errors.Wrap(types.ErrAttestationNotValsetRequest, "couldn't cast attestation to valset")
				}
				return valset, nil
			}
		}
		i++
	}
}

func (q *QGBQuerier) QueryDataCommitmentByNonce(ctx context.Context, nonce uint64) (*types.DataCommitment, error) {
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
func (q *QGBQuerier) QueryAttestationByNonce(
	ctx context.Context,
	nonce uint64,
) (types.AttestationRequestI, error) {
	return q.StoreLoader.GetAttestationByNonce(nonce)
}

func (q *QGBQuerier) QueryValsetByNonce(ctx context.Context, nonce uint64) (*types.Valset, error) {
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

func (q *QGBQuerier) QueryLatestAttestationNonce(ctx context.Context) (uint64, error) {
	return q.StoreLoader.GetLastAttestationNonce()
}

var _ TmQuerierI = &TmQuerier{}

type TmQuerierI interface {
	QueryCommitment(ctx context.Context, beginBlock uint64, endBlock uint64) (bytes.HexBytes, error)
	SubscribeEvents(ctx context.Context, subscriptionName string, eventName string) (<-chan coretypes.ResultEvent, error)
}

type TmQuerier struct {
	logger        tmlog.Logger
	tendermintRPC *http.HTTP
}

func NewTmQuerier(
	tendermintRPC string,
	logger tmlog.Logger,
) (*TmQuerier, error) {
	trpc, err := http.New(tendermintRPC, "/websocket")
	if err != nil {
		return nil, err
	}
	err = trpc.Start()
	if err != nil {
		return nil, err
	}

	return &TmQuerier{
		logger:        logger,
		tendermintRPC: trpc,
	}, nil
}

// TODO add the other stop methods for other clients.
func (q *TmQuerier) Stop() {
	err := q.tendermintRPC.Stop()
	if err != nil {
		q.logger.Error(err.Error())
	}
}

// QueryCommitment queries the commitment over a set of blocks defined in the query.
func (q *TmQuerier) QueryCommitment(ctx context.Context, beginBlock uint64, endBlock uint64) (bytes.HexBytes, error) {
	dcResp, err := q.tendermintRPC.DataCommitment(ctx, beginBlock, endBlock)
	if err != nil {
		return nil, err
	}
	return dcResp.DataCommitment, nil
}

func (q *TmQuerier) SubscribeEvents(ctx context.Context, subscriptionName string, eventName string) (<-chan coretypes.ResultEvent, error) {
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

func (q *QGBQuerier) GetStorageHeightsMilestone() int64 {
	return q.StoreLoader.GetStorageHeightsMilestone()
}
