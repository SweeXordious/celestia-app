package orchestrator

import (
	"context"
	"fmt"
	"github.com/celestiaorg/celestia-app/x/qgb/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/pkg/errors"
	tmlog "github.com/tendermint/tendermint/libs/log"
	"math/big"
	"time"
)

type QGBStoreQuerierI interface {
}

type QGBStoreQuerier struct {
	logger       tmlog.Logger
	StoreLoader  InMemoryQGBLoader
	StateQuerier RPCStateQuerierI // TODO remove if https://github.com/celestiaorg/celestia-app/issues/843
	// got accepted
}

func (q QGBStoreQuerier) QueryTwoThirdsDataCommitmentConfirms(
	ctx context.Context,
	timeout time.Duration,
	dc types.DataCommitment,
) ([]types.MsgDataCommitmentConfirm, error) {
	valset, err := q.StateQuerier.QueryLastValsetBeforeNonce(ctx, dc.Nonce)
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
			commitment, err := q.StateQuerier.QueryCommitment(ctx, dc.BeginBlock, dc.EndBlock)
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
		time.Sleep(10 * time.Second)
	}
}

// validateDCConfirm runs validation on the data commitment confirm to make sure it was well created.
// it tests if the commitment it carries is the correct commitment. Then, checks whether the signature
// is valid.
func validateDCConfirm(commitment string, confirm types.MsgDataCommitmentConfirm) error {
	// TODO make the checks more exhaustive
	if confirm.Commitment != commitment {
		return ErrInvalidCommitmentInConfirm
	}
	bCommitment := common.Hex2Bytes(commitment)
	dataRootHash := DataCommitmentTupleRootSignBytes(types.BridgeID, big.NewInt(int64(confirm.Nonce)), bCommitment)
	err := ValidateEthereumSignature(dataRootHash.Bytes(), common.Hex2Bytes(confirm.Signature), common.HexToAddress(confirm.EthAddress))
	if err != nil {
		return err
	}
	return nil
}

func (q QGBStoreQuerier) QueryTwoThirdsValsetConfirms(
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
		vs, err := q.StateQuerier.QueryLastValsetBeforeNonce(ctx, valset.Nonce)
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
		time.Sleep(10 * time.Second)
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
	err = ValidateEthereumSignature(signBytes.Bytes(), common.Hex2Bytes(confirm.Signature), common.HexToAddress(confirm.EthAddress))
	if err != nil {
		return err
	}
	return nil
}

func (q QGBStoreQuerier) QueryValsetConfirmByOrchestratorAddress(
	ctx context.Context,
	nonce uint64,
	address string,
) (types.MsgValsetConfirm, error) {
	return q.StoreLoader.GetValsetConfirmByOrchestratorAddress(nonce, address)
}

func (q QGBStoreQuerier) QueryDataCommitmentConfirmByOrchestratorAddress(
	ctx context.Context,
	nonce uint64,
	address string,
) (types.MsgDataCommitmentConfirm, error) {
	return q.StoreLoader.GetDataCommitmentConfirmByOrchestratorAddress(nonce, address)
}

func (q QGBStoreQuerier) QueryDataCommitmentConfirms(
	ctx context.Context,
	nonce uint64,
) ([]types.MsgDataCommitmentConfirm, error) {
	return q.StoreLoader.GetDataCommitmentConfirms(nonce)
}

func (q QGBStoreQuerier) QueryValsetConfirms(
	ctx context.Context,
	nonce uint64,
) ([]types.MsgValsetConfirm, error) {
	return q.StoreLoader.GetValsetConfirms(nonce)
}
