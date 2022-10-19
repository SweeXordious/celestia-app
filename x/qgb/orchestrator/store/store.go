package store

import "github.com/celestiaorg/celestia-app/x/qgb/types"

// QGBStore is a store interface for data commitment confirms and valset confirms.
type QGBStore interface {
	Start() error
	Stop() error

	AddAttestation(attestation types.AttestationRequestI) error

	// GetAttestation returns the attestation having the provided nonce.
	// returns nil attestation if not found.
	GetAttestation(nonce uint64) (types.AttestationRequestI, error)

	AddDataCommitmentConfirm(confirm types.MsgDataCommitmentConfirm) error

	// GetDataCommitmentConfirms returns data commitment confirms of a certain nonce.
	// returns empty slice if not found.
	GetDataCommitmentConfirms(nonce uint64) ([]types.MsgDataCommitmentConfirm, error)

	// GetDataCommitmentConfirmByOrchestratorAddress returns a data commitment confirm with the specified nonce
	// and orchestrator address.
	// returns empty confirm if not found.
	GetDataCommitmentConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgDataCommitmentConfirm, error)

	// GetDataCommitmentConfirmByEthereumAddress returns a data commitment confirm with the specified nonce
	// and ethereum address.
	// returns empty confirm if not found.
	GetDataCommitmentConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgDataCommitmentConfirm, error)

	AddValsetConfirm(confirm types.MsgValsetConfirm) error

	// GetValsetConfirms returns valset confirms of a certain nonce.
	// returns empty slice if not found.
	GetValsetConfirms(nonce uint64) ([]types.MsgValsetConfirm, error)

	// GetValsetConfirmByOrchestratorAddress returns a valset confirm with the specified nonce
	// and orchestrator address.
	// returns empty confirm if not found.
	GetValsetConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgValsetConfirm, error)

	// GetValsetConfirmByEthereumAddress returns a valset confirm with the specified nonce
	// and ethereum address.
	// returns empty confirm if not found.
	GetValsetConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgValsetConfirm, error)

	// SetLastUnbondingHeight sets the latest unbonding height.
	// returns error if the new height is lower than previous one.
	SetLastUnbondingHeight(height int64) error

	// SetLastUnbondingHeightAttestationNonce sets the last nonce before the last
	// unbonding height.
	// For the genesis, the nonce is set to zero.
	// returns error if the new nonce is lower than previous one.
	SetLastUnbondingHeightAttestationNonce(nonce uint64) error

	// GetLastUnbondingHeight returns the latest unbonding height.
	// returns DefaultLastUnbondingHeight if still not set for the first time.
	GetLastUnbondingHeight() (int64, error)

	// GetLastUnbondingHeightAttestationNonce returns the last nonce before the last
	// unbonding height.
	// returns DefaultLastUnbondingHeightAttestationNonce if still not set for the first time.
	GetLastUnbondingHeightAttestationNonce() (uint64, error)

	// AddIngestedHeight adds the ingested height to the heights slice.
	// the slice should stay ordered all the time.
	// if the height is already processed, doesn't add it.
	// TODO should we add some different behaviour when adding already processed height?
	AddIngestedHeight(height int64) error

	// AddIngestedAttestationNonce adds the ingested attestation nonce to the nonces slice.
	// the slice should stay ordered all the time.
	// if the nonce is already processed, doesn't add it.
	// TODO should we add some different behaviour when adding already processed nonce?
	AddIngestedAttestationNonce(nonce uint64) error

	// GetLastAttestationNonce returns the last attestation nonce.
	// returns DefaultLastAttestationNonce if no attestation have been processed yet.
	GetLastAttestationNonce() (uint64, error) // TODO add paging support and queries

	// GetIngestedHeights returns the slice of ingested block heights.
	GetIngestedHeights() ([]int64, error) // TODO add paging support and queries

	// GetIngestedAttestationsNonces returns the slice of processed attestations nonces.
	GetIngestedAttestationsNonces() ([]uint64, error)

	// TODO prune the data after an unbonding period
}
