package api

import (
	qgbstore "github.com/celestiaorg/celestia-app/x/qgb/orchestrator/store"
	"github.com/celestiaorg/celestia-app/x/qgb/types"
)

type QGBLoaderI interface {
	// TODO add docs same as qgb_store
	Start() error
	Stop() error
	// TODO probably add context to these (after adding a DB and seeing if needed)
	GetDataCommitmentConfirms(nonce uint64) ([]types.MsgDataCommitmentConfirm, error)
	GetDataCommitmentConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgDataCommitmentConfirm, error)
	GetDataCommitmentConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgDataCommitmentConfirm, error)
	GetValsetConfirms(nonce uint64) ([]types.MsgValsetConfirm, error)
	GetValsetConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgValsetConfirm, error)
	GetValsetConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgValsetConfirm, error)
	GetAttestationByNonce(nonce uint64) (types.AttestationRequestI, error)
	GetLastUnbondingHeight() (int64, error)
	GetLastUnbondingHeightAttestationNonce() (uint64, error)
	GetLastAttestationNonce() (uint64, error)
	GetIngestedAttestationsNonces() ([]uint64, error)
	GetIngestedHeights() ([]int64, error)
	GetStorageHeightsMilestone() int64
}

type InMemoryQGBLoader struct {
	store *qgbstore.InMemoryQGBStore
}

func (loader *InMemoryQGBLoader) GetStorageHeightsMilestone() int64 {
	return loader.store.GetHeightsMilestone()
}

var _ QGBLoaderI = &InMemoryQGBLoader{}

func NewInMemoryLoader(store *qgbstore.InMemoryQGBStore) *InMemoryQGBLoader {
	return &InMemoryQGBLoader{store: store}
}

func (loader *InMemoryQGBLoader) Start() error {
	return nil
}

func (loader *InMemoryQGBLoader) Stop() error {
	return nil
}

func (loader *InMemoryQGBLoader) GetDataCommitmentConfirms(nonce uint64) ([]types.MsgDataCommitmentConfirm, error) {
	return loader.store.GetDataCommitmentConfirms(nonce)
}

func (loader *InMemoryQGBLoader) GetDataCommitmentConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgDataCommitmentConfirm, error) {
	return loader.store.GetDataCommitmentConfirmByOrchestratorAddress(nonce, orch)
}

func (loader *InMemoryQGBLoader) GetDataCommitmentConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgDataCommitmentConfirm, error) {
	return loader.store.GetDataCommitmentConfirmByEthereumAddress(nonce, ethAddr)
}

func (loader *InMemoryQGBLoader) GetValsetConfirms(nonce uint64) ([]types.MsgValsetConfirm, error) {
	return loader.store.GetValsetConfirms(nonce)
}

func (loader *InMemoryQGBLoader) GetValsetConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgValsetConfirm, error) {
	return loader.store.GetValsetConfirmByEthereumAddress(nonce, ethAddr)
}

func (loader *InMemoryQGBLoader) GetValsetConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgValsetConfirm, error) {
	return loader.store.GetValsetConfirmByOrchestratorAddress(nonce, orch)
}

func (loader *InMemoryQGBLoader) GetAttestationByNonce(nonce uint64) (types.AttestationRequestI, error) {
	return loader.store.GetAttestation(nonce)
}

func (loader *InMemoryQGBLoader) GetLastUnbondingHeight() (int64, error) {
	return loader.store.GetLastUnbondingHeight()
}

func (loader *InMemoryQGBLoader) GetLastUnbondingHeightAttestationNonce() (uint64, error) {
	return loader.store.GetLastUnbondingHeightAttestationNonce()
}

func (loader *InMemoryQGBLoader) GetLastAttestationNonce() (uint64, error) {
	return loader.store.GetLastAttestationNonce()
}

func (loader *InMemoryQGBLoader) GetIngestedAttestationsNonces() ([]uint64, error) {
	return loader.store.GetIngestedAttestationsNonces()
}

func (loader *InMemoryQGBLoader) GetIngestedHeights() ([]int64, error) {
	return loader.store.GetIngestedHeights()
}
