package orchestrator

import "github.com/celestiaorg/celestia-app/x/qgb/types"

type QGBLoaderI interface {
	Start() error
	Stop() error
	// TODO probably add context to these (after adding a DB and seeing if needed)
	GetDataCommitmentConfirms(nonce uint64) ([]types.MsgDataCommitmentConfirm, error)
	GetDataCommitmentConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgDataCommitmentConfirm, error)
	GetDataCommitmentConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgDataCommitmentConfirm, error)
	GetValsetConfirms(nonce uint64) ([]types.MsgValsetConfirm, error)
	GetValsetConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgValsetConfirm, error)
	GetValsetConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgValsetConfirm, error)
}

type InMemoryQGBLoader struct {
	store InMemoryQGBStore
}

var _ QGBLoaderI = &InMemoryQGBLoader{}

func NewInMemoryLoader(store InMemoryQGBStore) *InMemoryQGBLoader {
	return &InMemoryQGBLoader{store: store}
}

func (loader InMemoryQGBLoader) Start() error {
	return nil
}

func (loader InMemoryQGBLoader) Stop() error {
	return nil
}

func (loader InMemoryQGBLoader) GetDataCommitmentConfirms(nonce uint64) ([]types.MsgDataCommitmentConfirm, error) {
	return loader.store.GetDataCommitmentConfirms(nonce)
}

func (loader InMemoryQGBLoader) GetDataCommitmentConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgDataCommitmentConfirm, error) {
	return loader.store.GetDataCommitmentConfirmByOrchestratorAddress(nonce, orch)
}

func (loader InMemoryQGBLoader) GetDataCommitmentConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgDataCommitmentConfirm, error) {
	return loader.store.GetDataCommitmentConfirmByEthereumAddress(nonce, ethAddr)
}

func (loader InMemoryQGBLoader) GetValsetConfirms(nonce uint64) ([]types.MsgValsetConfirm, error) {
	return loader.store.GetValsetConfirms(nonce)
}

func (loader InMemoryQGBLoader) GetValsetConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgValsetConfirm, error) {
	return loader.store.GetValsetConfirmByEthereumAddress(nonce, ethAddr)
}

func (loader InMemoryQGBLoader) GetValsetConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgValsetConfirm, error) {
	return loader.store.GetValsetConfirmByOrchestratorAddress(nonce, orch)
}
