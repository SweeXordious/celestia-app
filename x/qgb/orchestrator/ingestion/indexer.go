package ingestion

import (
	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/store"
	"github.com/celestiaorg/celestia-app/x/qgb/types"
)

// TODO add docs
type IndexerI interface {
	Start() error
	Stop() error
	AddAttestation(attestation types.AttestationRequestI) error
	AddDataCommitmentConfirm(confirm types.MsgDataCommitmentConfirm) error
	AddValsetConfirm(confirm types.MsgValsetConfirm) error
	AddIngestedHeight(height int64) error
	AddIngestedAttestationNonce(nonce uint64) error
	SetLastUnbondingHeight(height int64) error
	SetLastUnbondingHeightAttestationNonce(nonce uint64) error
	// Remove() error ?
}

var _ IndexerI = &InMemoryIndexer{}

// TODO update name to InMemoryQGBIndexer
type InMemoryIndexer struct {
	Store store.QGBStore
}

func NewInMemoryIndexer(store *store.InMemoryQGBStore) *InMemoryIndexer {
	return &InMemoryIndexer{
		Store: store,
	}
}

func (indexer InMemoryIndexer) Start() error {
	return nil
}

func (indexer InMemoryIndexer) Stop() error {
	return nil
}

func (indexer InMemoryIndexer) AddDataCommitmentConfirm(confirm types.MsgDataCommitmentConfirm) error {
	return indexer.Store.AddDataCommitmentConfirm(confirm)
}

func (indexer InMemoryIndexer) AddValsetConfirm(confirm types.MsgValsetConfirm) error {
	return indexer.Store.AddValsetConfirm(confirm)
}

func (indexer InMemoryIndexer) AddIngestedHeight(height int64) error {
	return indexer.Store.AddIngestedHeight(height)
}

func (indexer InMemoryIndexer) AddAttestation(attestation types.AttestationRequestI) error {
	return indexer.Store.AddAttestation(attestation)
}

func (indexer InMemoryIndexer) AddIngestedAttestationNonce(nonce uint64) error {
	return indexer.Store.AddIngestedAttestationNonce(nonce)
}

func (indexer InMemoryIndexer) SetLastUnbondingHeight(height int64) error {
	return indexer.Store.SetLastUnbondingHeight(height)
}

func (indexer InMemoryIndexer) SetLastUnbondingHeightAttestationNonce(nonce uint64) error {
	return indexer.Store.SetLastUnbondingHeightAttestationNonce(nonce)
}
