package orchestrator

import (
	"fmt"
	"sort"
	"sync"

	"github.com/celestiaorg/celestia-app/x/qgb/types"
)

// QGBStoreI is a store interface for data commitment confirms and valset confirms.
type QGBStoreI interface {
	// TODO add attestations also here
	AddDataCommitmentConfirm(confirm types.MsgDataCommitmentConfirm) error
	GetDataCommitmentConfirms(nonce uint64) ([]types.MsgDataCommitmentConfirm, error)
	GetDataCommitmentConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgDataCommitmentConfirm, error)
	GetDataCommitmentConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgDataCommitmentConfirm, error)
	AddValsetConfirm(confirm types.MsgValsetConfirm) error
	GetValsetConfirms(nonce uint64) ([]types.MsgValsetConfirm, error)
	GetValsetConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgValsetConfirm, error)
	GetValsetConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgValsetConfirm, error)
	// TODO prune the data after an unbonding period
}

// InMemoryQGBStore is simple in memory store for data commitment confirms and valset confirms.
// To be used with the InMemoryIndexer.
type InMemoryQGBStore struct {
	mutex                  *sync.Mutex
	DataCommitmentConfirms map[uint64][]types.MsgDataCommitmentConfirm
	ValsetConfirms         map[uint64][]types.MsgValsetConfirm
	Heights                []int64 // should this be pointers?
	// TODO add attestations if  https://github.com/celestiaorg/celestia-app/issues/843
	// is accepted
}

var _ QGBStoreI = &InMemoryQGBStore{}

func NewConfirmStore() *InMemoryQGBStore {
	return &InMemoryQGBStore{
		DataCommitmentConfirms: make(map[uint64][]types.MsgDataCommitmentConfirm),
		ValsetConfirms:         make(map[uint64][]types.MsgValsetConfirm),
		mutex:                  &sync.Mutex{},
		Heights:                make([]int64, 0),
	}
}

func (store *InMemoryQGBStore) AddDataCommitmentConfirm(confirm types.MsgDataCommitmentConfirm) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.DataCommitmentConfirms[confirm.Nonce] = append(store.DataCommitmentConfirms[confirm.Nonce], confirm)
	return nil
}

func (store *InMemoryQGBStore) GetDataCommitmentConfirms(nonce uint64) ([]types.MsgDataCommitmentConfirm, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	confirms, ok := store.DataCommitmentConfirms[nonce]
	if !ok {
		return nil, fmt.Errorf("not existant")
	}
	return confirms, nil
}

func (store *InMemoryQGBStore) GetDataCommitmentConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgDataCommitmentConfirm, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	confirms, ok := store.DataCommitmentConfirms[nonce]
	if !ok {
		return types.MsgDataCommitmentConfirm{}, fmt.Errorf("not existent")
	}
	for _, confirm := range confirms {
		if confirm.ValidatorAddress == orch {
			return confirm, nil
		}
	}
	return types.MsgDataCommitmentConfirm{}, fmt.Errorf("not existent")
}

func (store *InMemoryQGBStore) GetDataCommitmentConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgDataCommitmentConfirm, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	confirms, ok := store.DataCommitmentConfirms[nonce]
	if !ok {
		return types.MsgDataCommitmentConfirm{}, fmt.Errorf("not existent")
	}
	for _, confirm := range confirms {
		if confirm.EthAddress == ethAddr {
			return confirm, nil
		}
	}
	return types.MsgDataCommitmentConfirm{}, fmt.Errorf("not existent")
}

func (store *InMemoryQGBStore) AddValsetConfirm(confirm types.MsgValsetConfirm) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.ValsetConfirms[confirm.Nonce] = append(store.ValsetConfirms[confirm.Nonce], confirm)
	return nil
}

func (store *InMemoryQGBStore) GetValsetConfirms(nonce uint64) ([]types.MsgValsetConfirm, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	confirms, ok := store.ValsetConfirms[nonce]
	if !ok {
		return nil, fmt.Errorf("not existant")
	}
	return confirms, nil
}

func (store *InMemoryQGBStore) GetValsetConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgValsetConfirm, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	confirms, ok := store.ValsetConfirms[nonce]
	if !ok {
		return types.MsgValsetConfirm{}, fmt.Errorf("not existent")
	}
	for _, confirm := range confirms {
		if confirm.Orchestrator == orch {
			return confirm, nil
		}
	}
	return types.MsgValsetConfirm{}, fmt.Errorf("not existent")
}

func (store *InMemoryQGBStore) GetValsetConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgValsetConfirm, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	confirms, ok := store.ValsetConfirms[nonce]
	if !ok {
		return types.MsgValsetConfirm{}, fmt.Errorf("not existent")
	}
	for _, confirm := range confirms {
		if confirm.EthAddress == ethAddr {
			return confirm, nil
		}
	}
	return types.MsgValsetConfirm{}, fmt.Errorf("not existent")
}

// TODO We can use a map in here (if we  find something to store as value)
// Also, we could create a fixed sized array that doubles in size everytime and contains -1
// and whenever we add a height, we add it at its ID
func (store *InMemoryQGBStore) AddHeight(height int64) error {
	// https://gist.github.com/danielrangelmoreira/33b9b0686ac4e2ee2cb5f75a9896b28f
	i := sort.Search(len(store.Heights), func(i int) bool { return store.Heights[i] >= height })
	store.Heights = append(store.Heights, 0)
	copy(store.Heights[i+1:], store.Heights[i:])
	store.Heights[i] = height
	return nil
}
