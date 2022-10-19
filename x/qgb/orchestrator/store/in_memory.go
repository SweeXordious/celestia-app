package store

import (
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/celestiaorg/celestia-app/x/qgb/types"
)

// TODO update all queries to return empty result if not found instead of error

const (
	// DefaultLastUnbondingHeight default values written to the db when fresh starting
	DefaultLastUnbondingHeight                 = int64(-1)
	DefaultLastUnbondingHeightAttestationNonce = uint64(math.MaxUint64) // TODO check if this is alright as a default
	DefaultLastAttestationNonce                = uint64(math.MaxUint64) // TODO check if this is alright as a default
)

var _ QGBStore = &InMemoryQGBStore{}

// InMemoryQGBStore is simple in memory store for data commitment confirms and valset confirms.
// To be used with the InMemoryIndexer.
type InMemoryQGBStore struct {
	mutex                               *sync.Mutex
	Attestations                        map[uint64]types.AttestationRequestI
	IngestedAttestationsNonces          []uint64
	LastUnbondingHeightAttestationNonce uint64
	DataCommitmentConfirms              map[uint64][]types.MsgDataCommitmentConfirm
	ValsetConfirms                      map[uint64][]types.MsgValsetConfirm
	IngestedHeights                     []int64 // should this be pointers?
	LastUnbondingHeight                 int64
	HeightsMilestone                    int64
}

func NewInMemoryQGBStore() *InMemoryQGBStore {
	return &InMemoryQGBStore{
		mutex:                               &sync.Mutex{},
		Attestations:                        make(map[uint64]types.AttestationRequestI),
		IngestedAttestationsNonces:          make([]uint64, 0),
		LastUnbondingHeightAttestationNonce: DefaultLastUnbondingHeightAttestationNonce,
		DataCommitmentConfirms:              make(map[uint64][]types.MsgDataCommitmentConfirm),
		ValsetConfirms:                      make(map[uint64][]types.MsgValsetConfirm),
		IngestedHeights:                     make([]int64, 0),
		LastUnbondingHeight:                 DefaultLastUnbondingHeight,
		HeightsMilestone:                    0,
	}
}

func (store *InMemoryQGBStore) Start() error {
	return nil
}

func (store *InMemoryQGBStore) Stop() error {
	return nil
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
		return []types.MsgDataCommitmentConfirm{}, nil
	}
	return confirms, nil
}

func (store *InMemoryQGBStore) GetDataCommitmentConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgDataCommitmentConfirm, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	confirms, ok := store.DataCommitmentConfirms[nonce]
	if !ok {
		return types.MsgDataCommitmentConfirm{}, nil
	}
	for _, confirm := range confirms {
		if confirm.ValidatorAddress == orch {
			return confirm, nil
		}
	}
	return types.MsgDataCommitmentConfirm{}, nil
}

func (store *InMemoryQGBStore) GetDataCommitmentConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgDataCommitmentConfirm, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	confirms, ok := store.DataCommitmentConfirms[nonce]
	if !ok {
		return types.MsgDataCommitmentConfirm{}, nil
	}
	for _, confirm := range confirms {
		if confirm.EthAddress == ethAddr {
			return confirm, nil
		}
	}
	return types.MsgDataCommitmentConfirm{}, nil
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
		return []types.MsgValsetConfirm{}, nil
	}
	return confirms, nil
}

func (store *InMemoryQGBStore) GetValsetConfirmByOrchestratorAddress(nonce uint64, orch string) (types.MsgValsetConfirm, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	confirms, ok := store.ValsetConfirms[nonce]
	if !ok {
		return types.MsgValsetConfirm{}, nil
	}
	for _, confirm := range confirms {
		if confirm.Orchestrator == orch {
			return confirm, nil
		}
	}
	return types.MsgValsetConfirm{}, nil
}

func (store *InMemoryQGBStore) GetValsetConfirmByEthereumAddress(nonce uint64, ethAddr string) (types.MsgValsetConfirm, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	confirms, ok := store.ValsetConfirms[nonce]
	if !ok {
		return types.MsgValsetConfirm{}, nil
	}
	for _, confirm := range confirms {
		if confirm.EthAddress == ethAddr {
			return confirm, nil
		}
	}
	return types.MsgValsetConfirm{}, nil
}

// TODO We can use a map in here (if we  find something to store as value)
// Also, we could create a fixed sized array that doubles in size everytime and contains -1
// and whenever we add a height, we add it at its ID
func (store *InMemoryQGBStore) AddIngestedHeight(height int64) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	i := sort.Search(len(store.IngestedHeights), func(i int) bool { return store.IngestedHeights[i] >= height })
	// TODO uncomment and fix
	//if len(store.IngestedHeights) != 0 && store.IngestedHeights[i] == height {
	//	return nil
	//}
	store.IngestedHeights = append(store.IngestedHeights, 0)
	copy(store.IngestedHeights[i+1:], store.IngestedHeights[i:])
	store.IngestedHeights[i] = height
	if int64(len(store.IngestedHeights)) > store.HeightsMilestone {
		store.heightsMilestoneCatchup()
	}
	return nil
}

func (store *InMemoryQGBStore) AddIngestedAttestationNonce(nonce uint64) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	i := sort.Search(len(store.IngestedAttestationsNonces), func(i int) bool { return store.IngestedAttestationsNonces[i] >= nonce })
	// TODO uncomment and fix
	//if store.IngestedAttestationsNonces[i] == nonce {
	//	return nil
	//}
	store.IngestedAttestationsNonces = append(store.IngestedAttestationsNonces, 0)
	copy(store.IngestedAttestationsNonces[i+1:], store.IngestedAttestationsNonces[i:])
	store.IngestedAttestationsNonces[i] = nonce
	return nil
}

func (store *InMemoryQGBStore) AddAttestation(attestation types.AttestationRequestI) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	store.Attestations[attestation.GetNonce()] = attestation
	return nil
}

func (store *InMemoryQGBStore) GetAttestation(nonce uint64) (types.AttestationRequestI, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.Attestations[nonce], nil
}

func (store *InMemoryQGBStore) GetLastUnbondingHeight() (int64, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.LastUnbondingHeight, nil
}

func (store *InMemoryQGBStore) GetLastUnbondingHeightAttestationNonce() (uint64, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.LastUnbondingHeightAttestationNonce, nil
}

func (store *InMemoryQGBStore) SetLastUnbondingHeight(height int64) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.LastUnbondingHeight > height {
		return fmt.Errorf(
			"new unbonding height %d height is lower than previous one %d",
			height,
			store.LastUnbondingHeight,
		)
	}
	store.LastUnbondingHeight = height
	return nil
}

func (store *InMemoryQGBStore) SetLastUnbondingHeightAttestationNonce(nonce uint64) error {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if store.LastUnbondingHeightAttestationNonce != DefaultLastUnbondingHeightAttestationNonce &&
		store.LastUnbondingHeightAttestationNonce > nonce {
		return fmt.Errorf(
			"new unbonding height attestation nonce %d is lower than previous one %d",
			nonce,
			store.LastUnbondingHeightAttestationNonce,
		)
	}
	store.LastUnbondingHeightAttestationNonce = nonce
	return nil
}

func (store *InMemoryQGBStore) GetLastAttestationNonce() (uint64, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	if len(store.IngestedAttestationsNonces) == 0 {
		return DefaultLastAttestationNonce, nil
	}
	return store.IngestedAttestationsNonces[len(store.IngestedAttestationsNonces)-1], nil
}

func (store *InMemoryQGBStore) GetIngestedHeights() ([]int64, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.IngestedHeights, nil
}

func (store *InMemoryQGBStore) GetIngestedAttestationsNonces() ([]uint64, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.IngestedAttestationsNonces, nil
}

func (store *InMemoryQGBStore) GetHeightsMilestone() int64 {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.HeightsMilestone
}

// heightsMilestoneCatchup calculates the milestone at which all the previous heights
// were ingested.
// This is used mainly by the orchestrator to decide whether the old chain data, i.e data commitments
// and valsets, were all ingested, or to wait for more blocks to be ingested before deciding
// whether to sign an attestation or not.
func (store *InMemoryQGBStore) heightsMilestoneCatchup() {
	l := store.HeightsMilestone
	for {
		if int64(len(store.IngestedHeights)) == l || store.IngestedHeights[l] != l+1 {
			return
		}
		if store.IngestedHeights[l] == l+1 {
			store.HeightsMilestone = l + 1
		}
		l++
	}
}
