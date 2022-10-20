package ingestion

import (
	"context"
	"fmt"
	"github.com/celestiaorg/celestia-app/app/encoding"
	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/api"
	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/utils"
	"github.com/celestiaorg/celestia-app/x/qgb/types"
	tmlog "github.com/tendermint/tendermint/libs/log"
	"sync"
)

const (
	// RunningRoutinesNumber keeps track of the number of running go routines
	// aside from the worker pools.
	// These routines include:
	//	- enqueuing services such as EnqueuingMissingBlockHeights and EnqueuingNewBlockHeights
	//  - single processing services such as the orchestrator signing service
	//  - etc.
	RunningRoutinesNumber = 5 // TODO update the value to the correct one after finishing the implementation
)

// I don't like the name
type IngestorI interface {
	Start() error
	Stop() error
	Ingest() error
}

// TODO investigate which is better:
//   - the current design where there are predefined routines that are listening/processing
//   - or, a streaming like methodology where all routines are waiting for the same updates
type Ingestor struct {
	logger       tmlog.Logger // maybe use a more general interface
	tmExtractor  TmExtractorI
	qgbExtractor QGBExtractorI
	loader       api.QGBLoaderI
	indexer      IndexerI
	encCfg       encoding.Config
	workers      int
}

func NewIngestor(
	qgbExtractor QGBExtractorI,
	tmExtractor TmExtractorI,
	indexer IndexerI,
	logger tmlog.Logger,
	loader api.QGBLoaderI,
	encCfg encoding.Config,
	workers int,
) (*Ingestor, error) {
	return &Ingestor{
		tmExtractor:  tmExtractor,
		indexer:      indexer,
		workers:      workers,
		qgbExtractor: qgbExtractor,
		logger:       logger,
		loader:       loader,
		encCfg:       encCfg,
	}, nil
}

func (ingestor *Ingestor) Start(ctx context.Context, signalChan chan struct{}) error { // TODO update channel  type
	lastUnbondingHeight, err := ingestor.qgbExtractor.QueryLastUnbondingHeight(ctx)
	if err != nil {
		return err
	}
	lastUnbondingAttestationNonce, err := ingestor.qgbExtractor.QueryLastUnbondingAttestationNonce(ctx)
	if err != nil {
		return err
	}

	err = ingestor.initStorage(ctx, int64(lastUnbondingHeight), lastUnbondingAttestationNonce)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}

	// contains the nonces that will be stored by the indexer.
	jobChan := make(chan ingestionJob, ingestor.workers*100)
	defer close(jobChan)

	// TODO rename to either current or last on all orchestrator
	currentAttestationNonce, err := ingestor.qgbExtractor.QueryLatestAttestationNonce(ctx)
	if err != nil {
		return err
	}

	currentBlockHeight, err := ingestor.tmExtractor.QueryHeight(ctx)
	if err != nil {
		return err
	}

	blockSubscription := NewBlockSubscription(
		ingestor.tmExtractor,
		ingestor.logger,
		NewAttestationNoncesListener(
			currentAttestationNonce,
			jobChan,
			ingestor.qgbExtractor,
			ingestor.logger,
		),
		NewUnbondingHeightListener(
			ingestor.indexer,
			ingestor.qgbExtractor,
			int64(lastUnbondingAttestationNonce),
			ingestor.logger,
		),
		NewUnbondingAttestationNonceListener(
			ingestor.indexer,
			ingestor.qgbExtractor,
			lastUnbondingAttestationNonce,
			ingestor.logger,
		),
		NewBlockHeightsListener(
			currentBlockHeight,
			jobChan,
			ingestor.tmExtractor,
			ingestor.logger,
		),
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := blockSubscription.Start(ctx)
		if err != nil {
			ingestor.logger.Error("error listening for new block heights", "err", err)
			close(signalChan) // TODO handle these errors more carefully
		}
		ingestor.logger.Info("stopping enqueing missing blocks")

	}()

	for i := 0; i < ingestor.workers; i++ { // TODO this should be merged with the attestations one
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := ingestor.Ingest(ctx, jobChan, signalChan)
			if err != nil {
				ingestor.logger.Error("error ingesting", "err", err)
				close(signalChan) // TODO handle these errors more carefully
			}
			ingestor.logger.Info("stopping ingestion job")
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := ingestor.EnqueueMissingBlockHeights(ctx, jobChan, currentBlockHeight, int64(lastUnbondingHeight), signalChan)
		if err != nil {
			ingestor.logger.Error("error enqueing missing blocks", "err", err)
			close(signalChan) // TODO handle these errors more carefully
		}
		ingestor.logger.Info("stopping enqueing missing blocks")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := ingestor.EnqueueMissingAttestationNonces(ctx, jobChan, currentAttestationNonce, lastUnbondingAttestationNonce, signalChan)
		if err != nil {
			ingestor.logger.Error("error enqueing missing attestations", "err", err)
			close(signalChan)
		}
		ingestor.logger.Info("stopping enqueing missing attestations")
	}()

	// TODO add  another go routine that keep checking if all the attestations were processed

	wg.Wait()
	return nil
}

type ingestionJob interface {
}

var _ ingestionJob = &attestationJob{}

type attestationJob struct {
	nonce uint64
}

var _ ingestionJob = &blockJob{}

type blockJob struct {
	height int64
}

func (ingestor *Ingestor) EnqueueMissingAttestationNonces(
	ctx context.Context,
	jobChan chan<- ingestionJob,
	latestNonce uint64,
	lastUnbondingNonce uint64,
	signalChan <-chan struct{},
) error {
	ingestor.logger.Info("syncing missing attestation nonces", "latest_nonce", latestNonce, "last_unbonding_attestation_nonce", lastUnbondingNonce)

	for i := lastUnbondingNonce - 1; i < latestNonce; i++ { // FIXME this is not unbonding nonce, this is unbonding height!!!!
		select {
		case <-signalChan: // TODO handle signaling correctly
			return nil
		case <-ctx.Done(): // TODO handle signaling correctly
			return nil
		default:
			// TODO only log every 500 nonce or so
			ingestor.logger.Debug("enqueueing missing attestation nonce", "nonce", latestNonce-i)
			if i%100 == 0 {
				ingestor.logger.Info("enqueueing missing attestation nonces interval", "beginning_nonce", latestNonce, "end_nonce", latestNonce-i)
			}
			select {
			case <-signalChan: // TODO handle signaling correctly
				return nil
			case jobChan <- attestationJob{nonce: latestNonce - i}:
			}
		}
	}
	ingestor.logger.Info("finished syncing missing nonces", "latest_nonce", latestNonce, "last_unbonding_attestation_nonce", lastUnbondingNonce)
	return nil
}

func (ingestor *Ingestor) IngestAttestation(ctx context.Context, nonce uint64) error {
	att, err := ingestor.qgbExtractor.ExtractAttestationByNonce(ctx, nonce)
	if err != nil {
		return err
	}
	if att == nil {
		return types.ErrAttestationNotFound
	}
	existingAtt, err := ingestor.loader.GetAttestationByNonce(nonce)
	if err != nil {
		return err
	}
	if existingAtt != nil {
		ingestor.logger.Info("already ingested attestation. ignoring", "nonce", nonce)
		return nil
	}
	err = ingestor.indexer.AddAttestation(att)
	if err != nil {
		return err
	}
	err = ingestor.indexer.AddIngestedAttestationNonce(att.GetNonce())
	if err != nil {
		return err
	}
	return nil
}

func (ingestor *Ingestor) EnqueueMissingBlockHeights(
	ctx context.Context,
	jobChan chan<- ingestionJob,
	currentHeight int64,
	lastUnbondingHeight int64,
	signalChan chan<- struct{},
) error {
	ingestor.logger.Info("syncing missing block heights", "chain_height", currentHeight, "last_unbonding_height", lastUnbondingHeight)

	for i := int64(lastUnbondingHeight); i < currentHeight; i++ {
		height := currentHeight - i
		select {
		case <-ctx.Done():
			return nil
		default:
			// TODO only log every 500 block or so
			ingestor.logger.Debug("enqueueing missing block heights", "height", height)
			if i%100 == 0 { // TODO better logging on the one above this one
				ingestor.logger.Info("enqueueing missing block heights", "begin_height", currentHeight, "end_height", currentHeight-i)
			}
			select {
			case <-ctx.Done():
				return nil
			case jobChan <- blockJob{height: height}:
			}
		}
	}
	ingestor.logger.Info("finished syncing missing block heights", "chain_height", currentHeight, "last_unbonding_height", lastUnbondingHeight)
	return nil
}

// TODO rename to IngestionWorker
func (ingestor *Ingestor) IngestBlockHeight(ctx context.Context, height *int64, signalChan chan<- struct{}) error {
	ingestor.logger.Debug("ingesting block height", "height", height)
	block, err := ingestor.tmExtractor.ExtractBlock(ctx, height)
	if err != nil {
		return err
	}
	for _, coreTx := range block.Block.Txs {
		sdkTx, err := ingestor.encCfg.TxConfig.TxDecoder()(coreTx)
		if err != nil {
			// TODO concrete errors everywhere
			return fmt.Errorf("error while unpacking message: %s", err)
		}
		for _, msg := range sdkTx.GetMsgs() {
			switch m := msg.(type) {
			case *types.MsgDataCommitmentConfirm:
				err = ingestor.handleDataCommitmentConfirm(*m)
				if err != nil {
					return err
				}
			case *types.MsgValsetConfirm:
				err = ingestor.handleValsetConfirm(*m)
				if err != nil {
					return err
				}
			}
			// TODO handle errors in a retrier
		}
	}
	err = ingestor.indexer.AddIngestedHeight(*height)
	if err != nil {
		return err
	}
	return nil
}

func (ingestor *Ingestor) handleDataCommitmentConfirm(dcc types.MsgDataCommitmentConfirm) error {
	existingDcc, err := ingestor.loader.GetDataCommitmentConfirmByOrchestratorAddress(dcc.Nonce, dcc.ValidatorAddress)
	if err != nil {
		return err
	}
	if !utils.IsEmptyMsgDataCommitmentConfirm(existingDcc) {
		ingestor.logger.Info("data commitment confirm already indexed, ignoring", "nonce", dcc.Nonce, "orchestrator", dcc.ValidatorAddress)
		return nil
	}
	err = ingestor.indexer.AddDataCommitmentConfirm(dcc)
	if err != nil {
		return err
	}
	return nil
}

func (ingestor *Ingestor) handleValsetConfirm(vs types.MsgValsetConfirm) error {
	// TODO either use Orchestrator or validator address on the state machine proto file
	existingVs, err := ingestor.loader.GetValsetConfirmByOrchestratorAddress(vs.Nonce, vs.Orchestrator)
	if err != nil {
		return err
	}
	if !utils.IsEmptyMsgValsetConfirm(existingVs) {
		ingestor.logger.Info("valset confirm already indexed, ignoring", "nonce", vs.Nonce, "orchestrator", vs.Orchestrator)
		return nil
	}
	err = ingestor.indexer.AddValsetConfirm(vs)
	if err != nil {
		return err
	}
	return nil
}

func (ingestor *Ingestor) initStorage(ctx context.Context, lastUnbondingHeight int64, lastUnbondingAttestationNonce uint64) error {
	err := ingestor.indexer.SetLastUnbondingHeight(int64(lastUnbondingHeight))
	if err != nil {
		return err
	}
	err = ingestor.indexer.SetLastUnbondingHeightAttestationNonce(lastUnbondingAttestationNonce)
	if err != nil {
		return err
	}
	return nil
}

func (ingestor *Ingestor) Ingest(ctx context.Context, jobChan chan ingestionJob, signalChan chan struct{}) error {
	for {
		select {
		case <-ctx.Done():
			// close(signalChan) // TODO investigate if this is needed
			return nil
		case job := <-jobChan:
			switch j := job.(type) {
			case attestationJob:
				ingestor.logger.Debug("ingesting attestation nonce", "nonce", j.nonce)
				if err := ingestor.IngestAttestation(ctx, j.nonce); err != nil {
					ingestor.logger.Error("failed to ingest attestation nonce, retrying", "nonce", j.nonce, "err", err)
					//if err := ingestor.Retrier.Retry(ctx, nonce, ingestor.IngestAttestation); err != nil { // TODO add retrier for the other guy as well
					//	close(signalChan)
					//	return err
					//}
					return err
				}
			case blockJob:
				err := ingestor.IngestBlockHeight(ctx, &j.height, signalChan)
				if err != nil {
					return err
				}
			}
		}
	}
}
