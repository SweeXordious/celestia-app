package ingestion

import (
	"context"

	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/utils"
	tmlog "github.com/tendermint/tendermint/libs/log"
	corerpctypes "github.com/tendermint/tendermint/rpc/core/types"
	coretypes "github.com/tendermint/tendermint/types"
)

type BlockSubscriptionListener interface {
	execute(ctx context.Context, result corerpctypes.ResultEvent) error
	name() string
}

type BlockHeightsListener struct {
	currentHeight int64
	jobChan       chan<- ingestionJob
	tmExtractor   TmExtractorI
	logger        tmlog.Logger
}

var _ BlockSubscriptionListener = &BlockHeightsListener{}

func NewBlockHeightsListener(
	currentHeight int64,
	jobChan chan<- ingestionJob,
	tmExtractor TmExtractorI,
	logger tmlog.Logger,
) *BlockHeightsListener {
	return &BlockHeightsListener{
		currentHeight: currentHeight,
		jobChan:       jobChan,
		tmExtractor:   tmExtractor,
		logger:        logger,
	}
}

func (listener *BlockHeightsListener) name() string {
	return "block heights listener"
}

func (listener *BlockHeightsListener) execute(ctx context.Context, result corerpctypes.ResultEvent) error {
	blockEvent := utils.MustGetEvent(result, coretypes.EventTypeKey)
	isBlock := blockEvent[0] == coretypes.EventNewBlock
	if !isBlock {
		// we only want to handle this when the block is committed
		return nil
	}
	height, err := listener.tmExtractor.QueryHeight(ctx)
	// listener.logger.Debug("found new block height", "height", height) // TODO better message
	if err != nil {
		return err
	}
	// This loop is not adding the new height to the queue, but the previous one.
	// Thus, a block is not ingested until the next one is found.
	// The reasons for this is sometimes a block gets ingested before the node indexes it
	// in its db, and its ingestion fails.
	for i := listener.currentHeight; i < height; i++ {
		listener.logger.Debug("enqueueing new block height", "height", i)
		select {
		case <-ctx.Done():
			return nil
		case listener.jobChan <- blockJob{height: i}: // TODO this is missing blocks when channel is full
		}
	}
	listener.currentHeight = height
	return nil
}

type AttestationNoncesListener struct {
	currentNonce uint64
	jobChan      chan<- ingestionJob
	qgbExtractor QGBExtractorI
	logger       tmlog.Logger
}

var _ BlockSubscriptionListener = &AttestationNoncesListener{}

func NewAttestationNoncesListener(
	currentNonce uint64,
	jobChan chan<- ingestionJob,
	qgbExtractor QGBExtractorI,
	logger tmlog.Logger,
) *AttestationNoncesListener {
	return &AttestationNoncesListener{
		currentNonce: currentNonce,
		jobChan:      jobChan,
		qgbExtractor: qgbExtractor,
		logger:       logger,
	}
}

func (listener *AttestationNoncesListener) name() string {
	return "attestation nonces listener"
}

func (listener *AttestationNoncesListener) execute(ctx context.Context, result corerpctypes.ResultEvent) error {
	blockEvent := utils.MustGetEvent(result, coretypes.EventTypeKey)
	isBlock := blockEvent[0] == coretypes.EventNewBlock
	if !isBlock {
		// we only want to handle the attestation when the block is committed
		return nil
	}
	// TODO update lastNonce to latestNonce or the opposite. on the whole QGB side
	lastNonce, err := listener.qgbExtractor.QueryLatestAttestationNonce(ctx)
	if err != nil {
		return err
	}
	if lastNonce == listener.currentNonce {
		return nil
	}
	for i := listener.currentNonce + 1; i <= lastNonce; i++ {
		listener.logger.Debug("enqueueing new attestation nonce", "nonce", i)
		select {
		case <-ctx.Done(): // TODO handle signal correctly
			return nil
		case listener.jobChan <- attestationJob{nonce: lastNonce}:
		}
	}
	listener.currentNonce = lastNonce
	return nil
}

type UnbondingHeightListener struct {
	indexer                IndexerI
	extractor              QGBExtractorI
	currentUnbondingHeight int64
	logger                 tmlog.Logger
}

var _ BlockSubscriptionListener = &UnbondingHeightListener{}

func NewUnbondingHeightListener(
	indexer IndexerI,
	extractor QGBExtractorI,
	currentUnbondingHeight int64,
	logger tmlog.Logger,
) *UnbondingHeightListener {
	return &UnbondingHeightListener{
		indexer:                indexer,
		extractor:              extractor,
		currentUnbondingHeight: currentUnbondingHeight,
		logger:                 logger,
	}
}

func (listener *UnbondingHeightListener) name() string {
	return "unbonding heights listener"
}

func (listener *UnbondingHeightListener) execute(ctx context.Context, result corerpctypes.ResultEvent) error {
	// TODO extract this to a different function
	blockEvent := utils.MustGetEvent(result, coretypes.EventTypeKey)
	isBlock := blockEvent[0] == coretypes.EventNewBlock
	if !isBlock {
		// we only want to handle the unbonding height when the block is committed
		return nil
	}
	newUnbondingHeight, err := listener.extractor.QueryLastUnbondingHeight(ctx)
	if err != nil {
		return err
	}
	// TODO use the same type across state machine and orchestrator
	// for unbonding heights
	if listener.currentUnbondingHeight < int64(newUnbondingHeight) {
		err = listener.indexer.SetLastUnbondingHeight(int64(newUnbondingHeight))
		if err != nil {
			return err
		}
		listener.logger.Info("setting new unbonding height", "height", newUnbondingHeight)
	}
	listener.currentUnbondingHeight = int64(newUnbondingHeight)
	return nil
}

type UnbondingAttestationNonceListener struct {
	indexer                          IndexerI
	extractor                        QGBExtractorI
	currentUnbondingAttestationNonce uint64
	logger                           tmlog.Logger
}

var _ BlockSubscriptionListener = &UnbondingAttestationNonceListener{}

func NewUnbondingAttestationNonceListener(
	indexer IndexerI,
	extractor QGBExtractorI,
	currentUnbondingAttestationNonce uint64,
	logger tmlog.Logger,
) *UnbondingAttestationNonceListener {
	return &UnbondingAttestationNonceListener{
		indexer:                          indexer,
		extractor:                        extractor,
		currentUnbondingAttestationNonce: currentUnbondingAttestationNonce,
		logger:                           logger,
	}
}

func (listener *UnbondingAttestationNonceListener) name() string {
	return "unbonding attestation nonce listener"
}

func (listener *UnbondingAttestationNonceListener) execute(ctx context.Context, result corerpctypes.ResultEvent) error {
	// TODO extract this to a different function
	blockEvent := utils.MustGetEvent(result, coretypes.EventTypeKey)
	isBlock := blockEvent[0] == coretypes.EventNewBlock
	if !isBlock {
		// we only want to handle the unbonding height when the block is committed
		return nil
	}
	newUnbondingAttestationHeight, err := listener.extractor.QueryLastUnbondingAttestationNonce(ctx)
	if err != nil {
		return err
	}
	// TODO use the same type across state machine and orchestrator
	// for unbonding heights
	if listener.currentUnbondingAttestationNonce < newUnbondingAttestationHeight {
		err = listener.indexer.SetLastUnbondingHeight(int64(newUnbondingAttestationHeight))
		if err != nil {
			return err
		}
		listener.logger.Info("setting new unbonding attestation nonce", "nonce", newUnbondingAttestationHeight)
	}
	listener.currentUnbondingAttestationNonce = newUnbondingAttestationHeight
	return nil
}

type BlockSubscription struct {
	listeners []BlockSubscriptionListener
	extractor TmExtractorI
	logger    tmlog.Logger
}

func NewBlockSubscription(
	extractor TmExtractorI,
	logger tmlog.Logger,
	listeners ...BlockSubscriptionListener,
) *BlockSubscription {
	for _, listener := range listeners {
		logger.Info("registering block listener", "listener", listener.name())
	}
	return &BlockSubscription{
		extractor: extractor,
		logger:    logger,
		listeners: listeners,
	}
}

func (s BlockSubscription) Start(ctx context.Context) error {
	results, err := s.extractor.SubscribeNewBlocks(ctx, "new-blocks-updates")
	if err != nil {
		return err
	}
	s.logger.Info("listening for new blocks...")
	for {
		select {
		case <-ctx.Done():
			return nil
		case result := <-results:
			for _, listener := range s.listeners {
				err = listener.execute(ctx, result)
				if err != nil {
					return err
				}
			}
		}
	}
}
