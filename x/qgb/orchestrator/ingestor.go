package orchestrator

import (
	"context"
	"fmt"
	"github.com/celestiaorg/celestia-app/x/qgb/types"
	tmlog "github.com/tendermint/tendermint/libs/log"
	coretypes "github.com/tendermint/tendermint/types"
	"sync"
)

// I don't like the name
type IngestorI interface {
	Start() error
	Stop() error
	Ingest() error
}

type Ingestor struct {
	Logger    tmlog.Logger // maybe use a more general interface
	extractor ExtractorI
	parser    QGBParserI
	indexer   IndexerI
	Querier   Querier
	workers   int
}

func NewIngestor(extractor ExtractorI, parser QGBParserI, indexer IndexerI, querier Querier, logger tmlog.Logger, workers int) (*Ingestor, error) {
	return &Ingestor{
		extractor: extractor,
		parser:    parser,
		indexer:   indexer,
		workers:   workers,
		Querier:   querier,
		Logger:    logger,
	}, nil
}

func (ingestor Ingestor) Start(ctx context.Context, enqueueMissingSignalChan chan<- struct{}) error {
	// is it better for this channel to contain pointers or values?
	heightsChan := make(chan *int64, ingestor.workers*100)
	defer close(heightsChan)

	withCancel, cancel := context.WithCancel(ctx)

	wg := &sync.WaitGroup{}

	// used to send a signal when a worker wants to notify the enqueuing services to stop.
	signalChan := make(chan struct{})

	for i := 0; i < ingestor.workers; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()
			err := ingestor.Ingest(ctx, heightsChan, signalChan)
			if err != nil {
				ingestor.Logger.Error("error ingesting block", "err", err)
				cancel()
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := ingestor.StartNewBlocksListener(withCancel, heightsChan, signalChan)
		if err != nil {
			ingestor.Logger.Error("error listening to new blocks", "err", err)
			cancel()
		}
		ingestor.Logger.Error("stopping listening to new blocks")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := ingestor.EnqueueMissingBlockHeights(withCancel, heightsChan, signalChan)
		if err != nil {
			ingestor.Logger.Error("error enqueing missing blocks", "err", err)
			cancel()
		}
		enqueueMissingSignalChan <- struct{}{}
		ingestor.Logger.Error("stopping enqueing missing blocks")
	}()

	wg.Wait()

	return nil
}

func (ingestor Ingestor) StartNewBlocksListener(
	ctx context.Context,
	heights chan<- *int64,
	signalChan <-chan struct{},
) error {
	results, err := ingestor.extractor.SubscribeNewBlocks(ctx, "new-blocks-updates")
	if err != nil {
		return err
	}
	ingestor.Logger.Info("listening for new blocks...")
	for {
		select {
		case <-signalChan:
			return nil
		case <-ctx.Done():
			return nil
		case result := <-results:
			blockEvent := MustGetEvent(result, coretypes.EventTypeKey)
			isBlock := blockEvent[0] == coretypes.EventNewBlock
			if !isBlock {
				continue
			}
			height, err := ingestor.extractor.QueryHeight(ctx)
			if err != nil {
				return err
			}
			ingestor.Logger.Debug("enqueueing new block height", "height", height)
			select {
			case <-signalChan:
				return nil
			case heights <- &height: // this is missing blocks when channel is full
			}
		}
	}
}

func (ingestor Ingestor) EnqueueMissingBlockHeights(
	ctx context.Context,
	heights chan<- *int64,
	signalChan <-chan struct{},
) error {
	chainHeight, err := ingestor.extractor.QueryHeight(ctx)
	if err != nil {
		return err
	}

	lastUnbondingHeight, err := ingestor.Querier.QueryLastUnbondingHeight(ctx)
	if err != nil {
		return err
	}

	ingestor.Logger.Info("syncing missing block heights", "chain_height", chainHeight, "last_unbonding_height", lastUnbondingHeight)

	for i := int64(lastUnbondingHeight); i < chainHeight; i++ {
		height := chainHeight - i
		select {
		case <-signalChan:
			return nil
		case <-ctx.Done():
			return nil
		default:
			ingestor.Logger.Debug("enqueueing missing block height", "height", chainHeight-i)
			select {
			case <-signalChan:
				return nil
			case heights <- &height:
			}
		}
	}
	ingestor.Logger.Info("finished syncing missing block heights", "chain_height", chainHeight, "last_unbonding_height", lastUnbondingHeight)
	return nil
}

func (ingestor Ingestor) Ingest(ctx context.Context, heightChan <-chan *int64, signalChan <-chan struct{}) error {
	for {
		select {
		// add signal and stuff
		case height := <-heightChan:
			block, err := ingestor.extractor.ExtractBlock(ctx, height)
			if err != nil {
				return err
			}
			for _, coreTx := range block.Block.Txs {
				sdkTx, err := ingestor.parser.ParseCoreTx(coreTx)
				if err != nil {
					return fmt.Errorf("error while unpacking message: %s", err)
				}
				for _, msg := range sdkTx.Body.Messages {
					sdkMsg, err := ingestor.parser.ParseSdkTx(msg)
					if err != nil {
						return err
					}
					isDcc, err := ingestor.parser.IsDataCommitmentConfirm(sdkMsg)
					if err != nil {
						return nil
					}
					if isDcc {
						dcc, err := ingestor.parser.ParseDataCommitmentConfirm(sdkMsg)
						if err != nil {
							return err
						}
						err = ingestor.handleDataCommitmentConfirm(dcc)
						if err != nil {
							return err
						}
					}
					isVs, err := ingestor.parser.IsValsetConfirm(sdkMsg)
					if err != nil {
						return nil
					}
					if isVs {
						vs, err := ingestor.parser.ParseValsetConfirm(sdkMsg)
						if err != nil {
							return err
						}
						err = ingestor.handleValsetConfirm(vs)
						if err != nil {
							return err
						}
					}
				}
			}
			ingestor.indexer.AddHeight(*height) // handle error
		}
	}
}

func (ingestor Ingestor) handleDataCommitmentConfirm(dcc types.MsgDataCommitmentConfirm) error {
	err := ingestor.indexer.AddDataCommitmentConfirm(dcc)
	if err != nil {
		return err
	}
	return nil
}

func (ingestor Ingestor) handleValsetConfirm(vs types.MsgValsetConfirm) error {
	err := ingestor.indexer.AddValsetConfirm(vs)
	if err != nil {
		return err
	}
	return nil
}
