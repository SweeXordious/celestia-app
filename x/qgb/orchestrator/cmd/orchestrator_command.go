package cmd

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/celestiaorg/celestia-app/app"
	"github.com/celestiaorg/celestia-app/app/encoding"
	paytypes "github.com/celestiaorg/celestia-app/x/payment/types"
	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator"
	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/api"
	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/ingestion"
	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/store"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	"github.com/spf13/cobra"
	tmlog "github.com/tendermint/tendermint/libs/log"
)

func OrchCmd() *cobra.Command {
	command := &cobra.Command{
		Use:     "orchestrator <flags>",
		Aliases: []string{"orch"},
		RunE: func(cmd *cobra.Command, args []string) error {
			config, err := parseOrchestratorFlags(cmd)
			if err != nil {
				return err
			}
			logger := tmlog.NewTMLogger(os.Stdout)

			logger.Debug("initializing orchestrator")

			ctx, cancel := context.WithCancel(cmd.Context())

			encCfg := encoding.MakeConfig(app.ModuleEncodingRegisters...)

			querier, err := api.NewRPCStateQuerier(config.celesGRPC, config.tendermintRPC, logger, encCfg)
			if err != nil {
				panic(err)
			}

			// creates the Signer
			// TODO: optionally ask for input for a password
			ring, err := keyring.New("orchestrator", config.keyringBackend, config.keyringPath, strings.NewReader(""), encCfg.Codec)
			if err != nil {
				panic(err)
			}
			signer := paytypes.NewKeyringSigner(
				ring,
				config.keyringAccount,
				config.celestiaChainID,
			)

			broadcaster, err := orchestrator.NewBroadcaster(config.celesGRPC, signer, config.celestiaGasLimit)
			if err != nil {
				panic(err)
			}

			inMemoryStore := store.NewInMemoryQGBStore()
			loader := store.NewInMemoryLoader(*inMemoryStore)
			storeQuerier := api.NewQGBStoreQuerier(logger, loader, querier)

			retrier := orchestrator.NewRetrier(logger, 5)
			orch, err := orchestrator.NewOrchestrator(
				logger,
				querier,
				storeQuerier,
				broadcaster,
				retrier,
				signer,
				*config.privateKey,
			)
			if err != nil {
				panic(err)
			}

			logger.Debug("starting orchestrator")

			// Listen for and trap any OS signal to gracefully shutdown and exit
			go trapSignal(logger, cancel)

			extractor, err := ingestion.NewRPCExtractor(config.tendermintRPC)
			if err != nil {
				return err
			}

			enqueueSignalChan := make(chan struct{}, 1)

			ingestor, err := ingestion.NewIngestor(extractor, ingestion.NewQGBParser(ingestion.MakeDefaultAppCodec()),
				ingestion.NewInMemoryIndexer(inMemoryStore), querier, logger, 16, // make workers in config (default to number of threads or CPUs)
			)
			if err != nil {
				return err
			}

			// used to send a signal when a worker wants to notify the enqueuing services to stop.
			signalChan := make(chan struct{})

			// TODO kill processes

			wg := &sync.WaitGroup{}

			wg.Add(1)
			go func() {
				defer wg.Done()
				// TODO handle error
				// TODO no enqueueSignalChan and signalChan in params
				_ = ingestor.Start(ctx, enqueueSignalChan, signalChan)
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				// TODO handle error and return != 0
				// TODO no signalChan and enqueueSignalChan in params
				orch.Start(ctx, enqueueSignalChan, signalChan)
			}()

			wg.Wait()
			return nil
		},
	}
	return addOrchestratorFlags(command)
}

// trapSignal will listen for any OS signal and gracefully exit.
func trapSignal(logger tmlog.Logger, cancel context.CancelFunc) {
	// TODO investigate why the following doesn't exit:
	// - running orchestrator
	// - stopping celestia-appd node
	// - letting the orchestrator retry once
	// - ctrl-c the orchestrator
	// - even tho the services are killed and the interruption is captured. It hangs.
	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, syscall.SIGTERM)
	signal.Notify(sigCh, syscall.SIGINT)

	sig := <-sigCh
	logger.Info("caught signal; shutting down...", "signal", sig.String())
	cancel()
}
