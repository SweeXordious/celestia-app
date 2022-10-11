package cmd

import (
	"os"
	"sync"
	"time"

	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator"
	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/api"
	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/evm"
	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/ingestion"
	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/store"

	"github.com/celestiaorg/celestia-app/app"
	"github.com/celestiaorg/celestia-app/app/encoding"
	wrapper "github.com/celestiaorg/quantum-gravity-bridge/wrappers/QuantumGravityBridge.sol"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/spf13/cobra"
	tmlog "github.com/tendermint/tendermint/libs/log"
)

func RelayerCmd() *cobra.Command {
	command := &cobra.Command{
		Use: "relayer <flags>",
		RunE: func(cmd *cobra.Command, args []string) error {
			config, err := parseRelayerFlags(cmd)
			if err != nil {
				return err
			}

			logger := tmlog.NewTMLogger(os.Stdout)

			ethClient, err := ethclient.Dial(config.evmRPC)
			if err != nil {
				return err
			}
			qgbWrapper, err := wrapper.NewQuantumGravityBridge(config.contractAddr, ethClient)
			if err != nil {
				return err
			}

			encCfg := encoding.MakeConfig(app.ModuleEncodingRegisters...)

			querier, err := api.NewRPCStateQuerier(config.celesGRPC, config.tendermintRPC, logger, encCfg)
			if err != nil {
				return err
			}

			inMemoryStore := store.NewInMemoryQGBStore()
			loader := store.NewInMemoryLoader(*inMemoryStore)
			storeQuerier := api.NewQGBStoreQuerier(logger, loader, querier)

			relay, err := orchestrator.NewRelayer(
				querier,
				storeQuerier,
				evm.NewEvmClient(
					tmlog.NewTMLogger(os.Stdout),
					qgbWrapper,
					config.privateKey,
					config.evmRPC,
					config.evmGasLimit,
				),
				logger,
			)
			if err != nil {
				return err
			}

			wg := &sync.WaitGroup{}

			wg.Add(1)
			go func() {
				defer wg.Done()
				// TODO move this to a right coordination place
				for {
					select {
					case <-cmd.Context().Done():
						return
					default:
						err = relay.ProcessEvents(cmd.Context())
						if err != nil {
							logger.Error(err.Error())
							time.Sleep(time.Second * 30)
							continue
						}
						return
					}
				}
			}()

			extractor, err := ingestion.NewRPCExtractor(config.tendermintRPC)

			enqueueSignalChan := make(chan struct{}, 1)

			signalChan := make(chan struct{})

			ingestor, err := ingestion.NewIngestor(extractor, ingestion.NewQGBParser(ingestion.MakeDefaultAppCodec()),
				ingestion.NewInMemoryIndexer(inMemoryStore), querier, logger, 16, // make workers in config (default to number of CPUs/threads)
			)

			wg.Add(1)
			go func() {
				defer wg.Done()
				// TODO no enqueueSignalChan and signalChan
				// TODO handle error and return != 0
				_ = ingestor.Start(cmd.Context(), enqueueSignalChan, signalChan)
			}()

			wg.Wait()
			return nil
		},
	}
	return addRelayerFlags(command)
}
