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

			tmQuerier, err := api.NewTmQuerier(config.tendermintRPC, logger)
			if err != nil {
				return err
			}

			inMemoryStore := store.NewInMemoryQGBStore()
			loader := api.NewInMemoryLoader(inMemoryStore)
			storeQuerier := api.NewQGBQuerier(logger, loader, tmQuerier)

			relay, err := orchestrator.NewRelayer(
				tmQuerier,
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
				for {
					select {
					// TODO no one would cancel this context
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

			tmExtractor, err := ingestion.NewTmRPCExtractor(config.tendermintRPC)
			qgbExtractor, err := ingestion.NewQGBRPCExtractor(config.celesGRPC, encCfg)

			signalChan := make(chan struct{})

			ingestor, err := ingestion.NewIngestor(
				qgbExtractor,
				tmExtractor,
				ingestion.NewInMemoryIndexer(inMemoryStore),
				logger,
				loader,
				encoding.MakeConfig(app.ModuleEncodingRegisters...),
				16,
				// make workers in config (default to number of CPUs/threads)
			)

			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-cmd.Context().Done():
						return
					default:
						// TODO no enqueueSignalChan and signalChan
						// TODO handle error and return != 0
						err = ingestor.Start(cmd.Context(), signalChan)
						if err != nil {
							logger.Error(err.Error())
							time.Sleep(time.Second * 30)
							continue
						}
						return
					}
				}
			}()

			wg.Wait()
			return nil
		},
	}
	return addRelayerFlags(command)
}
