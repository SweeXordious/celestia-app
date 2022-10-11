package orchestrator

import (
	"os"
	"sync"
	"time"

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

			querier, err := NewRPCStateQuerier(config.celesGRPC, config.tendermintRPC, logger, encCfg)
			if err != nil {
				return err
			}

			store := NewInMemoryQGBStore()
			loader := NewInMemoryLoader(*store)
			storeQuerier := NewQGBStoreQuerier(logger, loader, querier)

			relay, err := NewRelayer(
				querier,
				storeQuerier,
				NewEvmClient(
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
						err = relay.processEvents(cmd.Context())
						if err != nil {
							logger.Error(err.Error())
							time.Sleep(time.Second * 30)
							continue
						}
						return
					}
				}
			}()

			extractor, err := NewRPCExtractor(config.tendermintRPC)

			enqueueSignalChan := make(chan struct{}, 1)

			signalChan := make(chan struct{})

			ingestor, err := NewIngestor(extractor, NewQGBParser(MakeDefaultAppCodec()),
				NewInMemoryIndexer(store), querier, logger, 16, // make workers in config (default to number of CPUs/threads)
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
