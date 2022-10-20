package cmd

import (
	"context"
	"os"
	"strconv"

	"github.com/celestiaorg/celestia-app/app"
	"github.com/celestiaorg/celestia-app/app/encoding"
	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/ingestion"

	"github.com/celestiaorg/celestia-app/x/qgb/orchestrator/evm"

	"github.com/celestiaorg/celestia-app/x/qgb/types"
	"github.com/cosmos/cosmos-sdk/types/errors"
	"github.com/spf13/cobra"
	tmlog "github.com/tendermint/tendermint/libs/log"
)

func DeployCmd() *cobra.Command {
	command := &cobra.Command{
		Use:   "deploy <flags>",
		Short: "Deploys the QGB contract and initializes it using the provided Celestia chain",
		RunE: func(cmd *cobra.Command, args []string) error {
			config, err := parseDeployFlags(cmd)
			if err != nil {
				return err
			}

			extractor, err := ingestion.NewQGBRPCExtractor(config.celesGRPC, encoding.MakeConfig(app.ModuleEncodingRegisters...))
			if err != nil {
				return err
			}

			vs, err := getStartingValset(cmd.Context(), extractor, config.startingNonce)
			if err != nil {
				return errors.Wrap(
					err,
					"cannot initialize the QGB contract without having a valset request: %s",
				)
			}

			evmClient := evm.NewEvmClient(
				tmlog.NewTMLogger(os.Stdout),
				nil,
				config.privateKey,
				config.evmRPC,
				config.evmGasLimit,
			)

			// the deploy QGB contract will handle the logging of the address
			_, _, _, err = evmClient.DeployQGBContract(
				cmd.Context(),
				*vs,
				vs.Nonce,
				config.evmChainID,
				true,
				false,
			)
			if err != nil {
				return err
			}

			return nil
		},
	}
	return addDeployFlags(command)
}

func getStartingValset(ctx context.Context, extractor ingestion.QGBExtractorI, snonce string) (*types.Valset, error) {
	switch snonce {
	case "latest":
		return extractor.QueryLatestValset(ctx)
	case "earliest":
		// TODO make the first nonce 1 a const
		att, err := extractor.ExtractAttestationByNonce(ctx, 1)
		if err != nil {
			return nil, err
		}
		vs, ok := att.(*types.Valset)
		if !ok {
			return nil, ErrUnmarshallValset
		}
		return vs, nil
	default:
		nonce, err := strconv.ParseUint(snonce, 10, 0)
		if err != nil {
			return nil, err
		}
		attestation, err := extractor.ExtractAttestationByNonce(ctx, nonce)
		if err != nil {
			return nil, err
		}
		if attestation == nil {
			return nil, types.ErrNilAttestation
		}
		if attestation.Type() == types.ValsetRequestType {
			value, ok := attestation.(*types.Valset)
			if !ok {
				return nil, ErrUnmarshallValset
			}
			return value, nil
		}
		return extractor.QueryLastValsetBeforeNonce(ctx, nonce)
	}
}
