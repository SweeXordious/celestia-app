// TODO better package name
package utils

import (
	"fmt"

	"github.com/celestiaorg/celestia-app/x/qgb/types"
	"github.com/pkg/errors"
	corerpctypes "github.com/tendermint/tendermint/rpc/core/types"
	coretypes "github.com/tendermint/tendermint/types"
)

// MustGetEvent takes a corerpctypes.ResultEvent and checks whether it has
// the provided eventName. If not, it panics.
// TODO can be moved to some utils file
func MustGetEvent(result corerpctypes.ResultEvent, eventName string) []string {
	ev := result.Events[eventName]
	if len(ev) == 0 {
		panic(errors.Wrap(
			types.ErrEmpty,
			fmt.Sprintf(
				"%s not found in event %s",
				coretypes.EventTypeKey,
				result.Events,
			),
		))
	}
	return ev
}

// GetEvent takes a corerpctypes.ResultEvent and checks whether it has
// the provided eventName. If not, returns empty slice.
// TODO can be moved to some utils file
func GetEvent(result corerpctypes.ResultEvent, eventName string) []string {
	ev := result.Events[eventName]
	return ev
}
