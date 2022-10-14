package keeper

import (
	"fmt"

	"github.com/celestiaorg/celestia-app/x/qgb/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
)

// Hooks is a wrapper struct around Keeper.
type Hooks struct {
	k Keeper
}

// Create new gravity hooks
func (k Keeper) Hooks() Hooks {
	// if startup is mis-ordered in app.go this hook will halt
	// the chain when called. Keep this check to make such a mistake
	// obvious
	if k.storeKey == nil {
		panic("hooks initialized before GravityKeeper")
	}
	return Hooks{k}
}

func (h Hooks) AfterValidatorBeginUnbonding(ctx sdk.Context, _ sdk.ConsAddress, _ sdk.ValAddress) error {
	// When Validator starts Unbonding, Persist the block height in the store
	// Later in endblocker, check if there is at least one validator who started unbonding and create a valset request.
	// The reason for creating valset requests in endblock is to create only one valset request per block,
	// if multiple validators starts unbonding at same block.

	// this hook IS called for jailing or unbonding triggered by users but it IS NOT called for jailing triggered
	// in the endblocker therefore we call the keeper function ourselves there.

	// TODO investigate if this uint64 conversion is needed
	h.k.SetLastUnBondingBlockHeight(ctx, uint64(ctx.BlockHeight()))
	// The latest attestation nonce is the nonce before the unbonding period.
	// The reason for this is this is executed before endBlock. Thus, the new nonce
	// is still not created at this level.
	h.k.SetLastUnbondingAttestationNonce(ctx, h.k.GetLatestAttestationNonce(ctx))

	// TODO add test to check if event is emitted
	ctx.EventManager().EmitEvent(
		sdk.NewEvent(
			types.EventTypeAttestationRequest,
			sdk.NewAttribute(sdk.AttributeKeyModule, types.ModuleName),
			sdk.NewAttribute(types.AttributeKeyHeight, fmt.Sprint(h.k.GetLatestAttestationNonce(ctx))),
			sdk.NewAttribute(types.AttributeKeyNonce, fmt.Sprint(ctx.BlockHeight())),
		),
	)
	return nil
}

func (h Hooks) BeforeDelegationCreated(_ sdk.Context, delAddr sdk.AccAddress, valAddr sdk.ValAddress) error {
	return nil
}

func (h Hooks) AfterValidatorCreated(ctx sdk.Context, valAddr sdk.ValAddress) error {
	return nil
}

func (h Hooks) BeforeValidatorModified(_ sdk.Context, _ sdk.ValAddress) error {
	return nil
}

func (h Hooks) AfterValidatorBonded(_ sdk.Context, _ sdk.ConsAddress, _ sdk.ValAddress) error {
	return nil
}

func (h Hooks) BeforeDelegationRemoved(_ sdk.Context, _ sdk.AccAddress, _ sdk.ValAddress) error {
	return nil
}

func (h Hooks) AfterValidatorRemoved(ctx sdk.Context, _ sdk.ConsAddress, valAddr sdk.ValAddress) error {
	return nil
}

func (h Hooks) BeforeValidatorSlashed(ctx sdk.Context, valAddr sdk.ValAddress, fraction sdk.Dec) error {
	return nil
}

func (h Hooks) BeforeDelegationSharesModified(ctx sdk.Context, delAddr sdk.AccAddress, valAddr sdk.ValAddress) error {
	return nil
}

func (h Hooks) AfterDelegationModified(ctx sdk.Context, delAddr sdk.AccAddress, valAddr sdk.ValAddress) error {
	return nil
}
