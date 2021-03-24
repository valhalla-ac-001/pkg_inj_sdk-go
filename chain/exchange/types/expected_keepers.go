package types

import (
	sdk "github.com/cosmos/cosmos-sdk/types"
	bankexported "github.com/cosmos/cosmos-sdk/x/bank/exported"
)

// BankKeeper defines the expected bank keeper methods
type BankKeeper interface {
	SendCoinsFromModuleToAccount(ctx sdk.Context, senderModule string, recipientAddr sdk.AccAddress, amt sdk.Coins) error
	SendCoinsFromAccountToModule(ctx sdk.Context, senderAddr sdk.AccAddress, recipientModule string, amt sdk.Coins) error
	GetSupply(ctx sdk.Context) bankexported.SupplyI
}
