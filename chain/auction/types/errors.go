package types

import (
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"
)

var (
	ErrBidInvalid = sdkerrors.Register(ModuleName, 1, "bad bid")
)
