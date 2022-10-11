package api

import "errors"

var (
	ErrValsetNotFound                  = errors.New("valset not found")
	ErrUnmarshallValset                = errors.New("couldn't unmarsall valset")
	ErrNotEnoughValsetConfirms         = errors.New("couldn't find enough valset confirms")
	ErrNotEnoughDataCommitmentConfirms = errors.New("couldn't find enough data commitment confirms")
	ErrInvalidCommitmentInConfirm      = errors.New("confirm not carrying the right commitment for expected range")
)
