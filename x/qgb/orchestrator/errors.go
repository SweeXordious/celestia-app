package orchestrator

import "errors"

var (
	ErrDataCommitmentNotFound   = errors.New("data commitment not found")
	ErrUnmarshallDataCommitment = errors.New("couldn't unmarsall data commitment")
	ErrConfirmSignatureNotFound = errors.New("confirm signature not found")
	ErrUnknownAttestationType   = errors.New("unknown attestation type")
	ErrFailedBroadcast          = errors.New("failed to broadcast transaction")
)
