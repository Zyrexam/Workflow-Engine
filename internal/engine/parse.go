package engine

import (
	"errors"
	"fmt"
	"strconv"
)

// ErrBadRunID is returned by ParseRunID for malformed input.
var ErrBadRunID = errors.New("invalid run id")

func ParseRunID(arg string) (int64, error) {
	runID, err := strconv.ParseInt(arg, 10, 64)
	if err != nil || runID < 1 {
		return 0, fmt.Errorf("%w %q", ErrBadRunID, arg)
	}
	return runID, nil
}
