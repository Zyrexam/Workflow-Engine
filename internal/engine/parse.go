package engine

import (
	"fmt"
	"strconv"
)

func ParseRunID(arg string) (int64, error) {
	runID, err := strconv.ParseInt(arg, 10, 64)
	if err != nil || runID < 1 {
		return 0, fmt.Errorf("invalid run id %q", arg)
	}
	return runID, nil
}
