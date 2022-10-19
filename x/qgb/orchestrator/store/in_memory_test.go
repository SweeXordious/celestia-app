package store

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestHeightMilestoneCatchup(t *testing.T) {
	type test struct {
		ingestedHeights   []int64
		currentMilestone  int64
		expectedMilestone int64
	}

	tests := []test{
		{ingestedHeights: []int64{}, currentMilestone: 0, expectedMilestone: 0},
		{ingestedHeights: []int64{1}, currentMilestone: 0, expectedMilestone: 1},
		{ingestedHeights: []int64{1}, currentMilestone: 1, expectedMilestone: 1},
		{ingestedHeights: []int64{1, 5}, currentMilestone: 0, expectedMilestone: 1},
		{ingestedHeights: []int64{1, 5}, currentMilestone: 1, expectedMilestone: 1},
		{ingestedHeights: []int64{1, 2, 5}, currentMilestone: 0, expectedMilestone: 2},
		{ingestedHeights: []int64{1, 2, 5}, currentMilestone: 1, expectedMilestone: 2},
		{ingestedHeights: []int64{1, 2, 5}, currentMilestone: 2, expectedMilestone: 2},
		{ingestedHeights: []int64{1, 2, 3, 5}, currentMilestone: 0, expectedMilestone: 3},
		{ingestedHeights: []int64{1, 2, 3, 5}, currentMilestone: 1, expectedMilestone: 3},
		{ingestedHeights: []int64{1, 2, 3, 5}, currentMilestone: 2, expectedMilestone: 3},
		{ingestedHeights: []int64{1, 2, 3, 5}, currentMilestone: 3, expectedMilestone: 3},
		{ingestedHeights: []int64{1, 2, 3, 4, 5}, currentMilestone: 0, expectedMilestone: 5},
		{ingestedHeights: []int64{1, 2, 3, 4, 5}, currentMilestone: 1, expectedMilestone: 5},
		{ingestedHeights: []int64{1, 2, 3, 4, 5}, currentMilestone: 2, expectedMilestone: 5},
		{ingestedHeights: []int64{1, 2, 3, 4, 5}, currentMilestone: 3, expectedMilestone: 5},
		{ingestedHeights: []int64{1, 2, 3, 4, 5}, currentMilestone: 4, expectedMilestone: 5},
		{ingestedHeights: []int64{1, 2, 3, 4, 5}, currentMilestone: 5, expectedMilestone: 5},
	}

	for _, tc := range tests {
		store := &InMemoryQGBStore{
			HeightsMilestone: tc.currentMilestone,
			IngestedHeights:  tc.ingestedHeights,
		}
		store.heightsMilestoneCatchup()
		require.Equal(t, tc.expectedMilestone, store.HeightsMilestone)
	}
}
