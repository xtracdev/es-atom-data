package atom

import (
	. "github.com/gucumber/gucumber"
	"github.com/stretchr/testify/assert"
	ad "github.com/xtracdev/es-atom-data"
	"os"
)

func init() {
	Given(`^some initial events and no archived events and no feeds$`, func() {
		os.Setenv("FEED_THRESHOLD", "2")
		ad.ReadFeedThresholdFromEnv()
		assert.Equal(T, 2, ad.FeedThreshold)
	})
}
