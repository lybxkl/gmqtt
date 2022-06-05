package pkid

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMax(t *testing.T) {
	a := assert.New(t)
	p := newPacketIDLimiter(65535)
	ids := p.PollPacketIDs(65535)
	a.Len(ids, 65535)
	p.BatchRelease([]PacketID{1, 2, 3, 65535})
	a.Equal([]PacketID{1, 2, 3}, p.PollPacketIDs(3))
	a.Equal([]PacketID{65535}, p.PollPacketIDs(3))
}
