package identify

import (
	"context"
	"fmt"
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/record"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
)

// peerRecordManager creates new signed peer.PeerRecords that can
// be shared with other peers to inform them of our listen addresses in
// a secure and authenticated way.
//
// New signed records are created in response to EvtLocalAddressesUpdated events,
// and are emitted in EvtLocalPeerRecordUpdated events.
//
// EvtLocalPeerRecordUpdated is emitted using a Stateful emitter, so new subscribers
// will immediately receive the current record when they subscribe, with future
// records delivered in future events.
type peerRecordManager struct {
	latest     *record.Envelope
	hostID     peer.ID
	signingKey crypto.PrivKey

	ctx               context.Context
	includeLocalAddrs bool
	subscriptions     struct {
		localAddrsUpdated event.Subscription
	}
	emitters struct {
		evtLocalRoutingStateUpdated event.Emitter
	}
}

func NewPeerRecordManager(ctx context.Context, bus event.Bus, hostKey crypto.PrivKey, initialAddrs []multiaddr.Multiaddr, includeLocalAddrs bool) (*peerRecordManager, error) {
	hostID, err := peer.IDFromPrivateKey(hostKey)
	if err != nil {
		return nil, err
	}

	m := &peerRecordManager{
		ctx:               ctx,
		signingKey:        hostKey,
		hostID:            hostID,
		includeLocalAddrs: includeLocalAddrs,
	}

	if len(initialAddrs) != 0 {
		initialRec, err := m.makeSignedPeerRecord(initialAddrs)
		if err != nil {
			return nil, fmt.Errorf("error constructing initial peer record: %w", err)
		}
		m.latest = initialRec
	}

	m.subscriptions.localAddrsUpdated, err = bus.Subscribe(&event.EvtLocalAddressesUpdated{}, eventbus.BufSize(128))
	if err != nil {
		return nil, err
	}
	m.emitters.evtLocalRoutingStateUpdated, err = bus.Emitter(&event.EvtLocalPeerRecordUpdated{}, eventbus.Stateful)
	if err != nil {
		return nil, err
	}

	go m.handleEvents()

	if m.latest != nil {
		m.emitLatest()
	}

	return m, nil
}

func (m *peerRecordManager) LatestRecord() *record.Envelope {
	if m == nil {
		return nil
	}
	return m.latest
}

func (m *peerRecordManager) handleEvents() {
	sub := m.subscriptions.localAddrsUpdated
	defer func() {
		_ = sub.Close()
		// drain the channel.
		for range sub.Out() {
		}
	}()

	for {
		select {
		case evt, more := <-sub.Out():
			if !more {
				return
			}
			m.update(evt.(event.EvtLocalAddressesUpdated))
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *peerRecordManager) update(evt event.EvtLocalAddressesUpdated) {
	envelope, err := m.makeSignedPeerRecord(addrsFromEvent(evt))
	if err != nil {
		log.Warnf("error creating signed peer record: %v", err)
		return
	}
	m.latest = envelope
	m.emitLatest()
}

func (m *peerRecordManager) emitLatest() {
	stateEvt := event.EvtLocalPeerRecordUpdated{SignedRecord: m.latest}
	err := m.emitters.evtLocalRoutingStateUpdated.Emit(stateEvt)
	if err != nil {
		log.Warnf("error emitting event for updated peer record: %v", err)
	}
}

func (m *peerRecordManager) makeSignedPeerRecord(current []multiaddr.Multiaddr) (*record.Envelope, error) {
	addrs := make([]multiaddr.Multiaddr, 0, len(current))
	for _, a := range current {
		if a == nil {
			continue
		}
		if m.includeLocalAddrs || manet.IsPublicAddr(a) {
			addrs = append(addrs, a)
		}
	}

	rec := peer.NewPeerRecord()
	rec.PeerID = m.hostID
	rec.Addrs = addrs
	return rec.Sign(m.signingKey)
}

func addrsFromEvent(evt event.EvtLocalAddressesUpdated) []multiaddr.Multiaddr {
	addrs := make([]multiaddr.Multiaddr, len(evt.Current))
	for _, a := range evt.Current {
		addrs = append(addrs, a.Address)
	}
	return addrs
}
