package identify

import (
	"context"
	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"
)

// routingStateManager creates new SignedRoutingState records that can
// be shared with other peers to inform them of our listen addresses in
// a secure and authenticated way.
//
// New signed records are created in response to EvtLocalAddressesUpdated events,
// and are emitted in EvtLocalRoutingStateUpdated events.
type routingStateManager struct {
	latestState *routing.SignedRoutingState

	ctx               context.Context
	host              host.Host
	includeLocalAddrs bool
	subscriptions     struct {
		localAddrsUpdated event.Subscription
	}
	emitters struct {
		evtLocalRoutingStateUpdated event.Emitter
	}
}

func NewRoutingStateManager(ctx context.Context, host host.Host, includeLocalAddrs bool) (*routingStateManager, error) {
	m := &routingStateManager{
		ctx:               ctx,
		host:              host,
		includeLocalAddrs: includeLocalAddrs,
	}
	bus := host.EventBus()
	var err error
	m.subscriptions.localAddrsUpdated, err = bus.Subscribe(&event.EvtLocalAddressesUpdated{}, eventbus.BufSize(128))
	if err != nil {
		return nil, err
	}
	m.emitters.evtLocalRoutingStateUpdated, err = bus.Emitter(&event.EvtLocalPeerRoutingStateUpdated{}, eventbus.Stateful)
	if err != nil {
		return nil, err
	}

	m.updateRoutingState()
	go m.handleEvents()
	return m, nil
}

func (m *routingStateManager) LatestState() *routing.SignedRoutingState {
	if m == nil {
		return nil
	}
	return m.latestState
}

func (m *routingStateManager) handleEvents() {
	sub := m.subscriptions.localAddrsUpdated
	defer func() {
		_ = sub.Close()
		// drain the channel.
		for range sub.Out() {
		}
	}()

	for {
		select {
		case _, more := <-sub.Out():
			if !more {
				return
			}
			m.updateRoutingState()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *routingStateManager) updateRoutingState() {
	state, err := m.makeRoutingState()
	if err != nil {
		log.Warnf("error creating routing state record: %v", err)
		return
	}
	m.latestState = state
	stateEvt := event.EvtLocalPeerRoutingStateUpdated{State: m.latestState}
	err = m.emitters.evtLocalRoutingStateUpdated.Emit(stateEvt)
	if err != nil {
		log.Warnf("error emitting routing state event: %v", err)
	}
}

func (m *routingStateManager) makeRoutingState() (*routing.SignedRoutingState, error) {
	privKey := m.host.Peerstore().PrivKey(m.host.ID())
	if privKey == nil {
		log.Warn("error making routing state: unable to find host's private key in peerstore")
	}

	var addrs []multiaddr.Multiaddr
	if m.includeLocalAddrs {
		addrs = m.host.Addrs()
	} else {
		for _, a := range m.host.Addrs() {
			if manet.IsPublicAddr(a) {
				addrs = append(addrs, a)
			}
		}
	}

	return routing.MakeSignedRoutingState(privKey, addrs)
}
