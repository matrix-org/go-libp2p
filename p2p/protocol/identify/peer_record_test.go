package identify

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-eventbus"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/event"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/multiformats/go-multiaddr"
)

func TestPeerRecordManagerEmitsPeerRecord(t *testing.T) {
	bus, emitter, sub := setupEventBus(t)
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatalf("error generating test keypair: %v", err)
	}

	ctx := context.Background()
	var initialAddrs []multiaddr.Multiaddr
	_, err = NewPeerRecordManager(ctx, bus, priv, initialAddrs, false)
	if err != nil {
		t.Fatalf("error creating peerRecordManager: %v", err)
	}

	addrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/42"),
	}
	addrEvent := makeAddrEvent(addrs)
	err = emitter.Emit(addrEvent)
	test.AssertNilError(t, err)

	evt := getNextPeerRecordEvent(t, sub)
	peerRec := peerRecordFromEvent(t, evt)
	test.AssertAddressesEqual(t, addrs, peerRec.Addrs)
}

func TestPeerRecordExcludesLocalAddrsIfFlagIsUnset(t *testing.T) {
	bus, emitter, sub := setupEventBus(t)
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatalf("error generating test keypair: %v", err)
	}

	ctx := context.Background()
	var initialAddrs []multiaddr.Multiaddr
	_, err = NewPeerRecordManager(ctx, bus, priv, initialAddrs, false)
	if err != nil {
		t.Fatalf("error creating peerRecordManager: %v", err)
	}

	addrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/42"),
		multiaddr.StringCast("/ip4/192.168.1.3/tcp/4321"),
		multiaddr.StringCast("/ip4/127.0.0.1/tcp/1234"),
	}
	addrEvent := makeAddrEvent(addrs)
	err = emitter.Emit(addrEvent)
	test.AssertNilError(t, err)

	evt := getNextPeerRecordEvent(t, sub)
	peerRec := peerRecordFromEvent(t, evt)
	expected := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/42"),
	}
	test.AssertAddressesEqual(t, expected, peerRec.Addrs)
}

func TestPeerRecordIncludesLocalAddrsIfFlagIsSet(t *testing.T) {
	bus, emitter, sub := setupEventBus(t)
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatalf("error generating test keypair: %v", err)
	}

	ctx := context.Background()
	var initialAddrs []multiaddr.Multiaddr
	_, err = NewPeerRecordManager(ctx, bus, priv, initialAddrs, true)
	if err != nil {
		t.Fatalf("error creating peerRecordManager: %v", err)
	}

	addrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/42"),
		multiaddr.StringCast("/ip4/192.168.1.3/tcp/4321"),
		multiaddr.StringCast("/ip4/127.0.0.1/tcp/1234"),
	}
	addrEvent := makeAddrEvent(addrs)
	err = emitter.Emit(addrEvent)
	test.AssertNilError(t, err)

	evt := getNextPeerRecordEvent(t, sub)
	peerRec := peerRecordFromEvent(t, evt)
	test.AssertAddressesEqual(t, addrs, peerRec.Addrs)
}

func TestPeerRecordManagerEmitsRecordImmediatelyIfInitialAddrsAreProvided(t *testing.T) {
	bus, _, sub := setupEventBus(t)
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatalf("error generating test keypair: %v", err)
	}

	ctx := context.Background()
	initialAddrs := []multiaddr.Multiaddr{
		multiaddr.StringCast("/ip4/1.2.3.4/tcp/42"),
	}
	_, err = NewPeerRecordManager(ctx, bus, priv, initialAddrs, true)
	if err != nil {
		t.Fatalf("error creating peerRecordManager: %v", err)
	}

	recEvent := getNextPeerRecordEvent(t, sub)
	rec, err := recEvent.SignedRecord.Record()
	test.AssertNilError(t, err)
	test.AssertAddressesEqual(t, initialAddrs, rec.(*peer.PeerRecord).Addrs)
}

func setupEventBus(t *testing.T) (bus event.Bus, emitter event.Emitter, sub event.Subscription) {
	t.Helper()
	bus = eventbus.NewBus()
	emitter, err := bus.Emitter(&event.EvtLocalAddressesUpdated{})
	if err != nil {
		t.Fatalf("error creating event emitter: %v", err)
	}

	sub, err = bus.Subscribe(&event.EvtLocalPeerRecordUpdated{})
	if err != nil {
		t.Fatalf("error creating event subscription: %v", err)
	}
	return bus, emitter, sub
}

func getNextPeerRecordEvent(t *testing.T, sub event.Subscription) event.EvtLocalPeerRecordUpdated {
	t.Helper()
	select {
	case e := <-sub.Out():
		return e.(event.EvtLocalPeerRecordUpdated)
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for peer record")
		return event.EvtLocalPeerRecordUpdated{}
	}
}

func peerRecordFromEvent(t *testing.T, evt event.EvtLocalPeerRecordUpdated) *peer.PeerRecord {
	t.Helper()
	rec, err := evt.SignedRecord.Record()
	if err != nil {
		t.Fatalf("error getting PeerRecord from event: %v", err)
		return nil
	}
	peerRec, ok := rec.(*peer.PeerRecord)
	if !ok {
		t.Fatalf("wrong type for peer record")
		return nil
	}
	return peerRec
}

func makeAddrEvent(addrs []multiaddr.Multiaddr) event.EvtLocalAddressesUpdated {
	current := make([]event.UpdatedAddress, len(addrs))
	for i, a := range addrs {
		current[i] = event.UpdatedAddress{Address: a}
	}
	return event.EvtLocalAddressesUpdated{Current: current}
}
