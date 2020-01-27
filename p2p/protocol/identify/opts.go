package identify

type config struct {
	userAgent                       string
	disableSignedAddrSupport        bool
	excludeLocalAddrsFromPeerRecord bool
}

// Option is an option function for identify.
type Option func(*config)

// UserAgent sets the user agent this node will identify itself with to peers.
func UserAgent(ua string) Option {
	return func(cfg *config) {
		cfg.userAgent = ua
	}
}

// DisableSignedAddrSupportForTesting prevents the identify service from sending or parsing
// routing.SignedRoutingState messages during the exchange. Used for testing
// compatibility with older versions that do not support signed addresses.
// Do not use in production!
func DisableSignedAddrSupportForTesting() Option {
	return func(cfg *config) {
		cfg.disableSignedAddrSupport = true
	}
}

// ExcludeLocalAddrsFromPeerRecord controls whether the signed PeerRecords that are exchanged
// with other peers during the identify exchange will include local addresses.
//
// Addresses are considered "local" if they are IP loopback addresses, or if they belong to
// a reserved private IP address range.
//
// By default, local addresses are included in PeerRecords. To exclude them,
// pass ExcludeLocalAddrsFromPeerRecord(true) into NewIDService.
func ExcludeLocalAddrsFromPeerRecord(exclude bool) Option {
	return func(cfg *config) {
		cfg.excludeLocalAddrsFromPeerRecord = exclude
	}
}
