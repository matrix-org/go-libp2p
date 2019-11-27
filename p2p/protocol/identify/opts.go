package identify

type config struct {
	userAgent                string
	disableSignedAddrSupport bool
}

// Option is an option function for identify.
type Option func(*config)

// UserAgent sets the user agent this node will identify itself with to peers.
func UserAgent(ua string) Option {
	return func(cfg *config) {
		cfg.userAgent = ua
	}
}

// DisableSignedAddrSupport prevents the identify service from sending or parsing
// routing.SignedRoutingState messages during the exchange. Used for testing
// compatibility with older versions that do not support signed addresses.
func DisableSignedAddrSupport() Option {
	return func(cfg *config) {
		cfg.disableSignedAddrSupport = true
	}
}
