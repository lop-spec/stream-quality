# Loopback TLS fixture

`loopback-test.cert.pem` and `loopback-test.key.pem` are deliberately public, synthetic test material for localhost / 127.0.0.1 only. Never use them for deployed services or install this certificate in an OS/browser trust store.

The HTTPS tests add the certificate only to their isolated Node test child's `NODE_EXTRA_CA_CERTS`. Certificate validation remains enabled. A separate untrusted child must reject the same certificate. The child remains alive until the parent has checked that TLS sockets and CONNECT tunnels closed, preventing process termination from hiding transport leaks.

The full HTTPS test uses the real 401-frame/20-second source, without a shortened profile. This is transport regression coverage, not evidence of public-line accuracy or the 173-node three-round time target.
