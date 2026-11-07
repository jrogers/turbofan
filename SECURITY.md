# Security

A Turbofan node accepts connections and parses messages from clients, from its peers and on its
admin listener, so a flaw in it may be reachable from the network. Please report security
problems privately, not in a public issue.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting: the **Security** tab of this repository, then
**Report a vulnerability**. Include what's affected (crate, version or commit, features), how to
reproduce it, and what an attacker could do with it.

## What counts

Anything a client, or someone who can reach a listening port, can do that they shouldn't: crash or
hang a node, exhaust its memory or disk, get input logged that should be refused, read or change
another session's state, make the replicas diverge, or get past a peer or admin token, TLS or
certificate checks. Denial of service through sheer volume (more connections or messages than the
host can take) is out of scope, as is anything that needs a peer's or admin's credentials.

## Status

Turbofan is pre-1.0 and provided as is, without warranty of any kind; see the license.
