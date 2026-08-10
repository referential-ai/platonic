# Security Policy

## Supported Version

Only current `main` is supported.

## Reporting

Do not open a public issue. Submit a [private vulnerability report](https://github.com/referential-ai/platonic/security/advisories/new) through the repository **Security** tab. Reports are confidential.

## Current Containment

The Discord gateway defaults to denying unknown identities. A configured
principal may act only in a mapped channel and within its remote ceiling;
`/approve` and `/deny` bind to one exact pending operation, and the server
records the actor.

Thread authority is immutable and recorded before execution. On Linux with
Landlock support, server-created thread children are write-confined to their
private repositories and scratch directory. macOS and Linux hosts without
Landlock record `confinement: "none"`; `[confinement] require = true` refuses
those spawns.
