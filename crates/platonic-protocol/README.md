# platonic-protocol

Sans-IO protocol v2 wire types for Plato Agent and Platonic. The native NDJSON
surface is closed and profile-based: incompatible envelope versions fail before
dispatch, and no `agent.*` method aliases are accepted. Legacy v1 ledger events
remain a storage/replay compatibility concern rather than a live wire surface.
