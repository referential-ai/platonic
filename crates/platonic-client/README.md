# platonic-client

Bounded protocol v2 daemon client and local IPC transport for Plato Agent. It
owns the typed `profile.create|list|status|update|open` calls used by operator
and TUI surfaces; it does not recreate server policy or thread semantics.
