"""Data ingest adapters. Each market/source adapter parses an external feed into the
engine's §3 table shapes (ohlcv_raw, symbol_master, …). One validated ingress per source;
provenance stamped per bar (handoff §15)."""
