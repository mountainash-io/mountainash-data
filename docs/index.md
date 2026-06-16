---
title: 'Mountainash Data'
description: 'A unified Python interface for connecting to any database or lakehouse through typed settings, consistent metadata inspection, and fluent queries.'
---


[← Back to Ecosystem](../)
# Mountainash Data

A single, consistent way to connect to any supported database or lakehouse — import `IbisBackend` for SQL databases across every Ibis dialect, or `IcebergBackend` for Apache Iceberg catalogs.

## Why a Guided Manual?

Most library documentation tells you *what* the API does. This manual explains *why* the library is designed the way it is — why there is a Backend protocol, why settings classes auto-register, why inspection returns frozen dataclasses — so that when you need to extend it or debug a connection, you understand the system rather than just the surface.

## What's Inside

- [**Chapters**](chapters/index.md) — 9 chapters covering everything from foundation concepts through advanced integration patterns
- [**MicroSims**](sims/index.md) — Interactive simulations that let you experiment with concepts hands-on
- [**Learning Graph**](learning-graph/index.md) — A dependency map showing how concepts build on each other
- [**API Reference**](api/index.md) — Auto-generated reference for backends, core protocol, settings, and inspection models

## Who This Is For

Python developers and data engineers who work with multiple database backends and want a single library instead of per-database boilerplate. Whether you are connecting to DuckDB locally, PostgreSQL in staging, or Snowflake in production, this manual will get you productive quickly. See the [About](about.md) page for prerequisites and a full reading guide.
