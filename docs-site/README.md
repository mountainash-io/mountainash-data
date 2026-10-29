# Textbook maintenance

The [production textbook](https://docs.mountainash.io/mountainash-data/) is
built from `main`; the [development textbook](https://docs.mountainash.io/mountainash-data/dev/)
is built from `develop`. Each push to either branch builds both snapshots and
publishes them together. A failed build leaves the previous paired site live.

## Publishing

For initial activation, merge the textbook changes into both branches and
configure Pages, its environment, and the shared custom domain first. Then set
the repository Actions variable `TEXTBOOK_PUBLISHING_ENABLED` to `true` and
manually dispatch `deploy-textbook.yml`. Until enabled, publishing runs are
skipped; a one-sided bootstrap cannot deploy an incomplete site.

The source artifacts live together in this directory:

- `profile/`: package profile and source provenance.
- `editorial-brief.md`, `chapter-plan.md`: the confirmed brief and approved chapter plan the book implements.
- `learning-graph/`: canonical concept graph (internal; not published) and the legacy FAQ artifacts.
- `site/`: MkDocs configuration, textbook Markdown, and refresh state.

## Local preview

From the repository root, preview without installing the source package or sibling
repositories:

```bash
uv run --no-project --with-requirements docs-site/requirements.txt \
  python -m mkdocs serve --config-file docs-site/site/mkdocs.yml
```

## Refresh

Refreshes are manual. Load `textbook-refresh` from the `mountainash-ibook-skills`
tooling and supply this repository's
absolute root as `source_repo`, starting with `mode: check`. For a separate
profile update, supply `docs-site/profile/` as the profiler's explicit output.
Do not regenerate content merely to publish it or advance source baselines on
a directory move. Preserve the existing FAQ format; the marker-only FAQ
exporter does not support it and must not overwrite its JSON.

## Published surfaces

The site publishes the chapters, FAQ, glossary, home, About, license and contact
pages. The learning graph, its reports, the course description and the MicroSims
index were retired from the published site in the 2026-10-29 replacement; the
canonical graph stays internal under `learning-graph/`.
