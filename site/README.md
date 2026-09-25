# Static site

Serve or deploy this directory directly. No frontend build is required.

- `index.html`: homepage, including the earlier M3 Ultra indexing measurements.
- `docs.html`: operator documentation.
- `benchmarks.html`: September 2026 Ryzen serving comparison and optional zaino
  historical-indexing plots. Defaults to ztreamer v0.1.0 versus v0.0.1.
- `site.css`: shared styles; `benchmarks.css` and `benchmarks.js` provide the
  comparison page's layout and controls.
- `benchmarks/2026-09-11/`: source CSV/JSON data and generated SVG plots.

To preview locally from the repository root:

```sh
python3 -m http.server 8000 --directory site
```

Visit `http://localhost:8000/benchmarks.html`.

To regenerate the report after changing its dataset or renderer:

```sh
python3 -m venv /tmp/ztreamer-report-venv
/tmp/ztreamer-report-venv/bin/pip install -r scripts/benchmark-report-requirements.txt
/tmp/ztreamer-report-venv/bin/python scripts/render-benchmarks.py
```

The generator never runs benchmarks or starts a node. Commit generated HTML and
plots along with changes to their inputs. Detailed measurement provenance is in
`benchmarks/2026-09-11/README.md` and `metadata.json`. The benchmark generator
and runner remain separate; serving the site needs no Python dependencies.

## Deployment

GitHub Actions deploys `site/` to the existing Cloudflare Worker `ztreamer`
at https://ztreamer.sh when site files, `wrangler.jsonc`, or the deployment
workflow change on `master`. The **Deploy site** workflow can also be run manually
on `master`. No build step is needed.

The root `wrangler.jsonc` defines the assets and custom domain, and disables
`workers.dev` and preview URLs. This uses Workers Static Assets, not Pages.

Repository Actions secrets:

- `CLOUDFLARE_ACCOUNT_ID`: the Cloudflare account containing `ztreamer`.
- `CLOUDFLARE_API_TOKEN`: a deployment token with Workers Scripts Edit on that
  account and Workers Routes Edit plus Zone Read on the `ztreamer.sh` zone
  (or the equivalent Workers Editor and zone route permissions).

A Pages-only token is insufficient. `CLOUDFLARE_PAGES_PROJECT` is no longer used.
See [Cloudflare's permissions guide](https://developers.cloudflare.com/workers/authorization/workers/).

To validate the deployment locally without publishing:

```sh
npx wrangler@4.81.0 deploy --config wrangler.jsonc --dry-run
```
