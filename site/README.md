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
