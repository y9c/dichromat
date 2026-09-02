#!/usr/bin/env python3
"""Self-contained HTML report builder for dichromat (MultiQC replacement).

Stdlib-only (html.escape + inline SVG). Reads the pipeline's QC products -
falco fastqc_data.txt / summary.txt, the mqc_* TSV tables and HISAT2-style
.summary files - and renders one self-contained report.html.

Usage:
  report_html.py qc OUT.html fastqc_data.txt [fastqc_data.txt ...]
  report_html.py tables OUT.html [FILE ...]          (.tsv -> table, .summary -> block)
  report_html.py metagene OUT.html PROFILE.tsv       (coralsnake metagene --export-profile)
  report_html.py logo OUT.html MATRIX.tsv            (coralsnake logo --matrix)
  report_html.py motifconv OUT.html BY_MOTIF.tsv [...]
  report_html.py assemble OUT.html SECTION.html [SECTION.html ...]
"""

import csv
import html
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# palette / shared constants
# ---------------------------------------------------------------------------
PALETTE = ("#0b7285", "#d9480f", "#7048e8", "#c92a2a", "#5f3dc4", "#2b8a3e",
           "#e8590c", "#1971c2", "#e64980", "#74b816")

CSS = """
:root{
  color-scheme:light dark;
  --bg:#ffffff; --fg:#1f2328; --muted:#57606a; --line:#d8dee4; --line2:#eaeef2;
  --head:#f6f8fa; --zebra:#fafbfc; --accent:#0b7285; --accent-2:#0c8599;
  --pass:#1a7f37; --warn:#9a6700; --fail:#cf222e; --chip:#eff5f8; --shadow:0 1px 3px rgba(0,0,0,.08);
}
@media (prefers-color-scheme:dark){
  :root{--bg:#0d1117; --fg:#e6edf3; --muted:#8b949e; --line:#30363d; --line2:#21262d;
    --head:#161b22; --zebra:#161b22; --chip:#21262d;
    --pass:#3fb950; --warn:#d29922; --fail:#f85149; --accent:#4da3c2; --accent-2:#79c0ff; --shadow:0 1px 3px rgba(0,0,0,.5);}
}
*{box-sizing:border-box}
body{font-family:-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;margin:0;
     color:var(--fg);background:var(--bg);line-height:1.5;font-size:14px}
a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}
header{background:linear-gradient(120deg,var(--accent),var(--accent-2));
       color:#fff;padding:22px 30px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px}
header h1{margin:0;font-size:21px;font-weight:650}
header .sub{opacity:.92;font-size:13px;margin-top:2px}
header .meta{font-size:12px;text-align:right;opacity:.95}
header .meta div{white-space:nowrap}
nav{position:sticky;top:0;z-index:10;background:var(--head);border-bottom:1px solid var(--line);
    padding:8px 30px;font-size:13px;display:flex;flex-wrap:wrap;gap:4px}
nav a{color:var(--fg);border:1px solid var(--line);padding:2px 9px;border-radius:12px;background:var(--bg)}
nav a:hover{background:var(--chip);text-decoration:none}
main{padding:20px 30px;max-width:1280px;margin:0 auto}
h2{font-size:17px;font-weight:650;border-bottom:2px solid var(--line2);
   padding-bottom:6px;margin:30px 0 12px;position:relative}
h2 .count{color:var(--muted);font-weight:500;font-size:13px}
section{margin-bottom:8px;scroll-margin-top:46px}
table{border-collapse:collapse;width:100%;font-size:12.5px;margin:8px 0}
th,td{border:1px solid var(--line);padding:5px 9px;text-align:right;white-space:nowrap}
th{background:var(--head);font-weight:600;user-select:none;cursor:pointer;position:sticky;top:36px}
td:first-child,th:first-child{text-align:left}
tbody tr:nth-child(even) td{background:var(--zebra)}
tbody tr:hover td{background:var(--chip)}
tr td.wrapcell{white-space:normal;word-break:break-word}
.pass{color:var(--pass)} .warn{color:var(--warn)} .fail{color:var(--fail)}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;font-size:12px;
      background:var(--head);border:1px solid var(--line);padding:1px 5px;border-radius:4px}
.svgwrap{background:var(--bg);border:1px solid var(--line);border-radius:6px;
         padding:6px;display:inline-block;max-width:100%;overflow-x:auto}
.svgwrap svg{max-width:100%;height:auto}
.small{color:var(--muted);font-size:12px}
.badge{display:inline-block;font-size:11px;font-weight:600;padding:1px 8px;border-radius:10px}
.badge.pass{background:rgba(26,127,55,.12);color:var(--pass)}
.badge.warn{background:rgba(154,102,0,.14);color:var(--warn)}
.badge.fail{background:rgba(207,34,46,.12);color:var(--fail)}
.badge.gray{background:var(--chip);color:var(--muted)}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:12px;margin:14px 0}
.card{background:var(--bg);border:1px solid var(--line);border-radius:8px;box-shadow:var(--shadow);padding:12px 14px}
.card .k{font-size:11.5px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}
.card .v{font-size:20px;font-weight:650;margin-top:2px}
.pre{white-space:pre-wrap;background:var(--head);border:1px solid var(--line);border-radius:6px;padding:8px 12px;font-family:ui-monospace,Menlo,monospace;font-size:12px}
footer{color:var(--muted);font-size:12px;text-align:center;padding:22px;border-top:1px solid var(--line);margin-top:30px}
legend{display:inline-block;margin:2px 10px 2px 0;font-size:12px}
legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:4px;vertical-align:-1px}
@media print{
  nav,header .meta,footer{display:none}
  a{text-decoration:none}
  section{page-break-inside:avoid}
  body{font-size:11px}
}
""".strip()


def esc(x):
    return html.escape(str(x), quote=True)


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# falco / FastQC parsing
# ---------------------------------------------------------------------------
def parse_fastqc_data(path):
    modules = {}
    cur = None
    with open(path, "r", errors="replace") as fh:
        for line in fh:
            line = line.rstrip("\n")
            if line.startswith(">>END_MODULE"):
                cur = None
            elif line.startswith(">>"):
                parts = line[2:].split("\t")
                cur = parts[0]
                modules[cur] = {"status": parts[1] if len(parts) > 1 else "",
                                "header": None, "rows": []}
            elif cur is not None:
                if modules[cur]["header"] is None and line.startswith("#"):
                    modules[cur]["header"] = [c.strip() for c in line[1:].split("\t")]
                elif line.strip():
                    modules[cur]["rows"].append([c.strip() for c in line.split("\t")])
    return modules


def parse_summary_txt(path):
    rows = []
    with open(path, "r", errors="replace") as fh:
        for line in fh:
            parts = line.rstrip("\n").split("\t")
            if len(parts) >= 2:
                st = parts[0].upper()
                rows.append((st, parts[1], parts[2] if len(parts) > 2 else ""))
    return rows


# ---------------------------------------------------------------------------
# inline-SVG charts (with axes, ticks, tooltips)
# ---------------------------------------------------------------------------
def _axis(pad_l, pad_r, pad_t, pad_b, width, height):
    return (f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{height - pad_b}" stroke="#999"/>'
            f'<line x1="{pad_l}" y1="{height - pad_b}" x2="{width - pad_r}" y2="{height - pad_b}" stroke="#999"/>')


def _y_grid(pad_l, pad_r, pad_t, pad_b, width, height, ymin, ymax):
    out = ["<g>"]
    for i in range(5):
        gy = pad_t + ((height - pad_t - pad_b) * i / 4)
        val = ymax - (ymax - ymin) * i / 4
        out.append(f'<line x1="{pad_l}" y1="{gy:.1f}" x2="{width - pad_r}" y2="{gy:.1f}" stroke="rgba(128,128,128,.25)"/>')
        out.append(f'<text x="{pad_l - 5}" y="{gy + 3:.1f}" text-anchor="end" font-size="9.5" fill="#8f9aa4">{val:.4g}</text>')
    out.append("</g>")
    return "".join(out)


def _x_ticks(pad_l, pad_r, pad_t, pad_b, width, height, xs, label=None):
    if not xs:
        return ""
    out = []
    n = len(xs)
    step = max(1, n // 8)
    for i in range(0, n, step):
        x = pad_l + (xs[i] - xs[0]) / max(xs[-1] - xs[0], 1e-12) * (width - pad_l - pad_r)
        out.append(f'<line x1="{x:.1f}" y1="{height - pad_b}" x2="{x:.1f}" y2="{height - pad_b + 3}" stroke="#999"/>')
        out.append(f'<text x="{x:.1f}" y="{height - pad_b + 13}" text-anchor="middle" font-size="9" fill="#8f9aa4">{xs[i]:.4g}</text>')
    if label:
        out.append(f'<text x="{(pad_l + width - pad_r) / 2:.0f}" y="{height - 3}" text-anchor="middle" font-size="10" fill="#57606a">{esc(label)}</text>')
    return "".join(out)


def svg_line(points, width=760, height=210, color="#0b7285", ylab="", xlab="",
             fill=True):
    """Single-series line chart with axes, y-grid, x-ticks and optional area fill."""
    pts = [(float(a), float(b)) for a, b in points if _num(a) is not None and _num(b) is not None]
    if len(pts) < 2:
        return ""
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    ymin, ymax = min(ys), max(ys)
    if ymax - ymin < 1e-9:
        ymax, ymin = ymin + 1, ymin - 1
    pad_l, pad_r, pad_t, pad_b = 52, 14, 12, 26 if xlab else 20
    pw, ph = width - pad_l - pad_r, height - pad_t - pad_b

    def S(p):
        x = pad_l + (p[0] - xs[0]) / max(xs[-1] - xs[0], 1e-12) * pw
        y = pad_t + (ymax - p[1]) / (ymax - ymin) * ph
        return x, y

    coords = " ".join(f"{S(p)[0]:.1f},{S(p)[1]:.1f}" for p in pts)
    area = ""
    if fill:
        x0, y0 = S((xs[0], ymin))
        x1, y1 = S((xs[-1], ymin))
        area = (f'<polygon points="{x0:.1f},{y0:.1f} {coords} {x1:.1f},{y1:.1f}" '
                f'fill="{color}" opacity="0.08"/>')
    dots = "".join(f'<circle cx="{S(p)[0]:.1f}" cy="{S(p)[1]:.1f}" r="1.6" fill="{color}"/>' for p in pts)
    ylab_el = (f'<text transform="rotate(-90 {16} {(pad_t + height - pad_b) / 2:.0f})" '
               f'x="{16}" y="{(pad_t + height - pad_b) / 2:.0f}" text-anchor="middle" '
               f'font-size="10" fill="#57606a">{esc(ylab)}</text>') if ylab else ""
    return (f'<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}">'
            f'{ylab_el}{_axis(pad_l, pad_r, pad_t, pad_b, width, height)}'
            f'{_y_grid(pad_l, pad_r, pad_t, pad_b, width, height, ymin, ymax)}'
            f'{_x_ticks(pad_l, pad_r, pad_t, pad_b, width, height, xs, xlab)}'
            f'{area}<polyline points="{coords}" fill="none" stroke="{color}" stroke-width="1.7" stroke-linejoin="round"/>'
            f'{dots}</svg>')


def svg_multi(series, width=760, height=220, ylab="", xlab=""):
    """Overlaid line series with a separate HTML legend below (no overflow)."""
    clean = []
    for label, pts in series:
        pp = [(float(a), float(b)) for a, b in pts
              if _num(a) is not None and _num(b) is not None]
        if len(pp) > 1:
            clean.append((label, pp))
    if not clean:
        return ""
    xs = [p[0] for _, pp in clean for p in pp]
    ys = [p[1] for _, pp in clean for p in pp]
    xmin, xmax, ymin, ymax = min(xs), max(xs), min(ys), max(ys)
    if ymax - ymin < 1e-9:
        ymax, ymin = ymin + 1, ymin - 1
    pad_l, pad_r, pad_t, pad_b = 52, 14, 12, 26 if xlab else 20
    pw, ph = width - pad_l - pad_r, height - pad_t - pad_b

    def S(x, y):
        return (pad_l + (x - xmin) / max(xmax - xmin, 1e-12) * pw,
                pad_t + (ymax - y) / (ymax - ymin) * ph)

    polys = ""
    legend = ""
    for i, (label, pp) in enumerate(clean):
        c = PALETTE[i % len(PALETTE)]
        coords = " ".join(f"{S(x, y)[0]:.1f},{S(x, y)[1]:.1f}" for x, y in pp)
        polys += (f'<polyline points="{coords}" fill="none" stroke="{c}" '
                  f'stroke-width="1.6" stroke-linejoin="round"/>')
        legend += (f'<legend><i style="background:{c}"></i>{esc(label)}</legend>')
    ylab_el = (f'<text transform="rotate(-90 {16} {(pad_t + height - pad_b) / 2:.0f})" '
               f'x="{16}" y="{(pad_t + height - pad_b) / 2:.0f}" text-anchor="middle" '
               f'font-size="10" fill="#57606a">{esc(ylab)}</text>') if ylab else ""
    svg = (f'<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}">'
           f'{ylab_el}{_axis(pad_l, pad_r, pad_t, pad_b, width, height)}'
           f'{_y_grid(pad_l, pad_r, pad_t, pad_b, width, height, ymin, ymax)}'
           f'{_x_ticks(pad_l, pad_r, pad_t, pad_b, width, height, xs, xlab)}'
           f'{polys}</svg>')
    return f'<div style="overflow-x:auto">{svg}<div style="padding:4px 2px">{legend}</div></div>'


# ---------------------------------------------------------------------------
# HTML pieces
# ---------------------------------------------------------------------------
def table_html(header, rows, sortable=True, wrap_cols=None, show=200):
    hdr = "".join(f"<th>{esc(h)}</th>" for h in header)
    cls = " sortable" if sortable else ""
    body = []
    for r in rows[:show]:
        tds = []
        for j, c in enumerate(r):
            wc = ' class="wrapcell"' if (wrap_cols and j in wrap_cols) else ""
            stat = c.strip().upper()
            cc = {"PASS": "pass", "WARN": "warn", "FAIL": "fail"}.get(stat)
            cls2 = f' class="{cc}"' if cc else ""
            tds.append(f"<td{wc}{cls2}>{esc(c)}</td>")
        body.append(f"<tr>{''.join(tds)}</tr>")
    note = "" if len(rows) <= show else (
        f"<p class='small'>… showing {show} of {len(rows)} rows</p>")
    return f'<table class="{cls.strip() or "plain"}"><thead><tr>{hdr}</tr></thead><tbody>' + "".join(body) + f"</tbody></table>{note}"


def section(title, body, anchor=""):
    a = f' id="{anchor}"' if anchor else ""
    return f'<section{a}><h2>{esc(title)}</h2>{body}</section>'


def badges(counts):
    """counts: dict status->int -> '<span class=badge pass>PASS 3</span>...'"""
    out = []
    for st in ("PASS", "WARN", "FAIL"):
        if counts.get(st, 0):
            out.append(f'<span class="badge {st.lower()}">{st} {counts[st]}</span>')
    return " ".join(out)


def _module_key_figure(mods):
    """Pull (total_sequences, seq_len, gc) from Basic Statistics if present."""
    m = mods.get("Basic Statistics")
    if not m:
        return None
    d = {r[0].strip(): r[1].strip() for r in m["rows"] if len(r) >= 2}
    return d


# ---------------------------------------------------------------------------
# rendering
# ---------------------------------------------------------------------------
def render_fastqc(dir_or_file, name):
    p = Path(dir_or_file)
    data_path = p if p.name == "fastqc_data.txt" else p / "fastqc_data.txt"
    summary_path = data_path.parent / "summary.txt"
    mods = parse_fastqc_data(data_path)
    sid = re.sub(r"[^A-Za-z0-9_-]", "_", name)

    # module status badges + basic stats
    counts = {"PASS": 0, "WARN": 0, "FAIL": 0}
    if summary_path.exists():
        for st, mod, _ in parse_summary_txt(summary_path):
            counts[st] = counts.get(st, 0) + 1
    key = _module_key_figure(mods)

    head = f'<p>{badges(counts)}'
    if key:
        keep = [("Filename", key.get("Filename")), ("File type", key.get("File type")),
                ("Total Sequences", key.get("Total Sequences")),
                ("Sequence length", key.get("Sequence length")),
                ("%GC", key.get("%GC"))]
        cells = "".join(f"<span class='badge gray'>{esc(k)}: {esc(v or '')}</span> "
                        for k, v in keep if v)
        head += " " + cells
    head += "</p>"

    parts = [f'<section id="{sid}"><h2>{esc(name)} <span class="count">QC</span></h2>{head}']

    for mod, cols in (("Per base sequence quality", ["#Base", "Mean", "Median"]),
                      ("Per sequence quality scores", ["#Quality", "Count"]),
                      ("Per sequence GC content", ["#GC Content", "Count"]),
                      ("Per base sequence content", None),
                      ("Adapter Content", None)):
        m = mods.get(mod)
        if not m or not m["rows"]:
            continue
        hdr = m["header"] or []
        idx = {h.lower(): i for i, h in enumerate(hdr)}
        xj = min(idx.values()) if idx else 0
        series = []
        for col, j in sorted(idx.items(), key=lambda kv: kv[1]):
            if j == xj:
                continue
            pts = []
            for r in m["rows"]:
                try:
                    pts.append((float(r[xj]), float(r[j])))
                except (ValueError, IndexError):
                    pass
            series.append((col, pts))
        if series:
            xlab = "position" if "base" in mod.lower() else mod.split()[-1]
            parts.append(f'<h3 style="font-size:14px;margin:14px 0 4px">{esc(mod)}</h3>'
                         f'<div class="svgwrap">{svg_multi(series, xlab=xlab)}</div>')

    m = mods.get("Overrepresented sequences")
    if m and m["rows"]:
        parts.append(f"<h3 style='font-size:14px;margin:14px 0 4px'>Overrepresented sequences</h3>"
                     + table_html(m["header"] or ["Sequence", "Count", "Percentage", "Possible Source"],
                                  m["rows"], wrap_cols={0}))

    parts.append("</section>")
    return "".join(parts)


def render_overview(files):
    """MultiQC-style overview: one row per sample with key stats + badges."""
    rows, header = [], ["Sample", "Total Sequences", "Sequence Length", "%GC",
                        "Adapter", "Partial Status"]
    for f in files:
        p = Path(f)
        data_path = p if p.name == "fastqc_data.txt" else p / "fastqc_data.txt"
        summary_path = data_path.parent / "summary.txt"
        name = data_path.parent.name
        counts = {"PASS": 0, "WARN": 0, "FAIL": 0}
        if summary_path.exists():
            for st, mod, _ in parse_summary_txt(summary_path):
                counts[st] = counts.get(st, 0) + 1
        key = _module_key_figure(parse_fastqc_data(data_path)) or {}
        rows.append([name,
                     key.get("Total Sequences", "-"),
                     key.get("Sequence length", "-"),
                     key.get("%GC", "-"),
                     key.get("Overrepresented sequences", "0"),
                     " ".join(f"{st} {counts[st]}" for st in ("PASS", "WARN", "FAIL") if counts[st]) or "-"])
    return table_html(header, rows, wrap_cols={5})


def page(title, meta, nav, body_plain):
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{esc(title)}</title><style>{CSS}</style></head>
<body><header><div><h1>{esc(title)}</h1><div class="sub">dichromat self-contained report</div></div>
<div class="meta"><div>generated {stamp}</div>{meta}</div></header>
<nav>{nav}</nav><main>{body_plain}</main>
<footer>dichromat report • self-contained • no external assets</footer></body></html>"""


TINY_JS = """
<script>
document.addEventListener('click',function(e){
  var th=e.target.closest('th'); if(!th) return;
  var t=th.closest('table'); var tb=t.tBodies[0]; if(!tb) return;
  var rows=Array.from(tb.rows); var ci=Array.prototype.indexOf.call(th.parentNode.children,th);
  var asc=th.dataset.dir!=='asc';
  rows.sort(function(a,b){var x=a.cells[ci].textContent.trim(),y=b.cells[ci].textContent.trim();
    var nx=parseFloat(x),ny=parseFloat(y);var vx=isNaN(nx)?x:nx,vy=isNaN(ny)?y:ny;
    return (vx>vy?1:vx<vy?-1:0)*(asc?1:-1);});
  rows.forEach(function(r){tb.appendChild(r);}); th.dataset.dir=asc?'asc':'desc';
});
</script>
""".strip()


def cmd_qc(out, files):
    overview = render_overview(files)
    bodies = "".join(render_fastqc(f, Path(f).parent.name) for f in files)
    nav = "".join(f'<a href="#{sid}">{esc(name)}</a>'
                  for f in files
                  for name, sid in [(Path(f).parent.name,
                                     re.sub(r"[^A-Za-z0-9_-]", "_", Path(f).parent.name))])
    body = (f'<section id="overview"><h2>Overview</h2>{overview}</section>'
            + bodies + "\n" + TINY_JS)
    Path(out).write_text(page(Path(out).stem, "", nav, body))
    print(f"[report_html] wrote {out} ({len(files)} samples)")


def cmd_tables(out, files):
    parts = []
    for f in files:
        p = Path(f)
        if p.suffix == ".summary":
            parts.append(section(p.name, f'<div class="pre">{esc(p.read_text(errors="replace"))}</div>'))
            continue
        try:
            with open(p, newline="", errors="replace") as fh:
                sample = fh.read(4096)
                dialect = csv.Sniffer().sniff(sample, delimiters="\t,")
                fh.seek(0)
                rows = list(csv.reader(fh, dialect))
        except Exception:
            rows = [ln.rstrip("\n").split("\t") for ln in open(p, errors="replace")]
        if not rows:
            continue
        header = rows[0]
        sid = re.sub(r"[^A-Za-z0-9_-]", "_", p.stem)
        parts.append(section(p.stem, table_html(header, rows[1:]), anchor=sid))
    nav = "".join(f'<a href="#{re.sub(chr(92) + "W+", "_", e)}">{esc(e)}</a>'
                  for e in [Path(f).stem for f in files])
    Path(out).write_text(page(Path(out).stem, "", nav, "".join(parts) + "\n" + TINY_JS))
    print(f"[report_html] wrote {out} ({len(files)} tables)")


# ---------------------------------------------------------------------------
# metagene profile / sequence logo / motif conversion  (matrix -> inline SVG)
# ---------------------------------------------------------------------------
def _read_tsv(path):
    with open(path, newline="", errors="replace") as fh:
        rows = list(csv.reader(fh, delimiter="\t"))
    return rows[0], [r for r in rows[1:] if any(c.strip() for c in r)]


def cmd_metagene(out, files):
    """Render a metagene coverage-distribution chart from a profile matrix TSV.

    Input (from `coralsnake metagene --export-profile`): columns
    feature_type, feature_midpoint, count[_<name>]...
    """
    if not files:
        print("usage: report_html.py metagene OUT.html PROFILE.tsv", file=sys.stderr)
        return 1
    header, rows = _read_tsv(files[0])
    hdr = [h.strip() for h in header]
    if "feature_midpoint" not in hdr:
        print(f"[report_html] metagene: no feature_midpoint column in {files[0]}", file=sys.stderr)
        return 1
    xj = hdr.index("feature_midpoint")
    ycols = [j for j, h in enumerate(hdr) if j != xj and h.startswith("count")]
    tcol = hdr.index("feature_type") if "feature_type" in hdr else None

    # region boundaries in normalized position (transitions of feature_type)
    def _splits():
        if tcol is None:
            return []
        up5 = [float(r[xj]) for r in rows if r[tcol].strip() == "5UTR"]
        dn3 = [float(r[xj]) for r in rows if r[tcol].strip() == "3UTR"]
        b = []
        if up5:
            b.append(max(up5))
        if dn3:
            b.append(min(dn3))
        return sorted(b)

    splits = _splits()
    series = []
    for j in ycols:
        label = hdr[j].removeprefix("count").lstrip("_") or "coverage"
        pts = []
        for r in rows:
            x, y = _num(r[xj]), _num(r[j]) if j < len(r) else None
            if x is not None and y is not None:
                pts.append((x, y))
        if len(pts) > 1:
            series.append((label, pts))
    body = ""
    if series:
        if len(series) == 1:
            body = svg_line(series[0][1], ylab="Density",
                            xlab="Normalized gene position")
        else:
            body = svg_multi(series, ylab="Density",
                             xlab="Normalized gene position")
    if body and splits:
        # overlay the 5'UTR / CDS / 3'UTR region band + labels
        marks = ""
        names = ["5'UTR", "CDS"] + (["3'UTR"] if len(splits) > 1 else [])
        edges = [0.0] + splits + [1.0]
        for i, (a, b) in enumerate(zip(edges, edges[1:])):
            mid = (a + b) / 2
            marks += (f'<text x="{40 + mid * 680:.0f}" y="16" text-anchor="middle" '
                      f'font-size="10" font-weight="bold" fill="#57606a">{esc(names[i])}</text>')
        marks += "".join(f'<line x1="{40 + s * 680:.0f}" y1="20" x2="{40 + s * 680:.0f}" y2="190" '
                         f'stroke="#c92a2a" stroke-dasharray="4,3" stroke-width="1.5"/>'
                         for s in splits)
        body = body.replace("<svg ", f'<svg style="position:relative" ', 1).replace(
            "</svg>", f"{marks}</svg>", 1)
    if not body:
        body = '<p class="small">No metagene profile points to render.</p>'
    nav = f'<a href="#metagene">Metagene</a>'
    Path(out).write_text(page("Metagene coverage", "", nav,
                              section("Metagene coverage distribution",
                                      f'<div class="svgwrap">{body}</div>',
                                      anchor="metagene")))
    print(f"[report_html] wrote {out} (metagene)")


_LOGO_GLYPHS = {
    # unit-square polygons (x right, y up), simple legible letter shapes
    "A": [(0, 0), (0.35, 1), (0.65, 1), (1, 0), (0.72, 0), (0.63, 0.30),
          (0.37, 0.30), (0.28, 0)],
    "C": [(1, 0.85), (1, 1), (0.5, 1), (0, 0.5), (0.5, 0), (1, 0), (1, 0.15),
          (0.55, 0.15), (0.18, 0.5), (0.55, 0.85)],
    "G": [(1, 0.85), (1, 1), (0.5, 1), (0, 0.5), (0.5, 0), (1, 0), (1, 0.15),
          (0.55, 0.15), (0.18, 0.5), (0.55, 0.85), (0.45, 0.40), (1, 0.40),
          (1, 0.58), (0.55, 0.58)],
    "T": [(0.05, 1), (0.95, 1), (0.95, 0.78), (0.58, 0.78), (0.58, 0),
          (0.42, 0), (0.42, 0.78), (0.05, 0.78)],
    "U": [(0.05, 1), (0.25, 1), (0.25, 0.15), (0.35, 0.02), (0.65, 0.02),
          (0.75, 0.15), (0.75, 1), (0.95, 1), (0.95, 0.12), (0.80, 0),
          (0.20, 0), (0.05, 0.12)],
    "N": [(0.08, 0), (0.92, 0), (0.92, 1), (0.08, 1)],
}
_LOGO_COLORS = {"A": "#d9480f", "C": "#0b7285", "G": "#e8590c", "T": "#2b8a3e",
                "U": "#2b8a3e", "N": "#57606a"}


def cmd_logo(out, files):
    """Render an inline SVG sequence logo from a position x base matrix TSV.

    Input (from `coralsnake logo --matrix`): header `position A C G T U ...`,
    one row per position, values = per-base score.
    """
    if not files:
        print("usage: report_html.py logo OUT.html MATRIX.tsv", file=sys.stderr)
        return 1
    header, rows = _read_tsv(files[0])
    hdr = [h.strip().upper() for h in header]
    base_cols = [(j, h) for j, h in enumerate(hdr)
                 if j > 0 and h in _LOGO_GLYPHS]
    if not base_cols or not rows:
        print(f"[report_html] logo: no base columns found in {files[0]}", file=sys.stderr)
        return 1
    ncols, nrows = len(base_cols), len(rows)
    colw, colh, pad_l, pad_b, pad_t = 34, 160, 22, 26, 12
    # x-axis advances per POSITION (one row of the matrix), not per base column
    width = pad_l + nrows * colw + 14
    height = pad_t + colh + pad_b

    # per-column max (y-scale is per column: heights sum to the tallest column)
    def _col_scores(r):
        d = {}
        for j, b in base_cols:
            v = _num(r[j]) if j < len(r) else None
            if v and v > 0:
                d[b] = v
        return d

    col_scores = [_col_scores(r) for r in rows]
    ymax = max((sum(d.values()) for d in col_scores), default=1.0) or 1.0
    yscale = colh / ymax

    parts = [f'<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}">']
    # baseline + y ticks
    y0 = pad_t + colh
    parts.append(f'<line x1="{pad_l}" y1="{y0}" x2="{width - 6}" y2="{y0}" stroke="#999"/>')
    for i in range(5):
        gy = pad_t + colh * i / 4
        parts.append(f'<line x1="{pad_l - 3}" y1="{gy:.1f}" x2="{pad_l}" y2="{gy:.1f}" stroke="#999"/>')
        parts.append(f'<text x="{pad_l - 5}" y="{gy + 3:.1f}" text-anchor="end" '
                     f'font-size="8.5" fill="#8f9aa4">{ymax * (1 - i / 4):.4g}</text>')
    for ci, d in enumerate(col_scores):
        x0 = pad_l + ci * colw + colw * 0.12
        w = colw * 0.76
        y = y0
        for b, v in sorted(d.items(), key=lambda kv: -kv[1]):
            h = v * yscale
            if h < 0.4:
                continue
            glyph = _LOGO_GLYPHS.get(b) or _LOGO_GLYPHS["A"]
            pts = " ".join(f"{x0 + gx * w:.2f},{y - gy * h:.2f}" for gx, gy in glyph)
            color = _LOGO_COLORS.get(b, "#57606a")
            parts.append(f'<polygon points="{pts}" fill="{color}" opacity="0.92"/>')
            y -= h
        pos = _num(rows[ci][0])
        if pos is not None:
            parts.append(f'<text x="{x0 + w / 2:.1f}" y="{y0 + 12}" text-anchor="middle" '
                         f'font-size="8.5" fill="#8f9aa4">{pos:.0f}</text>')
    parts.append("</svg>")
    legend = "".join(f'<legend><i style="background:{_LOGO_COLORS[b]}"></i>{esc(b)}</legend>'
                     for b in [h for _, h in base_cols])
    nav = '<a href="#logo">Logo</a>'
    Path(out).write_text(page("Sequence logo", "", nav,
                              section(f"Sequence logo ({nrows} positions)",
                                      f'<div class="svgwrap">{"".join(parts)}</div>'
                                      f'<div style="padding:4px 2px">{legend}</div>',
                                      anchor="logo")))
    print(f"[report_html] wrote {out} (logo, {nrows} positions x {len(base_cols)} bases)")


def cmd_motifconv(out, files):
    """Render per-motif conversion rates (table + horizontal bar chart).

    Input: a TSV whose header contains a `Motif` column and at least one
    ratio column (e.g. Motif/Count/Unconverted/Depth/Ratio from
    motif_conversion_rate_stat).  The first ratio column is the primary
    (group 1); a second one (e.g. Ratio_all over all kept groups) is shown
    next to it in the table.
    """
    if not files:
        print("usage: report_html.py motifconv OUT.html BY_MOTIF.tsv [...]", file=sys.stderr)
        return 1
    all_rows = []
    for f in files:
        hdr, rows = _read_tsv(f)
        hdr = [h.strip() for h in hdr]
        mi = next((i for i, h in enumerate(hdr) if h.lower() == "motif"), None)
        ri_list = [i for i, h in enumerate(hdr)
                   if "ratio" in h.lower() or "rate" in h.lower()]
        if mi is None or not ri_list:
            print(f"[report_html] motifconv: need Motif + ratio columns in {f}",
                  file=sys.stderr)
            continue
        ri, ri2 = ri_list[0], (ri_list[1] if len(ri_list) > 1 else None)
        tag = re.sub(r"[^A-Za-z0-9_-]", "_", Path(f).stem)
        for r in rows:
            need = max([mi, ri] + ([ri2] if ri2 is not None else []))
            if len(r) <= need:
                continue
            m, v = r[mi].strip(), _num(r[ri])
            v2 = _num(r[ri2]) if ri2 is not None else None
            if m and v is not None:
                all_rows.append((tag, m, v, v2))
    if not all_rows:
        print("[report_html] motifconv: no data rows found", file=sys.stderr)
        return 1

    # bar chart (one row per motif, grouped per source file)
    bar_w, row_h, pad_l, pad_r, pad_t = 8, 20, 96, 56, 8
    height = pad_t + len(all_rows) * row_h + 8
    width = 720
    svg_parts = [f'<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}">']
    for i, (tag, m, v, _v2) in enumerate(all_rows):
        y = pad_t + i * row_h
        c = PALETTE[hash(tag) % len(PALETTE)]
        bw = max(1.0, min(1.0, v) * (width - pad_l - pad_r))
        svg_parts.append(f'<text x="{pad_l - 6}" y="{y + row_h * 0.68}" text-anchor="end" '
                         f'font-size="9.5" fill="var(--fg)">{esc(m)}</text>')
        svg_parts.append(f'<rect x="{pad_l}" y="{y + 3}" width="{bw:.1f}" '
                         f'height="{row_h - 7}" rx="2" fill="{c}" opacity="0.85"/>')
        svg_parts.append(f'<text x="{pad_l + bw + 5:.1f}" y="{y + row_h * 0.68}" '
                         f'font-size="9" fill="#57606a">{v:.4g}</text>')
    svg_parts.append("</svg>")

    groups = sorted({t for t, _, _, _ in all_rows})
    rows_out = [[g] + [f"{m}: {v:.4g}" + (f" (all: {v2:.4g})" if v2 is not None else "")
                       for t, m, v, v2 in all_rows if t == g] for g in groups]
    body = (table_html(["Source file", "Motif: ratio"], rows_out, wrap_cols={1})
            + f'<h3 style="font-size:14px;margin:14px 0 4px">Conversion rate by motif</h3>'
              f'<div class="svgwrap">{"".join(svg_parts)}</div>')
    nav = '<a href="#motifconv">Motif conversion</a>'
    Path(out).write_text(page("Motif conversion rate", "", nav,
                              section("Motif conversion rate", body,
                                      anchor="motifconv")))
    print(f"[report_html] wrote {out} (motifconv, {len(all_rows)} motifs)")


def _body_ids(bodies):
    """Collect stable element ids from rendered bodies for the TOC."""
    ids = ["overview"]
    for m in re.finditer(r'\bid="([A-Za-z0-9_-]+)"', "".join(bodies)):
        if m.group(1) not in ids:
            ids.append(m.group(1))
    return ids


def cmd_assemble(out, sections):
    styles = [m.group(1) for m in
              (re.search(r"<style>(.*?)</style>", Path(s).read_text(errors="replace"), re.S) for s in sections)
              if m]
    bodies = []
    for s in sections:
        txt = Path(s).read_text(errors="replace")
        m = re.search(r"<body>(.*)</body>", txt, re.S)
        if m:
            bodies.append(m.group(1))
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    toc = " ".join(f'<a href="#{i}">{esc(i.replace("_", " ").title())}</a>'
                   for i in _body_ids(bodies))
    html_out = (f'<!doctype html><html lang="en"><head><meta charset="utf-8">'
                f'<title>dichromat final report</title>'
                f'<style>{CSS}\n{" ".join(styles)}</style></head>'
                f'<body><header><div><h1>dichromat final report</h1>'
                f'<div class="sub">QC • mapping • sites</div></div>'
                f'<div class="meta"><div>{stamp}</div><div>pipeline-generated</div></div></header>'
                f'<nav>{toc}</nav>'
                f'<main>{"".join(bodies)}</main>'
                f'<footer>dichromat report • self-contained</footer>{TINY_JS}</body></html>')
    Path(out).write_text(html_out)
    print(f"[report_html] assembled {out} from {len(sections)} sections")


def main():
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        return 0
    mode, out, rest = args[0], args[1], args[2:]
    try:
        if mode == "qc":
            cmd_qc(out, rest)
        elif mode == "tables":
            cmd_tables(out, rest)
        elif mode == "assemble":
            cmd_assemble(out, rest)
        elif mode == "metagene":
            cmd_metagene(out, rest)
        elif mode == "logo":
            cmd_logo(out, rest)
        elif mode == "motifconv":
            cmd_motifconv(out, rest)
        else:
            print(f"unknown mode {mode}", file=sys.stderr)
            return 1
    except Exception as exc:
        import traceback
        traceback.print_exc()
        print(f"[report_html] ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
