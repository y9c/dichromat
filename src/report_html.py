#!/usr/bin/env python3
"""Self-contained HTML report builder for dichromat (MultiQC replacement).

Stdlib-only (html.escape + inline SVG). Reads the pipeline's QC products -
falco fastqc_data.txt / summary.txt, the mqc_* TSV tables and HISAT2-style
.summary files - and renders one self-contained report.html.

Usage:
  report_html.py qc OUT.html fastqc_data.txt [fastqc_data.txt ...]
  report_html.py tables OUT.html [FILE ...]          (.tsv -> table, .summary -> block)
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
