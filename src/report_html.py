#!/usr/bin/env python3
"""Self-contained HTML report builder for dichromat (MultiQC replacement).

Stdlib-only (html.escape + basic SVG rendering) so the image stays small.
Reads the pipeline's own QC products - falco fastqc_data.txt/summary.txt,
the mqc_* TSV tables and the HISAT2-style .summary files - and renders them
as one self-contained report.html (plus per-section pages).

Usage:
  report_html.py qc OUT.html fastqc_data.txt [fastqc_data.txt ...]
  report_html.py tables OUT.html [FILE ...]          (.tsv -> table, .summary -> block)
  report_html.py assemble OUT.html SECTION.html [SECTION.html ...]
"""

import csv
import html
import re
import sys
from pathlib import Path

CSS = """
:root{color-scheme:light dark}
body{font-family:-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;
     margin:0;color:#24292f;background:#fff;line-height:1.45}
header{background:#0b7285;color:#fff;padding:18px 28px}
header h1{margin:0;font-size:20px}
header .sub{opacity:.85;font-size:13px}
nav{background:#f6f8fa;border-bottom:1px solid #d8dee4;padding:6px 28px;font-size:13px}
nav a{color:#0b7285;text-decoration:none;margin-right:14px}
main{padding:20px 28px;max-width:1200px;margin:0 auto}
section{margin:0 0 34px}
h2{font-size:17px;border-bottom:2px solid #d0e3ea;padding-bottom:6px;margin-top:34px}
table{border-collapse:collapse;width:100%;font-size:12.5px;margin:8px 0}
th,td{border:1px solid #d8dee4;padding:4px 8px;text-align:right;white-space:nowrap}
th{background:#f0f5f8}
td:first-child,th:first-child{text-align:left}
tr:nth-child(even) td{background:#fafbfc}
.pass{color:#1a7f37}.warn{color:#9a6700}.fail{color:#cf222e}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;
      background:#f6f8fa;border:1px solid #d8de4e;padding:1px 5px}
.wrap td{white-space:normal}
.svgwrap{background:#fff;border:1px solid #d8de4e;display:inline-block}
.small{color:#57606a;font-size:12px}
""".strip()


def esc(x):
    return html.escape(str(x), quote=True)


# ---------------------------------------------------------------------------
# falco / FastQC text parsing
# ---------------------------------------------------------------------------
def parse_fastqc_data(path):
    """Return {module: {"status": str, "header": list|None, "rows": [list]}}."""
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
# tiny inline-SVG chart helpers
# ---------------------------------------------------------------------------
def svg_line_chart(points, width=720, height=180, ylab="", color="#0b7285",
                   xlabel_bias=0):
    """points: list of (x, y).  Renders a polyline with light grid."""
    if not points:
        return ""
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    ymin, ymax = min(ys), max(ys)
    if ymax - ymin < 1e-9:
        ymax, ymin = ymin + 1, ymin - 1
    pad_l, pad_r, pad_t, pad_b = 48, 12, 12, 22 + xlabel_bias
    pw, ph = width - pad_l - pad_r, height - pad_t - pad_b

    def S(p):
        x, y = p
        return f"{pad_l + (x - xs[0]) / (xs[-1] - xs[0] + 1e-12) * pw:.1f}," \
               f"{pad_t + (ymax - y) / (ymax - ymin) * ph:.1f}"

    coords = " ".join(S(p) for p in points)
    grid = ""
    for i in range(0, 5):
        gy = pad_t + ph * i / 4
        val = ymax - (ymax - ymin) * i / 4
        grid += (f'<line x1="{pad_l}" y1="{gy:.1f}" x2="{width - pad_r}" '
                 f'y2="{gy:.1f}" stroke="#eef2f5"/><text x="{pad_l - 5}" '
                 f'y="{gy + 3:.1f}" text-anchor="end" font-size="9" fill="#888">{val:.1f}</text>')
    return (f'<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
            f'style="background:#fff">'
            f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t + ph}" '
            f'stroke="#ccc"/>{grid}'
            f'<polyline points="{coords}" fill="none" stroke="{color}" stroke-width="1.6"/>'
            f'</svg>')


def svg_multi_line(series, width=720, height=180, palette=("#0b7285", "#d9491a",
                                                          "#6f42c1", "#cf222e", "#9a6700")):
    """series: list of (label, [(x, y)]) -> overlaid polylines."""
    all_pts = [p for _, pts in series for p in pts]
    if not all_pts:
        return ""
    xs = [p[0] for p in all_pts]
    ys = [p[1] for p in all_pts]
    ymin, ymax = min(ys), max(ys)
    if ymax - ymin < 1e-9:
        ymax, ymin = ymin + 1, ymin - 1
    pad_l, pad_r, pad_t, pad_b = 48, 12, 14, 22
    pw, ph = width - pad_l - pad_r, height - pad_t - pad_b

    def S(p):
        return f"{pad_l + (p[0] - xs[0]) / max(xs[-1] - xs[0], 1e-12) * pw:.1f}," \
               f"{pad_t + (ymax - p[1]) / (ymax - ymin) * ph:.1f}"

    polys = ""
    for i, (label, pts) in enumerate(series):
        c = palette[i % len(palette)]
        if len(pts) < 2:
            continue
        polys += (f'<polyline points="{" ".join(S(p) for p in pts)}" '
                  f'fill="none" stroke="{c}" stroke-width="1.5"/>')
    legend = "".join(f'<rect x="{pad_l + i * 130}" y="2" width="9" height="9" '
                     f'fill="{palette[i % len(palette)]}"/>'
                     f'<text x="{pad_l + i * 130 + 13}" y="10" font-size="10" '
                     f'fill="#444">{esc(label[:18])}</text>'
                     for i, (label, _) in enumerate(series))
    return (f'<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
            f'style="background:#fff">{legend}'
            f'<line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{pad_t + ph}" stroke="#ccc"/>'
            f'{polys}</svg>')


# ---------------------------------------------------------------------------
# rendering pieces
# ---------------------------------------------------------------------------
def table_html(header, rows, status=None):
    out = [f'<table><thead><tr>{"".join(f"<th>{esc(h)}</th>" for h in header)}</tr></thead><tbody>']
    for r in rows:
        if not r:
            continue
        cls = ""
        if status is not None and r[0].upper() in ("PASS", "WARN", "FAIL"):
            cls = f' class="{r[0].lower()}"'
        out.append(f"<tr>{''.join(f'<td{cls}>{esc(c)}</td>' for c in r)}</tr>")
    out.append("</tbody></table>")
    return "".join(out)


def section(title, body, anchor=""):
    a = f' id="{anchor}"' if anchor else ""
    return f'<section{a}><h2>{esc(title)}</h2>{body}</section>'


def render_fastqc(dir_or_file, name):
    p = Path(dir_or_file)
    data_path = p if p.name == "fastqc_data.txt" else p / "fastqc_data.txt"
    summary_path = data_path.parent / "summary.txt"
    mods = parse_fastqc_data(data_path)
    parts = []
    # summary (pass/warn/fail)
    if summary_path.exists():
        rows = parse_summary_txt(summary_path)
        parts.append(section(
            f"{esc(name)} — module status",
            table_html(["Status", "Module", "File"], rows),
        ))
    # Basic Statistics
    if "Basic Statistics" in mods:
        m = mods["Basic Statistics"]
        parts.append(section(f"{esc(name)} — basic statistics",
                             table_html(m["header"] or ["Measure", "Value"], m["rows"])))
    # compact per-base quality + sequence quality
    for mod, cols in (
        ("Per base sequence quality", ["#Base", "Mean", "Median"]),
        ("Per sequence quality scores", ["#Quality", "Count"]),
        ("Per sequence GC content", ["#GC Content", "Count"]),
    ):
        m = mods.get(mod)
        if not m or not m["rows"]:
            continue
        hdr = m["header"] or []
        idx = {h.lower(): i for i, h in enumerate(hdr)}
        xi = idx.get(list(idx)[0] if idx else "", 0)
        series = []
        for col in cols[1:]:
            j = idx.get(col.lower())
            if j is None:
                continue
            pts = []
            for r in m["rows"]:
                try:
                    pts.append((float(r[xi]), float(r[j])))
                except (ValueError, IndexError):
                    pass
            series.append((col, pts))
        if series:
            svg = svg_multi_line(series) if len(series) > 1 else \
                svg_line_chart(series[0][1])
            parts.append(section(f"{esc(name)} — {mod}",
                                 f'<div class="svgwrap">{svg}</div>'))
    # base content / adapter content as multi-line
    for mod in ("Per base sequence content", "Adapter Content"):
        m = mods.get(mod)
        if not m or not m["rows"]:
            continue
        hdr = m["header"] or []
        idx = {h.lower(): i for i, h in enumerate(hdr)}
        bcol = list(idx.keys())[0] if idx else ""
        if not bcol:
            continue
        series = []
        for col, j in sorted(idx.items(), key=lambda kv: kv[1]):
            if col == bcol or j is None:
                continue
            pts = []
            for r in m["rows"]:
                try:
                    pts.append((float(r[idx[bcol]]), float(r[j])))
                except (ValueError, IndexError):
                    pass
            series.append((col, pts))
        if series:
            parts.append(section(f"{esc(name)} — {mod}",
                                 f'<div class="svgwrap">{svg_multi_line(series)}</div>'))
    # overrepresented sequences table
    m = mods.get("Overrepresented sequences")
    if m and m["rows"]:
        parts.append(section(f"{esc(name)} — overrepresented sequences",
                             table_html(m["header"] or ["Sequence", "Count", "Percentage",
                                                        "Possible Source"],
                                        m["rows"])))
    if not parts:
        return section(f"{esc(name)}", "<p class='small'>No parseable QC data.</p>")
    return "".join(parts)


def render_tables(files):
    parts = []
    for f in files:
        p = Path(f)
        if p.suffix == ".summary":
            body = "<pre class='wrap'>" + esc(p.read_text(errors="replace")) + "</pre>"
            parts.append(section(str(p.parent.name) + "/" + p.name, body))
            continue
        try:
            with open(p, newline="", errors="replace") as fh:
                sniffer = csv.Sniffer()
                dialect = sniffer.sniff(fh.read(4096), delimiters="\t,")
                fh.seek(0)
                rows = list(csv.reader(fh, dialect))
        except Exception:
            rows = [ln.rstrip("\n").split("\t") for ln in
                    open(p, errors="replace")][:1000]
        if not rows:
            continue
        header = rows[0]
        body_rows = rows[1:200]
        note = "" if len(rows) <= 201 else f"<p class='small'>showing 200 of {len(rows) - 1} rows</p>"
        parts.append(section(p.name, table_html(header, body_rows) + note))
    return "".join(parts)


# ---------------------------------------------------------------------------
# commands
# ---------------------------------------------------------------------------
def page(title, nav, body_plain):
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{esc(title)}</title><style>{CSS}</style></head>
<body><header><h1>{esc(title)}</h1><div class="sub">dichromat self-contained report</div></header>
{nav}<main>{body_plain}</main></body></html>"""


def cmd_qc(out, files):
    bodies = "".join(render_fastqc(f, Path(f).parent.name) for f in files)
    section_files = {Path(f).parent.name: Path(f).parent.name for f in files}
    nav = "".join(f'<a href="#{esc(a)}">{esc(a)}</a>' for a in section_files)
    Path(out).write_text(page(Path(out).stem, nav, bodies))
    print(f"[report_html] wrote {out} ({len(bodies)} chars of QC)")


def cmd_tables(out, files):
    bodies = render_tables(files)
    Path(out).write_text(page(Path(out).stem, "", bodies))
    print(f"[report_html] wrote {out} ({len(files)} tables)")


def cmd_assemble(out, sections):
    style_blocks = [re.search(r"<style>(.*?)</style>",
                              Path(s).read_text(errors="replace"), re.S)
                    for s in sections]
    merged_style = "\n".join(m.group(1) for m in style_blocks if m)
    bodies = []
    for s in sections:
        m = re.search(r"<body>(.*)</body>", Path(s).read_text(errors="replace"), re.S)
        if m:
            bodies.append(m.group(1))
    nav = "".join(f'<a href="#{Path(s).stem}">{esc(Path(s).stem)}</a>' for s in sections)
    html_out = (f'<!doctype html><html lang="en"><head><meta charset="utf-8">'
                f'<title>dichromat final report</title>'
                f'<style>{CSS}\n{merged_style}</style></head>'
                f'<body><header><h1>dichromat final report</h1>'
                f'<div class="sub">QC • mapping • sites</div></header>'
                f'<nav>{nav}</nav><main>{"".join(bodies)}</main></body></html>')
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
    except Exception as exc:  # never silently fail the whole pipeline
        import traceback
        traceback.print_exc()
        print(f"[report_html] ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
