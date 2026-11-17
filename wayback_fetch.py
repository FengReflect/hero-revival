#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
英雄之境（4399）网页时光机资源抓取工具

背景：hero.4399.com 已下线（NXDOMAIN），但 Wayback Machine 保存了历史页面与
      静态资源快照。本脚本用于批量拉取这些快照，重点拿回加密 SWF 客户端及其
      配置表 / 图片 / 音频。

用法（需要能访问 archive.org，即代理已开启）：

    # 0) 先设置代理（PowerShell）
    #    $env:HTTPS_PROXY="http://127.0.0.1:7890"
    #    $env:HTTP_PROXY="http://127.0.0.1:7890"

    python wayback_fetch.py list          # 拉取 CDX 索引，生成 cdx.tsv / assets.tsv
    python wayback_fetch.py download      # 按索引批量下载（断点续传，已存在则跳过）
    python wayback_fetch.py scan          # 扫描已下载的 html/js/css，补充资源地址
    python wayback_fetch.py probe         # 探测原站/4399 CDN 是否仍可直接下载
    python wayback_fetch.py all           # 依次执行 list -> download -> scan -> download

常用参数：
    --out DIR         输出目录，默认 ./wayback_yxzj
    --workers N       并发数，默认 4（不要调太高，Wayback 会 429）
    --proxy URL       显式代理，如 http://127.0.0.1:7890
    --only swf,xml    只下载指定扩展名
    --limit N         仅处理前 N 条（先小规模试跑）
    --dry-run         只打印将下载的地址

只依赖 Python 标准库。
"""

import argparse
import hashlib
import json
import os
import random
import re
import shutil
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.error import HTTPError, URLError
from urllib.parse import quote, unquote, urlencode, urlparse
from urllib.request import Request, build_opener

CDX_API = "https://web.archive.org/cdx/search/cdx"
RAW_HOST = "https://web.archive.org/web"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# ---------------------------------------------------------------------------
# 抓取目标：游戏本体所在的域名 / 路径
# 说明：match_type 用 domain 会抓整个子域；用 prefix 只抓该路径下。
# 如果后面发现资源在通用 CDN 上，可放开注释里的按关键词过滤查询。
# ---------------------------------------------------------------------------
QUERIES = [
    # 视频实证：英雄之境的真实服务器（客户端所有模块 SWF 都在根目录）
    {"url": "hero999.aiwan4399.com", "match_type": "domain"},
    {"url": "hero.4399.com", "match_type": "domain"},
    {"url": "web.4399.com/yxzj", "match_type": "prefix"},
    # 注意：sda.4399.com 是 4399 全平台 SWF 的公共 CDN，直接按前缀抓会拉回
    # 其它游戏的文件。正确做法是先从 hero.4399.com 的页面/JS 里拿到确切路径，
    # 再按路径单独抓（或用 probe 直连探测）。
    # 备选（体积可能很大，按需放开）：
    # {"url": "sda.4399.com/4399swf", "match_type": "prefix", "limit": 200000},
    # {"url": "s.4399.com", "match_type": "domain", "filters": ["original:.*(yxzj|hero).*"]},
    # {"url": "my.4399.com/forums", "match_type": "prefix", "filters": ["original:.*yxzj.*"]},
]

# 值得抓取/分析的扩展名（不含 html/js 也会抓，用于 scan 阶段挖地址）
ASSET_EXT = {
    "swf", "xml", "json", "zip", "rar", "7z", "bin", "dat", "amf", "fnt",
    "png", "jpg", "jpeg", "gif", "webp", "bmp",
    "mp3", "ogg", "wav", "m4a",
    "txt", "cfg", "ini", "as", "ab", "atf", "zlib",
}
PAGE_EXT = {"html", "htm", "js", "css", "php", "aspx"}

_print_lock = threading.Lock()


def log(msg):
    with _print_lock:
        print(msg, flush=True)


def make_opener(proxy=None):
    if proxy:
        return build_opener(urllib_proxy(proxy))
    # 不传 proxy 时 urllib 自动读取 HTTP(S)_PROXY 环境变量
    return build_opener()


def urllib_proxy(proxy):
    from urllib.request import ProxyHandler
    return ProxyHandler({"http": proxy, "https": proxy})


def http_read(url, opener, timeout=90, retries=5):
    """带退避重试的 GET，返回 bytes；404 返回 None。"""
    last = None
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": UA})
            with opener.open(req, timeout=timeout) as resp:
                return resp.read()
        except HTTPError as e:
            if e.code == 404:
                return None
            if e.code in (429, 500, 502, 503, 504):
                wait = min(60, 5 * (attempt + 1)) + random.uniform(0, 3)
                log(f"  [{e.code}] 退避 {wait:.0f}s -> {url}")
                time.sleep(wait)
                last = e
                continue
            last = e
            time.sleep(2 + attempt * 2)
        except (URLError, TimeoutError, ConnectionError, OSError) as e:
            last = e
            time.sleep(2 + attempt * 2)
    raise last if last else RuntimeError("unknown error")


def http_download(url, dest, opener, timeout=300, retries=5):
    """流式下载到文件，成功返回字节数。"""
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    tmp = dest + ".part"
    last = None
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": UA})
            with opener.open(req, timeout=timeout) as resp, open(tmp, "wb") as f:
                shutil.copyfileobj(resp, f, length=1 << 20)
            size = os.path.getsize(tmp)
            os.replace(tmp, dest)
            return size
        except HTTPError as e:
            if e.code == 404:
                return 0
            if e.code in (429, 500, 502, 503, 504):
                wait = min(60, 5 * (attempt + 1)) + random.uniform(0, 3)
                log(f"  [{e.code}] 退避 {wait:.0f}s -> {url}")
                time.sleep(wait)
                last = e
                continue
            last = e
            time.sleep(2 + attempt * 2)
        except (URLError, TimeoutError, ConnectionError, OSError) as e:
            last = e
            time.sleep(2 + attempt * 2)
    if os.path.exists(tmp):
        try:
            os.remove(tmp)
        except OSError:
            pass
    raise last if last else RuntimeError("unknown error")


def safe_parts(path):
    """把 URL 路径拆成 Windows 安全的分段。"""
    out = []
    for seg in path.split("/"):
        if seg in ("", ".", ".."):
            continue
        seg = re.sub(r'[<>:"|?*\\]', "_", seg)
        seg = seg.rstrip(" .")
        if len(seg) > 120:
            base, ext = os.path.splitext(seg)
            seg = base[:100] + "_" + hashlib.md5(seg.encode()).hexdigest()[:8] + ext
        out.append(seg)
    return out


def local_path(out_dir, original):
    p = urlparse(original)
    path = unquote(p.path) or "/"
    if path.endswith("/"):
        path += "index.html"
    parts = safe_parts(path)
    if p.query:
        h = hashlib.md5(p.query.encode()).hexdigest()[:8]
        base, ext = os.path.splitext(parts[-1])
        parts[-1] = f"{base}_{h}{ext}"
    return os.path.join(out_dir, p.netloc.replace(":", "_"), *parts)


def ext_of(url):
    path = urlparse(url).path
    base = os.path.basename(path)
    return base.rsplit(".", 1)[-1].lower() if "." in base else ""


# ---------------------------------------------------------------------------
# list：拉 CDX 索引
# ---------------------------------------------------------------------------
def build_cdx_url(q):
    params = [
        ("url", q["url"]),
        ("matchType", q.get("match_type", "domain")),
        ("output", "json"),
        ("fl", "timestamp,original,mimetype,statuscode,length"),
        ("collapse", "urlkey"),
        ("filter", "statuscode:200"),
        ("limit", str(q.get("limit", 100000))),
    ]
    for f in q.get("filters", []):
        params.append(("filter", f))
    return CDX_API + "?" + urlencode(params)


def cmd_list(args):
    opener = make_opener(args.proxy)
    rows = []
    for q in QUERIES:
        url = build_cdx_url(q)
        log(f"[CDX] {q['url']} ({q.get('match_type')}) ...")
        try:
            raw = http_read(url, opener)
        except Exception as e:
            log(f"  失败：{e!r}（检查代理是否生效）")
            continue
        if not raw:
            log("  无结果")
            continue
        try:
            data = json.loads(raw.decode("utf-8", "replace"))
        except json.JSONDecodeError:
            log("  返回不是 JSON，可能被代理/网关拦截")
            continue
        if len(data) <= 1:
            log("  快照为空")
            continue
        for item in data[1:]:
            ts, original, mime, code, length = (item + [""] * 5)[:5]
            rows.append({
                "timestamp": ts, "original": original,
                "mimetype": mime, "statuscode": code, "length": length,
            })
        log(f"  +{len(data) - 1} 条")

    # 去重：同一 URL 保留最新时间戳
    best = {}
    for r in rows:
        cur = best.get(r["original"])
        if cur is None or r["timestamp"] > cur["timestamp"]:
            best[r["original"]] = r
    rows = sorted(best.values(), key=lambda r: r["original"])

    os.makedirs(args.out, exist_ok=True)
    cdx_path = os.path.join(args.out, "cdx.tsv")
    with open(cdx_path, "w", encoding="utf-8") as f:
        f.write("timestamp\tstatuscode\tmimetype\tsize\toriginal\n")
        for r in rows:
            f.write(f"{r['timestamp']}\t{r['statuscode']}\t{r['mimetype']}\t{r['length']}\t{r['original']}\n")

    # 分类统计
    stat = {}
    assets, pages = [], []
    for r in rows:
        e = ext_of(r["original"])
        mime = r["mimetype"] or ""
        stat[mime] = stat.get(mime, 0) + 1
        if e in ASSET_EXT or "shockwave" in mime or "application/xml" in mime:
            assets.append(r)
        elif e in PAGE_EXT or "html" in mime or "javascript" in mime:
            pages.append(r)

    assets_path = os.path.join(args.out, "assets.tsv")
    with open(assets_path, "w", encoding="utf-8") as f:
        f.write("timestamp\tstatuscode\tmimetype\tsize\toriginal\n")
        for r in assets:
            f.write(f"{r['timestamp']}\t{r['statuscode']}\t{r['mimetype']}\t{r['length']}\t{r['original']}\n")

    log(f"\n共 {len(rows)} 个唯一 URL，其中资源 {len(assets)} 个，页面/脚本 {len(pages)} 个")
    log("按 MIME 统计（前 15）：")
    for mime, cnt in sorted(stat.items(), key=lambda kv: -kv[1])[:15]:
        log(f"  {cnt:>6}  {mime or '(空)'}")
    log(f"\n索引已写入：{cdx_path}\n资源清单：{assets_path}")
    swf = [r for r in assets if ext_of(r["original"]) == "swf"]
    if swf:
        log(f"\n发现 {len(swf)} 个 SWF，前 20 个：")
        for r in swf[:20]:
            log(f"  {r['timestamp']}  {r['original']}")
    else:
        log("\n没有直接命中 .swf。下一步：运行 download 抓页面，再运行 scan 从 JS 里挖地址。")


def read_tsv(path):
    rows = []
    if not os.path.exists(path):
        return rows
    with open(path, encoding="utf-8") as f:
        next(f, None)
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 5:
                continue
            rows.append({"timestamp": parts[0], "statuscode": parts[1],
                         "mimetype": parts[2], "length": parts[3], "original": parts[4]})
    return rows


# ---------------------------------------------------------------------------
# download：按索引批量下载
# ---------------------------------------------------------------------------
def raw_url(timestamp, original):
    return f"{RAW_HOST}/{timestamp}id_/{original}"


def collect_targets(args, include_pages=True):
    targets = []
    for name in ("assets.tsv", "extra_urls.tsv"):
        path = os.path.join(args.out, name)
        for r in read_tsv(path):
            targets.append(r)
    if include_pages:
        for r in read_tsv(os.path.join(args.out, "cdx.tsv")):
            e = ext_of(r["original"])
            if e in PAGE_EXT:
                targets.append(r)
    # 去重
    seen, out = set(), []
    for r in targets:
        if r["original"] in seen:
            continue
        seen.add(r["original"])
        out.append(r)
    return out


def cmd_download(args):
    targets = collect_targets(args, include_pages=True)
    if args.only:
        allow = {x.strip().lower().lstrip(".") for x in args.only.split(",") if x.strip()}
        targets = [t for t in targets if ext_of(t["original"]) in allow]
    if args.limit:
        targets = targets[:args.limit]
    if not targets:
        log("没有可下载条目，请先运行 list")
        return

    opener = make_opener(args.proxy)
    ok = skipped = failed = 0
    fail_log = os.path.join(args.out, "failed.txt")
    fail_fp = open(fail_log, "w", encoding="utf-8")
    done = 0
    total = len(targets)

    def work(row):
        dest = local_path(args.out, row["original"])
        if os.path.exists(dest) and os.path.getsize(dest) > 0:
            return ("skip", row, 0)
        url = raw_url(row["timestamp"], row["original"])
        if args.dry_run:
            return ("dry", row, 0)
        size = http_download(url, dest, opener)
        return ("ok" if size else "miss", row, size)

    log(f"开始下载 {total} 个文件（并发 {args.workers}）-> {args.out}")
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(work, t): t for t in targets}
        for fut in as_completed(futures):
            row = futures[fut]
            done += 1
            try:
                state, r, size = fut.result()
            except Exception as e:
                failed += 1
                fail_fp.write(f"{row['original']}\t{e!r}\n")
                fail_fp.flush()
                log(f"  [{done}/{total}] FAIL {row['original']}  {e!r}")
                continue
            if state == "ok":
                ok += 1
                log(f"  [{done}/{total}] OK   {size:>9}  {r['original']}")
            elif state == "skip":
                skipped += 1
            elif state == "miss":
                fail_fp.write(f"{row['original']}\t404\n")
                fail_fp.flush()
                log(f"  [{done}/{total}] 404  {r['original']}")
            else:
                log(f"  [{done}/{total}] DRY  {raw_url(r['timestamp'], r['original'])}")
            time.sleep(random.uniform(0.1, 0.3))

    fail_fp.close()
    log(f"\n完成：成功 {ok}，跳过 {skipped}，失败 {failed}。失败清单：{fail_log}")


# ---------------------------------------------------------------------------
# scan：从已下载页面中挖资源地址
# ---------------------------------------------------------------------------
URL_RE = re.compile(r"""(?:"|'|\()((?:https?:)?//[^"'()\s]+?\.(?:swf|xml|json|zip|dat|bin|amf|png|jpg|jpeg|mp3|ogg|wav))(?:["')\s])""", re.I)
REL_RE = re.compile(r"""(?:"|')((?:\.{0,2}/)?[\w\-./]+?\.(?:swf|xml|json|zip|dat|bin|amf))(?:["'])""", re.I)


def cmd_scan(args):
    extra = {}
    scanned = 0
    for root, _dirs, files in os.walk(args.out):
        for name in files:
            if ext_of(name) not in PAGE_EXT:
                continue
            fp = os.path.join(root, name)
            try:
                text = open(fp, encoding="utf-8", errors="ignore").read()
            except OSError:
                continue
            scanned += 1
            # 用该文件自身的原始域名解析相对路径（out/<host>/…）
            relparts = os.path.relpath(fp, args.out).split(os.sep)
            origin = f"https://{relparts[0]}/" if relparts else "https://hero999.aiwan4399.com/"
            for m in URL_RE.finditer(text):
                u = m.group(1)
                if u.startswith("//"):
                    u = "https:" + u
                extra.setdefault(u, "")
            for m in REL_RE.finditer(text):
                u = m.group(1)
                if u.startswith("http"):
                    extra.setdefault(u, "")
                else:
                    base = origin + u.lstrip("./")
                    extra.setdefault(base, "")

    path = os.path.join(args.out, "extra_urls.tsv")
    with open(path, "w", encoding="utf-8") as f:
        f.write("timestamp\tstatuscode\tmimetype\tsize\toriginal\n")
        for u in sorted(extra):
            f.write(f"0\t200\t\t0\t{u}\n")
    log(f"扫描 {scanned} 个页面/脚本，挖到 {len(extra)} 个候选地址 -> {path}")
    log("注意：这些地址的时间戳为 0，download 时会走 Wayback 的自动重定向（可能较慢）。")


# ---------------------------------------------------------------------------
# probe：直连原站 / 4399 CDN 探测
# ---------------------------------------------------------------------------
def cmd_probe(args):
    opener = make_opener(args.proxy)
    rows = read_tsv(os.path.join(args.out, "assets.tsv"))
    if not rows:
        rows = read_tsv(os.path.join(args.out, "cdx.tsv"))
    if args.limit:
        rows = rows[:args.limit]
    hosts = {"sda.4399.com"}
    if args.direct_hosts:
        hosts |= {h.strip() for h in args.direct_hosts.split(",") if h.strip()}

    hits, tries = [], 0
    for r in rows:
        u = r["original"]
        if urlparse(u).netloc not in hosts:
            continue
        tries += 1
        try:
            req = Request(u, headers={"User-Agent": UA}, method="HEAD")
            with opener.open(req, timeout=20) as resp:
                size = resp.headers.get("Content-Length", "?")
                hits.append(f"{size}\t{u}")
                log(f"  ALIVE {size:>9}  {u}")
        except Exception:
            pass
    out = os.path.join(args.out, "alive.txt")
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(hits))
    log(f"\n探测 {tries} 个原始地址，仍可直连 {len(hits)} 个 -> {out}")


def cmd_all(args):
    cmd_list(args)
    cmd_download(args)
    cmd_scan(args)
    cmd_download(args)
    cmd_probe(args)


def main():
    ap = argparse.ArgumentParser(description="英雄之境 Wayback 资源抓取")
    ap.add_argument("cmd", choices=["list", "download", "scan", "probe", "all"])
    ap.add_argument("--out", default="wayback_yxzj")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--proxy", default=None)
    ap.add_argument("--only", default=None, help="如 swf,xml,png")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--direct-hosts", default=None)
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    {"list": cmd_list, "download": cmd_download, "scan": cmd_scan,
     "probe": cmd_probe, "all": cmd_all}[args.cmd](args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
