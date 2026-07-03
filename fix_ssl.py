"""
fix_ssl.py — 修復公司網路 TLS 檢查造成的 yfinance SSL 失敗（Windows）

症狀
────────────────────────────────────────────────────────────────────────
  Failed to get ticker 'XXX' reason: curl: (60) SSL certificate problem:
  unable to get local issuer certificate

成因：公司網路的 TLS 檢查代理（Zscaler 等）用企業自簽 CA 重簽所有 HTTPS，
Python 的 certifi 憑證庫不認得企業根憑證 → yfinance（curl_cffi）驗證失敗。
症狀會隨網路路徑時好時壞（辦公室/VPN 失敗、家用網路正常）。

解法：把 Windows 憑證庫（已含企業 CA）與 certifi 標準根合併成一份 bundle，
並以環境變數指向它。合併檔是「超集」——公司路徑與乾淨路徑皆可通。

用法
────────────────────────────────────────────────────────────────────────
  python fix_ssl.py            # 產生/更新 bundle + 本次驗證（不改系統設定）
  python fix_ssl.py --persist  # 另用 setx 寫入使用者環境變數（新開的終端機生效）

還原：setx CURL_CA_BUNDLE "" 等三個變數設空即可（或於系統環境變數 UI 刪除）。
certifi 套件升級後建議重跑一次本工具（bundle 內含 certifi 內容的快照）。
"""
import argparse
import os
import ssl
import subprocess
import sys

ENV_VARS = ("CURL_CA_BUNDLE", "REQUESTS_CA_BUNDLE", "SSL_CERT_FILE")
BUNDLE_DIR = os.path.join(os.path.expanduser("~"), ".us-stock")
BUNDLE_PATH = os.path.join(BUNDLE_DIR, "ca-bundle.pem")
MARKER = "# == merged: certifi + Windows cert store (fix_ssl.py) =="


def build_bundle() -> str:
    import certifi
    if sys.platform != "win32":
        raise SystemExit("本工具僅適用 Windows（其他平台請將企業 CA 加入系統信任後匯出 PEM）")
    pems = []
    for store in ("ROOT", "CA"):
        for der, enc, _trust in ssl.enum_certificates(store):
            if enc == "x509_asn":
                pems.append(ssl.DER_cert_to_PEM_cert(der))
    os.makedirs(BUNDLE_DIR, exist_ok=True)
    with open(BUNDLE_PATH, "w", encoding="utf-8") as f:
        f.write(MARKER + "\n")
        f.write(open(certifi.where(), encoding="utf-8").read())
        f.write("\n")
        f.write("\n".join(pems))
    print(f"✅ 已產生合併 bundle：{BUNDLE_PATH}")
    print(f"   （certifi 標準根 + Windows 憑證庫 {len(pems)} 張，含企業 CA）")
    return BUNDLE_PATH


def verify(bundle: str) -> bool:
    env = dict(os.environ)
    for k in ENV_VARS:
        env[k] = bundle
    code = ("from curl_cffi import requests as c;"
            "r=c.get('https://query1.finance.yahoo.com/v8/finance/chart/AAPL',"
            "impersonate='chrome',timeout=15);print('HTTP',r.status_code)")
    p = subprocess.run([sys.executable, "-c", code], capture_output=True,
                       text=True, env=env, timeout=60)
    ok = "HTTP 200" in p.stdout
    print(("✅ 驗證通過：curl_cffi 以合併 bundle 連 Yahoo Finance 成功"
           if ok else f"❌ 驗證失敗：{(p.stdout + p.stderr).strip()[:200]}"))
    return ok


def persist(bundle: str) -> None:
    for k in ENV_VARS:
        subprocess.run(["setx", k, bundle], capture_output=True, text=True)
    print(f"✅ 已 setx 使用者環境變數 {ENV_VARS}（新開的終端機/程式生效；"
          f"目前這個視窗需重開或手動 $env: 設定）")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1].strip())
    ap.add_argument("--persist", action="store_true",
                    help="用 setx 寫入使用者環境變數（永久生效）")
    args = ap.parse_args()

    bundle = build_bundle()
    ok = verify(bundle)
    if args.persist:
        persist(bundle)
    elif ok:
        print("\n下一步：確認無誤後執行 `python fix_ssl.py --persist` 永久生效，")
        print("或單次使用：PowerShell 先設 $env:CURL_CA_BUNDLE、$env:REQUESTS_CA_BUNDLE、"
              "$env:SSL_CERT_FILE 為上述路徑再跑 backtest/optimize。")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()
