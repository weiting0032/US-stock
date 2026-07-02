# US-stock · 量化投資組合 Pro

美股趨勢動能交易系統：多因子評分 + ATR 風控 + 分批出場，半導體宇宙為主、可跨產業發現。
含 Streamlit 儀表板、GitHub Actions 每日排程掃描（Telegram 推播）、事件驅動回測與參數最佳化。

## 模組

| 檔案 | 角色 | 怎麼跑 |
|---|---|---|
| `app.py` | Streamlit 儀表板（持倉／掃描／策略／交易／績效／半導體／訊號驗證／**回測**） | `streamlit run app.py` |
| `core.py` | 全部邏輯：行情、指標、Google Sheets、策略評估、掃描器、訊號成效追蹤 | — |
| `scanner.py` | GitHub Actions 排程入口（盤前／盤後／半導體／廣度） | `python scanner.py [--semi\|--broad]` |
| `backtest.py` | 事件驅動回測，重放 `evaluate_strategy`，對標 SOXX/SPY | `python backtest.py --universe semi` |
| `optimize.py` | 參數掃描 + walk-forward（樣本外驗證，防過擬合） | `python optimize.py --study stops` |
| `tests/` | 離線測試套件（合成資料，不連網；CI 於 push 自動跑） | `pytest tests/ -q` |

## 研究 → 上線工作流程

```
optimize.py / App 回測分頁   ──▶  找到候選參數（看「訓練段」冠軍）
        │
        ▼
   檢查「測試段（樣本外）」是否仍穩健   ──✗──▶  疑似過擬合，捨棄
        │ ✅ 樣本外仍佳
        ▼
   App「⚡ 套用冠軍參數」試穿（僅本工作階段，可一鍵還原，重啟即失效）
        │ 確認訊號/持倉評估符合預期
        ▼
   複製 App 產生的 Secrets 片段 → 人工設到對應執行環境（見下）→ 永久生效
```

> ⚠️ **不會自動套用到正式策略**：回測/最佳化只「唯讀重放」歷史。App 的「套用」
> 僅作用於當前工作階段（試穿用）；排程掃描永遠只讀你人工設定的 Secrets。
> 這是刻意的安全設計——2 年歷史極易過擬合，正式採用必須經過樣本外驗證＋人工核可。

## 策略參數套用指南（重點）

所有策略參數皆由 `core.get_env_*` 讀取，解析順序為 **環境變數 → Streamlit secrets → 程式碼預設值**。
因此依「哪個執行環境」設定即可，三處各自獨立：

| 執行環境 | 設定位置 | 生效時機 |
|---|---|---|
| **GitHub Actions 每日排程**（`scanner.py`） | repo **Settings → Secrets and variables → Actions** 新增 Secret | 下次排程觸發。三種掃描模式（portfolio/semi/broad）皆自動繼承 `scanner.yml` 的 job 層級 env |
| **Streamlit App**（`app.py`／回測分頁） | App 的 **Secrets**（`.streamlit/secrets.toml` 或 Streamlit Cloud Secrets） | App 重啟 |
| **本機命令列**（`backtest.py`／`scanner.py`） | shell 環境變數，如 PowerShell `\$env:EXIT_INIT_STOP_ATR="2.5"` | 該次執行 |

> 未設定的參數一律回落到程式碼預設值；**完全不設 = 目前行為不變**。

### 可調參數速查（節錄，預設值見 `core.py`）

| 群組 | 參數 | 預設 | 說明 |
|---|---|---|---|
| 出場 | `EXIT_INIT_STOP_ATR` | 2.0 | 初始硬止損＝進場 − N×ATR |
| 出場 | `EXIT_TRAIL_ATR` | 3.0 | Chandelier 移動停損 High20 − N×ATR |
| 出場 | `EXIT_TP1_R` / `EXIT_TP1_PCT` | 2.0 / 0.20 | 第一獲利目標（+NR 與 +N% 取較早） |
| 出場 | `EXIT_SCALE_OUT_PCT` | 0.34 | 觸及 TP1 分批賣出比例 |
| 出場 | `EXIT_BREAKEVEN_AT_R` | 1.0 | 浮盈達 +NR 後止損上移保本 |
| 出場 | `EXIT_MIN_HOLD_BARS` | 1 | 新倉保護期（日線根數） |
| 進場 | `SCORE_BUY_NOW_THRESHOLD` | 3.5 | 新倉評分門檻 |
| 進場 | `ENTRY_MAX_EXT_ATR` | 4.0 | 追高保護：收盤 ≤ SMA20 + N×ATR |
| 加碼 | `ADD_MIN_PROFIT_R` | 0.5 | 加碼前需站上 進場 + N×R |
| 時間止損 | `TIME_STOP_BARS` / `TIME_STOP_MIN_R` | 20 / 1.0 | N 根內未達 +NR 且弱於大盤 → 釋出 |
| 半導體 | `US_SEMI_SCORE_STRONG` / `_BUY` | 5.5 / 3.5 | 強力／積極買進門檻 |
| 資料層 | `YF_MAX_RETRIES` / `YF_RETRY_BASE_SLEEP` | 3 / 1.0 | yfinance 退避重試；耗盡仍失敗會登記並由掃描器回報（不再靜默漏掉） |

`optimize.py` 的四個研究（`stops` / `exits` / `grace` / `entry`）即對應上表主要欄位。

## 環境變數 / Secrets（憑證類）

| 名稱 | 用途 |
|---|---|
| `GCP_SERVICE_ACCOUNT` | Google Sheets 服務帳號 JSON（App 亦可用 st.secrets 的 `gcp_service_account`） |
| `PORTFOLIO_SHEET_TITLE` | Google 試算表名稱（預設 `US Stock`） |
| `TG_TOKEN` / `TG_CHAT_ID` | Telegram 推播 |
| `INITIAL_CAPITAL` | 初始本金（預設 32000） |

## 免責

本系統僅供研究與輔助參考，不構成投資建議。回測結果基於有限歷史（yfinance 約 2 年）且含近似假設，不代表未來績效。
