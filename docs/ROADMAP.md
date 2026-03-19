# Roadmap / MVP Checklist (token-usage-dashboard)

> 目標：打造一個功能全面、可擴展的 LLM token 使用監控與管理平台。

## MVP-1：核心功能與數據可視化

- [x] 基礎數據收集與解析：支持主流 LLM API (如 OpenAI, Anthropic, Gemini) 的 token 使用數據。
- [x] 儀表板核心展示：日/週/月總體成本、總 token 消耗、活躍模型數量。
- [x] 模型使用明細：按模型、按日期展示 token 消耗與成本。
- [x] 成本趨勢圖：可視化歷史成本與 token 使用量變化。
- [x] 儀表板性能優化：優化數據載入和渲染性能，支援更大數據量。
- [x] 儀表板自定義報表生成：允許用戶選擇指標、模型和時間粒度。

## MVP-2：企業級功能與智能分析

*   [x] 多租戶與組織管理
    *   實現真正的多租戶架構，確保不同組織或部門的數據嚴格隔離，互不干擾。
    *   提供組織層級的用戶管理，包括創建、編輯、刪除用戶，並分配預設或自定義角色（如：組織管理員、部門主管、數據分析師、普通用戶），實現精細化的權限控制。
    *   允許組織管理員或指定用戶創建和管理多個儀表板視圖，並根據組織結構或特定需求分配給不同的用戶組。

*   [x] 成本預測與異常消耗預警
    *   基於歷史 token 使用數據，應用時間序列分析或機器學習模型，預測未來（如：未來 7 天、30 天）的 token 消耗與成本趨勢，幫助用戶提前規劃預算。
    *   實時監測 token 消耗模式，自動識別異常飆升、不尋常的使用模式或意外的成本增長。
    *   提供靈活的預警規則配置，允許用戶設定預算閾值、特定模型使用量激增、或在成本達到某一百分比時觸發告警。
    *   目前已實作「rule evaluation」與通知通道標記（summary/dashboard 可見）。
    *   主動 dispatch（郵件、Slack、Discord Webhook 等）尚未實作，將在後續階段補上。

*   [x] LLM 使用模式深入分析
    *   提供基於 prompt 長度、completion 長度、模型類型、應用場景、用戶或項目等維度的詳細分析報告。
    *   自動識別和標記出高消耗的特定 API 調用、用戶會話或業務流程，幫助用戶定位成本優化點。
    *   提供不同 LLM 模型在處理特定任務時的效率（如：生成速度、響應時間）與成本效益對比分析，輔助模型選擇。
    *   透過對 prompt 內容的匿名化分析，識別出高頻使用的關鍵詞或主題，從業務層面提供成本優化線索。

*   [x] 成本歸因與優化建議
    *   支援將 token 成本精確歸因到具體項目、部門、用戶、特定應用或業務線，實現精細化成本管理。
    *   自動提供可行的成本優化建議（如：建議替換為更經濟的模型、優化 prompt 結構、實施批量調用）。
    *   預留接口，支援與企業現有的 Cloud Cost Management 工具（如 AWS Cost Explorer、Google Cloud Billing）集成，提供統一的成本視圖。
    *   目前已實作：call-level 成本歸因（project/department/user/application/business line）、規則式優化建議、Cloud cost integration hooks placeholder。

*   [x] 報表自動化生成與排程
    *   允許用戶根據業務需求，自定義報表內容（選擇圖表、數據表格、關鍵指標）、佈局和格式。
    *   支援設定報表定期（每日、每週、每月、季度）自動生成，並通過郵件、應用內通知或集成到企業協作平台分發給指定收件人列表。
    *   提供一個集中的報表下載中心，用戶可以方便地查看和下載過去生成的報表，並支持報表歷史版本管理。
    *   報表的分發應嚴格遵守用戶或角色的數據訪問權限，確保敏感數據不會發送給未經授權的用戶。
    *   目前已實作：scheduler job config（daily/weekly/monthly/quarterly）、JSON/CSV 自動產出、report history/download center (`report_history.json` + artifacts)、recipient role guardrail（未授權收件者會被 block 並記錄）。

### Iteration-1 (MVP-3: 成本控制與智能優化)
- [x] 實時成本控制策略：設置多層級預算限制，當成本達到閾值時，自動觸發降級、切換模型或停止調用。
  - 已實作：`realTimeCostControls` 引擎（多層 policy layers，支援 global forecast / actual total / anomaly count / dimension cost），可輸出動作建議 `degrade` / `switch_model` / `stop_calls`，並在 dashboard 顯示 layer 評估結果與觸發動作。
- [x] Prompt 優化建議引擎：自動分析高消耗 prompt，提供優化建議（如壓縮、替換模型），並支持 A/B 測試。
  - 已實作：高消耗 prompt family 排序、可配置壓縮/上下文重構建議、A/B 測試方案（流量切分與成功門檻可由 config 調整）。
- [x] 多雲/多模型成本聚合：統一管理來自不同雲服務提供商和 LLM 模型的成本數據，提供統一視圖。
  - 已實作（首個 production-ready increment）：normalized multi-provider aggregation model（provider/model/day 統一聚合）+ dashboard 統一視圖（providers totals + cross-provider top models）+ summary JSON 輸出 `multiProviderAggregation`。
- [x] 預算分配與使用權限管理：更精細地控制各部門/用戶的 LLM 資源預算分配，並設定使用權限。
  - 已實作：dimension 預算配置（project/department/user/application/businessLine/model）、角色/使用者權限矩陣、call log 權限違規偵測（模型白名單與單次成本上限），並於 Dashboard 顯示 allocation/violation 視圖。
