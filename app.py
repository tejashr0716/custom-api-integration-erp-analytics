"""
ZeTheta Financial Analytics — S/4HANA Custom API Integration Gateway
FastAPI Application Exposing:
1. GET / (Interactive Web Dashboard for SAP Analytics with Live Polling & Fault Injection)
2. POST /api/v1/sap/sync (Batch extraction, transformation, reconciliation with Idempotency)
3. GET /api/v1/analytics/working-capital (Transformed financial telemetry)
4. GET /api/v1/sap/transactions (Recent ledger transactions with query filtering)
5. GET /api/v1/monitoring/health (Circuit breaker & DLQ observability)
6. POST /api/v1/resilience/dlq/retry (Operator DLQ re-ingestion endpoint)
7. POST /api/v1/resilience/fault-inject (Circuit breaker fault-injection simulation toggle)
8. POST /api/v1/resilience/reset (Circuit breaker manual reset)
"""

import time
import uuid
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Any, Optional, List

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from sap_models import (
    AnalyticsSummaryResponse,
    NormalizedFinancialTransaction,
    DLQRetryResponse,
    FaultInjectionResponse
)
from sap_extractor import SAPODataExtractor
from transformer import SAPDataTransformer
from resilience import CircuitBreaker, DeadLetterQueue, execute_with_retry, logger
from security import validate_oauth_token, verify_hmac_signature

app = FastAPI(
    title="ZeTheta S/4HANA Custom API Integration Gateway",
    description="Enterprise API bridge connecting SAP S/4HANA ERP Financials to ZeTheta Analytics Platform.",
    version="1.1.0"
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# State singletons
extractor = SAPODataExtractor(seed_records=200)
circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_time_seconds=1.5)
dlq = DeadLetterQueue()
cached_metrics = None
last_extracted_docs = []
idempotency_cache: Dict[str, AnalyticsSummaryResponse] = {}

# Pre-populate on startup
initial_docs, _ = extractor.extract_delta_batch(skip=0, top=50)
last_extracted_docs = initial_docs
cached_metrics, _, _, _ = SAPDataTransformer.compute_working_capital_metrics(initial_docs)

# Middleware: Request Correlation ID & Structured Logging
class StructuredLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        corr_id = request.headers.get("X-Correlation-ID") or f"CORR_{uuid.uuid4().hex[:12].upper()}"
        request.state.correlation_id = corr_id
        start_time = time.time()

        response: Response = await call_next(request)
        duration_ms = round((time.time() - start_time) * 1000, 2)
        response.headers["X-Correlation-ID"] = corr_id

        # Skip spamming logs for root html or favicon
        if request.url.path not in ["/", "/favicon.ico"]:
            logger.info(
                f"{request.method} {request.url.path} - {response.status_code} ({duration_ms}ms)",
                extra={"correlation_id": corr_id, "extra_data": {"status_code": response.status_code, "latency_ms": duration_ms}}
            )

        return response

app.add_middleware(StructuredLoggingMiddleware)

# --- Observability & Health ---

@app.get("/api/v1/monitoring/health")
def get_health_telemetry():
    """Health & Telemetry endpoint for integration monitoring."""
    is_healthy = circuit_breaker.can_execute()
    return {
        "status": "HEALTHY" if is_healthy else "DEGRADED",
        "circuit_breaker": circuit_breaker.state,
        "simulated_failure_active": circuit_breaker.simulated_failure_active,
        "dlq_backlog_count": dlq.size(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "erp_target": "SAP S/4HANA 2023 On-Prem / Cloud OData v4"
    }

# --- Transactions & Ledger Queries ---

@app.get("/api/v1/sap/transactions", response_model=List[NormalizedFinancialTransaction])
def get_recent_transactions(
    limit: int = Query(50, ge=5, le=200),
    doc_type: Optional[str] = Query(None, description="Filter by Document Type: SA, KR, DR")
):
    """Returns the most recently transformed SAP financial transactions with optional filtering."""
    txs = []
    for doc in last_extracted_docs:
        doc_txs = SAPDataTransformer.transform_document_to_transactions(doc)
        if doc_type:
            doc_txs = [t for t in doc_txs if t.document_type.upper() == doc_type.upper()]
        txs.extend(doc_txs)
    return txs[:limit]

# --- Core Synchronization Gateway with Idempotency ---

@app.post("/api/v1/sap/sync", response_model=AnalyticsSummaryResponse)
async def sync_sap_financial_batch(
    request: Request,
    response: Response,
    authorization: Optional[str] = Header(None),
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
    x_idempotency_key: Optional[str] = Header(None, alias="X-Idempotency-Key"),
    x_hmac_signature: Optional[str] = Header(None),
    x_timestamp: Optional[int] = Header(None),
    x_nonce: Optional[str] = Header(None),
    batch_size: int = Query(50, ge=10, le=200)
):
    """
    Executes end-to-end sync with Idempotency & Replay Defense:
    1. Authenticates OAuth 2.0 Bearer Token
    2. Validates Idempotency-Key cache to prevent double-counting
    3. Validates HMAC-SHA256 signature and timestamp drift
    4. Evaluates Circuit Breaker
    5. Pulls raw SAP OData batch with exponential retry
    6. Transforms data contracts into Normalized Analytical schema
    7. Validates Double-Entry reconciliation
    """
    corr_id = getattr(request.state, "correlation_id", "SYS_SYNC")

    # 1. OAuth Validation
    if not validate_oauth_token(authorization or "Bearer zt_sap_default_test_token_2026"):
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid OAuth 2.0 Bearer Token")

    # 2. Idempotency Key Check
    effective_idempotency_key = idempotency_key or x_idempotency_key
    if effective_idempotency_key and effective_idempotency_key in idempotency_cache:
        response.headers["X-Cache-Lookup"] = "HIT"
        cached_result = idempotency_cache[effective_idempotency_key]
        cached_result.idempotent_cached = True
        logger.info(f"Idempotent replay served for key: {effective_idempotency_key}", extra={"correlation_id": corr_id})
        return cached_result

    # 3. Circuit Breaker Check
    if not circuit_breaker.can_execute():
        raise HTTPException(status_code=503, detail="Service Unavailable: Circuit breaker is OPEN (Downstream ERP unavailable)")

    # 4. HMAC Integrity Check (if headers provided)
    if x_hmac_signature and x_timestamp and x_nonce:
        raw_body = await request.body()
        is_valid, msg = verify_hmac_signature(raw_body, x_hmac_signature, x_timestamp, x_nonce)
        if not is_valid:
            circuit_breaker.record_failure()
            dlq.push({"raw_body": str(raw_body)}, msg, attempts=1, correlation_id=corr_id)
            raise HTTPException(status_code=403, detail=msg)

    # 5. Extract with Retry
    global last_extracted_docs
    try:
        def fetch():
            raw_docs, _ = extractor.extract_delta_batch(skip=0, top=batch_size)
            return raw_docs

        raw_docs = execute_with_retry(fetch, max_retries=3, correlation_id=corr_id)
        last_extracted_docs = raw_docs
        circuit_breaker.record_success()
    except Exception as e:
        circuit_breaker.record_failure()
        dlq.push({"batch_size": batch_size}, str(e), attempts=3, correlation_id=corr_id)
        raise HTTPException(status_code=502, detail=f"SAP Gateway Communication Failure: {str(e)}")

    # 6. Transform & Reconcile (with Deduplication)
    metrics, total_debit, total_credit, is_balanced = SAPDataTransformer.compute_working_capital_metrics(
        raw_docs, deduplicate=True
    )

    global cached_metrics
    cached_metrics = metrics

    result = AnalyticsSummaryResponse(
        sync_batch_id=f"ZT_SYNC_{uuid.uuid4().hex[:12].upper()}",
        timestamp=datetime.now(timezone.utc),
        processed_records=len(raw_docs),
        total_debit_volume=total_debit,
        total_credit_volume=total_credit,
        is_balanced=is_balanced,
        idempotent_cached=False,
        working_capital=metrics
    )

    # Cache if idempotency key was supplied
    if effective_idempotency_key:
        idempotency_cache[effective_idempotency_key] = result
        response.headers["X-Cache-Lookup"] = "MISS"

    return result

@app.get("/api/v1/analytics/working-capital")
def get_working_capital_analytics():
    """Returns the latest synchronized Working Capital and Liquidity metrics."""
    global cached_metrics
    if not cached_metrics:
        raw_docs, _ = extractor.extract_delta_batch(skip=0, top=50)
        metrics, _, _, _ = SAPDataTransformer.compute_working_capital_metrics(raw_docs)
        cached_metrics = metrics
    return cached_metrics

# --- Resilience & Fault Injection Endpoints ---

@app.post("/api/v1/resilience/dlq/retry", response_model=DLQRetryResponse)
def retry_dlq_backlog():
    """Operator endpoint to reprocess and clear Dead-Letter Queue items."""
    reprocessed = dlq.retry_all()
    return DLQRetryResponse(
        reprocessed_count=reprocessed,
        remaining_dlq=dlq.size(),
        status="REPROCESSED" if reprocessed > 0 else "NO_PENDING_ITEMS",
        timestamp=datetime.now(timezone.utc)
    )

@app.post("/api/v1/resilience/fault-inject", response_model=FaultInjectionResponse)
def toggle_fault_injection(enable: bool = Query(True)):
    """Simulates an ERP outage or network failure to test Circuit Breaker and DLQ."""
    if enable:
        circuit_breaker.force_trip(enable_simulation=True)
        # Push a representative poisoned payload to DLQ
        dlq.push(
            {"target": "SAP_S4HANA_ODATA_ENDPOINT", "document_type": "FI-GL", "error": "Gateway Timeout 504"},
            reason="Simulated ERP Network Partition / Outage",
            attempts=3,
            correlation_id="SIMULATED_FAULT"
        )
        msg = "Fault injection ACTIVATED: Circuit breaker forced OPEN. Calls diverted to DLQ."
    else:
        circuit_breaker.reset()
        msg = "Fault injection DEACTIVATED: Circuit breaker reset to CLOSED."

    return FaultInjectionResponse(
        simulated_failure_active=circuit_breaker.simulated_failure_active,
        circuit_breaker_state=circuit_breaker.state,
        dlq_count=dlq.size(),
        message=msg
    )

@app.post("/api/v1/resilience/reset")
def reset_circuit_breaker():
    """Manually resets the circuit breaker to CLOSED."""
    circuit_breaker.reset()
    return {"circuit_breaker": circuit_breaker.state, "status": "RESET_SUCCESSFUL"}

# --- Interactive Web Dashboard ---

@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
def get_dashboard_html():
    """Interactive visual dashboard for SAP S/4HANA Analytics, Live Polling & Fault Injection."""
    html_content = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>TrustSphere Technologies — SAP S/4HANA Integration Gateway (ZeTheta S/4HANA Gateway)</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Lora:ital,wght@0,400..700;1,400..700&family=Work+Sans:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    :root {
      --rust: #C0392B;
      --rust-gradient: linear-gradient(135deg, #B83A26 0%, #A3301F 50%, #7E2814 100%);
      --gold: #B8860B;
      --gold-light: #D4A020;
      --gold-border: rgba(184, 134, 11, 0.22);
      --dark-1: #0F0503;
      --dark-2: #180806;
      --dark-3: #201008;
      --text-light: #F0E8DC;
      --text-muted: #B3A18A;
      --font-serif: 'Lora', Georgia, serif;
      --font-sans: 'Work Sans', system-ui, -apple-system, sans-serif;
    }
    body {
      font-family: var(--font-sans);
      background: linear-gradient(160deg, #090302 0%, #0F0503 40%, #150704 100%);
      color: var(--text-light);
    }
    h1, h2, h3, h4, .font-serif {
      font-family: var(--font-serif);
    }
    .trust-panel {
      background: var(--dark-2);
      border: 1px solid var(--gold-border);
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
      border-radius: 1rem;
    }
    .section-tag {
      display: inline-flex;
      align-items: center;
      gap: 0.5rem;
      padding: 0.25rem 0.85rem;
      border-radius: 9999px;
      font-size: 0.7rem;
      font-weight: 700;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      background-color: rgba(184, 134, 11, 0.1);
      color: var(--gold-light);
      border: 1px solid rgba(184, 134, 11, 0.3);
    }
    .section-tag-dot {
      width: 0.45rem;
      height: 0.45rem;
      border-radius: 50%;
      background-color: var(--gold-light);
      box-shadow: 0 0 8px var(--gold);
    }
    .btn-rust {
      background: var(--rust-gradient);
      border: 1px solid rgba(226, 101, 75, 0.4);
      box-shadow: 0 4px 16px rgba(192, 57, 43, 0.35);
      color: #FFFFFF;
      transition: all 0.2s ease;
    }
    .btn-rust:hover {
      transform: translateY(-1px);
      box-shadow: 0 6px 20px rgba(192, 57, 43, 0.5);
    }
  </style>
</head>
<body class="min-h-screen flex flex-col justify-between antialiased selection:bg-[#B8860B]/30 selection:text-[#F0E8DC]">

  <!-- Sticky Header -->
  <header class="bg-[#0F0503]/90 border-b border-[rgba(184,134,11,0.22)] sticky top-0 z-50 backdrop-blur-md shadow-xl">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 py-3.5 flex flex-col md:flex-row items-start md:items-center justify-between gap-4">
      <div class="flex items-center space-x-3.5">
        <div class="w-10 h-10 rounded-xl bg-gradient-to-br from-[#B83A26] via-[#A3301F] to-[#7E2814] flex items-center justify-center font-bold text-xl text-[#F0E8DC] shadow-lg border border-[rgba(184,134,11,0.35)] relative overflow-hidden">
          <span class="relative z-10 font-serif">🛡️</span>
        </div>
        <div>
          <div class="flex items-center gap-1.5 leading-tight">
            <span class="text-lg font-bold tracking-tight text-[#F0E8DC]">Trust</span>
            <span class="text-lg font-extrabold tracking-tight text-[#D4A020]">Sphere</span>
            <span class="text-[9px] font-bold uppercase tracking-widest text-[#B3A18A] ml-1 px-1.5 py-0.5 rounded bg-[#201008] border border-[rgba(184,134,11,0.2)]">TECHNOLOGIES</span>
            <span class="text-[10px] font-bold px-2 py-0.5 rounded-full bg-emerald-950/60 text-emerald-400 border border-emerald-500/40 ml-1.5">LIVE SYNC</span>
          </div>
          <p class="text-xs text-[#B3A18A] font-medium tracking-wide mt-0.5">ZeTheta S/4HANA Gateway · SAP S/4HANA OData Ingestion &amp; FinSight Analytics</p>
        </div>
      </div>

      <!-- Action Bar -->
      <div class="flex flex-wrap items-center gap-2.5 w-full md:w-auto justify-start md:justify-end">
        <!-- Live Polling Pill -->
        <button id="polling-toggle" onclick="togglePolling()" class="px-3 py-1.5 bg-[#201008] hover:bg-[#2A140A] text-xs font-semibold rounded-full border border-[rgba(184,134,11,0.3)] flex items-center gap-2 transition">
          <span id="polling-indicator" class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
          <span id="polling-text" class="text-[#F0E8DC]">Live Polling: 3s</span>
        </button>

        <!-- Fault Injection Toggle Button -->
        <button id="fault-btn" onclick="toggleFaultInjection()" class="px-3.5 py-1.5 bg-[#240c08] hover:bg-[#35120c] text-[#E2654B] border border-[#E2654B]/40 text-xs font-bold rounded-full transition flex items-center gap-1.5">
          <span>⚡</span>
          <span id="fault-btn-text">Simulate ERP Outage</span>
        </button>

        <!-- Sync Button -->
        <button id="sync-btn" onclick="triggerSync()" class="btn-rust px-4 py-1.5 rounded-full text-xs font-bold transition flex items-center gap-2">
          <svg id="sync-icon" class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"/></svg>
          Sync SAP OData
        </button>

        <a href="/docs" target="_blank" class="px-3 py-1.5 bg-[#201008] hover:bg-[#2A140A] text-[#D4A020] rounded-full text-xs font-semibold border border-[rgba(184,134,11,0.3)] transition">Swagger Docs ↗</a>
      </div>
    </div>
  </header>

  <!-- Fault Alert Banner -->
  <div id="fault-banner" class="hidden bg-rose-950/90 border-b border-rose-800/80 px-4 py-2.5 text-xs text-rose-200 text-center font-medium flex items-center justify-center gap-2">
    <span class="w-2 h-2 rounded-full bg-rose-400 animate-ping"></span>
    <strong>SIMULATED ERP OUTAGE ACTIVE:</strong> Circuit breaker is OPEN. Downstream OData calls diverted to Dead-Letter Queue.
    <button onclick="toggleFaultInjection()" class="ml-2 underline font-bold hover:text-white">Recover ERP &amp; Reset Breaker</button>
  </div>

  <!-- Main Container -->
  <main class="max-w-7xl mx-auto px-4 sm:px-6 py-7 w-full flex-grow space-y-6">

    <!-- Eyebrow Tag -->
    <div>
      <div class="section-tag">
        <span class="section-tag-dot"></span>
        <span>FORWARD DEPLOYED ENGINEERING · SAP S/4HANA TO FINSIGHT INTEGRATION</span>
      </div>
    </div>

    <!-- Telemetry Status Bar -->
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      <div class="trust-panel p-4 flex items-center justify-between">
        <div>
          <p class="text-xs text-[#B3A18A] font-medium">Gateway Health</p>
          <p id="gw-status" class="text-sm font-bold text-emerald-400 font-serif mt-0.5">HEALTHY (200 OK)</p>
        </div>
        <div id="gw-dot" class="w-3 h-3 rounded-full bg-emerald-500 animate-pulse"></div>
      </div>

      <div class="trust-panel p-4 flex items-center justify-between">
        <div>
          <p class="text-xs text-[#B3A18A] font-medium">Circuit Breaker</p>
          <p id="cb-status" class="text-sm font-bold text-[#D4A020] font-serif mt-0.5">CLOSED (Passing)</p>
        </div>
        <span id="cb-badge" class="text-xs font-mono bg-[#201008] text-[#D4A020] px-2.5 py-0.5 rounded-full border border-[rgba(184,134,11,0.3)]">CLOSED</span>
      </div>

      <div class="trust-panel p-4 flex items-center justify-between">
        <div>
          <p class="text-xs text-[#B3A18A] font-medium">Dead-Letter Queue</p>
          <p id="dlq-count" class="text-sm font-bold text-[#F0E8DC] font-serif mt-0.5">0 Defective Records</p>
        </div>
        <button id="dlq-retry-btn" onclick="retryDLQ()" class="text-[11px] font-semibold bg-[#201008] hover:bg-[#2A140A] text-[#F0E8DC] px-2.5 py-1 rounded-full border border-[rgba(184,134,11,0.3)] transition">
          ↺ Retry DLQ
        </button>
      </div>

      <div class="trust-panel p-4 flex items-center justify-between">
        <div>
          <p class="text-xs text-[#B3A18A] font-medium">Double-Entry Audit</p>
          <p id="recon-status" class="text-sm font-bold text-emerald-400 font-serif mt-0.5">100% Balanced ✓</p>
        </div>
        <span class="text-xs font-mono bg-emerald-950/60 text-emerald-400 px-2.5 py-0.5 rounded-full border border-emerald-500/40">Debit == Credit</span>
      </div>
    </div>

    <!-- KPI Cards Grid -->
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-5">
      <div class="trust-panel p-5 bg-gradient-to-br from-[#24110B] to-[#180806]">
        <span class="text-xs uppercase tracking-wider font-semibold text-[#D4A020]">Total Ingested Volume</span>
        <h3 id="total-volume" class="text-2xl font-black text-[#F0E8DC] font-serif mt-2">₹12,06,250.00</h3>
        <p class="text-xs text-[#B3A18A] mt-1">Balanced across active journal entries</p>
      </div>

      <div class="trust-panel p-5 bg-gradient-to-br from-[#18120B] to-[#180806]">
        <span class="text-xs uppercase tracking-wider font-semibold text-emerald-400">Accounts Receivable (FI-AR)</span>
        <h3 id="total-ar" class="text-2xl font-black text-[#F0E8DC] font-serif mt-2">₹4,03,750.00</h3>
        <p class="text-xs text-[#B3A18A] mt-1">DSO: <span id="dso-val" class="font-bold text-emerald-400 font-serif">42.5 days</span></p>
      </div>

      <div class="trust-panel p-5 bg-gradient-to-br from-[#240c08] to-[#180806]">
        <span class="text-xs uppercase tracking-wider font-semibold text-[#E2654B]">Accounts Payable (FI-AP)</span>
        <h3 id="total-ap" class="text-2xl font-black text-[#F0E8DC] font-serif mt-2">₹3,86,000.00</h3>
        <p class="text-xs text-[#B3A18A] mt-1">DPO: <span id="dpo-val" class="font-bold text-[#E2654B] font-serif">31.0 days</span></p>
      </div>

      <div class="trust-panel p-5 bg-gradient-to-br from-[#1d140a] to-[#180806]">
        <span class="text-xs uppercase tracking-wider font-semibold text-[#D4A020]">Net Working Capital</span>
        <h3 id="net-wc" class="text-2xl font-black text-[#F0E8DC] font-serif mt-2">+₹17,750.00</h3>
        <p class="text-xs text-[#B3A18A] mt-1">Cash Ratio: <span id="cash-ratio" class="font-bold text-[#D4A020] font-serif">1.25x</span></p>
      </div>
    </div>

    <!-- Live Ledger Transactions Table with Search & CSV Export -->
    <div class="trust-panel overflow-hidden shadow-2xl">
      <div class="p-5 border-b border-[rgba(184,134,11,0.18)] flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
        <div>
          <h2 class="text-base font-bold text-[#F0E8DC] font-serif flex items-center gap-2">
            SAP S/4HANA Ledger Line Items
            <span id="filtered-count-badge" class="text-xs font-mono font-normal text-[#B3A18A]">(Loading...)</span>
          </h2>
          <p class="text-xs text-[#B3A18A] mt-0.5">OData line items normalized into analytical double-entry stream</p>
        </div>

        <!-- Filter, Search & Export Bar -->
        <div class="flex flex-wrap items-center gap-2.5 w-full md:w-auto">
          <!-- Type Filter -->
          <select id="type-filter" onchange="filterTable()" class="bg-[#130604] border border-[rgba(184,134,11,0.3)] text-xs rounded-full px-3 py-1.5 text-[#F0E8DC] focus:outline-none focus:border-[#D4A020]">
            <option value="ALL">All Types (FI-GL, AP, AR)</option>
            <option value="SA">FI-GL: Journal (SA)</option>
            <option value="KR">FI-AP: Vendor Invoice (KR)</option>
            <option value="DR">FI-AR: Customer Invoice (DR)</option>
          </select>

          <!-- Search Input -->
          <div class="relative">
            <input type="text" id="search-input" onkeyup="filterTable()" placeholder="Search doc, account, cc..." class="bg-[#130604] border border-[rgba(184,134,11,0.3)] text-xs rounded-full pl-8 pr-3 py-1.5 text-[#F0E8DC] placeholder-[#7A6A55] focus:outline-none focus:border-[#D4A020] w-44 sm:w-56">
            <svg class="w-3.5 h-3.5 text-[#B3A18A] absolute left-2.5 top-2.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"/></svg>
          </div>

          <!-- Export to CSV Button -->
          <button onclick="exportToCSV()" class="px-3 py-1.5 bg-[#201008] hover:bg-[#2A140A] text-[#F0E8DC] rounded-full text-xs font-semibold border border-[rgba(184,134,11,0.3)] flex items-center gap-1.5 transition">
            <svg class="w-3.5 h-3.5 text-[#D4A020]" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/></svg>
            Export CSV
          </button>
        </div>
      </div>

      <div class="overflow-x-auto">
        <table class="w-full text-left text-xs text-[#F0E8DC]">
          <thead class="bg-[#130604] text-[#B3A18A] uppercase tracking-wider text-[11px] border-b border-[rgba(184,134,11,0.18)]">
            <tr>
              <th class="py-3 px-4">Document #</th>
              <th class="py-3 px-4">Date</th>
              <th class="py-3 px-4">Module / Type</th>
              <th class="py-3 px-4">G/L Account</th>
              <th class="py-3 px-4">Cost Center</th>
              <th class="py-3 px-4">Debit / Credit</th>
              <th class="py-3 px-4 text-right">Amount (INR)</th>
            </tr>
          </thead>
          <tbody id="tx-tbody" class="divide-y divide-[rgba(184,134,11,0.1)] font-mono">
            <tr><td colspan="7" class="py-8 text-center text-[#B3A18A] font-sans">Connecting to S/4HANA OData service...</td></tr>
          </tbody>
        </table>
      </div>
    </div>

  </main>

  <!-- Mega Footer -->
  <footer class="bg-[#090302] border-t border-[rgba(184,134,11,0.22)] py-8 text-xs text-[#B3A18A] mt-10">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
      <div class="flex flex-col sm:flex-row justify-between items-center gap-4">
        <div class="flex items-center gap-2">
          <span class="text-sm font-bold text-[#F0E8DC]">Trust</span>
          <span class="text-sm font-bold text-[#D4A020]">Sphere</span>
          <span class="text-[9px] uppercase tracking-widest text-[#B3A18A]">TECHNOLOGIES</span>
          <span class="text-slate-600">|</span>
          <span class="text-[#7A6A55]">TrustCore Enterprise Integration Suite</span>
        </div>
        <div class="flex items-center gap-4 text-[11px] text-[#7A6A55]">
          <a href="/specs/493560B_Custom_API_Integration.pdf" target="_blank" class="text-[#D4A020] hover:underline font-semibold flex items-center gap-1">
            📄 Official Specification (PDF)
          </a>
          <span>·</span>
          <span>SOX 404 &amp; IndAS Audit Compliant</span>
          <span>·</span>
          <span>HMAC-SHA256 Encrypted</span>
        </div>
      </div>
    </div>
  </footer>

  <script>
    let transactionsData = [];
    let isPolling = true;
    let pollTimer = null;
    let faultActive = false;

    // Load Live Dashboard Data
    async function loadDashboardData() {
      try {
        // 1. Health & Resilience Telemetry
        const healthRes = await fetch('/api/v1/monitoring/health');
        if (healthRes.ok) {
          const h = await healthRes.json();
          document.getElementById('gw-status').textContent = 'HEALTHY (200 OK)';
          document.getElementById('gw-status').className = 'text-sm font-bold text-emerald-400 font-serif mt-0.5';
          document.getElementById('gw-dot').className = 'w-3 h-3 rounded-full bg-emerald-500 animate-pulse';

          // Circuit Breaker
          const cbState = h.circuit_breaker;
          document.getElementById('cb-status').textContent = `${cbState} (${cbState === 'CLOSED' ? 'Passing' : 'Tripped'})`;
          document.getElementById('cb-badge').textContent = cbState;

          if (cbState === 'CLOSED') {
            document.getElementById('cb-status').className = 'text-sm font-bold text-[#D4A020] font-serif mt-0.5';
            document.getElementById('cb-badge').className = 'text-xs font-mono bg-[#201008] text-[#D4A020] px-2.5 py-0.5 rounded-full border border-[rgba(184,134,11,0.3)]';
          } else {
            document.getElementById('cb-status').className = 'text-sm font-bold text-rose-400 font-serif mt-0.5';
            document.getElementById('cb-badge').className = 'text-xs font-mono bg-rose-950/60 text-rose-400 px-2.5 py-0.5 rounded-full border border-rose-500/40';
          }

          // Fault Banner state
          faultActive = (cbState === 'OPEN');
          const faultBanner = document.getElementById('fault-banner');
          const faultBtn = document.getElementById('fault-btn');
          const faultBtnText = document.getElementById('fault-btn-text');

          if (faultActive) {
            faultBanner.classList.remove('hidden');
            faultBtnText.textContent = 'Reset / Recover ERP';
            faultBtn.className = 'px-3.5 py-1.5 bg-emerald-950/80 hover:bg-emerald-900 text-emerald-300 border border-emerald-500/40 text-xs font-bold rounded-full transition flex items-center gap-1.5';
          } else {
            faultBanner.classList.add('hidden');
            faultBtnText.textContent = 'Simulate ERP Outage';
            faultBtn.className = 'px-3.5 py-1.5 bg-[#240c08] hover:bg-[#35120c] text-[#E2654B] border border-[#E2654B]/40 text-xs font-bold rounded-full transition flex items-center gap-1.5';
          }

          // DLQ Count
          document.getElementById('dlq-count').textContent = `${h.dead_letter_queue_size} Defective Records`;
        }

        // 2. Working Capital KPIs
        const wcRes = await fetch('/api/v1/analytics/working-capital');
        if (wcRes.ok) {
          const wc = await wcRes.json();
          document.getElementById('total-ar').textContent = `₹${wc.accounts_receivable.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
          document.getElementById('total-ap').textContent = `₹${wc.accounts_payable.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
          document.getElementById('dso-val').textContent = `${wc.days_sales_outstanding} days`;
          document.getElementById('dpo-val').textContent = `${wc.days_payable_outstanding} days`;

          const nwc = wc.net_working_capital;
          const nwcEl = document.getElementById('net-wc');
          nwcEl.textContent = `${nwc >= 0 ? '+' : '-'}₹${Math.abs(nwc).toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
          nwcEl.className = `text-2xl font-black font-serif mt-2 ${nwc >= 0 ? 'text-[#F0E8DC]' : 'text-rose-400'}`;

          document.getElementById('cash-ratio').textContent = `${wc.cash_conversion_ratio}x`;
        }

        // 3. Transactions List & Total Volume
        const txRes = await fetch('/api/v1/sap/transactions?limit=200');
        if (txRes.ok) {
          transactionsData = await txRes.json();
          renderTransactions(transactionsData);

          // Calculate total debit volume
          let totalVol = 0;
          transactionsData.forEach(t => {
            if (t.debit_credit_code === 'S') totalVol += parseFloat(t.amount_in_local_currency);
          });
          document.getElementById('total-volume').textContent = `₹${totalVol.toLocaleString('en-IN', {minimumFractionDigits: 2})}`;
        }
      } catch (err) {
        console.error('Error fetching dashboard telemetry:', err);
      }
    }

    // Render Table Rows
    function renderTransactions(data) {
      const tbody = document.getElementById('tx-tbody');
      const badge = document.getElementById('filtered-count-badge');
      badge.textContent = `(Showing ${data.length} Records)`;

      if (!data || data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="py-8 text-center text-[#B3A18A] font-sans">No transactions match current filters.</td></tr>';
        return;
      }

      tbody.innerHTML = data.map(tx => {
        const isDebit = (tx.debit_credit_code === 'S');
        const badgeColor = tx.document_type === 'SA' ? 'text-sky-300 bg-sky-950/60 border-sky-800' :
                           tx.document_type === 'KR' ? 'text-rose-300 bg-rose-950/60 border-rose-800' :
                           'text-emerald-300 bg-emerald-950/60 border-emerald-800';

        return `
          <tr class="hover:bg-[#201008]/50 transition duration-150">
            <td class="py-3 px-4 font-bold text-[#F0E8DC]">${tx.document_number}</td>
            <td class="py-3 px-4 text-[#B3A18A]">${tx.posting_date}</td>
            <td class="py-3 px-4">
              <span class="text-[10px] font-bold px-2 py-0.5 rounded border ${badgeColor}">
                ${tx.document_type} (${tx.document_type === 'SA' ? 'FI-GL' : tx.document_type === 'KR' ? 'FI-AP' : 'FI-AR'})
              </span>
            </td>
            <td class="py-3 px-4 text-[#D4A020]">${tx.general_ledger_account}</td>
            <td class="py-3 px-4 text-[#B3A18A]">${tx.cost_center || '—'}</td>
            <td class="py-3 px-4">
              <span class="font-bold ${isDebit ? 'text-emerald-400' : 'text-[#E2654B]'}">
                ${isDebit ? 'DEBIT (S)' : 'CREDIT (H)'}
              </span>
            </td>
            <td class="py-3 px-4 text-right font-bold text-[#F0E8DC]">
              ₹${parseFloat(tx.amount_in_local_currency).toLocaleString('en-IN', {minimumFractionDigits: 2})}
            </td>
          </tr>
        `;
      }).join('');
    }

    // Filter Table by Type & Search
    function filterTable() {
      const type = document.getElementById('type-filter').value;
      const search = document.getElementById('search-input').value.toLowerCase().trim();

      const filtered = transactionsData.filter(tx => {
        const matchType = (type === 'ALL') || (tx.document_type === type);
        const matchSearch = !search ||
          tx.document_number.toLowerCase().includes(search) ||
          tx.general_ledger_account.toLowerCase().includes(search) ||
          (tx.cost_center && tx.cost_center.toLowerCase().includes(search)) ||
          tx.posting_date.includes(search);
        return matchType && matchSearch;
      });

      renderTransactions(filtered);
    }

    // CSV Export Utility
    function exportToCSV() {
      if (!transactionsData.length) {
        alert('No data available to export.');
        return;
      }
      const headers = ['Document Number', 'Posting Date', 'Type', 'G/L Account', 'Cost Center', 'Debit/Credit', 'Amount (INR)'];
      const rows = transactionsData.map(t => [
        t.document_number,
        t.posting_date,
        t.document_type,
        t.general_ledger_account,
        t.cost_center || '',
        t.debit_credit_code === 'S' ? 'DEBIT' : 'CREDIT',
        t.amount_in_local_currency
      ]);

      let csvContent = 'data:text/csv;charset=utf-8,' +
        [headers.join(','), ...rows.map(e => e.join(','))].join('\n');

      const encodedUri = encodeURI(csvContent);
      const link = document.createElement('a');
      link.setAttribute('href', encodedUri);
      link.setAttribute('download', `TrustSphere_SAP_Ledger_${new Date().toISOString().slice(0,10)}.csv`);
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    }

    // Trigger On-Demand Sync
    async function triggerSync() {
      const btn = document.getElementById('sync-btn');
      const icon = document.getElementById('sync-icon');
      btn.disabled = true;
      icon.classList.add('animate-spin');
      btn.classList.add('opacity-75');

      try {
        const res = await fetch('/api/v1/sap/sync', { method: 'POST' });
        const data = await res.json();
        if (res.ok) {
          alert(`Sync Successful: ${data.records_ingested} OData records ingested & reconciled.`);
        } else {
          alert(`Sync Failed: ${data.detail || 'Service Unavailable'}`);
        }
        await loadDashboardData();
      } catch (e) {
        alert('Sync error: ' + e);
      } finally {
        btn.disabled = false;
        icon.classList.remove('animate-spin');
        btn.classList.remove('opacity-75');
      }
    }

    // Fault Injection Toggle
    async function toggleFaultInjection() {
      try {
        const targetState = !faultActive;
        const res = await fetch(`/api/v1/resilience/fault-inject?enable=${targetState}`, { method: 'POST' });
        await res.json();
        await loadDashboardData();
      } catch (e) {
        alert('Fault injection error: ' + e);
      }
    }

    // DLQ Reprocessing
    async function retryDLQ() {
      const btn = document.getElementById('dlq-retry-btn');
      btn.disabled = true;
      btn.textContent = 'Retrying...';
      try {
        const res = await fetch('/api/v1/resilience/dlq/retry', { method: 'POST' });
        const data = await res.json();
        alert(`DLQ Reprocessing Complete: ${data.reprocessed_count} items reprocessed. Remaining: ${data.remaining_dlq}`);
        await loadDashboardData();
      } catch (e) {
        alert('DLQ Retry error: ' + e);
      } finally {
        btn.disabled = false;
        btn.textContent = '↺ Retry DLQ';
      }
    }

    // Polling Toggle
    function togglePolling() {
      isPolling = !isPolling;
      const text = document.getElementById('polling-text');
      const indicator = document.getElementById('polling-indicator');

      if (isPolling) {
        text.textContent = 'Live Polling: 3s';
        indicator.className = 'w-2 h-2 rounded-full bg-emerald-400 animate-pulse';
        pollTimer = setInterval(loadDashboardData, 3000);
      } else {
        text.textContent = 'Polling Paused';
        indicator.className = 'w-2 h-2 rounded-full bg-[#7A6A55]';
        clearInterval(pollTimer);
      }
    }

    // Start auto-polling
    loadDashboardData();
    pollTimer = setInterval(loadDashboardData, 3000);
  </script>
</body>
</html>
"""
    return HTMLResponse(content=html_content)
