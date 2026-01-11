#!/usr/bin/env node
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import path from "node:path";
import fs from "node:fs/promises";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PYTHON_BRIDGE = path.resolve(__dirname, "..", "python", "yf_bridge.py");
const SERVER_NAME = "finmcp";
const responseFormatSchema = z.enum(["json", "markdown"]).default("json");
const saveSchema = z
    .object({
    format: z.enum(["csv", "json"]),
    filename: z.string().min(1).optional(),
})
    .optional();
const outputOptionsSchema = z.object({
    response_format: responseFormatSchema.optional(),
    preview_limit: z.number().int().min(1).max(200).optional(),
    save: saveSchema,
});
function pythonCommand() {
    if (process.platform === "win32") {
        return "python";
    }
    return "python3";
}
function extractPreviewLimit(options) {
    return options?.preview_limit ?? 25;
}
async function callBridge(action, args) {
    const payload = JSON.stringify({ action, args });
    return new Promise((resolve, reject) => {
        const proc = spawn(pythonCommand(), [PYTHON_BRIDGE], {
            stdio: ["pipe", "pipe", "pipe"],
        });
        let stdout = "";
        let stderr = "";
        proc.stdout.on("data", (chunk) => {
            stdout += chunk.toString();
        });
        proc.stderr.on("data", (chunk) => {
            stderr += chunk.toString();
        });
        proc.on("error", (err) => {
            reject(err);
        });
        proc.on("close", (code) => {
            if (code !== 0) {
                reject(new Error(`Python bridge failed (${code}): ${stderr.trim()}`));
                return;
            }
            try {
                const parsed = JSON.parse(stdout);
                if (!parsed.ok) {
                    const error = new Error(parsed.error || "Unknown error from bridge");
                    error.details = parsed.details;
                    reject(error);
                    return;
                }
                resolve(parsed.result);
            }
            catch (err) {
                reject(err);
            }
        });
        proc.stdin.write(payload);
        proc.stdin.end();
    });
}
function formatErrorMessage(toolName, message) {
    const lowered = message.toLowerCase();
    if (lowered.includes("ticker is required")) {
        return `${message} Try passing a ticker like "AAPL" or use yf_search to discover tickers.`;
    }
    if (lowered.includes("query is required")) {
        return `${message} Provide a query string or use yf_equity_query_fields to build a query.`;
    }
    if (lowered.includes("query_type")) {
        return `${message} Set query_type to "equity" or "fund" when using custom queries.`;
    }
    if (lowered.includes("calendar_type")) {
        return `${message} Try calendar_type="earnings" or "ipo" for a supported calendar.`;
    }
    if (lowered.includes("count") && lowered.includes("250")) {
        return `${message} Reduce count or size to 250 or fewer.`;
    }
    return `Tool ${toolName} failed: ${message} Try adjusting inputs or using field lookup tools for valid values.`;
}
function stringifyJson(value) {
    return JSON.stringify(value, null, 2);
}
function isDataFrame(value) {
    return typeof value === "object" && value !== null && value.__type__ === "dataframe";
}
function isSeries(value) {
    return typeof value === "object" && value !== null && value.__type__ === "series";
}
function escapeCsvValue(value) {
    if (value === null || value === undefined) {
        return "";
    }
    const text = String(value).replace(/\r?\n/g, " ");
    if (text.includes(",") || text.includes("\"") || text.includes("\n")) {
        return `"${text.replace(/"/g, "\"\"")}"`;
    }
    return text;
}
function toCsv(result) {
    if (isDataFrame(result)) {
        const header = ["index", ...result.columns];
        const rows = result.data.map((row, idx) => [result.index[idx], ...row]);
        return [header, ...rows].map((row) => row.map(escapeCsvValue).join(",")).join("\n");
    }
    if (isSeries(result)) {
        const header = ["index", result.name ?? "value"];
        const rows = result.data.map((value, idx) => [result.index[idx], value]);
        return [header, ...rows].map((row) => row.map(escapeCsvValue).join(",")).join("\n");
    }
    if (Array.isArray(result)) {
        if (result.length === 0) {
            return "";
        }
        if (typeof result[0] === "object" && result[0] !== null) {
            const columns = Array.from(new Set(result.flatMap((row) => Object.keys(row))));
            const rows = result.map((row) => columns.map((col) => row[col]));
            return [columns, ...rows].map((row) => row.map(escapeCsvValue).join(",")).join("\n");
        }
    }
    throw new Error("CSV export requires tabular data. Try response_format=json or choose a table-producing tool.");
}
async function saveResult(result, save, toolName) {
    const cwd = process.cwd();
    const extension = save.format === "csv" ? "csv" : "json";
    const filename = save.filename ?? `${toolName}-${Date.now()}.${extension}`;
    const resolved = path.resolve(cwd, filename);
    if (!resolved.startsWith(cwd)) {
        throw new Error("save.filename must stay within the server working directory.");
    }
    const content = save.format === "csv" ? toCsv(result) : stringifyJson(result);
    await fs.writeFile(resolved, content, "utf8");
    return resolved;
}
function toMarkdown(result, previewLimit) {
    if (isDataFrame(result)) {
        const rows = result.data.slice(0, previewLimit);
        const header = ["index", ...result.columns];
        const separator = header.map(() => "---");
        const body = rows.map((row, idx) => [result.index[idx], ...row]);
        const table = [header, separator, ...body]
            .map((row) => row.map((cell) => String(cell ?? "")).join(" | "))
            .join("\n");
        return table;
    }
    if (isSeries(result)) {
        const rows = result.data.slice(0, previewLimit).map((value, idx) => [result.index[idx], value]);
        const header = ["index", result.name ?? "value"];
        const separator = header.map(() => "---");
        const table = [header, separator, ...rows]
            .map((row) => row.map((cell) => String(cell ?? "")).join(" | "))
            .join("\n");
        return table;
    }
    return `\n\n\`\`\`json\n${stringifyJson(result)}\n\`\`\`\n`;
}
async function buildResponse(toolName, result, options) {
    let savedPath;
    if (options?.save) {
        savedPath = await saveResult(result, options.save, toolName);
    }
    if ((options?.response_format ?? "json") === "markdown") {
        const preview = toMarkdown(result, extractPreviewLimit(options));
        const savedNote = savedPath ? `\n\nSaved to ${savedPath}` : "";
        return preview + savedNote;
    }
    const payload = {
        data: result,
        saved_path: savedPath ?? null,
    };
    return stringifyJson(payload);
}
function registerYfTool(server, name, description, schema, action) {
    server.registerTool(name, {
        description,
        inputSchema: schema,
        annotations: {
            readOnlyHint: false,
            destructiveHint: false,
            idempotentHint: true,
            openWorldHint: true,
        },
    }, async (params, _extra) => {
        const { response_format, preview_limit, save, ...args } = params;
        try {
            const result = await callBridge(action, args);
            const text = await buildResponse(name, result, { response_format, preview_limit, save });
            return {
                content: [{ type: "text", text }],
            };
        }
        catch (err) {
            const message = err instanceof Error ? err.message : "Unknown error";
            const text = formatErrorMessage(name, message);
            return {
                content: [{ type: "text", text }],
            };
        }
    });
}
const server = new McpServer({
    name: SERVER_NAME,
    version: "0.1.0",
});
const tickerSchema = z.object({
    ticker: z.string().min(1).describe("Ticker symbol, e.g. AAPL"),
});
const downloadSchema = z
    .object({
    tickers: z.string().min(1).describe("Space-separated tickers, e.g. 'SPY AAPL'"),
    period: z.string().optional(),
    interval: z.string().optional(),
    start: z.string().optional(),
    end: z.string().optional(),
    group_by: z.string().optional(),
    auto_adjust: z.boolean().optional(),
    actions: z.boolean().optional(),
    threads: z.boolean().optional(),
})
    .merge(outputOptionsSchema);
const historySchema = tickerSchema
    .extend({
    period: z.string().optional(),
    interval: z.string().optional(),
    start: z.string().optional(),
    end: z.string().optional(),
    auto_adjust: z.boolean().optional(),
    actions: z.boolean().optional(),
    prepost: z.boolean().optional(),
    repair: z.boolean().optional(),
    keepna: z.boolean().optional(),
})
    .merge(outputOptionsSchema);
const tickersHistorySchema = z
    .object({
    tickers: z.string().min(1).describe("Space-separated tickers, e.g. 'SPY AAPL'"),
    period: z.string().optional(),
    interval: z.string().optional(),
    start: z.string().optional(),
    end: z.string().optional(),
    auto_adjust: z.boolean().optional(),
    actions: z.boolean().optional(),
    prepost: z.boolean().optional(),
    repair: z.boolean().optional(),
    keepna: z.boolean().optional(),
    group_by: z.enum(["column", "ticker"]).optional(),
})
    .merge(outputOptionsSchema);
const tickersSchema = z
    .object({
    tickers: z.string().min(1).describe("Space-separated tickers, e.g. 'SPY AAPL'"),
})
    .merge(outputOptionsSchema);
const financialSchema = tickerSchema
    .extend({
    as_dict: z.boolean().optional(),
    pretty: z.boolean().optional(),
    freq: z.enum(["yearly", "quarterly", "trailing"]).optional(),
})
    .merge(outputOptionsSchema);
const earningsDatesSchema = tickerSchema
    .extend({
    limit: z.number().int().min(1).max(400).optional(),
    sort: z.string().optional(),
})
    .merge(outputOptionsSchema);
const sharesFullSchema = tickerSchema
    .extend({
    start: z.string().optional(),
    end: z.string().optional(),
})
    .merge(outputOptionsSchema);
const optionsSchema = tickerSchema
    .extend({
    date: z.string().optional(),
    tz: z.string().optional(),
})
    .merge(outputOptionsSchema);
const searchSchema = z
    .object({
    query: z.string().min(1).describe("Search query, e.g. 'AAPL' or company name"),
    max_results: z.number().int().min(1).max(50).optional(),
    news_count: z.number().int().min(0).max(20).optional(),
    include_research: z.boolean().optional(),
})
    .merge(outputOptionsSchema);
const lookupSchema = z
    .object({
    query: z.string().min(1).describe("Lookup query, e.g. 'AAPL'"),
    kind: z
        .enum(["all", "stock", "mutualfund", "etf", "index", "future", "currency", "cryptocurrency"])
        .optional(),
    count: z.number().int().min(1).max(100).optional(),
})
    .merge(outputOptionsSchema);
const marketSchema = z
    .object({
    market: z.string().min(1).describe("Market key, e.g. 'EUROPE'"),
    item: z.enum(["summary", "status"]).optional(),
})
    .merge(outputOptionsSchema);
const sectorSchema = z
    .object({
    key: z.string().min(1).describe("Sector key, e.g. 'technology'"),
    field: z
        .enum([
        "key",
        "name",
        "symbol",
        "ticker",
        "overview",
        "top_companies",
        "research_reports",
        "top_etfs",
        "top_mutual_funds",
        "industries",
    ])
        .optional(),
})
    .merge(outputOptionsSchema);
const industrySchema = z
    .object({
    key: z.string().min(1).describe("Industry key, e.g. 'software-infrastructure'"),
    field: z
        .enum([
        "key",
        "name",
        "symbol",
        "ticker",
        "overview",
        "top_companies",
        "research_reports",
        "sector_key",
        "sector_name",
        "top_performing_companies",
        "top_growth_companies",
    ])
        .optional(),
})
    .merge(outputOptionsSchema);
const calendarsSchema = z
    .object({
    calendar_type: z.enum(["earnings", "ipo", "splits", "economic_events"]),
    start: z.string().optional(),
    end: z.string().optional(),
    limit: z.number().int().min(1).max(500).optional(),
    market_cap: z.number().optional(),
    filter_most_active: z.boolean().optional(),
})
    .merge(outputOptionsSchema);
const screenSchema = z
    .object({
    query: z.union([
        z.string(),
        z.object({
            operator: z.string().min(1),
            operands: z.array(z.union([z.string(), z.number(), z.boolean(), z.record(z.any())])),
        }),
    ]),
    query_type: z.enum(["equity", "fund"]).optional(),
    offset: z.number().int().min(0).optional(),
    size: z.number().int().min(1).max(250).optional(),
    count: z.number().int().min(1).max(250).optional(),
    sortField: z.string().optional(),
    sortAsc: z.boolean().optional(),
    userId: z.string().optional(),
    userIdType: z.string().optional(),
})
    .merge(outputOptionsSchema);
const fieldDefinitionsSchema = outputOptionsSchema.partial();
registerYfTool(server, "yf_download", "Download market data for multiple tickers.", downloadSchema, "download");
registerYfTool(server, "yf_ticker_history", "Get historical market data for a ticker.", historySchema, "ticker_history");
registerYfTool(server, "yf_tickers_history", "Get historical market data for multiple tickers.", tickersHistorySchema, "tickers_history");
registerYfTool(server, "yf_tickers_news", "Get news for multiple tickers.", tickersSchema, "tickers_news");
registerYfTool(server, "yf_ticker_info", "Get info dictionary for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_info");
registerYfTool(server, "yf_ticker_fast_info", "Get fast info (lightweight) for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_fast_info");
registerYfTool(server, "yf_ticker_actions", "Get corporate actions for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_actions");
registerYfTool(server, "yf_ticker_dividends", "Get dividends for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_dividends");
registerYfTool(server, "yf_ticker_splits", "Get split history for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_splits");
registerYfTool(server, "yf_ticker_capital_gains", "Get capital gains for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_capital_gains");
registerYfTool(server, "yf_ticker_news", "Get news for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_news");
registerYfTool(server, "yf_ticker_income_stmt", "Get income statement for a ticker.", financialSchema, "ticker_income_stmt");
registerYfTool(server, "yf_ticker_balance_sheet", "Get balance sheet for a ticker.", financialSchema, "ticker_balance_sheet");
registerYfTool(server, "yf_ticker_cash_flow", "Get cash flow for a ticker.", financialSchema, "ticker_cash_flow");
registerYfTool(server, "yf_ticker_earnings", "Get earnings for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_earnings");
registerYfTool(server, "yf_ticker_calendar", "Get calendar events for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_calendar");
registerYfTool(server, "yf_ticker_earnings_dates", "Get earnings dates for a ticker.", earningsDatesSchema, "ticker_earnings_dates");
registerYfTool(server, "yf_ticker_sec_filings", "Get SEC filings for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_sec_filings");
registerYfTool(server, "yf_ticker_shares", "Get shares data for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_shares");
registerYfTool(server, "yf_ticker_shares_full", "Get full shares history for a ticker.", sharesFullSchema, "ticker_shares_full");
registerYfTool(server, "yf_ticker_recommendations", "Get analyst recommendations for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_recommendations");
registerYfTool(server, "yf_ticker_recommendations_summary", "Get recommendations summary for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_recommendations_summary");
registerYfTool(server, "yf_ticker_upgrades_downgrades", "Get upgrades/downgrades for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_upgrades_downgrades");
registerYfTool(server, "yf_ticker_sustainability", "Get sustainability data for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_sustainability");
registerYfTool(server, "yf_ticker_analyst_price_targets", "Get analyst price targets for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_analyst_price_targets");
registerYfTool(server, "yf_ticker_earnings_estimate", "Get earnings estimate for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_earnings_estimate");
registerYfTool(server, "yf_ticker_revenue_estimate", "Get revenue estimate for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_revenue_estimate");
registerYfTool(server, "yf_ticker_earnings_history", "Get earnings history for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_earnings_history");
registerYfTool(server, "yf_ticker_eps_trend", "Get EPS trend for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_eps_trend");
registerYfTool(server, "yf_ticker_eps_revisions", "Get EPS revisions for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_eps_revisions");
registerYfTool(server, "yf_ticker_growth_estimates", "Get growth estimates for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_growth_estimates");
registerYfTool(server, "yf_ticker_major_holders", "Get major holders for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_major_holders");
registerYfTool(server, "yf_ticker_institutional_holders", "Get institutional holders for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_institutional_holders");
registerYfTool(server, "yf_ticker_mutualfund_holders", "Get mutual fund holders for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_mutualfund_holders");
registerYfTool(server, "yf_ticker_insider_purchases", "Get insider purchases for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_insider_purchases");
registerYfTool(server, "yf_ticker_insider_transactions", "Get insider transactions for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_insider_transactions");
registerYfTool(server, "yf_ticker_insider_roster_holders", "Get insider roster holders for a ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_insider_roster_holders");
registerYfTool(server, "yf_ticker_funds_data", "Get funds data for an ETF/mutual fund ticker.", tickerSchema.merge(outputOptionsSchema), "ticker_funds_data");
registerYfTool(server, "yf_ticker_options", "Get options chain for a ticker.", optionsSchema, "ticker_options");
registerYfTool(server, "yf_search", "Search quotes/news/research for a query.", searchSchema, "search");
registerYfTool(server, "yf_lookup", "Lookup ticker symbols and asset types.", lookupSchema, "lookup");
registerYfTool(server, "yf_market", "Fetch market summary or status.", marketSchema, "market");
registerYfTool(server, "yf_sector", "Fetch sector data.", sectorSchema, "sector");
registerYfTool(server, "yf_industry", "Fetch industry data.", industrySchema, "industry");
registerYfTool(server, "yf_calendars", "Fetch calendar data.", calendarsSchema, "calendars");
registerYfTool(server, "yf_screen", "Run equity or fund screener queries.", screenSchema, "screen");
registerYfTool(server, "yf_field_definitions", "List yfinance field definitions and valid modules.", fieldDefinitionsSchema, "field_definitions");
registerYfTool(server, "yf_field_categories", "List field category groups for yfinance data.", fieldDefinitionsSchema, "field_categories");
const transport = new StdioServerTransport();
await server.connect(transport);
console.error(`${SERVER_NAME} running on stdio`);
