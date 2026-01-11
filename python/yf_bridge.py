#!/usr/bin/env python3
import json
import sys
import traceback
from typing import Any, Dict, List, Optional, cast
import math

try:
    import numpy as np
    import pandas as pd
    import yfinance as yf
    _IMPORT_ERROR = None
except Exception as exc:  # pragma: no cover - import guard
    np = cast(Any, None)
    pd = cast(Any, None)
    yf = cast(Any, None)
    _IMPORT_ERROR = str(exc)


def _to_iso(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, pd.Period):
        return str(value)
    return value


def _serialize_value(value: Any) -> Any:
    if isinstance(value, pd.DataFrame):
        df = value.copy()
        df = df.where(pd.notnull(df), None)
        return {
            "__type__": "dataframe",
            "columns": [str(c) for c in df.columns],
            "index": [_to_iso(i) for i in df.index.tolist()],
            "data": [[_serialize_value(v) for v in row] for row in df.to_numpy().tolist()],
        }
    if isinstance(value, pd.Series):
        series = value.where(pd.notnull(value), None)
        return {
            "__type__": "series",
            "name": str(series.name) if series.name is not None else None,
            "index": [_to_iso(i) for i in series.index.tolist()],
            "data": [_serialize_value(v) for v in series.tolist()],
        }
    if hasattr(value, "keys") and hasattr(value, "__getitem__"):
        try:
            return {str(k): _serialize_value(value[k]) for k in value.keys()}
        except Exception:
            pass
    if isinstance(value, (np.generic,)):
        val = value.item()
        if isinstance(val, float) and math.isnan(val):
            return None
        return val
    if isinstance(value, (np.ndarray,)):
        return [_serialize_value(v) for v in value.tolist()]
    if isinstance(value, (dict,)):
        return {str(k): _serialize_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_value(v) for v in value]
    if isinstance(value, (pd.Timestamp, pd.Period)):
        return _to_iso(value)
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def _read_payload() -> Dict[str, Any]:
    raw = sys.stdin.read()
    if not raw.strip():
        return {}
    return json.loads(raw)


def _ok(result: Any) -> None:
    payload = {"ok": True, "result": _serialize_value(result)}
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, allow_nan=False))


def _err(message: str, details: Optional[str] = None) -> None:
    payload = {"ok": False, "error": message}
    if details:
        payload["details"] = details
    sys.stdout.write(json.dumps(payload, ensure_ascii=False))


def _build_query(query_type: str, query_payload: Dict[str, Any]):
    if query_type == "equity":
        query_cls = yf.EquityQuery
    elif query_type == "fund":
        query_cls = yf.FundQuery
    else:
        raise ValueError("query_type must be 'equity' or 'fund'")

    operator = query_payload.get("operator")
    operands = query_payload.get("operands")
    if not operator or operands is None:
        raise ValueError("Query must include operator and operands")

    parsed_operands = []
    for operand in operands:
        if isinstance(operand, dict) and "operator" in operand:
            parsed_operands.append(_build_query(query_type, operand))
        else:
            parsed_operands.append(operand)

    return query_cls(operator, parsed_operands)


def _handle_download(args: Dict[str, Any]):
    tickers = args.get("tickers")
    if not tickers:
        raise ValueError("tickers is required")

    kwargs = {k: v for k, v in args.items() if k != "tickers" and v is not None}
    return yf.download(tickers, **kwargs)


def _handle_ticker(args: Dict[str, Any], fn_name: str):
    ticker = args.get("ticker")
    if not ticker:
        raise ValueError("ticker is required")
    tkr = yf.Ticker(ticker)
    fn = getattr(tkr, fn_name)
    kwargs = {k: v for k, v in args.items() if k != "ticker" and v is not None}
    return fn(**kwargs)


def _handle_tickers(args: Dict[str, Any], fn_name: str):
    tickers = args.get("tickers")
    if not tickers:
        raise ValueError("tickers is required")
    tks = yf.Tickers(tickers)
    fn = getattr(tks, fn_name)
    kwargs = {k: v for k, v in args.items() if k != "tickers" and v is not None}
    return fn(**kwargs)


def _handle_ticker_attr(args: Dict[str, Any], attr_name: str):
    ticker = args.get("ticker")
    if not ticker:
        raise ValueError("ticker is required")
    tkr = yf.Ticker(ticker)
    return getattr(tkr, attr_name)


def _handle_options(args: Dict[str, Any]):
    ticker = args.get("ticker")
    if not ticker:
        raise ValueError("ticker is required")
    tkr = yf.Ticker(ticker)
    date = args.get("date")
    tz = args.get("tz")
    chain = tkr.option_chain(date=date, tz=tz)
    return {
        "calls": chain.calls,
        "puts": chain.puts,
        "underlying": chain.underlying,
    }


def _handle_search(args: Dict[str, Any]):
    query = args.get("query")
    if not query:
        raise ValueError("query is required")
    max_results = args.get("max_results")
    news_count = args.get("news_count")
    include_research = args.get("include_research")
    kwargs = {}
    if max_results is not None:
        kwargs["max_results"] = max_results
    if news_count is not None:
        kwargs["news_count"] = news_count
    if include_research is not None:
        kwargs["include_research"] = include_research
    search = yf.Search(query, **kwargs)
    return {
        "quotes": search.quotes,
        "news": search.news,
        "research": search.research,
    }


def _handle_lookup(args: Dict[str, Any]):
    query = args.get("query")
    if not query:
        raise ValueError("query is required")
    kind = args.get("kind", "all")
    count = args.get("count")
    lookup = yf.Lookup(query)
    if kind == "all":
        return lookup.get_all(count=count) if count else lookup.all
    if kind == "stock":
        return lookup.get_stock(count=count) if count else lookup.stock
    if kind == "mutualfund":
        return lookup.get_mutualfund(count=count) if count else lookup.mutualfund
    if kind == "etf":
        return lookup.get_etf(count=count) if count else lookup.etf
    if kind == "index":
        return lookup.get_index(count=count) if count else lookup.index
    if kind == "future":
        return lookup.get_future(count=count) if count else lookup.future
    if kind == "currency":
        return lookup.get_currency(count=count) if count else lookup.currency
    if kind == "cryptocurrency":
        return lookup.get_cryptocurrency(count=count) if count else lookup.cryptocurrency
    raise ValueError("kind must be one of: all, stock, mutualfund, etf, index, future, currency, cryptocurrency")


def _handle_market(args: Dict[str, Any]):
    market = args.get("market")
    if not market:
        raise ValueError("market is required")
    item = args.get("item", "summary")
    market_obj = yf.Market(market)
    if item == "status":
        return market_obj.status
    if item == "summary":
        return market_obj.summary
    raise ValueError("item must be one of: status, summary")


def _handle_sector(args: Dict[str, Any]):
    key = args.get("key")
    if not key:
        raise ValueError("key is required")
    sector = yf.Sector(key)
    field = args.get("field")
    data = {
        "key": sector.key,
        "name": sector.name,
        "symbol": sector.symbol,
        "ticker": sector.ticker,
        "overview": sector.overview,
        "top_companies": sector.top_companies,
        "research_reports": sector.research_reports,
        "top_etfs": sector.top_etfs,
        "top_mutual_funds": sector.top_mutual_funds,
        "industries": sector.industries,
    }
    return data if field is None else data.get(field)


def _handle_industry(args: Dict[str, Any]):
    key = args.get("key")
    if not key:
        raise ValueError("key is required")
    industry = yf.Industry(key)
    field = args.get("field")
    data = {
        "key": industry.key,
        "name": industry.name,
        "symbol": industry.symbol,
        "ticker": industry.ticker,
        "overview": industry.overview,
        "top_companies": industry.top_companies,
        "research_reports": industry.research_reports,
        "sector_key": industry.sector_key,
        "sector_name": industry.sector_name,
        "top_performing_companies": industry.top_performing_companies,
        "top_growth_companies": industry.top_growth_companies,
    }
    return data if field is None else data.get(field)


def _handle_calendars(args: Dict[str, Any]):
    calendar_type = args.get("calendar_type")
    if not calendar_type:
        raise ValueError("calendar_type is required")
    start = args.get("start")
    end = args.get("end")
    calendar_kwargs = {}
    if start is not None:
        calendar_kwargs["start"] = start
    if end is not None:
        calendar_kwargs["end"] = end
    calendar_cls = getattr(yf, "Calendars")
    calendar = calendar_cls(**calendar_kwargs)

    if calendar_type == "earnings":
        return calendar.get_earnings_calendar(
            limit=args.get("limit"),
            market_cap=args.get("market_cap"),
            filter_most_active=args.get("filter_most_active"),
        )
    if calendar_type == "ipo":
        return calendar.get_ipo_info_calendar(limit=args.get("limit"))
    if calendar_type == "splits":
        return calendar.get_splits_calendar(limit=args.get("limit"))
    if calendar_type == "economic_events":
        return calendar.get_economic_events_calendar(limit=args.get("limit"))

    raise ValueError("calendar_type must be one of: earnings, ipo, splits, economic_events")


def _handle_screen(args: Dict[str, Any]):
    query = args.get("query")
    query_type = args.get("query_type")
    if isinstance(query, dict):
        if not query_type:
            raise ValueError("query_type is required when query is a dict")
        query = _build_query(query_type, query)
    if query is None:
        raise ValueError("query is required")

    kwargs = {}
    for key in ["offset", "size", "count", "sortField", "sortAsc", "userId", "userIdType"]:
        value = args.get(key)
        if value is not None:
            kwargs[key] = value

    return yf.screen(query, **kwargs)


def _handle_field_definitions() -> Dict[str, Any]:
    from yfinance import const
    from yfinance.scrapers.quote import FastInfo

    return {
        "fundamentals_keys": const.fundamentals_keys,
        "quote_summary_valid_modules": const.quote_summary_valid_modules,
        "equity_screener_fields": const.EQUITY_SCREENER_FIELDS,
        "fund_screener_fields": const.FUND_SCREENER_FIELDS,
        "equity_screener_eq_map": const.EQUITY_SCREENER_EQ_MAP,
        "fund_screener_eq_map": const.FUND_SCREENER_EQ_MAP,
        "fast_info_keys": FastInfo(yf.Ticker("SPY")).keys(),
    }


def _handle_field_categories() -> Dict[str, Any]:
    from yfinance import const
    return {
        "fundamentals_keys": list(const.fundamentals_keys.keys()),
        "equity_screener_fields": list(const.EQUITY_SCREENER_FIELDS.keys()),
        "fund_screener_fields": list(const.FUND_SCREENER_FIELDS.keys()),
        "quote_summary_modules": list(const.quote_summary_valid_modules),
    }


def _dispatch(action: str, args: Dict[str, Any]):
    if action == "download":
        return _handle_download(args)

    if action == "ticker_history":
        return _handle_ticker(args, "history")
    if action == "tickers_history":
        return _handle_tickers(args, "history")
    if action == "tickers_news":
        return _handle_tickers(args, "news")
    if action == "ticker_info":
        return _handle_ticker_attr(args, "info")
    if action == "ticker_fast_info":
        return _handle_ticker_attr(args, "fast_info")
    if action == "ticker_actions":
        return _handle_ticker_attr(args, "actions")
    if action == "ticker_dividends":
        return _handle_ticker_attr(args, "dividends")
    if action == "ticker_splits":
        return _handle_ticker_attr(args, "splits")
    if action == "ticker_capital_gains":
        return _handle_ticker_attr(args, "capital_gains")
    if action == "ticker_news":
        return _handle_ticker_attr(args, "news")

    if action == "ticker_income_stmt":
        return _handle_ticker(args, "get_income_stmt")
    if action == "ticker_balance_sheet":
        return _handle_ticker(args, "get_balance_sheet")
    if action == "ticker_cash_flow":
        return _handle_ticker(args, "get_cash_flow")
    if action == "ticker_earnings":
        return _handle_ticker_attr(args, "earnings")
    if action == "ticker_calendar":
        return _handle_ticker_attr(args, "calendar")
    if action == "ticker_earnings_dates":
        return _handle_ticker(args, "get_earnings_dates")
    if action == "ticker_sec_filings":
        return _handle_ticker_attr(args, "sec_filings")
    if action == "ticker_shares":
        return _handle_ticker(args, "get_shares")
    if action == "ticker_shares_full":
        return _handle_ticker(args, "get_shares_full")

    if action == "ticker_recommendations":
        return _handle_ticker(args, "get_recommendations")
    if action == "ticker_recommendations_summary":
        return _handle_ticker(args, "get_recommendations_summary")
    if action == "ticker_upgrades_downgrades":
        return _handle_ticker(args, "get_upgrades_downgrades")
    if action == "ticker_sustainability":
        return _handle_ticker(args, "get_sustainability")
    if action == "ticker_analyst_price_targets":
        return _handle_ticker(args, "get_analyst_price_targets")
    if action == "ticker_earnings_estimate":
        return _handle_ticker(args, "get_earnings_estimate")
    if action == "ticker_revenue_estimate":
        return _handle_ticker(args, "get_revenue_estimate")
    if action == "ticker_earnings_history":
        return _handle_ticker(args, "get_earnings_history")
    if action == "ticker_eps_trend":
        return _handle_ticker(args, "get_eps_trend")
    if action == "ticker_eps_revisions":
        return _handle_ticker(args, "get_eps_revisions")
    if action == "ticker_growth_estimates":
        return _handle_ticker(args, "get_growth_estimates")

    if action == "ticker_major_holders":
        return _handle_ticker(args, "get_major_holders")
    if action == "ticker_institutional_holders":
        return _handle_ticker(args, "get_institutional_holders")
    if action == "ticker_mutualfund_holders":
        return _handle_ticker(args, "get_mutualfund_holders")
    if action == "ticker_insider_purchases":
        return _handle_ticker(args, "get_insider_purchases")
    if action == "ticker_insider_transactions":
        return _handle_ticker(args, "get_insider_transactions")
    if action == "ticker_insider_roster_holders":
        return _handle_ticker(args, "get_insider_roster_holders")
    if action == "ticker_funds_data":
        return _handle_ticker_attr(args, "funds_data")

    if action == "ticker_options":
        return _handle_options(args)

    if action == "search":
        return _handle_search(args)
    if action == "lookup":
        return _handle_lookup(args)
    if action == "market":
        return _handle_market(args)
    if action == "sector":
        return _handle_sector(args)
    if action == "industry":
        return _handle_industry(args)
    if action == "calendars":
        return _handle_calendars(args)
    if action == "screen":
        return _handle_screen(args)

    if action == "field_definitions":
        return _handle_field_definitions()
    if action == "field_categories":
        return _handle_field_categories()

    raise ValueError(f"Unknown action '{action}'")


def main() -> None:
    if _IMPORT_ERROR:
        _err(
            "Missing Python dependencies for yfinance bridge.",
            "Install with: pip install -r mcp/python/requirements.txt",
        )
        return

    payload = _read_payload()
    action = payload.get("action")
    args = payload.get("args", {})

    if not action:
        _err("Missing action in payload")
        return

    try:
        result = _dispatch(action, args)
        _ok(result)
    except Exception as exc:
        detail = traceback.format_exc()
        _err(str(exc), detail)


if __name__ == "__main__":
    main()
