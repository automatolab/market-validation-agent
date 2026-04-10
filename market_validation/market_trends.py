from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def get_google_trends_data(
    keyword: str,
    geography: str = "US",
    timeframe: str = "today 3-m",
) -> dict[str, Any]:
    try:
        from pytrends.request import TrendReq

        pytrends = TrendReq(hl="en-US", tz=360)
        pytrends.build_payload(
            kw_list=[keyword],
            cat=0,
            timeframe=timeframe,
            geo=geography,
            gprop="",
        )

        interest_over_time = pytrends.interest_over_time()
        interest_by_region = pytrends.interest_by_region()
        related_queries = pytrends.related_queries()

        result = {
            "result": "ok",
            "keyword": keyword,
            "geography": geography,
            "timeframe": timeframe,
            "fetched_at": _iso_now(),
        }

        if not interest_over_time.empty:
            data = interest_over_time[keyword].tolist()
            result["interest_values"] = data
            result["interest_avg"] = round(sum(data) / len(data), 2) if data else 0
            result["interest_peak"] = max(data) if data else 0
            result["interest_trend"] = "rising" if len(data) >= 2 and data[-1] > data[0] else "falling"

        if not interest_by_region.empty:
            region_data = interest_by_region[keyword].sort_values(ascending=False).head(10)
            result["top_regions"] = [
                {"region": str(idx), "interest": int(val)}
                for idx, val in region_data.items()
                if val > 0
            ]

        if related_queries and keyword in related_queries:
            queries = related_queries[keyword]
            if queries and queries.get("top") is not None:
                result["related_queries"] = [
                    {"query": str(row["query"]), "value": int(row["value"])}
                    for _, row in queries["top"].iterrows()
                    if row.get("value") is not None and str(row.get("value")).replace("+", "").isdigit()
                ][:10]

        return result

    except ImportError:
        return {
            "result": "ok",
            "keyword": keyword,
            "geography": geography,
            "fetched_at": _iso_now(),
            "skipped": True,
            "reason": "pytrends not installed. Run: pip install pytrends",
        }
    except Exception as e:
        return {
            "result": "ok",
            "keyword": keyword,
            "geography": geography,
            "fetched_at": _iso_now(),
            "error": str(e),
        }


def get_market_demand_report(
    target_product: str,
    geography: str = "US",
) -> dict[str, Any]:
    keywords = [
        f"{target_product}",
        f"{target_product} catering",
        f"{target_product} wholesale",
        f"buy {target_product}",
    ]

    report = {
        "result": "ok",
        "target_product": target_product,
        "geography": geography,
        "fetched_at": _iso_now(),
        "keywords": {},
    }

    for kw in keywords:
        data = get_google_trends_data(kw, geography)
        if data.get("result") == "ok" and not data.get("skipped"):
            report["keywords"][kw] = {
                "interest_avg": data.get("interest_avg", 0),
                "interest_peak": data.get("interest_peak", 0),
                "interest_trend": data.get("interest_trend", "unknown"),
                "top_regions": data.get("top_regions", [])[:5],
            }

    all_avgs = [v["interest_avg"] for v in report["keywords"].values() if v.get("interest_avg", 0) > 0]
    if all_avgs:
        report["market_demand_score"] = round(sum(all_avgs) / len(all_avgs), 2)
        report["demand_level"] = (
            "high" if report["market_demand_score"] > 50
            else "medium" if report["market_demand_score"] > 20
            else "low"
        )
    else:
        report["market_demand_score"] = 0
        report["demand_level"] = "unknown"

    return report


def build_parser() -> Any:
    import argparse
    parser = argparse.ArgumentParser(description="Fetch Google Trends data for market validation")
    parser.add_argument("--keyword", required=True, help="Search keyword (e.g., brisket)")
    parser.add_argument("--geography", default="US", help="Geography code (e.g., US, US-CA)")
    parser.add_argument("--timeframe", default="today 3-m", help="Timeframe for trends")
    parser.add_argument("--output-json", action="store_true", help="Output as JSON")
    return parser


def main() -> None:
    import argparse
    import sys

    parser = build_parser()
    args = parser.parse_args()

    result = get_google_trends_data(
        keyword=args.keyword,
        geography=args.geography,
        timeframe=args.timeframe,
    )

    if args.output_json:
        print(json.dumps(result, ensure_ascii=True))
    else:
        print(json.dumps(result, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
