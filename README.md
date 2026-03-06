# dataxlr8-analytics-mcp

Event tracking and analytics for DataXLR8. Track user interactions, analyze funnels, monitor retention, and export events for downstream analysis.

## Tools

| Tool | Description |
|------|-------------|
| track_event | Log an analytics event with optional properties JSONB (page_view, button_click, api_call, email_open, etc) |
| query_events | Filter events by type, date range, user_id, session_id. Returns matching events with pagination. |
| funnel_analysis | Given ordered event types, show conversion between steps. Shows how many unique users completed each step. |
| daily_metrics | Aggregate counts by event type per day with pagination. Returns daily_aggregates rows. |
| top_events | Most frequent events in a time range, ordered by count descending. |
| user_journey | All events for a specific user/contact in chronological order with pagination. |
| retention_cohort | Day-0 to day-N retention by cohort. Groups users by first event date and tracks how many return on subsequent days. |
| export_events | Export filtered events as JSON array with pagination. |

## Setup

```bash
DATABASE_URL=postgres://dataxlr8:dataxlr8@localhost:5432/dataxlr8 cargo run
```

## Schema

Creates `analytics.*` schema in PostgreSQL:
- `events` - Event log with type, user_id, session_id, and JSONB properties
- `daily_aggregates` - Per-day counts by event type with unique user tracking

## Part of

[DataXLR8](https://github.com/pdaxt) - AI-powered recruitment platform
