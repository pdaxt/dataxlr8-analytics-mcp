use dataxlr8_mcp_core::mcp::{error_result, get_i64, get_str, json_result, make_schema};
use dataxlr8_mcp_core::Database;
use rmcp::model::*;
use rmcp::service::{RequestContext, RoleServer};
use rmcp::ServerHandler;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 1000;
const DEFAULT_TOP_LIMIT: i64 = 10;
const MAX_TOP_LIMIT: i64 = 100;
const DEFAULT_JOURNEY_LIMIT: i64 = 500;
const MAX_JOURNEY_LIMIT: i64 = 5000;
const DEFAULT_EXPORT_LIMIT: i64 = 10000;
const MAX_EXPORT_LIMIT: i64 = 50000;
const MAX_RETENTION_DAYS: i64 = 30;
const DEFAULT_RETENTION_DAYS: i64 = 7;
const MAX_STRING_LEN: usize = 500;
const MAX_EVENT_TYPE_LEN: usize = 100;

// ============================================================================
// Validation helpers
// ============================================================================

/// Trim a string input and return None if it becomes empty.
fn trim_str(args: &serde_json::Value, key: &str) -> Option<String> {
    get_str(args, key).map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

/// Validate a date string is in YYYY-MM-DD format.
fn validate_date(date: &str) -> Result<(), String> {
    chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .map(|_| ())
        .map_err(|_| format!("Invalid date format '{}'. Expected YYYY-MM-DD.", date))
}

/// Validate a string doesn't exceed a maximum length.
fn validate_length(value: &str, field: &str, max: usize) -> Result<(), String> {
    if value.len() > max {
        Err(format!("{field} exceeds maximum length of {max} characters"))
    } else {
        Ok(())
    }
}

/// Clamp a limit value to [1, max] with a default.
fn clamp_limit(args: &serde_json::Value, default: i64, max: i64) -> i64 {
    get_i64(args, "limit").unwrap_or(default).max(1).min(max)
}

/// Get offset, defaulting to 0 and clamping to >= 0.
fn get_offset(args: &serde_json::Value) -> i64 {
    get_i64(args, "offset").unwrap_or(0).max(0)
}

// ============================================================================
// Data types
// ============================================================================

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct Event {
    pub id: uuid::Uuid,
    pub event_type: String,
    pub user_id: Option<String>,
    pub session_id: Option<String>,
    pub properties: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct DailyAggregate {
    pub date: chrono::NaiveDate,
    pub event_type: String,
    pub count: i64,
    pub unique_users: i64,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct EventCount {
    pub event_type: String,
    pub count: i64,
}

#[derive(Debug, Serialize)]
pub struct FunnelStep {
    pub step: usize,
    pub event_type: String,
    pub users: i64,
    pub conversion_pct: f64,
}

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct CohortRow {
    pub cohort_date: chrono::NaiveDate,
    pub day_offset: i32,
    pub retained_users: i64,
}

// ============================================================================
// Tool definitions
// ============================================================================

fn build_tools() -> Vec<Tool> {
    vec![
        Tool {
            name: "track_event".into(),
            title: None,
            description: Some("Log an analytics event with optional properties JSONB (page_view, button_click, api_call, email_open, etc)".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "event_type": { "type": "string", "description": "Event type (e.g. page_view, button_click, api_call, email_open)" },
                    "user_id": { "type": "string", "description": "User or contact identifier" },
                    "session_id": { "type": "string", "description": "Session identifier" },
                    "properties": { "type": "object", "description": "Arbitrary JSONB properties for the event" }
                }),
                vec!["event_type"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "query_events".into(),
            title: None,
            description: Some("Filter events by type, date range, user_id, session_id. Returns matching events with pagination.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "event_type": { "type": "string", "description": "Filter by event type" },
                    "user_id": { "type": "string", "description": "Filter by user ID" },
                    "session_id": { "type": "string", "description": "Filter by session ID" },
                    "start_date": { "type": "string", "description": "Start date (YYYY-MM-DD)" },
                    "end_date": { "type": "string", "description": "End date (YYYY-MM-DD)" },
                    "limit": { "type": "integer", "description": "Max results (default 100, max 1000)" },
                    "offset": { "type": "integer", "description": "Offset for pagination (default 0)" }
                }),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "funnel_analysis".into(),
            title: None,
            description: Some("Given ordered event types, show conversion between steps. Shows how many unique users completed each step.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "steps": { "type": "array", "items": { "type": "string" }, "description": "Ordered list of event types forming the funnel" },
                    "start_date": { "type": "string", "description": "Start date (YYYY-MM-DD)" },
                    "end_date": { "type": "string", "description": "End date (YYYY-MM-DD)" }
                }),
                vec!["steps"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "daily_metrics".into(),
            title: None,
            description: Some("Aggregate counts by event type per day with pagination. Returns daily_aggregates rows.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "event_type": { "type": "string", "description": "Filter by event type (optional, all types if omitted)" },
                    "start_date": { "type": "string", "description": "Start date (YYYY-MM-DD)" },
                    "end_date": { "type": "string", "description": "End date (YYYY-MM-DD)" },
                    "limit": { "type": "integer", "description": "Max results (default 100, max 1000)" },
                    "offset": { "type": "integer", "description": "Offset for pagination (default 0)" }
                }),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "top_events".into(),
            title: None,
            description: Some("Most frequent events in a time range, ordered by count descending.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "start_date": { "type": "string", "description": "Start date (YYYY-MM-DD)" },
                    "end_date": { "type": "string", "description": "End date (YYYY-MM-DD)" },
                    "limit": { "type": "integer", "description": "Number of top events (default 10, max 100)" },
                    "offset": { "type": "integer", "description": "Offset for pagination (default 0)" }
                }),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "user_journey".into(),
            title: None,
            description: Some("All events for a specific user/contact in chronological order with pagination.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "user_id": { "type": "string", "description": "The user/contact ID to get journey for" },
                    "start_date": { "type": "string", "description": "Start date (YYYY-MM-DD)" },
                    "end_date": { "type": "string", "description": "End date (YYYY-MM-DD)" },
                    "limit": { "type": "integer", "description": "Max events (default 500, max 5000)" },
                    "offset": { "type": "integer", "description": "Offset for pagination (default 0)" }
                }),
                vec!["user_id"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "retention_cohort".into(),
            title: None,
            description: Some("Day-0 to day-N retention by cohort. Groups users by first event date and tracks how many return on subsequent days.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "start_date": { "type": "string", "description": "Cohort start date (YYYY-MM-DD)" },
                    "end_date": { "type": "string", "description": "Cohort end date (YYYY-MM-DD)" },
                    "max_days": { "type": "integer", "description": "Max days to track retention (default 7, max 30)" }
                }),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "export_events".into(),
            title: None,
            description: Some("Export filtered events as JSON array with pagination.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "event_type": { "type": "string", "description": "Filter by event type" },
                    "user_id": { "type": "string", "description": "Filter by user ID" },
                    "start_date": { "type": "string", "description": "Start date (YYYY-MM-DD)" },
                    "end_date": { "type": "string", "description": "End date (YYYY-MM-DD)" },
                    "limit": { "type": "integer", "description": "Max results (default 10000, max 50000)" },
                    "offset": { "type": "integer", "description": "Offset for pagination (default 0)" }
                }),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
    ]
}

// ============================================================================
// MCP Server
// ============================================================================

#[derive(Clone)]
pub struct AnalyticsMcpServer {
    db: Database,
}

impl AnalyticsMcpServer {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    // ---- Tool handlers ----

    async fn handle_track_event(&self, args: &serde_json::Value) -> CallToolResult {
        let event_type = match trim_str(args, "event_type") {
            Some(t) => t,
            None => {
                warn!("track_event called without event_type");
                return error_result("Missing required parameter: event_type");
            }
        };
        if let Err(e) = validate_length(&event_type, "event_type", MAX_EVENT_TYPE_LEN) {
            return error_result(&e);
        }

        let user_id = trim_str(args, "user_id");
        if let Some(ref uid) = user_id {
            if let Err(e) = validate_length(uid, "user_id", MAX_STRING_LEN) {
                return error_result(&e);
            }
        }

        let session_id = trim_str(args, "session_id");
        if let Some(ref sid) = session_id {
            if let Err(e) = validate_length(sid, "session_id", MAX_STRING_LEN) {
                return error_result(&e);
            }
        }

        let properties = args
            .get("properties")
            .cloned()
            .unwrap_or(serde_json::json!({}));

        match sqlx::query_as::<_, Event>(
            r#"INSERT INTO analytics.events (event_type, user_id, session_id, properties)
               VALUES ($1, $2, $3, $4)
               RETURNING *"#,
        )
        .bind(&event_type)
        .bind(&user_id)
        .bind(&session_id)
        .bind(&properties)
        .fetch_one(self.db.pool())
        .await
        {
            Ok(event) => {
                // Upsert daily aggregate
                if let Err(e) = sqlx::query(
                    r#"INSERT INTO analytics.daily_aggregates (date, event_type, count, unique_users)
                       VALUES (CURRENT_DATE, $1, 1, CASE WHEN $2::text IS NOT NULL THEN 1 ELSE 0 END)
                       ON CONFLICT (date, event_type)
                       DO UPDATE SET
                           count = analytics.daily_aggregates.count + 1,
                           unique_users = (
                               SELECT COUNT(DISTINCT user_id)
                               FROM analytics.events
                               WHERE event_type = $1
                                 AND created_at::date = CURRENT_DATE
                                 AND user_id IS NOT NULL
                           )"#,
                )
                .bind(&event_type)
                .bind(&user_id)
                .execute(self.db.pool())
                .await
                {
                    error!(error = %e, event_type = %event_type, "Failed to upsert daily aggregate");
                }

                info!(event_type = %event_type, id = %event.id, "Tracked event");
                json_result(&event)
            }
            Err(e) => {
                error!(error = %e, event_type = %event_type, "Failed to insert event");
                error_result(&format!("Failed to track event: {e}"))
            }
        }
    }

    async fn handle_query_events(&self, args: &serde_json::Value) -> CallToolResult {
        let event_type = trim_str(args, "event_type");
        let user_id = trim_str(args, "user_id");
        let session_id = trim_str(args, "session_id");
        let start_date = trim_str(args, "start_date");
        let end_date = trim_str(args, "end_date");
        let limit = clamp_limit(args, DEFAULT_LIMIT, MAX_LIMIT);
        let offset = get_offset(args);

        // Validate string lengths
        if let Some(ref et) = event_type {
            if let Err(e) = validate_length(et, "event_type", MAX_EVENT_TYPE_LEN) {
                return error_result(&e);
            }
        }
        if let Some(ref uid) = user_id {
            if let Err(e) = validate_length(uid, "user_id", MAX_STRING_LEN) {
                return error_result(&e);
            }
        }
        if let Some(ref sid) = session_id {
            if let Err(e) = validate_length(sid, "session_id", MAX_STRING_LEN) {
                return error_result(&e);
            }
        }

        // Validate date formats
        if let Some(ref sd) = start_date {
            if let Err(e) = validate_date(sd) {
                return error_result(&e);
            }
        }
        if let Some(ref ed) = end_date {
            if let Err(e) = validate_date(ed) {
                return error_result(&e);
            }
        }

        let mut sql = String::from("SELECT * FROM analytics.events WHERE 1=1");
        let mut bind_idx = 1u32;
        let mut binds: Vec<String> = Vec::new();

        if let Some(ref et) = event_type {
            sql.push_str(&format!(" AND event_type = ${bind_idx}"));
            bind_idx += 1;
            binds.push(et.clone());
        }
        if let Some(ref uid) = user_id {
            sql.push_str(&format!(" AND user_id = ${bind_idx}"));
            bind_idx += 1;
            binds.push(uid.clone());
        }
        if let Some(ref sid) = session_id {
            sql.push_str(&format!(" AND session_id = ${bind_idx}"));
            bind_idx += 1;
            binds.push(sid.clone());
        }
        if let Some(ref sd) = start_date {
            sql.push_str(&format!(" AND created_at >= ${bind_idx}::timestamptz"));
            bind_idx += 1;
            binds.push(format!("{sd}T00:00:00Z"));
        }
        if let Some(ref ed) = end_date {
            sql.push_str(&format!(" AND created_at < ${bind_idx}::timestamptz"));
            bind_idx += 1;
            binds.push(format!("{ed}T00:00:00Z") + " + interval '1 day'");
        }
        sql.push_str(&format!(" ORDER BY created_at DESC LIMIT ${bind_idx}"));
        bind_idx += 1;
        sql.push_str(&format!(" OFFSET ${bind_idx}"));

        let mut query = sqlx::query_as::<_, Event>(&sql);
        for b in &binds {
            query = query.bind(b);
        }
        query = query.bind(limit);
        query = query.bind(offset);

        match query.fetch_all(self.db.pool()).await {
            Ok(events) => {
                info!(count = events.len(), limit, offset, "query_events returned results");
                json_result(&serde_json::json!({
                    "count": events.len(),
                    "limit": limit,
                    "offset": offset,
                    "events": events
                }))
            }
            Err(e) => {
                error!(error = %e, "query_events failed");
                error_result(&format!("Query failed: {e}"))
            }
        }
    }

    async fn handle_funnel_analysis(&self, args: &serde_json::Value) -> CallToolResult {
        let steps: Vec<String> = args
            .get("steps")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.trim().to_string()))
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        if steps.len() < 2 {
            warn!(step_count = steps.len(), "funnel_analysis requires at least 2 steps");
            return error_result("Funnel requires at least 2 steps");
        }

        // Validate step names
        for step in &steps {
            if let Err(e) = validate_length(step, "step event_type", MAX_EVENT_TYPE_LEN) {
                return error_result(&e);
            }
        }

        let start_date = trim_str(args, "start_date");
        let end_date = trim_str(args, "end_date");

        // Validate date formats
        if let Some(ref sd) = start_date {
            if let Err(e) = validate_date(sd) {
                return error_result(&e);
            }
        }
        if let Some(ref ed) = end_date {
            if let Err(e) = validate_date(ed) {
                return error_result(&e);
            }
        }

        let mut date_filter = String::new();
        let mut date_binds: Vec<String> = Vec::new();
        // We'll use bind index starting at 2 (1 is for event_type)
        let mut bind_offset = 2u32;
        if let Some(ref sd) = start_date {
            date_filter.push_str(&format!(" AND created_at >= ${bind_offset}::timestamptz"));
            bind_offset += 1;
            date_binds.push(format!("{sd}T00:00:00Z"));
        }
        if let Some(ref ed) = end_date {
            date_filter.push_str(&format!(" AND created_at < ${bind_offset}::timestamptz + interval '1 day'"));
            date_binds.push(format!("{ed}T00:00:00Z"));
        }

        let mut funnel: Vec<FunnelStep> = Vec::new();
        let mut prev_users: i64 = 0;

        for (i, step) in steps.iter().enumerate() {
            let sql = format!(
                "SELECT COUNT(DISTINCT user_id) as cnt FROM analytics.events WHERE event_type = $1 AND user_id IS NOT NULL{date_filter}"
            );

            let mut query = sqlx::query_as::<_, (i64,)>(&sql).bind(step);
            for db in &date_binds {
                query = query.bind(db);
            }

            match query.fetch_one(self.db.pool()).await {
                Ok((cnt,)) => {
                    let conversion = if i == 0 {
                        100.0
                    } else if prev_users > 0 {
                        (cnt as f64 / prev_users as f64) * 100.0
                    } else {
                        0.0
                    };
                    if i == 0 {
                        prev_users = cnt;
                    }
                    funnel.push(FunnelStep {
                        step: i + 1,
                        event_type: step.clone(),
                        users: cnt,
                        conversion_pct: (conversion * 100.0).round() / 100.0,
                    });
                    if i > 0 {
                        prev_users = cnt;
                    }
                }
                Err(e) => {
                    error!(error = %e, step_index = i + 1, step_name = %step, "Funnel query failed at step");
                    return error_result(&format!("Funnel query failed at step {}: {e}", i + 1));
                }
            }
        }

        info!(step_count = steps.len(), "funnel_analysis completed");
        json_result(&serde_json::json!({
            "steps": funnel,
            "total_steps": steps.len()
        }))
    }

    async fn handle_daily_metrics(&self, args: &serde_json::Value) -> CallToolResult {
        let event_type = trim_str(args, "event_type");
        let start_date = trim_str(args, "start_date");
        let end_date = trim_str(args, "end_date");
        let limit = clamp_limit(args, DEFAULT_LIMIT, MAX_LIMIT);
        let offset = get_offset(args);

        // Validate inputs
        if let Some(ref et) = event_type {
            if let Err(e) = validate_length(et, "event_type", MAX_EVENT_TYPE_LEN) {
                return error_result(&e);
            }
        }
        if let Some(ref sd) = start_date {
            if let Err(e) = validate_date(sd) {
                return error_result(&e);
            }
        }
        if let Some(ref ed) = end_date {
            if let Err(e) = validate_date(ed) {
                return error_result(&e);
            }
        }

        // Compute from raw events for accuracy
        let mut sql = String::from(
            r#"SELECT created_at::date as date, event_type,
                      COUNT(*) as count,
                      COUNT(DISTINCT user_id) FILTER (WHERE user_id IS NOT NULL) as unique_users
               FROM analytics.events WHERE 1=1"#,
        );
        let mut bind_idx = 1u32;
        let mut binds: Vec<String> = Vec::new();

        if let Some(ref et) = event_type {
            sql.push_str(&format!(" AND event_type = ${bind_idx}"));
            bind_idx += 1;
            binds.push(et.clone());
        }
        if let Some(ref sd) = start_date {
            sql.push_str(&format!(" AND created_at >= ${bind_idx}::timestamptz"));
            bind_idx += 1;
            binds.push(format!("{sd}T00:00:00Z"));
        }
        if let Some(ref ed) = end_date {
            sql.push_str(&format!(" AND created_at < ${bind_idx}::timestamptz + interval '1 day'"));
            bind_idx += 1;
            binds.push(format!("{ed}T00:00:00Z"));
        }
        sql.push_str(&format!(" GROUP BY created_at::date, event_type ORDER BY date DESC, event_type LIMIT ${bind_idx}"));
        bind_idx += 1;
        sql.push_str(&format!(" OFFSET ${bind_idx}"));

        let mut query = sqlx::query_as::<_, DailyAggregate>(&sql);
        for b in &binds {
            query = query.bind(b);
        }
        query = query.bind(limit);
        query = query.bind(offset);

        match query.fetch_all(self.db.pool()).await {
            Ok(metrics) => {
                info!(count = metrics.len(), limit, offset, "daily_metrics returned results");
                json_result(&serde_json::json!({
                    "count": metrics.len(),
                    "limit": limit,
                    "offset": offset,
                    "metrics": metrics
                }))
            }
            Err(e) => {
                error!(error = %e, "daily_metrics query failed");
                error_result(&format!("Daily metrics query failed: {e}"))
            }
        }
    }

    async fn handle_top_events(&self, args: &serde_json::Value) -> CallToolResult {
        let start_date = trim_str(args, "start_date");
        let end_date = trim_str(args, "end_date");
        let limit = clamp_limit(args, DEFAULT_TOP_LIMIT, MAX_TOP_LIMIT);
        let offset = get_offset(args);

        // Validate date formats
        if let Some(ref sd) = start_date {
            if let Err(e) = validate_date(sd) {
                return error_result(&e);
            }
        }
        if let Some(ref ed) = end_date {
            if let Err(e) = validate_date(ed) {
                return error_result(&e);
            }
        }

        let mut sql = String::from(
            "SELECT event_type, COUNT(*) as count FROM analytics.events WHERE 1=1",
        );
        let mut bind_idx = 1u32;
        let mut binds: Vec<String> = Vec::new();

        if let Some(ref sd) = start_date {
            sql.push_str(&format!(" AND created_at >= ${bind_idx}::timestamptz"));
            bind_idx += 1;
            binds.push(format!("{sd}T00:00:00Z"));
        }
        if let Some(ref ed) = end_date {
            sql.push_str(&format!(" AND created_at < ${bind_idx}::timestamptz + interval '1 day'"));
            bind_idx += 1;
            binds.push(format!("{ed}T00:00:00Z"));
        }
        sql.push_str(&format!(" GROUP BY event_type ORDER BY count DESC LIMIT ${bind_idx}"));
        bind_idx += 1;
        sql.push_str(&format!(" OFFSET ${bind_idx}"));

        let mut query = sqlx::query_as::<_, EventCount>(&sql);
        for b in &binds {
            query = query.bind(b);
        }
        query = query.bind(limit);
        query = query.bind(offset);

        match query.fetch_all(self.db.pool()).await {
            Ok(top) => {
                info!(count = top.len(), limit, offset, "top_events returned results");
                json_result(&serde_json::json!({
                    "count": top.len(),
                    "limit": limit,
                    "offset": offset,
                    "events": top
                }))
            }
            Err(e) => {
                error!(error = %e, "top_events query failed");
                error_result(&format!("Top events query failed: {e}"))
            }
        }
    }

    async fn handle_user_journey(&self, args: &serde_json::Value) -> CallToolResult {
        let user_id = match trim_str(args, "user_id") {
            Some(u) => u,
            None => {
                warn!("user_journey called without user_id");
                return error_result("Missing required parameter: user_id");
            }
        };
        if let Err(e) = validate_length(&user_id, "user_id", MAX_STRING_LEN) {
            return error_result(&e);
        }

        let start_date = trim_str(args, "start_date");
        let end_date = trim_str(args, "end_date");
        let limit = clamp_limit(args, DEFAULT_JOURNEY_LIMIT, MAX_JOURNEY_LIMIT);
        let offset = get_offset(args);

        // Validate date formats
        if let Some(ref sd) = start_date {
            if let Err(e) = validate_date(sd) {
                return error_result(&e);
            }
        }
        if let Some(ref ed) = end_date {
            if let Err(e) = validate_date(ed) {
                return error_result(&e);
            }
        }

        let mut sql = String::from("SELECT * FROM analytics.events WHERE user_id = $1");
        let mut bind_idx = 2u32;
        let mut binds: Vec<String> = Vec::new();

        if let Some(ref sd) = start_date {
            sql.push_str(&format!(" AND created_at >= ${bind_idx}::timestamptz"));
            bind_idx += 1;
            binds.push(format!("{sd}T00:00:00Z"));
        }
        if let Some(ref ed) = end_date {
            sql.push_str(&format!(" AND created_at < ${bind_idx}::timestamptz + interval '1 day'"));
            bind_idx += 1;
            binds.push(format!("{ed}T00:00:00Z"));
        }
        sql.push_str(&format!(" ORDER BY created_at ASC LIMIT ${bind_idx}"));
        bind_idx += 1;
        sql.push_str(&format!(" OFFSET ${bind_idx}"));

        let mut query = sqlx::query_as::<_, Event>(&sql).bind(&user_id);
        for b in &binds {
            query = query.bind(b);
        }
        query = query.bind(limit);
        query = query.bind(offset);

        match query.fetch_all(self.db.pool()).await {
            Ok(events) => {
                info!(user_id = %user_id, count = events.len(), limit, offset, "user_journey returned results");
                json_result(&serde_json::json!({
                    "user_id": user_id,
                    "event_count": events.len(),
                    "limit": limit,
                    "offset": offset,
                    "journey": events
                }))
            }
            Err(e) => {
                error!(error = %e, user_id = %user_id, "user_journey query failed");
                error_result(&format!("User journey query failed: {e}"))
            }
        }
    }

    async fn handle_retention_cohort(&self, args: &serde_json::Value) -> CallToolResult {
        let start_date = trim_str(args, "start_date").unwrap_or_else(|| "2020-01-01".into());
        let end_date = trim_str(args, "end_date").unwrap_or_else(|| "2099-12-31".into());
        let max_days = get_i64(args, "max_days")
            .unwrap_or(DEFAULT_RETENTION_DAYS)
            .max(1)
            .min(MAX_RETENTION_DAYS) as i32;

        // Validate date formats
        if let Err(e) = validate_date(&start_date) {
            return error_result(&e);
        }
        if let Err(e) = validate_date(&end_date) {
            return error_result(&e);
        }

        let sql = r#"
            WITH first_seen AS (
                SELECT user_id, MIN(created_at::date) as cohort_date
                FROM analytics.events
                WHERE user_id IS NOT NULL
                  AND created_at >= $1::date
                  AND created_at < $2::date + interval '1 day'
                GROUP BY user_id
            ),
            retention AS (
                SELECT
                    fs.cohort_date,
                    (e.created_at::date - fs.cohort_date) as day_offset,
                    COUNT(DISTINCT e.user_id) as retained_users
                FROM first_seen fs
                JOIN analytics.events e ON e.user_id = fs.user_id
                WHERE (e.created_at::date - fs.cohort_date) <= $3
                  AND (e.created_at::date - fs.cohort_date) >= 0
                GROUP BY fs.cohort_date, (e.created_at::date - fs.cohort_date)
            )
            SELECT cohort_date, day_offset::int4, retained_users
            FROM retention
            ORDER BY cohort_date, day_offset
        "#;

        match sqlx::query_as::<_, CohortRow>(sql)
            .bind(&start_date)
            .bind(&end_date)
            .bind(max_days)
            .fetch_all(self.db.pool())
            .await
        {
            Ok(rows) => {
                info!(cohort_rows = rows.len(), max_days, "retention_cohort completed");
                json_result(&serde_json::json!({
                    "cohort_rows": rows.len(),
                    "max_days": max_days,
                    "cohorts": rows
                }))
            }
            Err(e) => {
                error!(error = %e, "retention_cohort query failed");
                error_result(&format!("Retention query failed: {e}"))
            }
        }
    }

    async fn handle_export_events(&self, args: &serde_json::Value) -> CallToolResult {
        let event_type = trim_str(args, "event_type");
        let user_id = trim_str(args, "user_id");
        let start_date = trim_str(args, "start_date");
        let end_date = trim_str(args, "end_date");
        let limit = clamp_limit(args, DEFAULT_EXPORT_LIMIT, MAX_EXPORT_LIMIT);
        let offset = get_offset(args);

        // Validate string lengths
        if let Some(ref et) = event_type {
            if let Err(e) = validate_length(et, "event_type", MAX_EVENT_TYPE_LEN) {
                return error_result(&e);
            }
        }
        if let Some(ref uid) = user_id {
            if let Err(e) = validate_length(uid, "user_id", MAX_STRING_LEN) {
                return error_result(&e);
            }
        }

        // Validate date formats
        if let Some(ref sd) = start_date {
            if let Err(e) = validate_date(sd) {
                return error_result(&e);
            }
        }
        if let Some(ref ed) = end_date {
            if let Err(e) = validate_date(ed) {
                return error_result(&e);
            }
        }

        let mut sql = String::from("SELECT * FROM analytics.events WHERE 1=1");
        let mut bind_idx = 1u32;
        let mut binds: Vec<String> = Vec::new();

        if let Some(ref et) = event_type {
            sql.push_str(&format!(" AND event_type = ${bind_idx}"));
            bind_idx += 1;
            binds.push(et.clone());
        }
        if let Some(ref uid) = user_id {
            sql.push_str(&format!(" AND user_id = ${bind_idx}"));
            bind_idx += 1;
            binds.push(uid.clone());
        }
        if let Some(ref sd) = start_date {
            sql.push_str(&format!(" AND created_at >= ${bind_idx}::timestamptz"));
            bind_idx += 1;
            binds.push(format!("{sd}T00:00:00Z"));
        }
        if let Some(ref ed) = end_date {
            sql.push_str(&format!(" AND created_at < ${bind_idx}::timestamptz + interval '1 day'"));
            bind_idx += 1;
            binds.push(format!("{ed}T00:00:00Z"));
        }
        sql.push_str(&format!(" ORDER BY created_at ASC LIMIT ${bind_idx}"));
        bind_idx += 1;
        sql.push_str(&format!(" OFFSET ${bind_idx}"));

        let mut query = sqlx::query_as::<_, Event>(&sql);
        for b in &binds {
            query = query.bind(b);
        }
        query = query.bind(limit);
        query = query.bind(offset);

        match query.fetch_all(self.db.pool()).await {
            Ok(events) => {
                info!(count = events.len(), limit, offset, "Exported events");
                json_result(&serde_json::json!({
                    "exported": events.len(),
                    "limit": limit,
                    "offset": offset,
                    "events": events
                }))
            }
            Err(e) => {
                error!(error = %e, "export_events failed");
                error_result(&format!("Export failed: {e}"))
            }
        }
    }
}

// ============================================================================
// ServerHandler trait implementation
// ============================================================================

impl ServerHandler for AnalyticsMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "DataXLR8 Analytics MCP — track events, analyze funnels, retention cohorts, and user journeys"
                    .into(),
            ),
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_ {
        async {
            Ok(ListToolsResult {
                tools: build_tools(),
                next_cursor: None,
                meta: None,
            })
        }
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_ {
        async move {
            let args = serde_json::to_value(&request.arguments).unwrap_or(serde_json::Value::Null);
            let name_str: &str = request.name.as_ref();

            let result = match name_str {
                "track_event" => self.handle_track_event(&args).await,
                "query_events" => self.handle_query_events(&args).await,
                "funnel_analysis" => self.handle_funnel_analysis(&args).await,
                "daily_metrics" => self.handle_daily_metrics(&args).await,
                "top_events" => self.handle_top_events(&args).await,
                "user_journey" => self.handle_user_journey(&args).await,
                "retention_cohort" => self.handle_retention_cohort(&args).await,
                "export_events" => self.handle_export_events(&args).await,
                _ => {
                    warn!(tool = %request.name, "Unknown tool called");
                    error_result(&format!("Unknown tool: {}", request.name))
                }
            };

            Ok(result)
        }
    }
}
