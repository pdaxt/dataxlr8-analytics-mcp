use dataxlr8_mcp_core::mcp::{error_result, get_i64, get_str, json_result, make_schema};
use dataxlr8_mcp_core::Database;
use rmcp::model::*;
use rmcp::service::{RequestContext, RoleServer};
use rmcp::ServerHandler;
use serde::{Deserialize, Serialize};
use tracing::info;

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
            description: Some("Filter events by type, date range, user_id, session_id. Returns matching events.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "event_type": { "type": "string", "description": "Filter by event type" },
                    "user_id": { "type": "string", "description": "Filter by user ID" },
                    "session_id": { "type": "string", "description": "Filter by session ID" },
                    "start_date": { "type": "string", "description": "Start date (YYYY-MM-DD)" },
                    "end_date": { "type": "string", "description": "End date (YYYY-MM-DD)" },
                    "limit": { "type": "integer", "description": "Max results (default 100)" }
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
            description: Some("Aggregate counts by event type per day. Returns daily_aggregates rows.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "event_type": { "type": "string", "description": "Filter by event type (optional, all types if omitted)" },
                    "start_date": { "type": "string", "description": "Start date (YYYY-MM-DD)" },
                    "end_date": { "type": "string", "description": "End date (YYYY-MM-DD)" }
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
                    "limit": { "type": "integer", "description": "Number of top events (default 10)" }
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
            description: Some("All events for a specific user/contact in chronological order.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "user_id": { "type": "string", "description": "The user/contact ID to get journey for" },
                    "start_date": { "type": "string", "description": "Start date (YYYY-MM-DD)" },
                    "end_date": { "type": "string", "description": "End date (YYYY-MM-DD)" },
                    "limit": { "type": "integer", "description": "Max events (default 500)" }
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
                    "max_days": { "type": "integer", "description": "Max days to track retention (default 7)" }
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
            description: Some("Export filtered events as JSON array. Same filters as query_events but no limit cap.".into()),
            input_schema: make_schema(
                serde_json::json!({
                    "event_type": { "type": "string", "description": "Filter by event type" },
                    "user_id": { "type": "string", "description": "Filter by user ID" },
                    "start_date": { "type": "string", "description": "Start date (YYYY-MM-DD)" },
                    "end_date": { "type": "string", "description": "End date (YYYY-MM-DD)" }
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
        let event_type = match get_str(args, "event_type") {
            Some(t) => t,
            None => return error_result("Missing required parameter: event_type"),
        };
        let user_id = get_str(args, "user_id");
        let session_id = get_str(args, "session_id");
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
                let _ = sqlx::query(
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
                .await;

                info!(event_type = event_type, "Tracked event");
                json_result(&event)
            }
            Err(e) => error_result(&format!("Failed to track event: {e}")),
        }
    }

    async fn handle_query_events(&self, args: &serde_json::Value) -> CallToolResult {
        let event_type = get_str(args, "event_type");
        let user_id = get_str(args, "user_id");
        let session_id = get_str(args, "session_id");
        let start_date = get_str(args, "start_date");
        let end_date = get_str(args, "end_date");
        let limit = get_i64(args, "limit").unwrap_or(100).min(1000);

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
        let _ = bind_idx;

        let mut query = sqlx::query_as::<_, Event>(&sql);
        for b in &binds {
            query = query.bind(b);
        }
        query = query.bind(limit);

        match query.fetch_all(self.db.pool()).await {
            Ok(events) => json_result(&serde_json::json!({
                "count": events.len(),
                "events": events
            })),
            Err(e) => error_result(&format!("Query failed: {e}")),
        }
    }

    async fn handle_funnel_analysis(&self, args: &serde_json::Value) -> CallToolResult {
        let steps: Vec<String> = args
            .get("steps")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();

        if steps.len() < 2 {
            return error_result("Funnel requires at least 2 steps");
        }

        let start_date = get_str(args, "start_date");
        let end_date = get_str(args, "end_date");

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
                Err(e) => return error_result(&format!("Funnel query failed at step {}: {e}", i + 1)),
            }
        }

        json_result(&serde_json::json!({
            "steps": funnel,
            "total_steps": steps.len()
        }))
    }

    async fn handle_daily_metrics(&self, args: &serde_json::Value) -> CallToolResult {
        let event_type = get_str(args, "event_type");
        let start_date = get_str(args, "start_date");
        let end_date = get_str(args, "end_date");

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
            binds.push(format!("{ed}T00:00:00Z"));
        }
        sql.push_str(" GROUP BY created_at::date, event_type ORDER BY date DESC, event_type");

        let mut query = sqlx::query_as::<_, DailyAggregate>(&sql);
        for b in &binds {
            query = query.bind(b);
        }

        match query.fetch_all(self.db.pool()).await {
            Ok(metrics) => json_result(&serde_json::json!({
                "count": metrics.len(),
                "metrics": metrics
            })),
            Err(e) => error_result(&format!("Daily metrics query failed: {e}")),
        }
    }

    async fn handle_top_events(&self, args: &serde_json::Value) -> CallToolResult {
        let start_date = get_str(args, "start_date");
        let end_date = get_str(args, "end_date");
        let limit = get_i64(args, "limit").unwrap_or(10).min(100);

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

        let mut query = sqlx::query_as::<_, EventCount>(&sql);
        for b in &binds {
            query = query.bind(b);
        }
        query = query.bind(limit);

        match query.fetch_all(self.db.pool()).await {
            Ok(top) => json_result(&top),
            Err(e) => error_result(&format!("Top events query failed: {e}")),
        }
    }

    async fn handle_user_journey(&self, args: &serde_json::Value) -> CallToolResult {
        let user_id = match get_str(args, "user_id") {
            Some(u) => u,
            None => return error_result("Missing required parameter: user_id"),
        };
        let start_date = get_str(args, "start_date");
        let end_date = get_str(args, "end_date");
        let limit = get_i64(args, "limit").unwrap_or(500).min(5000);

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

        let mut query = sqlx::query_as::<_, Event>(&sql).bind(&user_id);
        for b in &binds {
            query = query.bind(b);
        }
        query = query.bind(limit);

        match query.fetch_all(self.db.pool()).await {
            Ok(events) => json_result(&serde_json::json!({
                "user_id": user_id,
                "event_count": events.len(),
                "journey": events
            })),
            Err(e) => error_result(&format!("User journey query failed: {e}")),
        }
    }

    async fn handle_retention_cohort(&self, args: &serde_json::Value) -> CallToolResult {
        let start_date = get_str(args, "start_date").unwrap_or_else(|| "2020-01-01".into());
        let end_date = get_str(args, "end_date").unwrap_or_else(|| "2099-12-31".into());
        let max_days = get_i64(args, "max_days").unwrap_or(7).min(30) as i32;

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
            Ok(rows) => json_result(&serde_json::json!({
                "cohort_rows": rows.len(),
                "max_days": max_days,
                "cohorts": rows
            })),
            Err(e) => error_result(&format!("Retention query failed: {e}")),
        }
    }

    async fn handle_export_events(&self, args: &serde_json::Value) -> CallToolResult {
        let event_type = get_str(args, "event_type");
        let user_id = get_str(args, "user_id");
        let start_date = get_str(args, "start_date");
        let end_date = get_str(args, "end_date");

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
            binds.push(format!("{ed}T00:00:00Z"));
        }
        sql.push_str(" ORDER BY created_at ASC");

        let mut query = sqlx::query_as::<_, Event>(&sql);
        for b in &binds {
            query = query.bind(b);
        }

        match query.fetch_all(self.db.pool()).await {
            Ok(events) => {
                info!(count = events.len(), "Exported events");
                json_result(&serde_json::json!({
                    "exported": events.len(),
                    "events": events
                }))
            }
            Err(e) => error_result(&format!("Export failed: {e}")),
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
                _ => error_result(&format!("Unknown tool: {}", request.name)),
            };

            Ok(result)
        }
    }
}
