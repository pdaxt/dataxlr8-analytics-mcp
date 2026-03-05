use anyhow::Result;
use sqlx::PgPool;

pub async fn setup_schema(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(
        r#"
        CREATE SCHEMA IF NOT EXISTS analytics;

        CREATE TABLE IF NOT EXISTS analytics.events (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            event_type  TEXT NOT NULL,
            user_id     TEXT,
            session_id  TEXT,
            properties  JSONB NOT NULL DEFAULT '{}',
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_events_type ON analytics.events(event_type);
        CREATE INDEX IF NOT EXISTS idx_events_user ON analytics.events(user_id);
        CREATE INDEX IF NOT EXISTS idx_events_session ON analytics.events(session_id);
        CREATE INDEX IF NOT EXISTS idx_events_created ON analytics.events(created_at);
        CREATE INDEX IF NOT EXISTS idx_events_type_created ON analytics.events(event_type, created_at);

        CREATE TABLE IF NOT EXISTS analytics.daily_aggregates (
            date         DATE NOT NULL,
            event_type   TEXT NOT NULL,
            count        BIGINT NOT NULL DEFAULT 0,
            unique_users BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (date, event_type)
        );

        CREATE INDEX IF NOT EXISTS idx_daily_agg_date ON analytics.daily_aggregates(date);
        CREATE INDEX IF NOT EXISTS idx_daily_agg_type ON analytics.daily_aggregates(event_type);
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}
