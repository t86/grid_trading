from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s__%(column_0_N_name)s",
    "uq": "uq_%(table_name)s__%(column_0_N_name)s",
    "ck": "ck_%(table_name)s__%(constraint_name)s",
    "fk": "fk_%(table_name)s__%(column_0_N_name)s__%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = sa.MetaData(naming_convention=NAMING_CONVENTION)

UUID = postgresql.UUID(as_uuid=True)
JSONB = postgresql.JSONB(astext_type=sa.Text())


def uuid_pk() -> sa.Column:
    return sa.Column("id", UUID, primary_key=True, nullable=False, server_default=sa.text("gen_random_uuid()"))


def created_at() -> sa.Column:
    return sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()"))


def updated_at() -> sa.Column:
    return sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()"))


workspaces = sa.Table(
    "workspaces",
    metadata,
    uuid_pk(),
    sa.Column("name", sa.Text(), nullable=False),
    sa.Column("env", sa.Text(), nullable=False),
    sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'active'")),
    created_at(),
    updated_at(),
    sa.UniqueConstraint("name", "env"),
)

accounts = sa.Table(
    "accounts",
    metadata,
    uuid_pk(),
    sa.Column("workspace_id", UUID, sa.ForeignKey("workspaces.id", ondelete="CASCADE"), nullable=False),
    sa.Column("venue", sa.Text(), nullable=False),
    sa.Column("account_name", sa.Text(), nullable=False),
    sa.Column("account_alias", sa.Text(), nullable=False),
    sa.Column("market_type", sa.Text(), nullable=False),
    sa.Column("environment", sa.Text(), nullable=False),
    sa.Column("position_mode", sa.Text(), nullable=True),
    sa.Column("margin_mode", sa.Text(), nullable=True),
    sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'active'")),
    sa.Column("metadata_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    created_at(),
    updated_at(),
    sa.UniqueConstraint("workspace_id", "venue", "environment", "account_alias"),
)

account_secrets = sa.Table(
    "account_secrets",
    metadata,
    uuid_pk(),
    sa.Column("account_id", UUID, sa.ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False),
    sa.Column("secret_ref", sa.Text(), nullable=False),
    sa.Column("key_fingerprint", sa.Text(), nullable=False),
    sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'active'")),
    sa.Column("rotated_at", sa.DateTime(timezone=True), nullable=True),
    created_at(),
    sa.UniqueConstraint("account_id", "key_fingerprint"),
)

strategy_templates = sa.Table(
    "strategy_templates",
    metadata,
    uuid_pk(),
    sa.Column("workspace_id", UUID, sa.ForeignKey("workspaces.id", ondelete="CASCADE"), nullable=False),
    sa.Column("name", sa.Text(), nullable=False),
    sa.Column("code", sa.Text(), nullable=False),
    sa.Column("strategy_family", sa.Text(), nullable=False),
    sa.Column("market_scope", sa.Text(), nullable=False),
    sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'draft'")),
    sa.Column("owner_user_id", UUID, nullable=True),
    created_at(),
    updated_at(),
    sa.UniqueConstraint("workspace_id", "code"),
)

strategy_template_versions = sa.Table(
    "strategy_template_versions",
    metadata,
    uuid_pk(),
    sa.Column("template_id", UUID, sa.ForeignKey("strategy_templates.id", ondelete="CASCADE"), nullable=False),
    sa.Column("version_no", sa.Integer(), nullable=False),
    sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'draft'")),
    sa.Column("param_schema_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column("default_params_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column("risk_policy_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column("description_md", sa.Text(), nullable=True),
    sa.Column("changelog_md", sa.Text(), nullable=True),
    sa.Column("created_by", UUID, nullable=True),
    created_at(),
    sa.UniqueConstraint("template_id", "version_no"),
)

strategy_instances = sa.Table(
    "strategy_instances",
    metadata,
    uuid_pk(),
    sa.Column("workspace_id", UUID, sa.ForeignKey("workspaces.id", ondelete="CASCADE"), nullable=False),
    sa.Column("account_id", UUID, sa.ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False),
    sa.Column("template_id", UUID, sa.ForeignKey("strategy_templates.id", ondelete="RESTRICT"), nullable=False),
    sa.Column("symbol", sa.Text(), nullable=False),
    sa.Column("market_type", sa.Text(), nullable=False),
    sa.Column("instance_name", sa.Text(), nullable=False),
    sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'active'")),
    sa.Column("tags_json", JSONB, nullable=False, server_default=sa.text("'[]'::jsonb")),
    sa.Column("created_by", UUID, nullable=True),
    created_at(),
    updated_at(),
    sa.UniqueConstraint("account_id", "symbol", "instance_name"),
)

instance_config_versions = sa.Table(
    "instance_config_versions",
    metadata,
    uuid_pk(),
    sa.Column("instance_id", UUID, sa.ForeignKey("strategy_instances.id", ondelete="CASCADE"), nullable=False),
    sa.Column("version_no", sa.Integer(), nullable=False),
    sa.Column("override_params_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column("reason", sa.Text(), nullable=True),
    sa.Column("created_by", UUID, nullable=True),
    created_at(),
    sa.UniqueConstraint("instance_id", "version_no"),
)

deployments = sa.Table(
    "deployments",
    metadata,
    uuid_pk(),
    sa.Column("instance_id", UUID, sa.ForeignKey("strategy_instances.id", ondelete="CASCADE"), nullable=False),
    sa.Column(
        "template_version_id",
        UUID,
        sa.ForeignKey("strategy_template_versions.id", ondelete="RESTRICT"),
        nullable=False,
    ),
    sa.Column(
        "instance_config_version_id",
        UUID,
        sa.ForeignKey("instance_config_versions.id", ondelete="RESTRICT"),
        nullable=True,
    ),
    sa.Column("requested_by", UUID, nullable=True),
    sa.Column("approved_by", UUID, nullable=True),
    sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'requested'")),
    sa.Column("preflight_report_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column("effective_config_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    created_at(),
    sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
)

runs = sa.Table(
    "runs",
    metadata,
    uuid_pk(),
    sa.Column("instance_id", UUID, sa.ForeignKey("strategy_instances.id", ondelete="CASCADE"), nullable=False),
    sa.Column("deployment_id", UUID, sa.ForeignKey("deployments.id", ondelete="CASCADE"), nullable=False),
    sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'starting'")),
    sa.Column("start_reason", sa.Text(), nullable=True),
    sa.Column("stop_reason", sa.Text(), nullable=True),
    sa.Column("worker_id", sa.Text(), nullable=True),
    sa.Column("checkpoint_ref", sa.Text(), nullable=True),
    sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True),
)

run_checkpoints = sa.Table(
    "run_checkpoints",
    metadata,
    uuid_pk(),
    sa.Column("run_id", UUID, sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=False),
    sa.Column("checkpoint_seq", sa.BigInteger(), nullable=False),
    sa.Column("state_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column("storage_ref", sa.Text(), nullable=True),
    created_at(),
    sa.UniqueConstraint("run_id", "checkpoint_seq"),
)

order_intents = sa.Table(
    "order_intents",
    metadata,
    uuid_pk(),
    sa.Column("run_id", UUID, sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=False),
    sa.Column("intent_seq", sa.BigInteger(), nullable=False),
    sa.Column("intent_type", sa.Text(), nullable=False),
    sa.Column("side", sa.Text(), nullable=False),
    sa.Column("position_side", sa.Text(), nullable=True),
    sa.Column("symbol", sa.Text(), nullable=False),
    sa.Column("price", sa.Numeric(30, 12), nullable=True),
    sa.Column("qty", sa.Numeric(30, 12), nullable=True),
    sa.Column("notional", sa.Numeric(30, 12), nullable=True),
    sa.Column("reduce_only", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    sa.Column("idempotency_key", sa.Text(), nullable=False),
    sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'created'")),
    sa.Column("reason_code", sa.Text(), nullable=True),
    sa.Column("desired_order_hash", sa.Text(), nullable=True),
    created_at(),
    updated_at(),
    sa.UniqueConstraint("run_id", "intent_seq"),
    sa.UniqueConstraint("idempotency_key"),
)

exchange_orders = sa.Table(
    "exchange_orders",
    metadata,
    uuid_pk(),
    sa.Column("order_intent_id", UUID, sa.ForeignKey("order_intents.id", ondelete="SET NULL"), nullable=True),
    sa.Column("account_id", UUID, sa.ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False),
    sa.Column("venue_order_id", sa.Text(), nullable=False),
    sa.Column("venue_client_order_id", sa.Text(), nullable=True),
    sa.Column("symbol", sa.Text(), nullable=False),
    sa.Column("side", sa.Text(), nullable=False),
    sa.Column("position_side", sa.Text(), nullable=True),
    sa.Column("price", sa.Numeric(30, 12), nullable=True),
    sa.Column("orig_qty", sa.Numeric(30, 12), nullable=True),
    sa.Column("executed_qty", sa.Numeric(30, 12), nullable=False, server_default=sa.text("0")),
    sa.Column("status", sa.Text(), nullable=False),
    sa.Column("venue_status_payload_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    created_at(),
    updated_at(),
    sa.UniqueConstraint("account_id", "venue_order_id"),
    sa.UniqueConstraint("account_id", "venue_client_order_id"),
)

fills = sa.Table(
    "fills",
    metadata,
    uuid_pk(),
    sa.Column("exchange_order_id", UUID, sa.ForeignKey("exchange_orders.id", ondelete="CASCADE"), nullable=False),
    sa.Column("run_id", UUID, sa.ForeignKey("runs.id", ondelete="SET NULL"), nullable=True),
    sa.Column("venue_trade_id", sa.Text(), nullable=False),
    sa.Column("symbol", sa.Text(), nullable=False),
    sa.Column("side", sa.Text(), nullable=False),
    sa.Column("price", sa.Numeric(30, 12), nullable=False),
    sa.Column("qty", sa.Numeric(30, 12), nullable=False),
    sa.Column("quote_qty", sa.Numeric(30, 12), nullable=True),
    sa.Column("realized_pnl", sa.Numeric(30, 12), nullable=True),
    sa.Column("fee", sa.Numeric(30, 12), nullable=True),
    sa.Column("fee_asset", sa.Text(), nullable=True),
    sa.Column("is_maker", sa.Boolean(), nullable=True),
    sa.Column("filled_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("raw_payload_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.UniqueConstraint("exchange_order_id", "venue_trade_id"),
)

position_snapshots = sa.Table(
    "position_snapshots",
    metadata,
    uuid_pk(),
    sa.Column("run_id", UUID, sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=False),
    sa.Column("account_id", UUID, sa.ForeignKey("accounts.id", ondelete="CASCADE"), nullable=False),
    sa.Column("symbol", sa.Text(), nullable=False),
    sa.Column("position_amt", sa.Numeric(30, 12), nullable=False, server_default=sa.text("0")),
    sa.Column("entry_price", sa.Numeric(30, 12), nullable=True),
    sa.Column("break_even_price", sa.Numeric(30, 12), nullable=True),
    sa.Column("unrealized_pnl", sa.Numeric(30, 12), nullable=True),
    sa.Column("wallet_balance", sa.Numeric(30, 12), nullable=True),
    sa.Column("available_balance", sa.Numeric(30, 12), nullable=True),
    sa.Column("captured_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
)

risk_events = sa.Table(
    "risk_events",
    metadata,
    uuid_pk(),
    sa.Column("run_id", UUID, sa.ForeignKey("runs.id", ondelete="CASCADE"), nullable=False),
    sa.Column("instance_id", UUID, sa.ForeignKey("strategy_instances.id", ondelete="CASCADE"), nullable=False),
    sa.Column("severity", sa.Text(), nullable=False),
    sa.Column("risk_type", sa.Text(), nullable=False),
    sa.Column("reason_code", sa.Text(), nullable=False),
    sa.Column("message", sa.Text(), nullable=False),
    sa.Column("details_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column("triggered_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.Column("cleared_at", sa.DateTime(timezone=True), nullable=True),
)

incidents = sa.Table(
    "incidents",
    metadata,
    uuid_pk(),
    sa.Column("workspace_id", UUID, sa.ForeignKey("workspaces.id", ondelete="CASCADE"), nullable=False),
    sa.Column("account_id", UUID, sa.ForeignKey("accounts.id", ondelete="SET NULL"), nullable=True),
    sa.Column("instance_id", UUID, sa.ForeignKey("strategy_instances.id", ondelete="SET NULL"), nullable=True),
    sa.Column("run_id", UUID, sa.ForeignKey("runs.id", ondelete="SET NULL"), nullable=True),
    sa.Column("severity", sa.Text(), nullable=False),
    sa.Column("category", sa.Text(), nullable=False),
    sa.Column("title", sa.Text(), nullable=False),
    sa.Column("status", sa.Text(), nullable=False, server_default=sa.text("'open'")),
    sa.Column("owner_user_id", UUID, nullable=True),
    sa.Column("root_cause_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
)

event_log = sa.Table(
    "event_log",
    metadata,
    uuid_pk(),
    sa.Column("stream_seq", sa.BigInteger(), sa.Identity(always=False), nullable=False, unique=True),
    sa.Column("event_type", sa.Text(), nullable=False),
    sa.Column("schema_version", sa.Integer(), nullable=False, server_default=sa.text("1")),
    sa.Column("workspace_id", UUID, sa.ForeignKey("workspaces.id", ondelete="SET NULL"), nullable=True),
    sa.Column("account_id", UUID, sa.ForeignKey("accounts.id", ondelete="SET NULL"), nullable=True),
    sa.Column("instance_id", UUID, sa.ForeignKey("strategy_instances.id", ondelete="SET NULL"), nullable=True),
    sa.Column("deployment_id", UUID, sa.ForeignKey("deployments.id", ondelete="SET NULL"), nullable=True),
    sa.Column("run_id", UUID, sa.ForeignKey("runs.id", ondelete="SET NULL"), nullable=True),
    sa.Column("trace_id", UUID, nullable=True),
    sa.Column("causation_id", UUID, nullable=True),
    sa.Column("idempotency_key", sa.Text(), nullable=True),
    sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("producer", sa.Text(), nullable=False),
    sa.Column("payload_json", JSONB, nullable=False, server_default=sa.text("'{}'::jsonb")),
    sa.Column("raw_ref", sa.Text(), nullable=True),
    sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
)

run_hourly_facts = sa.Table(
    "run_hourly_facts",
    metadata,
    sa.Column("run_id", UUID, sa.ForeignKey("runs.id", ondelete="CASCADE"), primary_key=True, nullable=False),
    sa.Column("hour_bucket", sa.DateTime(timezone=True), primary_key=True, nullable=False),
    sa.Column("gross_notional", sa.Numeric(30, 12), nullable=False, server_default=sa.text("0")),
    sa.Column("trade_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
    sa.Column("realized_pnl", sa.Numeric(30, 12), nullable=False, server_default=sa.text("0")),
    sa.Column("fee_usdt", sa.Numeric(30, 12), nullable=False, server_default=sa.text("0")),
    sa.Column("funding_fee", sa.Numeric(30, 12), nullable=False, server_default=sa.text("0")),
    sa.Column("net_pnl", sa.Numeric(30, 12), nullable=False, server_default=sa.text("0")),
    sa.Column("maker_ratio", sa.Numeric(12, 8), nullable=True),
    sa.Column("buy_notional", sa.Numeric(30, 12), nullable=False, server_default=sa.text("0")),
    sa.Column("sell_notional", sa.Numeric(30, 12), nullable=False, server_default=sa.text("0")),
)

instance_daily_facts = sa.Table(
    "instance_daily_facts",
    metadata,
    sa.Column(
        "instance_id",
        UUID,
        sa.ForeignKey("strategy_instances.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False,
    ),
    sa.Column("biz_date", sa.Date(), primary_key=True, nullable=False),
    sa.Column("gross_notional", sa.Numeric(30, 12), nullable=False, server_default=sa.text("0")),
    sa.Column("realized_pnl", sa.Numeric(30, 12), nullable=False, server_default=sa.text("0")),
    sa.Column("unrealized_pnl_close", sa.Numeric(30, 12), nullable=False, server_default=sa.text("0")),
    sa.Column("funding_fee", sa.Numeric(30, 12), nullable=False, server_default=sa.text("0")),
    sa.Column("risk_event_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
    sa.Column("incident_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
)

sa.Index("ix_accounts__workspace_id_status", accounts.c.workspace_id, accounts.c.status)
sa.Index("ix_accounts__venue_environment", accounts.c.venue, accounts.c.environment)
sa.Index(
    "ix_strategy_template_versions__template_id_status",
    strategy_template_versions.c.template_id,
    strategy_template_versions.c.status,
)
sa.Index(
    "ix_strategy_instances__account_id_status_symbol",
    strategy_instances.c.account_id,
    strategy_instances.c.status,
    strategy_instances.c.symbol,
)
sa.Index(
    "ix_deployments__instance_id_status_created_at",
    deployments.c.instance_id,
    deployments.c.status,
    deployments.c.created_at,
)
sa.Index("ix_runs__instance_id_status_started_at", runs.c.instance_id, runs.c.status, runs.c.started_at)
sa.Index("ix_runs__deployment_id_started_at", runs.c.deployment_id, runs.c.started_at)
sa.Index("ix_order_intents__run_id_status_created_at", order_intents.c.run_id, order_intents.c.status, order_intents.c.created_at)
sa.Index(
    "ix_exchange_orders__account_id_symbol_status",
    exchange_orders.c.account_id,
    exchange_orders.c.symbol,
    exchange_orders.c.status,
)
sa.Index("ix_fills__run_id_filled_at", fills.c.run_id, fills.c.filled_at)
sa.Index("ix_fills__exchange_order_id_filled_at", fills.c.exchange_order_id, fills.c.filled_at)
sa.Index("ix_position_snapshots__run_id_captured_at", position_snapshots.c.run_id, position_snapshots.c.captured_at)
sa.Index("ix_risk_events__instance_id_triggered_at", risk_events.c.instance_id, risk_events.c.triggered_at)
sa.Index("ix_risk_events__run_id_triggered_at", risk_events.c.run_id, risk_events.c.triggered_at)
sa.Index("ix_incidents__status_severity_opened_at", incidents.c.status, incidents.c.severity, incidents.c.opened_at)
sa.Index("ix_incidents__instance_id_status", incidents.c.instance_id, incidents.c.status)
sa.Index("ix_event_log__event_type_occurred_at", event_log.c.event_type, event_log.c.occurred_at)
sa.Index("ix_event_log__run_id_occurred_at", event_log.c.run_id, event_log.c.occurred_at)
sa.Index("ix_event_log__instance_id_occurred_at", event_log.c.instance_id, event_log.c.occurred_at)
sa.Index("ix_run_hourly_facts__hour_bucket", run_hourly_facts.c.hour_bucket)
sa.Index("ix_instance_daily_facts__biz_date", instance_daily_facts.c.biz_date)
