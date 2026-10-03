CREATE DATABASE careplus_db;

CREATE TABLE public.support_logs (
    timestamp TIMESTAMP,
    log_level VARCHAR(50),
    component VARCHAR(255),
    ticket_id VARCHAR(50),
    session_id VARCHAR(100),
    ip VARCHAR(50),
    response_time INTEGER,
    cpu DOUBLE PRECISION,
    event_type VARCHAR(100),
    error BOOLEAN,
    user_agent VARCHAR(1000),
    message VARCHAR(MAX),
    debug VARCHAR(MAX)
);

CREATE TABLE public.support_tickets (
    ticket_id VARCHAR(50),
    created_at TIMESTAMP,
    resolved_at TIMESTAMP,
    agent VARCHAR(100),
    priority VARCHAR(50),
    num_interactions INTEGER,
    issue_category VARCHAR(100),
    channel VARCHAR(50),
    status VARCHAR(50)
);
