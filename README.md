---
title: SQL Query Workshop
emoji: 📊
colorFrom: blue
colorTo: indigo
sdk: docker
pinned: false
app_port: 7860
---

# SQL Query Workshop Benchmark


This is a custom OpenEnv benchmark testing how well Large Language Models can debug and optimize SQL queries iteratively.

Standard text-to-SQL benchmarks are single-shot. In reality, engineers write a query, check why it failed or why it's slow, and iterate. This project replicates that workflow. The agent is given a 5-table e-commerce database, an objective, and a broken or slow SQL query. It has up to 3 attempts to fix the query based on partial credit evaluation and execution plan analysis.

## Core Features
- Operates on a realistic 5-table schema (Customers, Orders, Order Items, Products, Reviews).
- Multi-step evaluation. Models are penalized if they get worse across their attempts.
- Partial credit scoring. The environment checks for partial logical correctness (e.g., getting the JOIN right but missing the GROUP BY).
- Uses SQLite EXPLAIN QUERY PLAN to penalize models that write functionally correct queries but cause full table scans when index seeks are possible.

## Evaluation Tasks
The environment includes 5 tasks:
- task_01_syntax_fix: Fix basic syntax errors and unquoted strings.
- task_02_join_logic: Fix wrong outer joins and missing ordinals.
- task_03_aggregation_fix: Fix aggregate functions that are missing GROUP BY clauses.
- task_04_correlated_subquery: Optimize a slow correlated subquery into a flat join.
- task_05_multi_table_report: Fix accidental cartesian cross-joins.

## Running the Project Locally

You can spin up the interactive UI playground right away.

Docker approach:
docker build -t sql-query-workshop .
docker run -p 7860:7860 sql-query-workshop

Local approach:
pip install -r requirements.txt
uvicorn server.app:app --host 0.0.0.0 --port 7860

Then visit http://127.0.0.1:7860 to test the environment manually or view the API documentation at /docs.

## Agent API

The platform exposes the standard OpenEnv endpoints: /reset, /step, /state. Response payload supplies specific syntax hints and execution trace warnings so the agent can learn and fix the query in its next attempt.
