<div align="center">
  <h1>SQL Query Workshop | OpenEnv Benchmark</h1>
  <p><strong>A rigorous environment for evaluating Agentic AI on iterative SQL query orchestration and performance tuning.</strong></p>
</div>

---

## 📌 Abstract

Standard Text2SQL benchmarks operate on a naive single-shot basis. However, in enterprise environments, data manipulation is inherently iterative. Human engineers author queries, observe execution failures or sub-optimal plans, and refine logic iteratively.

The **SQL Query Workshop** is an OpenEnv v2 benchmark designed to evaluate Large Language Models (LLMs) on this precise multi-step trajectory. Agents are provided a multi-table e-commerce schema, an objective, and an initially flawed SQL payload. They must refine the query through iterative execution, utilizing explicit multidimensional rubrics and `EXPLAIN QUERY PLAN` penalties to achieve correctness and computational efficiency.

## 🚀 Key Features

* **Real-World E-Commerce Schema:** Operates on an interconnected 5-table relational framework (Customers, Orders, Order Items, Products, Reviews).
* **Multi-Step Assessment Engine:** Tracks trajectory efficiency. Agents are penalized for score regression over their 3-attempt lifecycle per task, evaluating reasoning consistency over random exploration.
* **Dimensional Rubric Extraction:** Moving beyond binary validation. The environment scores logic granularly (e.g., partial credit for correct JOIN conditions despite missing GROUP BY aggregations).
* **Execution Plan Analysis (`EXPLAIN`):** Integrates native SQLite `EXPLAIN QUERY PLAN` parsing. Models receive optimization penalties if corrected syntax functionally triggers catastrophic Full Table Scans when Index Seeks are mathematically possible.

## 📋 Evaluation Pipeline

Tasks range dynamically from standard syntax violations to complex logical fallacies commonly found in production systems.

| Task ID | Level | Target Competency | Failure Profile Imposed |
|---|---|---|---|
| `task_01_syntax_fix` | Easy | Syntax & Types | Malformed projections, unquoted string literals |
| `task_02_join_logic` | Medium | Relational Algebra | Erroneous OUTER JOINS restricting cardinal limits, missing ordinals |
| `task_03_aggregation_fix` | Medium | Aggregation Vectors | Floating scalar aggregates unanchored by GROUP BY manifolds |
| `task_04_correlated_subquery` | Hard | Optimization | Correlated nested SELECTs triggering O(n^2) scaling; requires flattened INNER JOINS + HAVING constraints |
| `task_05_multi_table_report` | Hard | Graph Traversal | Implicit cartesian cross-joins disguised via forgotten ON constraints |

---

## 💻 Interactive UI Playground 

Because this environment is natively hosted via FastAPI, it exposes a visually rich, Monaco-powered Integrated Playground right at the root.

To experience the interactive playground yourself, or to expose the API for your Agent frameworks:

### ⚙️ Quick Start (Docker)

```bash
docker build -t sql-query-workshop .
docker run -p 7860:7860 sql-query-workshop
```
*Navigate to `http://127.0.0.1:7860/` for the Developer Playground, and `/docs` for the Swagger API.*

### ⚙️ Quick Start (Local)

```bash
git clone <repo-url>
cd sql-query-workshop
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 7860
```

---

## 🤖 Agent Interaction Signature 

The platform strictly conforms to the OpenEnv `/reset`, `/step`, `/state` paradigm.

### Observation Space
At every step, the agent receives structural feedback regarding failure vectors:
```json
{
  "task_id": "task_03_aggregation_fix",
  "level": "medium",
  "schema_tables": [{ "name": "customers", "columns": ["..."], "sample_rows": ["..."] }],
  "query": "SELECT c.name, COUNT(o.id) ... HAVING revenue > 100",
  "instructions": "This query will fail at runtime. It uses aggregate functions but is missing GROUP BY...",
  "attempt": 2,
  "max_attempts": 3,
  "best_score_so_far": 0.40,
  "previous_attempts": [ { /* historical trajectory JSON */ } ]
}
```

### Response Payload Feedback

When an Action is submitted, the engine issues explicit rubric vectors. This is designed so agents can chain-of-thought (CoT) reason regarding immediate syntax impact:

```json
{
  "error_type": "aggregation_error",
  "message": "Aggregate function misuse: misuse of aggregate function COUNT()",
  "hint": "Columns in SELECT that are not inside aggregate functions must appear in GROUP BY.",
  "rubric": {
    "syntax_valid":     { "score": 0.0, "max_score": 0.20, "passed": false, "detail": "Query raised: misuse of aggregate function COUNT()" },
    "group_by_present": { "score": 0.0, "max_score": 0.20, "passed": false, "detail": "Not evaluated." }
  },
  "optimization_notes": ["WARNING: FULL SCAN TABLE CUSTOMERS DETECTED"]
}
```

## 🏗️ Execution Architecture
1. **Agent issues stringified payload.**
2. **SQLite Runtime executes execution isolation.**
3. **`EXPLAIN QUERY PLAN` traces graph.**
4. **Independent logical rubrics compile partial score.**
5. **Reward differential analyzed for Regression Penalties.**
6. **Payload structured and propagated back to AI.**

---
<div align="center">
  <p><i>Ready for production inference.</i></p>
</div>
