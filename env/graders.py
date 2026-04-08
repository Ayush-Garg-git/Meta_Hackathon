import sqlite3
import re
import time
from env.models import StructuredFeedback, RubricItem
from env.tasks import SCHEMA_DDL, SEED_SQL


def _fresh_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_DDL)
    conn.executescript(SEED_SQL)
    return conn


def _execute_safe(conn: sqlite3.Connection, query: str) -> tuple[list | None, str | None, float]:
    try:
        t0 = time.perf_counter()
        cursor = conn.execute(query)
        rows = cursor.fetchall()
        elapsed_ms = (time.perf_counter() - t0) * 1000
        return rows, None, elapsed_ms
    except Exception as exc:
        return None, str(exc), 0.0


def _normalize(query: str) -> str:
    return re.sub(r"\s+", " ", query.strip().lower())


def _get_query_plan(conn: sqlite3.Connection, query: str) -> list[str]:
    try:
        cursor = conn.execute(f"EXPLAIN QUERY PLAN {query}")
        return [" ".join(str(v) for v in row) for row in cursor.fetchall()]
    except Exception:
        return []


def _analyze_plan(plan_lines: list[str]) -> tuple[list[str], float]:
    notes = []
    plan_penalty = 0.0
    full_scan_count = sum(1 for line in plan_lines if "SCAN" in line.upper() and "SEARCH" not in line.upper())
    search_count = sum(1 for line in plan_lines if "SEARCH" in line.upper())

    if full_scan_count > 1:
        plan_penalty += min(full_scan_count * 0.03, 0.09)
        notes.append(
            f"Query plan shows {full_scan_count} full table scan(s). "
            "Adding indexes or restructuring joins can reduce this."
        )
    if search_count > 0:
        notes.append(f"Query uses {search_count} index seek(s) — good.")
    if not plan_lines:
        notes.append("Could not retrieve query plan (likely a syntax error).")
    return notes, plan_penalty


def _result_set_matches(agent_rows, ref_rows, key_col_index: int = 0) -> bool:
    if agent_rows is None or ref_rows is None:
        return False
    agent_keys = sorted(set(row[key_col_index] for row in agent_rows))
    ref_keys = sorted(set(row[key_col_index] for row in ref_rows))
    return agent_keys == ref_keys


def _classify_sql_error(error: str) -> tuple[str, str, str]:
    err_lower = error.lower()
    if "syntax error" in err_lower:
        return (
            "syntax_error",
            f"SQL syntax error: {error}",
            "Check for missing commas, unquoted string literals, or mismatched parentheses.",
        )
    if "no such column" in err_lower:
        return (
            "column_reference_error",
            f"Column not found: {error}",
            "Verify column names against the schema. Use table aliases (e.g. c.name) to avoid ambiguity.",
        )
    if "no such table" in err_lower:
        return (
            "table_reference_error",
            f"Table not found: {error}",
            "Check that all table names match the schema exactly.",
        )
    if "ambiguous" in err_lower:
        return (
            "ambiguous_column_error",
            f"Ambiguous column reference: {error}",
            "Qualify columns with their table alias (e.g. o.status instead of status).",
        )
    if "misuse of aggregate" in err_lower or "aggregate" in err_lower:
        return (
            "aggregation_error",
            f"Aggregate function misuse: {error}",
            "Columns in SELECT that are not inside aggregate functions (SUM, COUNT, etc.) must appear in GROUP BY.",
        )
    return (
        "runtime_error",
        f"Query execution failed: {error}",
        "Run the query against the schema manually to debug the runtime error.",
    )


def _rubric_item(score: float, max_score: float, passed: bool, detail: str) -> RubricItem:
    return RubricItem(score=score, max_score=max_score, passed=passed, detail=detail)


def grade_task_01(agent_query: str, prev_best: float = 0.0) -> tuple[float, StructuredFeedback]:
    conn = _fresh_db()
    rows, error, _ = _execute_safe(conn, agent_query)
    plan = _get_query_plan(conn, agent_query) if not error else []
    conn.close()

    rubric: dict[str, RubricItem] = {}
    opt_notes: list[str] = []

    if error:
        err_type, msg, hint = _classify_sql_error(error)
        rubric["syntax_valid"] = _rubric_item(0.0, 0.25, False, f"Query raised: {error}")
        rubric["columns_correct"] = _rubric_item(0.0, 0.25, False, "Not evaluated — fix syntax first.")
        rubric["filter_correct"] = _rubric_item(0.0, 0.25, False, "Not evaluated — fix syntax first.")
        rubric["result_matches"] = _rubric_item(0.0, 0.25, False, "Not evaluated — fix syntax first.")
        return 0.0, StructuredFeedback(
            error_type=err_type, message=msg, hint=hint,
            rubric=rubric, optimization_notes=opt_notes,
        )

    n = _normalize(agent_query)

    syntax_score = 0.25
    rubric["syntax_valid"] = _rubric_item(syntax_score, 0.25, True, "Query parses and executes without error.")

    has_id    = bool(re.search(r"\bid\b", n))
    has_name  = bool(re.search(r"\bname\b", n))
    has_email = bool(re.search(r"\bemail\b", n))
    has_country = "country" in n
    has_comma = "," in n
    all_cols = has_id and has_name and has_email and has_country and has_comma
    cols_score = 0.25 if all_cols else (0.15 if (has_comma and (has_id or has_name or has_email)) else 0.0)
    rubric["columns_correct"] = _rubric_item(
        cols_score, 0.25, all_cols,
        "SELECT id, name, email, country — all present." if all_cols
        else "Missing one or more of: id, name, email, country, or comma separators.",
    )

    has_us_filter  = "country" in n and ("'us'" in n or '"us"' in n)
    has_tier_filter = "tier" in n and ("'gold'" in n or '"gold"' in n)
    filter_score = 0.0
    if has_us_filter and has_tier_filter:
        filter_score = 0.25
    elif has_us_filter or has_tier_filter:
        filter_score = 0.12
    rubric["filter_correct"] = _rubric_item(
        filter_score, 0.25, filter_score == 0.25,
        "WHERE country = 'US' AND tier = 'gold' — both conditions present." if filter_score == 0.25
        else "Missing or incomplete WHERE conditions. Need country = 'US' AND tier = 'gold'.",
    )

    ref_conn = _fresh_db()
    ref_rows, _, _ = _execute_safe(ref_conn, "SELECT id, name, email, country FROM customers WHERE country = 'US' AND tier = 'gold'")
    ref_conn.close()
    result_ok = _result_set_matches(rows, ref_rows)
    result_score = 0.25 if result_ok else 0.0
    rubric["result_matches"] = _rubric_item(
        result_score, 0.25, result_ok,
        f"Result set matches reference ({len(ref_rows)} rows expected, {len(rows) if rows else 0} returned)."
        if result_ok else
        f"Result mismatch — expected {len(ref_rows) if ref_rows else '?'} rows, got {len(rows) if rows else 0}.",
    )

    plan_notes, plan_penalty = _analyze_plan(plan)
    opt_notes.extend(plan_notes)
    if "select *" in n:
        opt_notes.append("Avoid SELECT * — list columns explicitly for clarity and future schema safety.")
        plan_penalty += 0.05

    raw_score = syntax_score + cols_score + filter_score + result_score
    final_score = max(0.0, min(raw_score - plan_penalty, 1.0))
    regression_penalty = max(0.0, prev_best - final_score) * 0.15
    final_score = max(0.0, final_score - regression_penalty)
    if regression_penalty > 0:
        opt_notes.append(f"Regression penalty applied: this attempt ({raw_score:.2f}) scored lower than your best ({prev_best:.2f}).")

    return round(final_score, 4), StructuredFeedback(
        error_type=None,
        message="Query executed successfully." if final_score >= 0.9 else "Query runs but has issues — see rubric.",
        hint="Looks correct!" if final_score >= 0.9 else "Review the rubric items marked as not passing.",
        rubric=rubric,
        optimization_notes=opt_notes,
    )


def grade_task_02(agent_query: str, prev_best: float = 0.0) -> tuple[float, StructuredFeedback]:
    conn = _fresh_db()
    rows, error, _ = _execute_safe(conn, agent_query)
    plan = _get_query_plan(conn, agent_query) if not error else []
    conn.close()

    rubric: dict[str, RubricItem] = {}
    opt_notes: list[str] = []

    if error:
        err_type, msg, hint = _classify_sql_error(error)
        for k in ("syntax_valid", "join_type", "join_condition", "filter_correct", "columns_correct", "ordering", "result_matches"):
            rubric[k] = _rubric_item(0.0, {"syntax_valid": 0.15, "join_type": 0.2, "join_condition": 0.15, "filter_correct": 0.15, "columns_correct": 0.15, "ordering": 0.1, "result_matches": 0.1}[k], False, "Not evaluated.")
        rubric["syntax_valid"] = _rubric_item(0.0, 0.15, False, f"Query raised: {error}")
        return 0.0, StructuredFeedback(error_type=err_type, message=msg, hint=hint, rubric=rubric, optimization_notes=opt_notes)

    n = _normalize(agent_query)
    score = 0.0

    rubric["syntax_valid"] = _rubric_item(0.15, 0.15, True, "Query executes without error.")
    score += 0.15

    uses_inner = "inner join" in n
    uses_left  = "left join" in n or "left outer join" in n
    join_type_ok = uses_inner and not uses_left
    join_score = 0.2 if join_type_ok else (0.05 if ("join" in n and not uses_left) else 0.0)
    rubric["join_type"] = _rubric_item(join_score, 0.2, join_type_ok,
        "INNER JOIN used — intent is explicit." if join_type_ok else
        ("LEFT JOIN used — semantically broken when combined with WHERE on the joined table. Use INNER JOIN." if uses_left else "No JOIN found."))
    score += join_score

    has_on = "on" in n and "customer_id" in n
    join_cond_score = 0.15 if has_on else 0.0
    rubric["join_condition"] = _rubric_item(join_cond_score, 0.15, has_on,
        "ON condition references customer_id — correct." if has_on else "Missing or incorrect ON join condition.")
    score += join_cond_score

    has_active = "status" in n and "'active'" in n
    filter_score = 0.15 if has_active else 0.0
    rubric["filter_correct"] = _rubric_item(filter_score, 0.15, has_active,
        "Filters to active orders." if has_active else "Missing WHERE o.status = 'active'.")
    score += filter_score

    has_name  = "c.name" in n or ("name" in n and "c." in n)
    has_email = "c.email" in n or ("email" in n and "c." in n)
    has_order = "order_id" in n or "o.id" in n
    cols_ok = has_name and has_email and has_order
    col_score = 0.15 if cols_ok else (0.07 if (has_name or has_email) else 0.0)
    rubric["columns_correct"] = _rubric_item(col_score, 0.15, cols_ok,
        "All required columns selected." if cols_ok else "Missing one or more of: c.name, c.email, order id.")
    score += col_score

    has_order_by = "order by" in n and "c.name" in n
    ord_score = 0.1 if has_order_by else 0.0
    rubric["ordering"] = _rubric_item(ord_score, 0.1, has_order_by,
        "Results ordered by c.name ASC." if has_order_by else "Missing ORDER BY c.name ASC.")
    score += ord_score

    ref_conn = _fresh_db()
    ref_rows, _, _ = _execute_safe(ref_conn,
        "SELECT c.name, c.email, o.id AS order_id, o.status FROM customers c "
        "INNER JOIN orders o ON c.id = o.customer_id WHERE o.status = 'active' ORDER BY c.name ASC")
    ref_conn.close()
    result_ok = _result_set_matches(rows, ref_rows)
    result_score = 0.1 if result_ok else 0.0
    rubric["result_matches"] = _rubric_item(result_score, 0.1, result_ok,
        f"Result set matches reference ({len(ref_rows)} rows)." if result_ok
        else f"Expected {len(ref_rows) if ref_rows else '?'} rows, got {len(rows) if rows else 0}.")
    score += result_score

    if uses_left:
        opt_notes.append("LEFT JOIN + WHERE on the right table is a common anti-pattern — it silently converts to an inner join while misleading the optimizer.")
    plan_notes, plan_penalty = _analyze_plan(plan)
    opt_notes.extend(plan_notes)

    final_score = max(0.0, min(score - plan_penalty, 1.0))
    regression_penalty = max(0.0, prev_best - final_score) * 0.15
    final_score = max(0.0, final_score - regression_penalty)
    if regression_penalty > 0:
        opt_notes.append(f"Regression penalty: this attempt ({score:.2f}) is worse than your best ({prev_best:.2f}).")

    return round(final_score, 4), StructuredFeedback(
        error_type=None,
        message="Query correct." if final_score >= 0.9 else "Query runs but has structural issues.",
        hint="Good work!" if final_score >= 0.9 else "Focus on the join type and filtering logic.",
        rubric=rubric, optimization_notes=opt_notes,
    )


def grade_task_03(agent_query: str, prev_best: float = 0.0) -> tuple[float, StructuredFeedback]:
    conn = _fresh_db()
    rows, error, _ = _execute_safe(conn, agent_query)
    plan = _get_query_plan(conn, agent_query) if not error else []
    conn.close()

    rubric: dict[str, RubricItem] = {}
    opt_notes: list[str] = []

    if error:
        err_type, msg, hint = _classify_sql_error(error)
        for k in ("syntax_valid", "group_by_present", "group_by_correct", "having_clause", "ordering", "result_matches"):
            rubric[k] = _rubric_item(0.0, {"syntax_valid": 0.2, "group_by_present": 0.2, "group_by_correct": 0.2, "having_clause": 0.2, "ordering": 0.1, "result_matches": 0.1}[k], False, "Not evaluated.")
        rubric["syntax_valid"] = _rubric_item(0.0, 0.2, False, f"Query raised: {error}")
        return 0.0, StructuredFeedback(error_type=err_type, message=msg, hint=hint, rubric=rubric, optimization_notes=opt_notes)

    n = _normalize(agent_query)
    score = 0.0

    rubric["syntax_valid"] = _rubric_item(0.2, 0.2, True, "Query executes without error.")
    score += 0.2

    has_group_by = "group by" in n
    gb_score = 0.2 if has_group_by else 0.0
    rubric["group_by_present"] = _rubric_item(gb_score, 0.2, has_group_by,
        "GROUP BY clause present." if has_group_by else
        "Missing GROUP BY. When using COUNT() or SUM(), non-aggregated SELECT columns must be in GROUP BY.")
    score += gb_score

    has_id_in_gb  = "c.id" in n and "group by" in n
    has_name_in_gb = "c.name" in n and "group by" in n
    gb_cols_ok = has_group_by and ("c.id" in n.split("group by")[-1] or "id" in n.split("group by")[-1])
    gb_col_score = 0.2 if gb_cols_ok else (0.1 if has_group_by else 0.0)
    rubric["group_by_correct"] = _rubric_item(gb_col_score, 0.2, gb_cols_ok,
        "GROUP BY includes customer id — correct key." if gb_cols_ok
        else "GROUP BY present but may be grouping on wrong columns. Use GROUP BY c.id, c.name.")
    score += gb_col_score

    has_having = "having" in n and "100" in n
    having_score = 0.2 if has_having else 0.0
    rubric["having_clause"] = _rubric_item(having_score, 0.2, has_having,
        "HAVING revenue > 100 present." if has_having else
        "Missing HAVING clause with threshold > 100. Use HAVING to filter after aggregation.")
    score += having_score

    has_order_by = "order by" in n and "revenue" in n
    ord_score = 0.1 if has_order_by else 0.0
    rubric["ordering"] = _rubric_item(ord_score, 0.1, has_order_by,
        "Results ordered by revenue." if has_order_by else "Missing ORDER BY revenue DESC.")
    score += ord_score

    ref_conn = _fresh_db()
    ref_rows, _, _ = _execute_safe(ref_conn,
        "SELECT c.name, COUNT(o.id) AS order_count, SUM(oi.quantity * oi.unit_price) AS revenue "
        "FROM customers c JOIN orders o ON c.id = o.customer_id "
        "JOIN order_items oi ON o.id = oi.order_id "
        "WHERE o.status = 'completed' GROUP BY c.id, c.name HAVING revenue > 100 ORDER BY revenue DESC")
    ref_conn.close()
    result_ok = _result_set_matches(rows, ref_rows, key_col_index=0)
    result_score = 0.1 if result_ok else 0.0
    rubric["result_matches"] = _rubric_item(result_score, 0.1, result_ok,
        f"Result set correct ({len(ref_rows)} customer rows)." if result_ok
        else f"Mismatch: expected {len(ref_rows) if ref_rows else '?'}, got {len(rows) if rows else 0} rows.")
    score += result_score

    plan_notes, plan_penalty = _analyze_plan(plan)
    opt_notes.extend(plan_notes)

    final_score = max(0.0, min(score - plan_penalty, 1.0))
    regression_penalty = max(0.0, prev_best - final_score) * 0.15
    final_score = max(0.0, final_score - regression_penalty)
    if regression_penalty > 0:
        opt_notes.append(f"Regression penalty: attempt ({score:.2f}) worse than best ({prev_best:.2f}).")

    return round(final_score, 4), StructuredFeedback(
        error_type=None,
        message="Aggregation query correct." if final_score >= 0.9 else "Aggregation query needs work.",
        hint="All good!" if final_score >= 0.9 else "Add GROUP BY and verify HAVING threshold.",
        rubric=rubric, optimization_notes=opt_notes,
    )


def grade_task_04(agent_query: str, prev_best: float = 0.0) -> tuple[float, StructuredFeedback]:
    conn = _fresh_db()
    rows, error, _ = _execute_safe(conn, agent_query)
    plan = _get_query_plan(conn, agent_query) if not error else []
    conn.close()

    rubric: dict[str, RubricItem] = {}
    opt_notes: list[str] = []

    if error:
        err_type, msg, hint = _classify_sql_error(error)
        for k in ("syntax_valid", "no_correlated_subquery", "explicit_joins", "where_filter", "group_by", "having_clause", "alias_present", "result_matches"):
            rubric[k] = _rubric_item(0.0, 0.1, False, "Not evaluated.")
        rubric["syntax_valid"] = _rubric_item(0.0, 0.1, False, f"Query raised: {error}")
        return 0.0, StructuredFeedback(error_type=err_type, message=msg, hint=hint, rubric=rubric, optimization_notes=opt_notes)

    n = _normalize(agent_query)
    score = 0.0

    rubric["syntax_valid"] = _rubric_item(0.1, 0.1, True, "Query executes.")
    score += 0.1

    has_correlated = re.search(r"where\s*\(?\s*select", n) is not None
    no_corr_score = 0.15 if not has_correlated else 0.0
    rubric["no_correlated_subquery"] = _rubric_item(no_corr_score, 0.15, not has_correlated,
        "No correlated subquery — good." if not has_correlated else
        "Still using a correlated subquery. Replace with JOIN + GROUP BY + HAVING.")
    score += no_corr_score
    if has_correlated:
        opt_notes.append("Correlated subqueries re-execute for each row in the outer query. This is O(n) scans and scales very poorly.")

    has_join = "join" in n and "on" in n and "customer_id" in n
    join_score = 0.15 if has_join else 0.0
    rubric["explicit_joins"] = _rubric_item(join_score, 0.15, has_join,
        "Explicit JOINs with ON conditions." if has_join else "Missing explicit JOINs or ON conditions.")
    score += join_score

    has_completed = "completed" in n and "status" in n
    where_score = 0.1 if has_completed else 0.0
    rubric["where_filter"] = _rubric_item(where_score, 0.1, has_completed,
        "Filters to completed orders." if has_completed else "Missing WHERE o.status = 'completed' filter.")
    score += where_score

    has_group_by = "group by" in n
    gb_score = 0.15 if has_group_by else 0.0
    rubric["group_by"] = _rubric_item(gb_score, 0.15, has_group_by,
        "GROUP BY present." if has_group_by else "Missing GROUP BY c.id, c.name, c.email.")
    score += gb_score

    has_having = "having" in n and "200" in n
    hav_score = 0.15 if has_having else 0.0
    rubric["having_clause"] = _rubric_item(hav_score, 0.15, has_having,
        "HAVING > 200 threshold correct." if has_having else "Missing HAVING SUM(...) > 200.")
    score += hav_score

    has_alias = "total_spent" in n or ("as total" in n)
    alias_score = 0.05 if has_alias else 0.0
    rubric["alias_present"] = _rubric_item(alias_score, 0.05, has_alias,
        "total_spent alias present." if has_alias else "Alias total_spent missing — makes the result harder to consume.")
    score += alias_score

    ref_conn = _fresh_db()
    ref_rows, _, _ = _execute_safe(ref_conn,
        "SELECT c.id, c.name, c.email, SUM(oi.quantity * oi.unit_price) AS total_spent "
        "FROM customers c INNER JOIN orders o ON c.id = o.customer_id "
        "INNER JOIN order_items oi ON o.id = oi.order_id "
        "WHERE o.status = 'completed' GROUP BY c.id, c.name, c.email "
        "HAVING SUM(oi.quantity * oi.unit_price) > 200 ORDER BY total_spent DESC")
    ref_conn.close()
    result_ok = _result_set_matches(rows, ref_rows)
    result_score = 0.15 if result_ok else 0.0
    rubric["result_matches"] = _rubric_item(result_score, 0.15, result_ok,
        f"Result set matches ({len(ref_rows)} rows)." if result_ok
        else f"Expected {len(ref_rows) if ref_rows else '?'} rows, got {len(rows) if rows else 0}.")
    score += result_score

    plan_notes, plan_penalty = _analyze_plan(plan)
    opt_notes.extend(plan_notes)

    final_score = max(0.0, min(score - plan_penalty, 1.0))
    regression_penalty = max(0.0, prev_best - final_score) * 0.15
    final_score = max(0.0, final_score - regression_penalty)
    if regression_penalty > 0:
        opt_notes.append(f"Regression penalty applied ({prev_best:.2f} → {score:.2f}).")

    return round(final_score, 4), StructuredFeedback(
        error_type=None,
        message="Optimized rewrite correct." if final_score >= 0.9 else "Rewrite incomplete — see rubric.",
        hint="Full marks!" if final_score >= 0.9 else "Ensure no correlated subquery, and use GROUP BY + HAVING.",
        rubric=rubric, optimization_notes=opt_notes,
    )


def grade_task_05(agent_query: str, prev_best: float = 0.0) -> tuple[float, StructuredFeedback]:
    conn = _fresh_db()
    rows, error, _ = _execute_safe(conn, agent_query)
    plan = _get_query_plan(conn, agent_query) if not error else []
    conn.close()

    rubric: dict[str, RubricItem] = {}
    opt_notes: list[str] = []

    if error:
        err_type, msg, hint = _classify_sql_error(error)
        for k in ("syntax_valid", "no_implicit_join", "all_tables_joined", "where_filter", "group_by_category", "alias_present", "ordering", "result_matches"):
            rubric[k] = _rubric_item(0.0, 0.1, False, "Not evaluated.")
        rubric["syntax_valid"] = _rubric_item(0.0, 0.15, False, f"Query raised: {error}")
        return 0.0, StructuredFeedback(error_type=err_type, message=msg, hint=hint, rubric=rubric, optimization_notes=opt_notes)

    n = _normalize(agent_query)
    score = 0.0

    rubric["syntax_valid"] = _rubric_item(0.15, 0.15, True, "Query executes.")
    score += 0.15

    has_implicit = bool(re.search(r"from\s+\w+\s*,\s*\w+", n))
    no_implicit_score = 0.15 if not has_implicit else 0.0
    rubric["no_implicit_join"] = _rubric_item(no_implicit_score, 0.15, not has_implicit,
        "No comma-join syntax — uses explicit JOINs." if not has_implicit else
        "Still using implicit comma-join (FROM a, b). Replace with explicit JOIN ... ON syntax.")
    score += no_implicit_score
    if has_implicit:
        opt_notes.append("Implicit comma joins are deprecated, harder to read, and can produce cartesian products silently when ON conditions are missing.")

    has_oi_o_join = "order_items" in n and "orders" in n and "oi.order_id" in n
    has_o_p_join  = "products" in n and "product_id" in n
    all_joined = has_oi_o_join and has_o_p_join
    join_score = 0.2 if all_joined else (0.1 if (has_oi_o_join or has_o_p_join) else 0.0)
    rubric["all_tables_joined"] = _rubric_item(join_score, 0.2, all_joined,
        "All three tables joined with correct ON conditions." if all_joined
        else "Missing or incorrect join between order_items ↔ orders ↔ products.")
    score += join_score

    has_completed = "completed" in n and "status" in n
    where_score = 0.15 if has_completed else 0.0
    rubric["where_filter"] = _rubric_item(where_score, 0.15, has_completed,
        "Filters to completed orders." if has_completed else "Missing WHERE o.status = 'completed'. Refunded/active orders inflate revenue.")
    score += where_score

    has_group = "group by" in n and "category" in n
    group_score = 0.15 if has_group else 0.0
    rubric["group_by_category"] = _rubric_item(group_score, 0.15, has_group,
        "GROUP BY p.category — correct." if has_group else "Missing GROUP BY p.category.")
    score += group_score

    has_alias = "total_revenue" in n or "as revenue" in n
    alias_score = 0.05 if has_alias else 0.0
    rubric["alias_present"] = _rubric_item(alias_score, 0.05, has_alias,
        "total_revenue alias present." if has_alias else "SUM column needs an alias like total_revenue.")
    score += alias_score

    has_order_by = "order by" in n and ("total_revenue" in n or "revenue" in n)
    ord_score = 0.1 if has_order_by else 0.0
    rubric["ordering"] = _rubric_item(ord_score, 0.1, has_order_by,
        "Results ordered by revenue." if has_order_by else "Missing ORDER BY total_revenue DESC.")
    score += ord_score

    ref_conn = _fresh_db()
    ref_rows, _, _ = _execute_safe(ref_conn,
        "SELECT p.category, SUM(oi.quantity * oi.unit_price) AS total_revenue "
        "FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id "
        "INNER JOIN products p ON oi.product_id = p.id "
        "WHERE o.status = 'completed' GROUP BY p.category ORDER BY total_revenue DESC")
    ref_conn.close()
    result_ok = _result_set_matches(rows, ref_rows, key_col_index=0)
    result_score = 0.2 if result_ok else 0.0
    rubric["result_matches"] = _rubric_item(result_score, 0.2, result_ok,
        f"Revenue per category matches reference ({len(ref_rows)} categories)." if result_ok
        else f"Revenue totals incorrect. Expected {len(ref_rows) if ref_rows else '?'} category rows, got {len(rows) if rows else 0}.")
    score += result_score

    plan_notes, plan_penalty = _analyze_plan(plan)
    opt_notes.extend(plan_notes)
    if has_implicit:
        opt_notes.append("Cartesian product risk: missing ON condition in a comma-join returns every combination of rows before filtering, which is extremely slow on large tables.")

    final_score = max(0.0, min(score - plan_penalty, 1.0))
    regression_penalty = max(0.0, prev_best - final_score) * 0.15
    final_score = max(0.0, final_score - regression_penalty)
    if regression_penalty > 0:
        opt_notes.append(f"Regression penalty ({prev_best:.2f} → {score:.2f}).")

    return round(final_score, 4), StructuredFeedback(
        error_type=None,
        message="Multi-table report query correct." if final_score >= 0.9 else "Report query needs fixes — see rubric.",
        hint="Perfect!" if final_score >= 0.9 else "Fix the join structure and status filter.",
        rubric=rubric, optimization_notes=opt_notes,
    )


GRADERS = {
    "task_01_syntax_fix":        grade_task_01,
    "task_02_join_logic":        grade_task_02,
    "task_03_aggregation_fix":   grade_task_03,
    "task_04_correlated_subquery": grade_task_04,
    "task_05_multi_table_report": grade_task_05,
}
