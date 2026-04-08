import sys
import os
import json
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from env.environment import SQLQueryWorkshop
from env.models import Action

SOLUTIONS = {
    "task_01_syntax_fix": [
        "SELECT id name email country FROM customers WHERE country = 'US' AND tier = gold",
        "SELECT id, name, email, country FROM customers WHERE country = 'US' AND tier = gold",
        "SELECT id, name, email, country FROM customers WHERE country = 'US' AND tier = 'gold'",
    ],
    "task_02_join_logic": [
        "SELECT c.name, c.email, o.id AS order_id, o.status FROM customers c LEFT JOIN orders o ON c.id = o.customer_id WHERE o.status = 'active'",
        "SELECT c.name, c.email, o.id AS order_id, o.status FROM customers c INNER JOIN orders o ON c.id = o.customer_id WHERE o.status = 'active' ORDER BY c.name ASC",
    ],
    "task_03_aggregation_fix": [
        "SELECT c.name, COUNT(o.id) AS order_count, SUM(oi.quantity * oi.unit_price) AS revenue FROM customers c JOIN orders o ON c.id = o.customer_id JOIN order_items oi ON o.id = oi.order_id WHERE o.status = 'completed' HAVING revenue > 100",
        "SELECT c.name, COUNT(o.id) AS order_count, SUM(oi.quantity * oi.unit_price) AS revenue FROM customers c JOIN orders o ON c.id = o.customer_id JOIN order_items oi ON o.id = oi.order_id WHERE o.status = 'completed' GROUP BY c.id, c.name HAVING revenue > 100 ORDER BY revenue DESC",
    ],
    "task_04_correlated_subquery": [
        "SELECT c.id, c.name, c.email, SUM(oi.quantity * oi.unit_price) AS total_spent FROM customers c INNER JOIN orders o ON c.id = o.customer_id INNER JOIN order_items oi ON o.id = oi.order_id WHERE o.status = 'completed' GROUP BY c.id, c.name, c.email HAVING SUM(oi.quantity * oi.unit_price) > 200 ORDER BY total_spent DESC",
    ],
    "task_05_multi_table_report": [
        "SELECT p.category, SUM(oi.quantity * oi.unit_price) AS total_revenue FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id INNER JOIN products p ON oi.product_id = p.id WHERE o.status = 'completed' GROUP BY p.category ORDER BY total_revenue DESC",
    ],
}

TASK_ORDER = [
    "task_01_syntax_fix",
    "task_02_join_logic",
    "task_03_aggregation_fix",
    "task_04_correlated_subquery",
    "task_05_multi_table_report",
]

DIVIDER = "─" * 70


def fmt_rubric(rubric: dict) -> str:
    lines = []
    for key, item in rubric.items():
        status = "✓" if item["passed"] else "✗"
        lines.append(f"    {status} {key:<30} {item['score']:.2f}/{item['max_score']:.2f}  {item['detail']}")
    return "\n".join(lines)


def run_baseline():
    env = SQLQueryWorkshop()
    result = env.reset()

    print(f"\n{'═'*70}")
    print("  SQL Query Workshop — Baseline Inference (multi-step)")
    print(f"{'═'*70}\n")
    print(f"Tasks: {result.info['total_tasks']}  |  Max attempts per task: {result.info['max_attempts_per_task']}  |  Max reward: {result.info['max_episode_reward']}\n")

    episode_log = []
    solution_pointers = {k: 0 for k in TASK_ORDER}
    done = False
    current_task_id = result.observation.task_id

    while not done:
        task_id = current_task_id
        solutions_for_task = SOLUTIONS.get(task_id, [])
        ptr = solution_pointers[task_id]

        if ptr >= len(solutions_for_task):
            print(f"  [baseline] No more solutions for {task_id}, skipping.")
            break

        query = solutions_for_task[ptr]
        solution_pointers[task_id] += 1

        print(DIVIDER)
        print(f"  Task:    {task_id}")
        print(f"  Attempt: {ptr + 1}")
        print(f"  Query:   {query[:100]}{'...' if len(query) > 100 else ''}")
        print()

        time.sleep(0.05)
        step_result = env.step(Action(query=query))
        done = step_result.done
        fb = step_result.feedback

        print(f"  Reward:       {step_result.reward:.4f}")
        print(f"  Improvement:  {step_result.info.get('is_improvement')}  |  Regression: {step_result.info.get('is_regression')}")
        print(f"  Best so far:  {step_result.info.get('best_score_this_task', 0.0):.4f}")
        if fb:
            if fb.error_type:
                print(f"  Error type:   {fb.error_type}")
            print(f"  Message:      {fb.message}")
            print(f"  Hint:         {fb.hint}")
            print(f"  Rubric breakdown:")
            print(fmt_rubric({k: v.model_dump() if hasattr(v, 'model_dump') else v.dict() if hasattr(v, 'dict') else v for k, v in fb.rubric.items()}))
            if fb.optimization_notes:
                print(f"  Optimization notes:")
                for note in fb.optimization_notes:
                    print(f"    → {note}")

        episode_log.append({
            "task_id": task_id,
            "attempt": ptr + 1,
            "query": query,
            "reward": step_result.reward,
            "info": step_result.info,
        })

        if step_result.observation:
            next_tid = step_result.observation.task_id
            if next_tid != current_task_id:
                print(f"\n  ✓ Advanced to next task: {next_tid}\n")
                current_task_id = next_tid
        elif done:
            print()

    final_state = env.state()
    print(f"\n{'═'*70}")
    print("  EPISODE COMPLETE")
    print(f"{'═'*70}")
    scores = final_state.scores
    for i, (task, score) in enumerate(zip(TASK_ORDER, scores)):
        bar = "█" * int(score * 20) + "░" * (20 - int(score * 20))
        print(f"  {task:<35} [{bar}] {score:.4f}")

    total = final_state.cumulative_reward
    max_r  = float(final_state.total_tasks)
    norm   = total / max_r

    print(f"\n  Cumulative reward : {total:.4f} / {max_r:.1f}")
    print(f"  Normalized score  : {norm:.4f}")
    print(f"  Attempts per task : {final_state.attempts_per_task}\n")

    output = {
        "scores": final_state.scores,
        "attempts": final_state.attempts_per_task,
        "cumulative_reward": total,
        "normalized_score": norm,
        "episode_log": episode_log,
        "progression_log": final_state.progression_log,
    }
    with open("baseline_results.json", "w") as f:
        json.dump(output, f, indent=2)
    print("  Results saved → baseline_results.json")


if __name__ == "__main__":
    run_baseline()
