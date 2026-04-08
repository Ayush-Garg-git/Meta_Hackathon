from env.models import (
    Observation,
    Action,
    StepResult,
    StateResult,
    ResetResult,
    SchemaTable,
    AttemptRecord,
    StructuredFeedback,
    RubricItem,
)
from env.tasks import TASKS, SCHEMA_TABLES
from env.graders import GRADERS

MAX_ATTEMPTS_PER_TASK = 3


def _empty_feedback() -> StructuredFeedback:
    return StructuredFeedback(
        error_type=None,
        message="No attempts yet.",
        hint="Submit your first query for this task.",
        rubric={},
        optimization_notes=[],
    )


class SQLQueryWorkshop:
    def __init__(self):
        self._reset_state()

    def _reset_state(self):
        n = len(TASKS)
        self._task_index: int = 0
        self._best_scores: list[float] = [0.0] * n
        self._attempts: list[int] = [0] * n
        self._done: bool = False
        self._attempt_history: list[list[AttemptRecord]] = [[] for _ in range(n)]
        self._best_queries: list[str | None] = [None] * n
        self._last_feedback: list[StructuredFeedback | None] = [None] * n
        self._progression_log: list[dict] = []

    def _build_observation(self) -> Observation:
        task = TASKS[self._task_index]
        idx = self._task_index
        return Observation(
            task_id=task["task_id"],
            level=task["level"],
            schema_tables=[SchemaTable(**t) for t in SCHEMA_TABLES],
            query=task["query"],
            instructions=task["instructions"],
            attempt=self._attempts[idx],
            max_attempts=MAX_ATTEMPTS_PER_TASK,
            previous_attempts=self._attempt_history[idx],
            best_score_so_far=self._best_scores[idx],
        )

    def reset(self) -> ResetResult:
        self._reset_state()
        return ResetResult(
            observation=self._build_observation(),
            info={
                "total_tasks": len(TASKS),
                "max_attempts_per_task": MAX_ATTEMPTS_PER_TASK,
                "max_episode_reward": float(len(TASKS)),
                "message": "Environment reset. Good luck.",
            },
        )

    def step(self, action: Action) -> StepResult:
        if self._done:
            return StepResult(
                observation=None,
                reward=0.0,
                done=True,
                info={"message": "Episode finished. Call /reset to start a new episode."},
                feedback=None,
            )

        idx = self._task_index
        task = TASKS[idx]
        grader = GRADERS[task["task_id"]]
        prev_best = self._best_scores[idx]

        self._attempts[idx] += 1
        attempt_num = self._attempts[idx]

        reward, feedback = grader(action.query, prev_best=prev_best)

        is_improvement = reward > prev_best
        is_regression = reward < prev_best and attempt_num > 1

        if is_improvement:
            self._best_scores[idx] = reward
            self._best_queries[idx] = action.query
        self._last_feedback[idx] = feedback

        record = AttemptRecord(
            attempt_number=attempt_num,
            query=action.query,
            score=reward,
            feedback=feedback,
        )
        self._attempt_history[idx].append(record)

        self._progression_log.append({
            "task_id": task["task_id"],
            "attempt": attempt_num,
            "score": reward,
            "prev_best": prev_best,
            "delta": round(reward - prev_best, 4),
            "is_improvement": is_improvement,
            "is_regression": is_regression,
        })

        at_limit = attempt_num >= MAX_ATTEMPTS_PER_TASK
        task_solved = reward >= 0.9
        advance = task_solved or at_limit

        if advance:
            self._task_index += 1

        episode_done = self._task_index >= len(TASKS)
        if episode_done:
            self._done = True

        base_info = {
            "task_id": task["task_id"],
            "level": task["level"],
            "attempt": attempt_num,
            "score_this_attempt": reward,
            "best_score_this_task": self._best_scores[idx],
            "delta_from_best": round(reward - prev_best, 4),
            "is_improvement": is_improvement,
            "is_regression": is_regression,
            "task_solved": task_solved,
            "advanced_to_next_task": advance,
            "attempts_remaining": max(0, MAX_ATTEMPTS_PER_TASK - attempt_num) if not advance else 0,
        }

        if episode_done:
            base_info.update({
                "final_scores": self._best_scores,
                "total_reward": sum(self._best_scores),
                "normalized_score": round(sum(self._best_scores) / len(TASKS), 4),
                "message": "All tasks complete. Episode finished.",
            })
            return StepResult(
                observation=None,
                reward=reward,
                done=True,
                info=base_info,
                feedback=feedback,
            )

        return StepResult(
            observation=self._build_observation(),
            reward=reward,
            done=False,
            info=base_info,
            feedback=feedback,
        )

    def state(self) -> StateResult:
        return StateResult(
            task_index=self._task_index,
            total_tasks=len(TASKS),
            scores=self._best_scores,
            attempts_per_task=self._attempts,
            done=self._done,
            cumulative_reward=round(sum(self._best_scores), 4),
            per_task_feedback=self._last_feedback,
            per_task_best_queries=self._best_queries,
            progression_log=self._progression_log,
        )
