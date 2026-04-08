from pydantic import BaseModel
from typing import Any


class SchemaTable(BaseModel):
    name: str
    columns: list[str]
    sample_rows: list[dict[str, Any]]


class RubricItem(BaseModel):
    score: float
    max_score: float
    passed: bool
    detail: str


class StructuredFeedback(BaseModel):
    error_type: str | None
    message: str
    hint: str
    rubric: dict[str, RubricItem]
    optimization_notes: list[str]


class AttemptRecord(BaseModel):
    attempt_number: int
    query: str
    score: float
    feedback: StructuredFeedback


class Observation(BaseModel):
    task_id: str
    level: str
    schema_tables: list[SchemaTable]
    query: str
    instructions: str
    attempt: int
    max_attempts: int
    previous_attempts: list[AttemptRecord]
    best_score_so_far: float


class Action(BaseModel):
    query: str


class StepResult(BaseModel):
    observation: Observation | None
    reward: float
    done: bool
    info: dict[str, Any]
    feedback: StructuredFeedback | None


class StateResult(BaseModel):
    task_index: int
    total_tasks: int
    scores: list[float]
    attempts_per_task: list[int]
    done: bool
    cumulative_reward: float
    per_task_feedback: list[StructuredFeedback | None]
    per_task_best_queries: list[str | None]
    progression_log: list[dict[str, Any]]


class ResetResult(BaseModel):
    observation: Observation
    info: dict[str, Any]
