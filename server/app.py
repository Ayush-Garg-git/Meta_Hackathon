from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from env.environment import SQLQueryWorkshop
from env.models import Action
from env.tasks import TASKS

app = FastAPI(
    title="SQL Query Workshop",
    description=(
        "An OpenEnv environment where an AI agent learns to debug and optimize "
        "real-world SQL queries across a 5-table e-commerce schema. "
        "5 tasks, partial rewards, multi-step interaction, EXPLAIN-based optimization scoring."
    ),
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_env = SQLQueryWorkshop()


@app.get("/", response_class=HTMLResponse)
def root():
    try:
        with open("frontend/index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Frontend building...</h1><p>Please wait or ensure frontend/index.html exists.</p>"


@app.post("/reset")
def reset():
    return _env.reset()


@app.post("/step")
def step(action: Action):
    if _env._done:
        raise HTTPException(
            status_code=400,
            detail="Episode finished. POST /reset to begin a new one.",
        )
    return _env.step(action)


@app.get("/state")
def state():
    return _env.state()


def main():
    import uvicorn
    uvicorn.run("server.app:app", host="0.0.0.0", port=7860)


if __name__ == "__main__":
    main()
