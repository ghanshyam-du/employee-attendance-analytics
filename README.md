# Employee Attendance & Analytics API

A Python/FastAPI application backed by MongoDB that tracks employee attendance with punch-in/out, manual corrections (with audit trail), and analytics endpoints for monthly summaries, department reports, late-comers leaderboard, and daily trends.

## Setup

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

Create a `.env` file (or set environment variables):

```
MONGO_URI=mongodb://localhost:27017
MONGO_DB=attendance_db
```

MongoDB 6.0 or newer is required. A local Docker container works:

```bash
docker run -p 27017:27017 mongo:7
```

Seed sample data:

```bash
python sample_seed.py
```

Run the server:

```bash
uvicorn app.main:app --port 8000 --reload
```

## What is implemented

- All CRUD endpoints: employee create/list, punch-in, punch-out, attendance list, regularize.
- All analytics: employee monthly, department summary, late leaderboard, department trend.
- Admin explain endpoint for query plan inspection.
- All business rules R1-R10 (timezone handling, late/overtime/half-day, overnight shifts, half-up rounding, pagination, concurrency guards).
- All indexes created at startup.
- All defects in the starter code found and fixed (see REVIEW.md).

## What is not implemented

Nothing — all endpoints from the OpenAPI spec are complete.
