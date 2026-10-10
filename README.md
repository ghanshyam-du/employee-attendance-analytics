# Employee Attendance & Analytics API

A production-grade Python/FastAPI service backed by MongoDB that manages employee attendance lifecycle tracking (punch-in, punch-out, and manual regularizations with an immutable audit trail) alongside analytics powered by MongoDB aggregation pipelines.

---

## 📌 Features & Business Rules Implemented

- **Employee Management:** Create and list employees with input validation and database-level unique constraints.
- **Punch-In / Punch-Out Lifecycle:**
  - **R1 (Timezone & Overnight Shifts):** All instants handled in UTC and converted to IST (`UTC+05:30`). Overnight shifts (e.g., 22:00–06:00) punched before shift end are assigned to the shift start day. Truncated to whole seconds.
  - **R2 (Late Grace Period):** 10-minute grace period. If late (> 10 min), floor of minutes elapsed since shift start is recorded.
  - **R3 (Overtime):** Recorded only when work extends >= 30 minutes past shift end.
  - **R4 (Work Hours):** Exact decimal calculation rounded to 2 decimal places half-up (`ROUND_HALF_UP`).
  - **R5 (Half-Day):** Marked `true` when rounded work hours are strictly less than 4.50. Counts as 0.5 of a present day.
- **R6 (Presence Statuses):** `PRESENT`, `WFH`, and `ON_DUTY` count as present; `ABSENT` and `LEAVE` do not.
- **R7 (Working Days):** Monday–Friday (excluding weekends). Mid-month joiners count working days from `joined_on`.
- **R8 (Rounding):** 2 decimals half-up for metrics; 4 decimals half-up for rates.
- **R9 (Headcount):** Counts all employees whose `joined_on` is on or before the period end, including those with zero logs.
- **R10 (Pagination):** 1-based indexing, configurable `page_size` (max 100), filtered collection counts.
- **Concurrency & Races:** Atomic unique compound index on `(emp_code, date)` guarantees exactly one punch-in per day (returning 409 for concurrent duplicates). Optimistic locking on history length prevents concurrent regularization overwrite.
- **Query Optimization & Explain:** All list and analytics endpoints are backed by database indexes (`IXSCAN` verified with zero `COLLSCAN`).

---

## 🚀 Getting Started & Setup Guide

### 1. Prerequisites
- **Python:** 3.10+ (tested on Python 3.10 and 3.11+)
- **MongoDB:** 6.0 or newer (uses `$densify` and `$setWindowFields`)
- **Git**

---

### 2. Start MongoDB

#### Option A: Docker (Recommended)
If you have Docker Desktop installed:
```bash
docker run -d -p 27017:27017 --name attendance-mongo mongo:7
```

#### Option B: Local MongoDB Service
If MongoDB is installed locally on Windows/Linux:
Make sure the service is running on the default port `27017`.

#### Option C: MongoDB Atlas (Cloud)
Set the environment variables or place them in a `.env` file (do not commit secrets):
```env
MONGO_URI=mongodb+srv://<username>:<password>@cluster0.mongodb.net/
MONGO_DB=attendance_db
```

---

### 3. Environment Setup & Dependency Installation

Clone the repository and navigate into the project directory:
```bash
cd employee-attendance-analytics
```

Create and activate a virtual environment:
```bash
# Windows PowerShell
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Linux / macOS
python -m venv .venv
source .venv/bin/activate
```

Install the required packages:
```bash
pip install -r requirements.txt
```

---

### 4. Seed Initial Sample Data

Load the 9 sample documents from `sample_data/` into MongoDB:
```bash
python sample_seed.py
```
*Expected output:*
```text
loaded  6 documents into 'attendance_db.employees'
loaded  9 documents into 'attendance_db.attendance_logs'
```

---

### 5. Run the Application Server

Start the FastAPI development server with Uvicorn:
```bash
uvicorn app.main:app --port 8000 --reload
```

The application will initialize all necessary database indexes idempotently on startup.

---

## 📖 API Documentation & Interactive Explorer

Once the server is running, explore the interactive documentation directly in your browser:

- **Swagger UI (Interactive API Explorer):**  
  👉 [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)  
  *Allows sending live HTTP requests, inspecting request/response schemas, and reviewing parameter validations.*

- **ReDoc (Specification Documentation):**  
  👉 [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

- **OpenAPI Schema (JSON):**  
  👉 [http://127.0.0.1:8000/openapi.json](http://127.0.0.1:8000/openapi.json)

---

## 📡 API Endpoints Overview & Examples

### System
| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | Readiness probe; pings MongoDB and returns `200` or `503`. |

```bash
curl http://127.0.0.1:8000/health
# {"status":"ok"}
```

---

### Employees
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/employees` | Register a new employee (atomic unique index on `emp_code`). |
| `GET` | `/employees` | Paginated employee list sorted by `emp_code` ascending. |

```bash
# List employees (with pagination and optional department filter)
curl "http://127.0.0.1:8000/employees?page=1&page_size=20"
```

---

### Attendance Lifecycle
| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/attendance/punch-in` | Record punch-in; calculates date (R1) & late minutes (R2). |
| `POST` | `/attendance/punch-out` | Record punch-out; calculates work hours, overtime, & half-day. |
| `GET` | `/attendance` | Paginated attendance log queries by date range, employee, or status. |
| `PATCH` | `/attendance/{emp_code}/{date}` | Manual regularization with full audit history trail. |

#### Punch-In Example:
```bash
curl -X POST "http://127.0.0.1:8000/attendance/punch-in" \
  -H "Content-Type: application/json" \
  -d '{"emp_code": "EMP0001", "status": "PRESENT"}'
```

#### Manual Regularization Example:
```bash
curl -X PATCH "http://127.0.0.1:8000/attendance/EMP0001/2026-07-06" \
  -H "Content-Type: application/json" \
  -d '{
    "status": "WFH",
    "reason": "Approved work from home request",
    "regularized_by": "HR Manager"
  }'
```

---

### Analytics (MongoDB Aggregation Pipelines)
| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/analytics/employees/{emp_code}/monthly` | Monthly employee attendance summary & rate. |
| `GET` | `/analytics/departments/summary` | Department-wide monthly roll-up across collections. |
| `GET` | `/analytics/leaderboard/late` | Late leaderboard with competition ranking (1, 2, 2, 4). |
| `GET` | `/analytics/departments/{department}/trend` | Daily attendance trend with gap filling (`$densify`) & 7-day moving avg. |

#### Monthly Department Summary:
```bash
curl "http://127.0.0.1:8000/analytics/departments/summary?month=2026-07"
```

#### Late-Comers Leaderboard:
```bash
curl "http://127.0.0.1:8000/analytics/leaderboard/late?month=2026-07&limit=10"
```

#### Department Daily Trend:
```bash
curl "http://127.0.0.1:8000/analytics/departments/Engineering/trend?from=2026-07-01&to=2026-07-31"
```

---

### Admin Query Explain Plan
| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/admin/explain/{endpoint}` | Runs MongoDB `explain` with `executionStats` for index proof (`IXSCAN`). |

Supported endpoints to explain:
- `attendance_list`
- `employee_monthly`
- `department_summary`
- `late_leaderboard`
- `department_trend`

```bash
curl "http://127.0.0.1:8000/admin/explain/department_summary?month=2026-07"
```

---

## 🧪 Testing

Run Python business rule assertions:
```bash
python -c "
from datetime import datetime, timezone
from app.main import round_half_up, compute_late_minutes, compute_work_hours, compute_overtime, IST

assert round_half_up(8.125, 2) == 8.13
assert round_half_up(0.00005, 4) == 0.0001
print('All unit tests passed successfully!')
"
```
