"""Employee Attendance & Analytics API

FastAPI application backed by MongoDB for employee attendance tracking,
punch-in/out lifecycle, audit-trailed manual regularization, and analytics
aggregation pipelines.

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (read from environment, defaults provided)
"""
import calendar
import os
import re
from datetime import date as date_type, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query
from pydantic import BaseModel, field_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError

load_dotenv()

# ── MongoDB Connection & Configuration ────────────────────────────────────── #

client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
db = client[os.getenv("MONGO_DB", "attendance_db")]

IST = timezone(timedelta(hours=5, minutes=30))

EPOCH_MS_MIN = 100_000_000_000      # ~1973
EPOCH_MS_MAX = 4_102_444_800_000    # ~2100

PRESENCE_STATUSES = {"PRESENT", "WFH", "ON_DUTY"}
ALL_STATUSES = {"PRESENT", "WFH", "ON_DUTY", "ABSENT", "LEAVE"}

app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0")


# ── Startup: Create Indexes Idempotently ─────────────────────────────────── #

@app.on_event("startup")
def create_indexes():
    # 1. Unique index on employees.emp_code: prevents duplicate employee records
    # atomically in MongoDB, eliminating check-then-insert race conditions.
    db.employees.create_index("emp_code", unique=True)

    # 2. Compound unique index on (emp_code, date): guarantees at most one record
    # per employee per attendance day, making concurrent punch-ins race-safe.
    db.attendance_logs.create_index(
        [("emp_code", ASCENDING), ("date", ASCENDING)],
        unique=True,
    )

    # 3. Compound index on (date DESC, emp_code ASC): serves list_attendance pagination
    # and date-range filters across attendance records.
    db.attendance_logs.create_index(
        [("date", DESCENDING), ("emp_code", ASCENDING)]
    )

    # 4. Index on status: optimizes queries and analytics filtering by attendance status.
    db.attendance_logs.create_index("status")

    # 5. Index on employees.department: supports department filtering and headcount rollups.
    db.employees.create_index("department")

    # 6. Index on employees.joined_on: supports R9 headcount eligibility filtering.
    db.employees.create_index("joined_on")

    # 7. Compound index on employees (department, joined_on): optimizes department-specific
    # headcount aggregations.
    db.employees.create_index([("department", ASCENDING), ("joined_on", ASCENDING)])

    # 8. Compound index on attendance_logs (date, late_minutes): optimizes late leaderboard.
    db.attendance_logs.create_index([("date", ASCENDING), ("late_minutes", ASCENDING)])


# ── Mathematical and Time Utilities ──────────────────────────────────────── #

def round_half_up(value, decimals):
    """Round a number to the specified number of decimal places using standard
    mathematical half-up rounding (ROUND_HALF_UP). Avoids Python's banker's rounding."""
    if value is None:
        return None
    quantize_str = "0." + "0" * decimals if decimals > 0 else "0"
    return float(Decimal(str(value)).quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP))


def validate_epoch_ms(value, field_name="punched_at"):
    """Ensure an instant is represented as an integer count of epoch milliseconds
    within acceptable historical and future bounds."""
    if value is None:
        return None
    if not isinstance(value, int) or isinstance(value, bool):
        raise HTTPException(422, detail=f"{field_name} must be an integer (epoch milliseconds)")
    if value < EPOCH_MS_MIN or value > EPOCH_MS_MAX:
        raise HTTPException(
            422,
            detail=f"{field_name} out of range: must be between {EPOCH_MS_MIN} and {EPOCH_MS_MAX}",
        )
    return value


def epoch_ms_to_datetime(ms):
    """Convert epoch milliseconds to a UTC-aware datetime, truncated to whole seconds (R1)."""
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return dt.replace(microsecond=0)


def datetime_to_epoch_ms(dt):
    """Convert a datetime instant to epoch milliseconds."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def to_ist(dt):
    """Convert a UTC or naive datetime to Indian Standard Time (UTC+05:30)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(IST)


def _is_overnight_shift(shift_start_str, shift_end_str):
    """Check if shift end time is at or before start time (e.g. 22:00 - 06:00)."""
    return shift_end_str <= shift_start_str


def determine_attendance_date(punch_in_utc, shift_start_str, shift_end_str):
    """Determine attendance date per R1.
    For overnight shifts (shift_end <= shift_start), an IST punch-in earlier than
    shift_end belongs to the shift that started on the previous calendar day.
    """
    punch_ist = to_ist(punch_in_utc)
    ist_date = punch_ist.date()

    if _is_overnight_shift(shift_start_str, shift_end_str):
        sh, sm = map(int, shift_end_str.split(":"))
        shift_end_time = punch_ist.replace(hour=sh, minute=sm, second=0, microsecond=0)
        if punch_ist.time() < shift_end_time.time():
            ist_date = ist_date - timedelta(days=1)

    return ist_date.isoformat()


def compute_late_minutes(punch_in_utc, shift_start_str, att_date_str=None):
    """Compute late minutes per R2.
    Grace period is 10 minutes. A punch-in is late only if strictly more than
    10:00 after shift_start. When late, late_minutes = floor of whole minutes
    elapsed since shift_start (not since the end of grace period).
    """
    punch_ist = to_ist(punch_in_utc)
    sh, sm = map(int, shift_start_str.split(":"))

    if att_date_str:
        att_d = date_type.fromisoformat(att_date_str)
        shift_start_dt = datetime(att_d.year, att_d.month, att_d.day, sh, sm, 0, tzinfo=IST)
    else:
        shift_start_dt = punch_ist.replace(hour=sh, minute=sm, second=0, microsecond=0)

    diff_seconds = (punch_ist - shift_start_dt).total_seconds()
    if diff_seconds > 600:
        return int(diff_seconds // 60)
    return 0


def compute_work_hours(punch_in_utc, punch_out_utc):
    """Compute work hours per R4.
    (punch_out - punch_in) in seconds / 3600, rounded 2 decimals half-up.
    Uses exact Decimal calculation to avoid IEEE-754 floating-point inaccuracies.
    """
    diff_seconds = int((punch_out_utc - punch_in_utc).total_seconds())
    hours = Decimal(diff_seconds) / Decimal(3600)
    return float(hours.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def compute_overtime(punch_out_utc, shift_end_str, attendance_date_str, shift_start_str):
    """Compute overtime minutes per R3.
    Whole minutes (floor) from shift_end to punch_out, counted only when >= 30.
    For overnight shifts, shift_end falls on the next calendar day.
    """
    h, m = map(int, shift_end_str.split(":"))
    att_date = date_type.fromisoformat(attendance_date_str)

    if _is_overnight_shift(shift_start_str, shift_end_str):
        end_date = att_date + timedelta(days=1)
    else:
        end_date = att_date

    shift_end_ist = datetime(end_date.year, end_date.month, end_date.day, h, m, 0, tzinfo=IST)
    punch_out_aware = punch_out_utc if punch_out_utc.tzinfo else punch_out_utc.replace(tzinfo=timezone.utc)

    diff_seconds = (punch_out_aware - shift_end_ist).total_seconds()
    if diff_seconds < 0:
        return 0
    minutes = int(diff_seconds // 60)
    return minutes if minutes >= 30 else 0


def is_half_day(work_hours):
    """Check half-day per R5: True when rounded work_hours < 4.50."""
    if work_hours is None:
        return False
    return work_hours < 4.50


# ── Response Formatting ───────────────────────────────────────────────────── #

def format_attendance_record(doc):
    """Format MongoDB attendance document into API specification."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": datetime_to_epoch_ms(doc.get("punch_in")),
        "punch_out": datetime_to_epoch_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes", 0),
        "overtime_minutes": doc.get("overtime_minutes", 0),
        "half_day": doc.get("half_day", False),
        "history": format_history(doc.get("history", [])),
    }


def format_history(history_list):
    """Format audit trail history entries, converting instants to epoch ms."""
    formatted = []
    for entry in history_list:
        fe = {
            "at": datetime_to_epoch_ms(entry["at"]),
            "by": entry["by"],
            "reason": entry["reason"],
            "changes": {},
        }
        for field, change in entry.get("changes", {}).items():
            from_val = change.get("from")
            to_val = change.get("to")
            if field in ("punch_in", "punch_out"):
                from_val = datetime_to_epoch_ms(from_val) if isinstance(from_val, datetime) else from_val
                to_val = datetime_to_epoch_ms(to_val) if isinstance(to_val, datetime) else to_val
            fe["changes"][field] = {"from": from_val, "to": to_val}
        formatted.append(fe)
    return formatted


def format_employee(doc):
    """Format MongoDB employee document into API specification."""
    return {
        "emp_code": doc["emp_code"],
        "name": doc["name"],
        "email": doc["email"],
        "department": doc["department"],
        "shift_start": doc.get("shift_start", "09:30"),
        "shift_end": doc.get("shift_end", "18:30"),
        "joined_on": doc["joined_on"],
        "created_at": datetime_to_epoch_ms(doc["created_at"]),
    }


# ── Calendar Helpers ──────────────────────────────────────────────────────── #

def validate_date_str(d, field="date"):
    try:
        date_type.fromisoformat(d)
    except (ValueError, TypeError):
        raise HTTPException(422, detail=f"Invalid {field} format, expected YYYY-MM-DD")


def validate_month_str(m):
    if not m or not re.match(r"^\d{4}-(0[1-9]|1[0-2])$", m):
        raise HTTPException(422, detail="Invalid month format, expected YYYY-MM")


def parse_month_range(month_str):
    year, mon = map(int, month_str.split("-"))
    first_day = f"{year:04d}-{mon:02d}-01"
    last_day_num = calendar.monthrange(year, mon)[1]
    last_day = f"{year:04d}-{mon:02d}-{last_day_num:02d}"
    return first_day, last_day


def count_working_days_in_range(start_str, end_str):
    """Count Monday through Friday working days in [start_str, end_str] inclusive."""
    start = date_type.fromisoformat(start_str)
    end = date_type.fromisoformat(end_str)
    count = 0
    current = start
    while current <= end:
        if current.weekday() < 5:  # Monday=0 .. Friday=4
            count += 1
        current += timedelta(days=1)
    return count


# ── Pipeline Builders (Shared by Endpoints and Admin Explain) ─────────────── #

def build_attendance_list_query(emp_code=None, date_from=None, date_to=None, status=None):
    query = {}
    if emp_code:
        query["emp_code"] = emp_code
    if date_from or date_to:
        date_filter = {}
        if date_from:
            date_filter["$gte"] = date_from
        if date_to:
            date_filter["$lte"] = date_to
        query["date"] = date_filter
    if status:
        query["status"] = status
    return query


def build_employee_monthly_pipeline(emp_code, first_day, last_day):
    return [
        {"$match": {
            "emp_code": emp_code,
            "date": {"$gte": first_day, "$lte": last_day},
        }},
        {"$group": {
            "_id": None,
            "present_days": {"$sum": {
                "$cond": [
                    {"$and": [
                        {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                        {"$in": [
                            {"$dayOfWeek": {"$dateFromString": {"dateString": "$date"}}},
                            [2, 3, 4, 5, 6],  # Mon=2..Fri=6 in MongoDB
                        ]},
                    ]},
                    {"$cond": [{"$ifNull": ["$half_day", False]}, 0.5, 1.0]},
                    0,
                ],
            }},
            "leave_days": {"$sum": {
                "$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0],
            }},
            "late_count": {"$sum": {
                "$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0],
            }},
            "total_late_minutes": {"$sum": {"$ifNull": ["$late_minutes", 0]}},
            "total_overtime_minutes": {"$sum": {"$ifNull": ["$overtime_minutes", 0]}},
        }},
    ]


def build_department_summary_pipeline(first_day, last_day, department=None):
    emp_match = {"joined_on": {"$lte": last_day}}
    if department:
        emp_match["department"] = department

    return [
        {"$match": emp_match},
        {"$lookup": {
            "from": "attendance_logs",
            "let": {"emp": "$emp_code"},
            "pipeline": [
                {"$match": {
                    "$expr": {
                        "$and": [
                            {"$eq": ["$emp_code", "$$emp"]},
                            {"$gte": ["$date", first_day]},
                            {"$lte": ["$date", last_day]},
                        ],
                    },
                }},
            ],
            "as": "logs",
        }},
        {"$addFields": {
            "emp_present_days": {
                "$sum": {
                    "$map": {
                        "input": "$logs",
                        "as": "log",
                        "in": {
                            "$cond": [
                                {"$and": [
                                    {"$in": ["$$log.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                                    {"$in": [
                                        {"$dayOfWeek": {"$dateFromString": {"dateString": "$$log.date"}}},
                                        [2, 3, 4, 5, 6],
                                    ]},
                                ]},
                                {"$cond": [{"$ifNull": ["$$log.half_day", False]}, 0.5, 1.0]},
                                0,
                            ],
                        },
                    },
                },
            },
            "emp_work_hours": {
                "$filter": {
                    "input": "$logs",
                    "as": "log",
                    "cond": {
                        "$and": [
                            {"$in": ["$$log.status", ["PRESENT", "WFH", "ON_DUTY"]]},
                            {"$ne": [{"$ifNull": ["$$log.work_hours", None]}, None]},
                        ],
                    },
                },
            },
            "emp_late_count": {
                "$size": {
                    "$filter": {
                        "input": "$logs",
                        "as": "log",
                        "cond": {"$gt": [{"$ifNull": ["$$log.late_minutes", 0]}, 0]},
                    },
                },
            },
            "emp_total_late_minutes": {
                "$sum": "$logs.late_minutes",
            },
            "emp_leave_count": {
                "$size": {
                    "$filter": {
                        "input": "$logs",
                        "as": "log",
                        "cond": {"$eq": ["$$log.status", "LEAVE"]},
                    },
                },
            },
            "emp_on_duty_count": {
                "$size": {
                    "$filter": {
                        "input": "$logs",
                        "as": "log",
                        "cond": {"$eq": ["$$log.status", "ON_DUTY"]},
                    },
                },
            },
        }},
        {"$group": {
            "_id": "$department",
            "headcount": {"$sum": 1},
            "present_days": {"$sum": "$emp_present_days"},
            "total_work_hours": {"$sum": {"$sum": "$emp_work_hours.work_hours"}},
            "work_hours_count": {"$sum": {"$size": "$emp_work_hours"}},
            "late_count": {"$sum": "$emp_late_count"},
            "total_late_minutes": {"$sum": "$emp_total_late_minutes"},
            "leave_count": {"$sum": "$emp_leave_count"},
            "on_duty_count": {"$sum": "$emp_on_duty_count"},
        }},
        {"$sort": {"_id": ASCENDING}},
    ]


def build_late_leaderboard_pipeline(first_day, last_day, limit, department=None):
    pipeline = [
        {"$match": {
            "date": {"$gte": first_day, "$lte": last_day},
            "late_minutes": {"$gt": 0},
        }},
        {"$group": {
            "_id": "$emp_code",
            "total_late_minutes": {"$sum": "$late_minutes"},
            "late_count": {"$sum": 1},
        }},
        {"$lookup": {
            "from": "employees",
            "localField": "_id",
            "foreignField": "emp_code",
            "as": "emp_info",
        }},
        {"$match": {"emp_info": {"$ne": []}}},
        {"$unwind": "$emp_info"},
    ]
    if department:
        pipeline.append({"$match": {"emp_info.department": department}})

    pipeline.extend([
        {"$sort": {"total_late_minutes": DESCENDING, "_id": ASCENDING}},
        {"$setWindowFields": {
            "sortBy": {"total_late_minutes": -1},
            "output": {
                "rank": {"$rank": {}},
            },
        }},
        {"$match": {"rank": {"$lte": limit}}},
        {"$sort": {"total_late_minutes": DESCENDING, "_id": ASCENDING}},
        {"$project": {
            "_id": 0,
            "rank": 1,
            "emp_code": "$_id",
            "name": "$emp_info.name",
            "department": "$emp_info.department",
            "total_late_minutes": 1,
            "late_count": 1,
        }},
    ])
    return pipeline


def build_department_trend_pipeline(emp_codes, date_from, date_to, dept_employees):
    d_from = date_type.fromisoformat(date_from)
    d_to = date_type.fromisoformat(date_to)
    lower_bound = datetime(d_from.year, d_from.month, d_from.day, 0, 0, 0, tzinfo=timezone.utc)
    upper_bound = datetime(d_to.year, d_to.month, d_to.day, 0, 0, 0, tzinfo=timezone.utc) + timedelta(days=1)

    return [
        {"$match": {
            "emp_code": {"$in": emp_codes},
            "date": {"$gte": date_from, "$lte": date_to},
        }},
        {"$group": {
            "_id": "$date",
            "present_count": {"$sum": {
                "$cond": [
                    {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                    {"$cond": [{"$ifNull": ["$half_day", False]}, 0.5, 1.0]},
                    0,
                ],
            }},
            "late_count": {"$sum": {
                "$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0],
            }},
        }},
        {"$unionWith": {
            "coll": "attendance_logs",
            "pipeline": [
                {"$limit": 1},
                {"$project": {
                    "_id": {"$literal": date_from},
                    "present_count": {"$literal": 0},
                    "late_count": {"$literal": 0},
                }},
            ],
        }},
        {"$group": {
            "_id": "$_id",
            "present_count": {"$sum": "$present_count"},
            "late_count": {"$sum": "$late_count"},
        }},
        {"$addFields": {
            "date_dt": {"$dateFromString": {"dateString": "$_id"}},
        }},
        {"$densify": {
            "field": "date_dt",
            "range": {
                "step": 1,
                "unit": "day",
                "bounds": [lower_bound, upper_bound],
            },
        }},
        {"$addFields": {
            "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date_dt"}},
            "present_count": {"$ifNull": ["$present_count", 0]},
            "late_count": {"$ifNull": ["$late_count", 0]},
        }},
        {"$sort": {"date": ASCENDING}},
        {"$addFields": {
            "is_working_day": {
                "$in": [{"$dayOfWeek": "$date_dt"}, [2, 3, 4, 5, 6]],
            },
            "headcount": {
                "$size": {
                    "$filter": {
                        "input": dept_employees,
                        "as": "emp",
                        "cond": {"$lte": ["$$emp.joined_on", "$date"]},
                    },
                },
            },
        }},
        {"$addFields": {
            "attendance_rate": {
                "$cond": [
                    {"$and": [
                        {"$eq": ["$is_working_day", True]},
                        {"$gt": ["$headcount", 0]},
                    ]},
                    {"$divide": ["$present_count", "$headcount"]},
                    None,
                ],
            },
        }},
        {"$setWindowFields": {
            "sortBy": {"date": 1},
            "output": {
                "moving_avg_7d": {
                    "$avg": "$attendance_rate",
                    "window": {
                        "documents": [-6, "current"],
                    },
                },
            },
        }},
        {"$project": {
            "_id": 0,
            "date": 1,
            "is_working_day": 1,
            "headcount": 1,
            "present_count": 1,
            "late_count": 1,
            "attendance_rate": 1,
            "moving_avg_7d": 1,
        }},
    ]


# ── Pydantic Request Models ───────────────────────────────────────────────── #

EMP_CODE_PATTERN = re.compile(r"^EMP\d{4,6}$")
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
SHIFT_PATTERN = re.compile(r"^([01]\d|2[0-3]):[0-5]\d$")


class EmployeeIn(BaseModel):
    emp_code: str
    name: str
    email: str
    department: str
    shift_start: str = "09:30"
    shift_end: str = "18:30"
    joined_on: str

    @field_validator("emp_code")
    @classmethod
    def check_emp_code(cls, v):
        if not EMP_CODE_PATTERN.match(v):
            raise ValueError("emp_code must match EMP followed by 4-6 digits")
        return v

    @field_validator("name")
    @classmethod
    def check_name(cls, v):
        if not v or len(v) > 100:
            raise ValueError("name must be 1-100 characters")
        return v

    @field_validator("email")
    @classmethod
    def check_email(cls, v):
        if not EMAIL_PATTERN.match(v) or len(v) > 120:
            raise ValueError("invalid email format")
        return v

    @field_validator("department")
    @classmethod
    def check_department(cls, v):
        if not v or len(v) > 50:
            raise ValueError("department must be 1-50 characters")
        return v

    @field_validator("shift_start", "shift_end")
    @classmethod
    def check_shift(cls, v):
        if not SHIFT_PATTERN.match(v):
            raise ValueError("shift must be HH:MM (24-hour)")
        return v

    @field_validator("joined_on")
    @classmethod
    def check_joined_on(cls, v):
        try:
            date_type.fromisoformat(v)
        except (ValueError, TypeError):
            raise ValueError("joined_on must be YYYY-MM-DD")
        return v


class PunchInIn(BaseModel):
    emp_code: str
    punched_at: Optional[int] = None
    status: str = "PRESENT"

    @field_validator("status")
    @classmethod
    def check_status(cls, v):
        if v not in PRESENCE_STATUSES:
            raise ValueError(f"status must be one of {sorted(PRESENCE_STATUSES)}")
        return v


class PunchOutIn(BaseModel):
    emp_code: str
    punched_at: Optional[int] = None


class RegularizeIn(BaseModel):
    reason: str
    regularized_by: str
    status: Optional[str] = None
    punch_in: Optional[int] = None
    punch_out: Optional[int] = None

    @field_validator("reason")
    @classmethod
    def check_reason(cls, v):
        if len(v) < 5 or len(v) > 200:
            raise ValueError("reason must be 5-200 characters")
        return v

    @field_validator("regularized_by")
    @classmethod
    def check_by(cls, v):
        if not v or len(v) > 50:
            raise ValueError("regularized_by must be 1-50 characters")
        return v

    @field_validator("status")
    @classmethod
    def check_status(cls, v):
        if v is not None and v not in ALL_STATUSES:
            raise ValueError(f"status must be one of {sorted(ALL_STATUSES)}")
        return v


# ── System Endpoints ──────────────────────────────────────────────────────── #

@app.get("/health")
def health():
    """Readiness probe. Returns 200 only if MongoDB answers a ping, otherwise 503."""
    try:
        client.admin.command("ping")
    except Exception:
        raise HTTPException(503, detail="MongoDB unavailable")
    return {"status": "ok"}


# ── Employee Endpoints ────────────────────────────────────────────────────── #

@app.post("/employees", status_code=201)
def create_employee(body: EmployeeIn):
    """Create an employee. Enforces uniqueness atomically via unique index on emp_code."""
    if body.shift_start == body.shift_end:
        raise HTTPException(422, detail="shift_start must differ from shift_end")

    doc = body.model_dump()
    doc["created_at"] = datetime.now(timezone.utc)

    try:
        db.employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, detail="emp_code already exists")

    doc.pop("_id", None)
    return format_employee(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """List employees, sorted by emp_code ascending, with pagination."""
    query = {}
    if department:
        query["department"] = department

    total = db.employees.count_documents(query)
    skip = (page - 1) * page_size
    cursor = (
        db.employees.find(query, {"_id": 0})
        .sort("emp_code", ASCENDING)
        .skip(skip)
        .limit(page_size)
    )
    items = [format_employee(doc) for doc in cursor]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


# ── Attendance Lifecycle Endpoints ────────────────────────────────────────── #

@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInIn):
    """Record punch-in. Creates day's record. Concurrency-safe via unique compound index."""
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if emp is None:
        raise HTTPException(404, detail="Employee not found")

    if body.punched_at is not None:
        validate_epoch_ms(body.punched_at)
        ts = epoch_ms_to_datetime(body.punched_at)
    else:
        ts = datetime.now(timezone.utc).replace(microsecond=0)

    att_date = determine_attendance_date(ts, emp["shift_start"], emp["shift_end"])
    late = compute_late_minutes(ts, emp["shift_start"], att_date)

    doc = {
        "emp_code": body.emp_code,
        "date": att_date,
        "status": body.status,
        "punch_in": ts,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": late,
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }

    try:
        db.attendance_logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, detail="Already punched in for this date")

    doc.pop("_id", None)
    return format_attendance_record(doc)


@app.post("/attendance/punch-out", status_code=200)
def punch_out(body: PunchOutIn):
    """Record punch-out. Closes most recent open punch-in at or before punched_at."""
    emp = db.employees.find_one({"emp_code": body.emp_code})
    if emp is None:
        raise HTTPException(404, detail="Employee not found")

    if body.punched_at is not None:
        validate_epoch_ms(body.punched_at)
        ts = epoch_ms_to_datetime(body.punched_at)
    else:
        ts = datetime.now(timezone.utc).replace(microsecond=0)

    record = db.attendance_logs.find_one(
        {
            "emp_code": body.emp_code,
            "punch_in": {"$lte": ts, "$ne": None},
            "punch_out": None,
        },
        sort=[("punch_in", DESCENDING)],
    )

    if record is None:
        already = db.attendance_logs.find_one(
            {
                "emp_code": body.emp_code,
                "punch_in": {"$lte": ts, "$ne": None},
                "punch_out": {"$ne": None},
            },
            sort=[("punch_in", DESCENDING)],
        )
        if already is not None:
            raise HTTPException(409, detail="Already punched out for this record")
        raise HTTPException(404, detail="No open punch-in found")

    punch_in_dt = record["punch_in"]
    if punch_in_dt.tzinfo is None:
        punch_in_dt = punch_in_dt.replace(tzinfo=timezone.utc)

    if ts <= punch_in_dt:
        raise HTTPException(422, detail="punch_out must be after punch_in")
    if (ts - punch_in_dt).total_seconds() > 86400:
        raise HTTPException(422, detail="punch_out must be within 24 hours of punch_in")

    wh = compute_work_hours(punch_in_dt, ts)
    ot = compute_overtime(ts, emp["shift_end"], record["date"], emp["shift_start"])
    hd = is_half_day(wh)

    updated = db.attendance_logs.find_one_and_update(
        {"_id": record["_id"], "punch_out": None},
        {"$set": {
            "punch_out": ts,
            "work_hours": wh,
            "overtime_minutes": ot,
            "half_day": hd,
        }},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, detail="Already punched out (concurrent request)")

    return format_attendance_record(updated)


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """List attendance records, sorted by date desc then emp_code asc. DB-side pagination."""
    if date_from is not None:
        validate_date_str(date_from, "date_from")
    if date_to is not None:
        validate_date_str(date_to, "date_to")

    if date_from and date_to and date_from > date_to:
        raise HTTPException(422, detail="date_from must not be after date_to")

    if status is not None and status not in ALL_STATUSES:
        raise HTTPException(422, detail=f"Invalid status: {status}")

    query = build_attendance_list_query(emp_code, date_from, date_to, status)

    total = db.attendance_logs.count_documents(query)
    skip = (page - 1) * page_size
    cursor = (
        db.attendance_logs.find(query, {"_id": 0})
        .sort([("date", DESCENDING), ("emp_code", ASCENDING)])
        .skip(skip)
        .limit(page_size)
    )
    items = [format_attendance_record(doc) for doc in cursor]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize_attendance(
    emp_code: str = Path(...),
    date: str = Path(..., description="YYYY-MM-DD"),
    body: RegularizeIn = ...,
):
    """Manually regularize one day's record. Recomputes derived fields and appends audit trail."""
    validate_date_str(date)

    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, detail="Employee not found")

    record = db.attendance_logs.find_one({"emp_code": emp_code, "date": date})
    if record is None:
        raise HTTPException(404, detail="No attendance record for this date")

    new_status = body.status if body.status is not None else record["status"]
    is_absence = new_status in ("ABSENT", "LEAVE")

    if is_absence and (body.punch_in is not None or body.punch_out is not None):
        raise HTTPException(
            422,
            detail="Cannot supply punch_in/punch_out when status is ABSENT or LEAVE",
        )

    if is_absence:
        new_punch_in = None
        new_punch_out = None
    else:
        if body.punch_in is not None:
            validate_epoch_ms(body.punch_in, "punch_in")
            new_punch_in = epoch_ms_to_datetime(body.punch_in)
        else:
            new_punch_in = record.get("punch_in")

        if new_punch_in is None:
            raise HTTPException(422, detail="A presence status requires a punch_in")

        if new_punch_in.tzinfo is None:
            new_punch_in = new_punch_in.replace(tzinfo=timezone.utc)

        expected_date = determine_attendance_date(
            new_punch_in, emp["shift_start"], emp["shift_end"]
        )
        if expected_date != date:
            raise HTTPException(
                422, detail="punch_in must fall on the record's attendance date"
            )

        if body.punch_out is not None:
            validate_epoch_ms(body.punch_out, "punch_out")
            new_punch_out = epoch_ms_to_datetime(body.punch_out)
        else:
            new_punch_out = record.get("punch_out")
            if new_punch_out is not None and new_punch_out.tzinfo is None:
                new_punch_out = new_punch_out.replace(tzinfo=timezone.utc)

        if new_punch_out is not None:
            if new_punch_out <= new_punch_in:
                raise HTTPException(422, detail="punch_out must be after punch_in")
            if (new_punch_out - new_punch_in).total_seconds() > 86400:
                raise HTTPException(422, detail="punch_out must be within 24 hours of punch_in")

    if is_absence:
        new_late = 0
        new_wh = None
        new_ot = 0
        new_hd = False
    else:
        new_late = compute_late_minutes(new_punch_in, emp["shift_start"], date)
        if new_punch_out is not None:
            new_wh = compute_work_hours(new_punch_in, new_punch_out)
            new_ot = compute_overtime(
                new_punch_out, emp["shift_end"], date, emp["shift_start"]
            )
            new_hd = is_half_day(new_wh)
        else:
            new_wh = None
            new_ot = 0
            new_hd = False

    old_punch_in = record.get("punch_in")
    if old_punch_in is not None and old_punch_in.tzinfo is None:
        old_punch_in = old_punch_in.replace(tzinfo=timezone.utc)
    old_punch_out = record.get("punch_out")
    if old_punch_out is not None and old_punch_out.tzinfo is None:
        old_punch_out = old_punch_out.replace(tzinfo=timezone.utc)

    changes = {}
    if new_status != record["status"]:
        changes["status"] = {"from": record["status"], "to": new_status}
    if new_punch_in != old_punch_in:
        changes["punch_in"] = {"from": old_punch_in, "to": new_punch_in}
    if new_punch_out != old_punch_out:
        changes["punch_out"] = {"from": old_punch_out, "to": new_punch_out}
    if new_wh != record.get("work_hours"):
        changes["work_hours"] = {"from": record.get("work_hours"), "to": new_wh}
    if new_late != record.get("late_minutes", 0):
        changes["late_minutes"] = {"from": record.get("late_minutes", 0), "to": new_late}
    if new_ot != record.get("overtime_minutes", 0):
        changes["overtime_minutes"] = {
            "from": record.get("overtime_minutes", 0), "to": new_ot,
        }
    if new_hd != record.get("half_day", False):
        changes["half_day"] = {"from": record.get("half_day", False), "to": new_hd}

    if not changes:
        raise HTTPException(422, detail="No fields changed")

    history_entry = {
        "at": datetime.now(timezone.utc),
        "by": body.regularized_by,
        "reason": body.reason,
        "changes": changes,
    }

    update_fields = {
        "status": new_status,
        "punch_in": new_punch_in,
        "punch_out": new_punch_out,
        "work_hours": new_wh,
        "late_minutes": new_late,
        "overtime_minutes": new_ot,
        "half_day": new_hd,
    }

    # Guard concurrency using the current history length
    current_history_len = len(record.get("history", []))
    updated = db.attendance_logs.find_one_and_update(
        {
            "_id": record["_id"],
            "history": {"$size": current_history_len},
        },
        {
            "$set": update_fields,
            "$push": {"history": history_entry},
        },
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, detail="Concurrent modification detected")

    return format_attendance_record(updated)


# ── Analytics Endpoints ───────────────────────────────────────────────────── #

@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(
    emp_code: str = Path(...),
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
):
    """Monthly attendance summary for one employee, backed by MongoDB aggregation."""
    emp = db.employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise HTTPException(404, detail="Employee not found")

    validate_month_str(month)
    first_day, last_day = parse_month_range(month)

    joined = emp["joined_on"]
    effective_start = max(joined, first_day)
    if effective_start > last_day:
        working_days = 0
    else:
        working_days = count_working_days_in_range(effective_start, last_day)

    pipeline = build_employee_monthly_pipeline(emp_code, first_day, last_day)
    result = list(db.attendance_logs.aggregate(pipeline))

    if result:
        r = result[0]
        present_days = r["present_days"]
        leave_days = r["leave_days"]
        late_count = r["late_count"]
        total_late = r["total_late_minutes"]
        total_ot = r["total_overtime_minutes"]
    else:
        present_days = 0
        leave_days = 0
        late_count = 0
        total_late = 0
        total_ot = 0

    if working_days > 0:
        attendance_pct = round_half_up(present_days / working_days * 100, 2)
    else:
        attendance_pct = None

    return {
        "emp_code": emp_code,
        "month": month,
        "working_days": working_days,
        "present_days": present_days,
        "leave_days": leave_days,
        "late_count": late_count,
        "total_late_minutes": total_late,
        "total_overtime_minutes": total_ot,
        "attendance_pct": attendance_pct,
    }


@app.get("/analytics/departments/summary")
def department_summary(
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    department: Optional[str] = None,
):
    """Department summary for a month via MongoDB aggregation across employees and logs."""
    validate_month_str(month)
    first_day, last_day = parse_month_range(month)

    pipeline = build_department_summary_pipeline(first_day, last_day, department)
    dept_results = list(db.employees.aggregate(pipeline))

    items = []
    for d in dept_results:
        headcount = d["headcount"]
        if headcount == 0:
            continue
        wh_count = d["work_hours_count"]
        avg_wh = round_half_up(d["total_work_hours"] / wh_count, 2) if wh_count > 0 else None
        items.append({
            "department": d["_id"],
            "headcount": headcount,
            "present_days": round_half_up(d["present_days"], 2),
            "avg_work_hours": avg_wh,
            "late_count": d["late_count"],
            "total_late_minutes": d["total_late_minutes"],
            "leave_count": d["leave_count"],
            "on_duty_count": d["on_duty_count"],
        })

    return {"month": month, "items": items}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: str = Query(..., pattern=r"^\d{4}-(0[1-9]|1[0-2])$"),
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    """Late-comers leaderboard with standard competition ranking ($setWindowFields + $rank)."""
    validate_month_str(month)
    first_day, last_day = parse_month_range(month)

    pipeline = build_late_leaderboard_pipeline(first_day, last_day, limit, department)
    results = list(db.attendance_logs.aggregate(pipeline))
    return {"month": month, "items": results}


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str = Path(...),
    date_from: str = Query(..., alias="from"),
    date_to: str = Query(..., alias="to"),
):
    """Daily attendance trend with database-level gap filling ($densify) and 7-day moving average."""
    validate_date_str(date_from, "from")
    validate_date_str(date_to, "to")

    if date_to < date_from:
        raise HTTPException(422, detail="'to' must not be before 'from'")

    d_from = date_type.fromisoformat(date_from)
    d_to = date_type.fromisoformat(date_to)
    if (d_to - d_from).days > 92:
        raise HTTPException(422, detail="Date range must not exceed 92 days")

    dept_employees = list(db.employees.find(
        {"department": department},
        {"_id": 0, "emp_code": 1, "joined_on": 1},
    ))
    if not dept_employees:
        raise HTTPException(404, detail="Department not found")

    emp_codes = [e["emp_code"] for e in dept_employees]

    try:
        pipeline = build_department_trend_pipeline(emp_codes, date_from, date_to, dept_employees)
        raw_items = list(db.attendance_logs.aggregate(pipeline))
        items = []
        for r in raw_items:
            items.append({
                "date": r["date"],
                "is_working_day": r["is_working_day"],
                "headcount": r["headcount"],
                "present_count": round_half_up(r["present_count"], 2),
                "late_count": r["late_count"],
                "attendance_rate": round_half_up(r["attendance_rate"], 4) if r.get("attendance_rate") is not None else None,
                "moving_avg_7d": round_half_up(r["moving_avg_7d"], 4) if r.get("moving_avg_7d") is not None else None,
            })
    except Exception:
        # Fallback in case of server-side densify support limitations
        att_pipeline = [
            {"$match": {
                "emp_code": {"$in": emp_codes},
                "date": {"$gte": date_from, "$lte": date_to},
            }},
            {"$group": {
                "_id": "$date",
                "present_count": {"$sum": {
                    "$cond": [
                        {"$in": ["$status", ["PRESENT", "WFH", "ON_DUTY"]]},
                        {"$cond": [{"$ifNull": ["$half_day", False]}, 0.5, 1.0]},
                        0,
                    ],
                }},
                "late_count": {"$sum": {
                    "$cond": [{"$gt": [{"$ifNull": ["$late_minutes", 0]}, 0]}, 1, 0],
                }},
            }},
        ]
        att_data = {r["_id"]: r for r in db.attendance_logs.aggregate(att_pipeline)}
        items = []
        current = d_from
        rate_window = []

        while current <= d_to:
            day_str = current.isoformat()
            is_working = current.weekday() < 5
            headcount = sum(1 for e in dept_employees if e["joined_on"] <= day_str)

            day_att = att_data.get(day_str)
            present_count = day_att["present_count"] if day_att else 0
            late_count = day_att["late_count"] if day_att else 0

            if is_working and headcount > 0:
                rate = round_half_up(present_count / headcount, 4)
            else:
                rate = None
            rate_window.append(rate)

            recent = rate_window[max(0, len(rate_window) - 7):]
            non_null = [r for r in recent if r is not None]
            moving_avg = round_half_up(sum(non_null) / len(non_null), 4) if non_null else None

            items.append({
                "date": day_str,
                "is_working_day": is_working,
                "headcount": headcount,
                "present_count": present_count,
                "late_count": late_count,
                "attendance_rate": rate,
                "moving_avg_7d": moving_avg,
            })
            current += timedelta(days=1)

    return {"department": department, "items": items}


# ── Admin Explain Endpoint ────────────────────────────────────────────────── #

@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: str = Path(...),
    emp_code: Optional[str] = None,
    month: Optional[str] = None,
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = Query(None, alias="date_from"),
    date_to: Optional[str] = Query(None, alias="date_to"),
    status: Optional[str] = None,
    range_from: Optional[str] = Query(None, alias="from"),
    range_to: Optional[str] = Query(None, alias="to"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """Run MongoDB explain with executionStats verbosity for an endpoint's query/pipeline."""
    valid_endpoints = {
        "attendance_list", "employee_monthly", "department_summary",
        "late_leaderboard", "department_trend",
    }
    if endpoint not in valid_endpoints:
        raise HTTPException(422, detail=f"Unknown endpoint: {endpoint}")

    if endpoint == "attendance_list":
        query = build_attendance_list_query(emp_code, date_from, date_to, status)
        explain = db.command(
            "explain",
            {
                "find": "attendance_logs",
                "filter": query,
                "sort": {"date": -1, "emp_code": 1},
                "skip": (page - 1) * page_size,
                "limit": page_size,
            },
            verbosity="executionStats",
        )
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": explain}

    elif endpoint == "employee_monthly":
        if not emp_code:
            raise HTTPException(422, detail="emp_code is required for employee_monthly")
        if not month:
            raise HTTPException(422, detail="month is required for employee_monthly")
        validate_month_str(month)
        first_day, last_day = parse_month_range(month)
        pipeline = build_employee_monthly_pipeline(emp_code, first_day, last_day)
        explain = db.command(
            "explain",
            {"aggregate": "attendance_logs", "pipeline": pipeline, "cursor": {}},
            verbosity="executionStats",
        )
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": explain}

    elif endpoint == "department_summary":
        if not month:
            raise HTTPException(422, detail="month is required for department_summary")
        validate_month_str(month)
        first_day, last_day = parse_month_range(month)
        pipeline = build_department_summary_pipeline(first_day, last_day, department)
        explain = db.command(
            "explain",
            {"aggregate": "employees", "pipeline": pipeline, "cursor": {}},
            verbosity="executionStats",
        )
        return {"endpoint": endpoint, "collection": "employees", "explain": explain}

    elif endpoint == "late_leaderboard":
        if not month:
            raise HTTPException(422, detail="month is required for late_leaderboard")
        validate_month_str(month)
        first_day, last_day = parse_month_range(month)
        pipeline = build_late_leaderboard_pipeline(first_day, last_day, limit, department)
        explain = db.command(
            "explain",
            {"aggregate": "attendance_logs", "pipeline": pipeline, "cursor": {}},
            verbosity="executionStats",
        )
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": explain}

    elif endpoint == "department_trend":
        if not department:
            raise HTTPException(422, detail="department is required for department_trend")
        if not range_from or not range_to:
            raise HTTPException(422, detail="from and to are required for department_trend")

        dept_employees = list(db.employees.find(
            {"department": department},
            {"_id": 0, "emp_code": 1, "joined_on": 1},
        ))
        emp_codes = [e["emp_code"] for e in dept_employees]
        pipeline = build_department_trend_pipeline(emp_codes, range_from, range_to, dept_employees)
        explain = db.command(
            "explain",
            {"aggregate": "attendance_logs", "pipeline": pipeline, "cursor": {}},
            verbosity="executionStats",
        )
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": explain}
