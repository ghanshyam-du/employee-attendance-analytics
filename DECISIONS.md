# DECISIONS.md

1. **Indexes.** I created four indexes: (a) a unique index on `employees.emp_code` so duplicate creates fail atomically in the database, not in a check-then-insert race; (b) a unique compound index on `attendance_logs (emp_code, date)` to enforce one record per employee per day and make concurrent punch-ins safe; (c) a compound index on `attendance_logs (date DESC, emp_code ASC)` to serve the attendance list and all analytics that match on date ranges; (d) single-field indexes on `employees.department` and `employees.joined_on` for headcount and department queries. I considered a compound index on `(status, date)` but rejected it because analytics filter on date first and the date index already covers it well enough.

2. **Punch-in race.** The unique compound index on `(emp_code, date)` means the second `insert_one` raises a `DuplicateKeyError`. My code catches it and returns 409. There is no `find_one` check before insert—the database enforces uniqueness, so exactly one request gets 201.

3. **Ties.** I use MongoDB's `$setWindowFields` with `$rank`, which gives standard competition ranking (1, 2, 2, 4). The `limit` filter is applied after ranking (`rank <= limit`), so if two employees tie at rank 10, both are returned even though that is more than `limit` rows. Within the same rank, rows are ordered by `emp_code` ascending.

4. **Headcount.** The department summary first aggregates `employees` by department, collecting every employee whose `joined_on <= last day of the month`. This count is the headcount. Attendance logs are looked up separately for those employee codes. An employee with zero logs contributes to headcount but adds nothing to present_days or late_count. People who joined after the month are excluded from both headcount and log queries.

5. **One thing I would change.** I would shard `attendance_logs` by `emp_code` (or a hashed key) to distribute the write and read load horizontally, and precompute monthly roll-ups into a separate `monthly_summaries` collection using a change-stream-driven worker, so analytics endpoints read a small summary document instead of scanning thousands of logs each time.
