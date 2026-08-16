import http from "k6/http";
import { check, sleep } from "k6";
import { Trend, Counter } from "k6/metrics";

// ---- Config ----
const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const LOG_JOB_COUNT = 1000;
const CODE_EXEC_JOB_COUNT = 100;
const POLL_INTERVAL_S = 0.5;
const POLL_TIMEOUT_S = 30;

// ---- Custom metrics reported at the end ----
const logEnqueueLatency = new Trend("log_enqueue_latency_ms");
const judgeEnqueueLatency = new Trend("judge_enqueue_latency_ms");
const judgePollLatency = new Trend("judge_total_wait_ms");
const judgeFailures = new Counter("judge_failures");
const logFailures = new Counter("log_failures");

const headers = { headers: { "Content-Type": "application/json" } };

// Matches POST /jobs: { "type": string, "payload": <raw json> }
function logJobBody() {
  return JSON.stringify({
    type: "log",
    payload: { message: `load test log job ${Date.now()}-${Math.random()}` },
  });
}

// Matches POST /judge request struct in handler.go exactly.
// Note: expected_output is required by the handler's validation, and
// compare is forced to true server-side regardless of what you send.
function judgeBody() {
  return JSON.stringify({
    language: "python3",
    source_code: 'print("hello world")',
    stdin: "",
    expected_output: "hello world\n",
    time_limit_ms: 2000,
    memory_limit_kb: 65536,
    compare: true,
  });
}

// Non-terminal job statuses (from jobs.Status* constants) — keep polling while in these
const NON_TERMINAL = ["pending", "processing", "retrying"];

export const options = {
  scenarios: {
    log_jobs: {
      executor: "shared-iterations",
      vus: 20,
      iterations: LOG_JOB_COUNT,
      maxDuration: "2m",
      exec: "enqueueLogJob",
    },
    code_execution_jobs: {
      executor: "shared-iterations",
      vus: 10,
      iterations: CODE_EXEC_JOB_COUNT,
      maxDuration: "5m",
      exec: "runCodeExecutionJob",
      startTime: "5s", // let log jobs start first
    },
  },
};

// ---- Log jobs: fire and forget, as fast as possible ----
export function enqueueLogJob() {
  const res = http.post(`${BASE_URL}/jobs`, logJobBody(), headers);
  logEnqueueLatency.add(res.timings.duration);
  const ok = check(res, { "log job enqueued (201)": (r) => r.status === 201 });
  if (!ok) logFailures.add(1);
}

// ---- Code execution jobs: POST /judge, then poll GET /judge/:id ----
export function runCodeExecutionJob() {
  const start = Date.now();

  const res = http.post(`${BASE_URL}/judge`, judgeBody(), headers);
  judgeEnqueueLatency.add(res.timings.duration);

  const enqueueOk = check(res, {
    "judge job enqueued (201)": (r) => r.status === 201,
  });
  if (!enqueueOk) {
    judgeFailures.add(1);
    return;
  }

  let jobId;
  try {
    jobId = JSON.parse(res.body).id;
  } catch (e) {
    judgeFailures.add(1);
    return;
  }
  if (!jobId) {
    judgeFailures.add(1);
    return;
  }

  // GET /judge/:id returns { "job_status": "...", "result": {...} }
  let elapsed = 0;
  let finished = false;
  let finalStatus = null;

  while (elapsed < POLL_TIMEOUT_S) {
    const pollRes = http.get(`${BASE_URL}/judge/${jobId}`, headers);
    if (pollRes.status === 200) {
      let body;
      try {
        body = JSON.parse(pollRes.body);
      } catch (e) {
        body = {};
      }
      if (body.job_status && !NON_TERMINAL.includes(body.job_status)) {
        finished = true;
        finalStatus = body.job_status;
        break;
      }
    }
    sleep(POLL_INTERVAL_S);
    elapsed += POLL_INTERVAL_S;
  }

  judgePollLatency.add(Date.now() - start);
  check(null, {
    "judge job completed (not failed)": () => finalStatus === "completed",
  });
  if (!finished || finalStatus !== "completed") judgeFailures.add(1);
}

// ---- Summary printed at the end of the run ----
export function handleSummary(data) {
  return {
    stdout: JSON.stringify(data, null, 2),
    "load_test_summary.json": JSON.stringify(data, null, 2),
  };
}
