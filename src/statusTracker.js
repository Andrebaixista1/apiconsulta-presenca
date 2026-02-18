const { randomUUID } = require("crypto");

const jobs = new Map();
let currentJobId = null;

function nowIso() {
  return new Date().toISOString();
}

function createJob({ type, total }) {
  const id = randomUUID();
  const job = {
    jobId: id,
    type,
    status: "running",
    startedAt: nowIso(),
    finishedAt: null,
    total: Number(total || 0),
    processed: 0,
    okCount: 0,
    errorCount: 0,
    skipped: 0,
    avgMsPerItem: null,
    etaMs: null,
    elapsedMs: 0,
    lastError: null,
  };
  jobs.set(id, job);
  currentJobId = id;
  return id;
}

function updateEta(job) {
  const started = new Date(job.startedAt).getTime();
  job.elapsedMs = Date.now() - started;
  if (job.processed > 0) {
    job.avgMsPerItem = Math.round(job.elapsedMs / job.processed);
    const remaining = Math.max(0, job.total - job.processed);
    job.etaMs = remaining * job.avgMsPerItem;
  } else {
    job.avgMsPerItem = null;
    job.etaMs = null;
  }
}

function markProgress(jobId, { ok, skipped = 0, errorMessage = null } = {}) {
  const job = jobs.get(jobId);
  if (!job || job.status !== "running") return;
  job.processed += 1;
  if (ok === true) job.okCount += 1;
  if (ok === false) job.errorCount += 1;
  if (skipped > 0) job.skipped += Number(skipped);
  if (errorMessage) job.lastError = String(errorMessage);
  updateEta(job);
}

function markSkipped(jobId, count) {
  const job = jobs.get(jobId);
  if (!job || job.status !== "running") return;
  job.skipped += Number(count || 0);
  updateEta(job);
}

function finishJob(jobId, { errorMessage = null } = {}) {
  const job = jobs.get(jobId);
  if (!job) return;
  if (errorMessage) {
    job.status = "error";
    job.lastError = String(errorMessage);
  } else if (job.status === "running") {
    job.status = "done";
  }
  job.finishedAt = nowIso();
  updateEta(job);
  job.etaMs = 0;
}

function getJob(jobId) {
  return jobs.get(jobId) || null;
}

function getCurrentJob() {
  if (!currentJobId) return null;
  return jobs.get(currentJobId) || null;
}

module.exports = {
  createJob,
  markProgress,
  markSkipped,
  finishJob,
  getJob,
  getCurrentJob,
};
