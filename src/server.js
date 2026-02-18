const express = require("express");
const multer = require("multer");
const {
  processClient,
  parseInputRowsFromUpload,
  processCsvBatch,
  InputFileValidationError,
} = require("./processor");
const { consumeConsultDay, DEFAULT_TOTAL_LIMIT, ConsultDayLimitError } = require("./consultDayRepo");
const { saveConsultaPresencaResults, insertPendingConsultaPresenca, getConsultedCpfsTodayByLogin } = require("./consultaPresencaRepo");
const { createJob, markProgress, markSkipped, finishJob, getJob, getCurrentJob } = require("./statusTracker");

const app = express();
const upload = multer({ storage: multer.memoryStorage() });
const MAX_CSV_ROWS = 1000;
const DEFAULT_PRESENCA_LOGIN = process.env.PRESENCA_LOGIN || "40138573832_HDSL";
const DEFAULT_PRESENCA_SENHA = process.env.PRESENCA_SENHA || "Presenca@1516";

app.use(express.json({ limit: "10mb" }));

app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "api-presenca-node" });
});

app.get("/api/status/current", (_req, res) => {
  const status = getCurrentJob();
  if (!status) {
    return res.status(404).json({ ok: false, error: "Nenhum processamento encontrado." });
  }
  return res.json({ ok: true, status });
});

app.get("/api/status/:jobId", (req, res) => {
  const status = getJob(req.params.jobId);
  if (!status) {
    return res.status(404).json({ ok: false, error: "jobId nao encontrado." });
  }
  return res.json({ ok: true, status });
});

app.post("/api/process/individual", async (req, res) => {
  let jobId = null;
  try {
    const { cpf, nome, telefone, produtoId, autoAcceptHeadless = true, stepDelayMs, login, senha } = req.body || {};
    if (!cpf || !nome || !telefone) {
      return res.status(400).json({ ok: false, error: "Informe cpf, nome e telefone" });
    }
    const loginValue = String(login || DEFAULT_PRESENCA_LOGIN);
    const senhaValue = String(senha || DEFAULT_PRESENCA_SENHA);
    jobId = createJob({ type: "individual", total: 1 });
    let consultDay;
    try {
      consultDay = await consumeConsultDay({
        loginP: loginValue,
        senhaP: senhaValue,
        usedDelta: 1,
        total: DEFAULT_TOTAL_LIMIT,
      });
    } catch (err) {
      if (err instanceof ConsultDayLimitError) {
        if (jobId) finishJob(jobId, { errorMessage: err.message });
        return res.status(403).json({
          ok: false,
          error: err.message,
          consultDay: err.meta,
        });
      }
      throw err;
    }
    const result = await processClient(
      { cpf, nome, telefone },
      { produtoId, autoAcceptHeadless, stepDelayMs, login: loginValue, senha: senhaValue }
    );
    markProgress(jobId, { ok: result.final_status === "OK", errorMessage: result.final_message });
    const persisted = await saveConsultaPresencaResults([result], {
      loginP: loginValue,
      tipoConsulta: "Individual",
    });
    finishJob(jobId);
    return res.json({ ok: result.final_status === "OK", jobId, consultDay, persisted, result });
  } catch (err) {
    if (jobId) finishJob(jobId, { errorMessage: String(err.message || err) });
    return res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.post("/api/process/csv", upload.single("file"), async (req, res) => {
  let jobId = null;
  try {
    if (!req.file) {
      return res.status(400).json({ ok: false, error: "Envie um arquivo CSV (;) ou XLSX no campo file" });
    }
    const { randomRows = 0, produtoId, autoAcceptHeadless = "true", stepDelayMs, login, senha } = req.body || {};
    const loginValue = String(login || DEFAULT_PRESENCA_LOGIN);
    const senhaValue = String(senha || DEFAULT_PRESENCA_SENHA);
    const rows = parseInputRowsFromUpload(req.file);
    if (rows.length > MAX_CSV_ROWS) {
      return res.status(400).json({
        ok: false,
        error: `Arquivo possui ${rows.length} registros. O limite maximo e ${MAX_CSV_ROWS}.`,
      });
    }
    const randomRowsNum = Number(randomRows || 0);
    let selectedRows = rows;
    if (randomRowsNum > 0 && rows.length > randomRowsNum) {
      selectedRows = [...rows].sort(() => Math.random() - 0.5).slice(0, randomRowsNum);
    }

    const seenInFile = new Set();
    const uniqueRows = selectedRows.filter((row) => {
      if (!row.cpf) return true;
      if (seenInFile.has(row.cpf)) return false;
      seenInFile.add(row.cpf);
      return true;
    });
    const skippedDuplicatedInFile = selectedRows.length - uniqueRows.length;

    const cpfsToCheck = uniqueRows.map((r) => r.cpf).filter(Boolean);
    const consultedToday = await getConsultedCpfsTodayByLogin(loginValue, cpfsToCheck);
    const rowsToProcess = uniqueRows.filter((r) => !r.cpf || !consultedToday.has(r.cpf));
    const skippedDuplicatedToday = uniqueRows.length - rowsToProcess.length;
    jobId = createJob({ type: "lote", total: rowsToProcess.length });
    markSkipped(jobId, skippedDuplicatedInFile + skippedDuplicatedToday);

    if (!rowsToProcess.length) {
      finishJob(jobId);
      return res.json({
        ok: true,
        jobId,
        total: 0,
        okCount: 0,
        erroCount: 0,
        consultDay: null,
        persisted: { insertedRows: 0, skippedRows: 0 },
        skippedDuplicatedInFile,
        skippedDuplicatedToday,
        results: [],
        message: "Nenhum registro para processar. Todos os CPFs ja foram consultados hoje para este login.",
      });
    }

    const fileNameValue = String(req.body?.fileName || req.file?.originalname || "Arquivo em lote").trim();
    const createdAt = new Date();
    await insertPendingConsultaPresenca(
      rowsToProcess.map((row) => ({
        cpf: row.cpf,
        nome: row.nome,
        telefone: row.telefone,
      })),
      {
        loginP: loginValue,
        tipoConsulta: fileNameValue,
        createdAt,
      }
    );
    const usedDelta = rowsToProcess.length;
    let consultDay;
    try {
      consultDay = await consumeConsultDay({
        loginP: loginValue,
        senhaP: senhaValue,
        usedDelta,
        total: DEFAULT_TOTAL_LIMIT,
      });
    } catch (err) {
      if (err instanceof ConsultDayLimitError) {
        if (jobId) finishJob(jobId, { errorMessage: err.message });
        return res.status(403).json({
          ok: false,
          error: err.message,
          consultDay: err.meta,
        });
      }
      throw err;
    }
    const results = await processCsvBatch(rowsToProcess, {
      randomRows: 0,
      produtoId: produtoId ? Number(produtoId) : 28,
      autoAcceptHeadless: String(autoAcceptHeadless).toLowerCase() !== "false",
      stepDelayMs: stepDelayMs != null ? Number(stepDelayMs) : undefined,
      login: loginValue,
      senha: senhaValue,
      onItemProcessed: async (item) => {
        markProgress(jobId, { ok: item.final_status === "OK", errorMessage: item.final_message });
      },
    });
    const persisted = await saveConsultaPresencaResults(results, {
      loginP: loginValue,
      tipoConsulta: fileNameValue,
      createdAt,
      status: "Concluido",
    });
    const okCount = results.filter((r) => r.final_status === "OK").length;
    finishJob(jobId);
    return res.json({
      ok: true,
      jobId,
      total: results.length,
      okCount,
      erroCount: results.length - okCount,
      consultDay,
      persisted,
      skippedDuplicatedInFile,
      skippedDuplicatedToday,
      results,
    });
  } catch (err) {
    if (jobId) finishJob(jobId, { errorMessage: String(err.message || err) });
    if (err instanceof InputFileValidationError) {
      return res.status(400).json({ ok: false, error: err.message });
    }
    return res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, () => {
  console.log(`API Presenca Node rodando na porta ${PORT}`);
  console.log("POST /api/process/individual");
  console.log("POST /api/process/csv");
});
