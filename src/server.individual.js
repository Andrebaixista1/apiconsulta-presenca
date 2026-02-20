const express = require("express");
const multer = require("multer");
const { processClient, parseInputRowsFromUpload, InputFileValidationError } = require("./processor");
const { consumeConsultDay, DEFAULT_TOTAL_LIMIT, ConsultDayLimitError } = require("./consultDayRepo");
const { saveConsultaPresencaResults, insertPendingConsultaPresenca } = require("./consultaPresencaRepo");
const { runInConsultationQueue } = require("./consultationQueue");
const { createJob, markProgress, finishJob, getJob, getCurrentJob } = require("./statusTracker");
const { startConsultDayResetMonitor } = require("./consultDayResetMonitor");
const { startPendingMonitor } = require("./pendingMonitor");

const app = express();
const upload = multer({ storage: multer.memoryStorage() });
const DEFAULT_PRESENCA_LOGIN = process.env.PRESENCA_LOGIN || "40138573832_HDSL";
const DEFAULT_PRESENCA_SENHA = process.env.PRESENCA_SENHA || "Presenca@1516";
const MAX_CSV_ROWS = Number(process.env.PRESENCA_LOTECSV_MAX_ROWS || 1000);

app.use(express.json({ limit: "10mb" }));

app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "api-presenca-node-individual" });
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

    const processed = await runInConsultationQueue(async () => {
      const consultDay = await consumeConsultDay({
        loginP: loginValue,
        senhaP: senhaValue,
        usedDelta: 1,
        total: DEFAULT_TOTAL_LIMIT,
      });

      const result = await processClient(
        { cpf, nome, telefone },
        { produtoId, autoAcceptHeadless, stepDelayMs, login: loginValue, senha: senhaValue }
      );

      const persisted = await saveConsultaPresencaResults([result], {
        loginP: loginValue,
        tipoConsulta: "Individual",
        status: result.final_status === "OK" ? "Concluido" : "Erro",
        mensagem: result.final_message,
      });

      return { consultDay, result, persisted };
    });

    const { consultDay, result, persisted } = processed;
    markProgress(jobId, { ok: result.final_status === "OK", errorMessage: result.final_message });
    finishJob(jobId);
    return res.json({ ok: result.final_status === "OK", jobId, consultDay, persisted, result });
  } catch (err) {
    if (err instanceof ConsultDayLimitError) {
      if (jobId) finishJob(jobId, { errorMessage: err.message });
      return res.status(403).json({
        ok: false,
        error: err.message,
        consultDay: err.meta,
      });
    }
    if (jobId) finishJob(jobId, { errorMessage: String(err.message || err) });
    return res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.post("/api/presencabank-lotecsv", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ ok: false, error: "Envie um arquivo CSV (;) no campo file." });
    }

    const originalName = String(req.file.originalname || "").trim();
    if (!originalName.toLowerCase().endsWith(".csv")) {
      return res.status(400).json({ ok: false, error: "Somente arquivo .csv (separado por ;) e permitido." });
    }

    const loginP = String(req.body?.loginP || "").trim();
    if (!loginP) {
      return res.status(400).json({ ok: false, error: "Campo loginP e obrigatorio no body." });
    }

    const idUserRaw = req.body?.id_user;
    const idUser = idUserRaw == null || String(idUserRaw).trim() === "" ? null : Number(idUserRaw);
    if (idUserRaw != null && idUserRaw !== "" && !Number.isFinite(idUser)) {
      return res.status(400).json({ ok: false, error: "Campo id_user invalido." });
    }

    const rows = parseInputRowsFromUpload(req.file);
    if (rows.length > MAX_CSV_ROWS) {
      return res.status(400).json({
        ok: false,
        error: `Arquivo possui ${rows.length} registros. O limite maximo e ${MAX_CSV_ROWS}.`,
      });
    }

    const createdAt = new Date();
    const insertedRows = await insertPendingConsultaPresenca(rows, {
      loginP,
      tipoConsulta: originalName || "presencabank-lotecsv",
      createdAt,
    });
    const skippedRows = Math.max(0, rows.length - insertedRows);

    return res.json({
      ok: true,
      route: "/api/presencabank-lotecsv",
      loginP,
      id_user: idUser,
      tipoConsulta: originalName || "presencabank-lotecsv",
      totalRows: rows.length,
      insertedRows,
      skippedRows,
      statusInserted: "Pendente",
      message: "Registros recebidos e inseridos na fila de consulta.",
    });
  } catch (err) {
    if (err instanceof InputFileValidationError) {
      return res.status(400).json({ ok: false, error: err.message });
    }
    return res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, () => {
  console.log(`API Presenca Node (individual) rodando na porta ${PORT}`);
  console.log("POST /api/process/individual");
  startPendingMonitor();
  startConsultDayResetMonitor();
});
