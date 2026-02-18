const express = require("express");
const multer = require("multer");
const { processClient, parseCsvBuffer, processCsvBatch, saveResultsFile } = require("./processor");
const { consumeConsultDay, DEFAULT_TOTAL_LIMIT, ConsultDayLimitError } = require("./consultDayRepo");
const { saveConsultaPresencaResults } = require("./consultaPresencaRepo");

const app = express();
const upload = multer({ storage: multer.memoryStorage() });
const MAX_CSV_ROWS = 1000;
const DEFAULT_PRESENCA_LOGIN = process.env.PRESENCA_LOGIN || "40138573832_HDSL";
const DEFAULT_PRESENCA_SENHA = process.env.PRESENCA_SENHA || "Presenca@1516";

app.use(express.json({ limit: "10mb" }));

app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "api-presenca-node" });
});

app.post("/api/process/individual", async (req, res) => {
  try {
    const { cpf, nome, telefone, produtoId, autoAcceptHeadless = true, stepDelayMs, login, senha } = req.body || {};
    if (!cpf || !nome || !telefone) {
      return res.status(400).json({ ok: false, error: "Informe cpf, nome e telefone" });
    }
    const loginValue = String(login || DEFAULT_PRESENCA_LOGIN);
    const senhaValue = String(senha || DEFAULT_PRESENCA_SENHA);
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
    const persisted = await saveConsultaPresencaResults([result], {
      loginP: loginValue,
      tipoConsulta: "Individual",
    });
    const outputFile = saveResultsFile([result]);
    return res.json({ ok: result.final_status === "OK", outputFile, consultDay, persisted, result });
  } catch (err) {
    return res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

app.post("/api/process/csv", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ ok: false, error: "Envie um CSV no campo file" });
    }
    const { randomRows = 0, produtoId, autoAcceptHeadless = "true", stepDelayMs, login, senha } = req.body || {};
    const loginValue = String(login || DEFAULT_PRESENCA_LOGIN);
    const senhaValue = String(senha || DEFAULT_PRESENCA_SENHA);
    const rows = parseCsvBuffer(req.file.buffer);
    if (!rows.length) {
      return res.status(400).json({ ok: false, error: "CSV vazio ou sem colunas CPF,NOME,TELEFONE" });
    }
    if (rows.length > MAX_CSV_ROWS) {
      return res.status(400).json({
        ok: false,
        error: `Arquivo CSV possui ${rows.length} registros. O limite maximo e ${MAX_CSV_ROWS}.`,
      });
    }
    const randomRowsNum = Number(randomRows || 0);
    const usedDelta = randomRowsNum > 0 ? Math.min(rows.length, randomRowsNum) : rows.length;
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
        return res.status(403).json({
          ok: false,
          error: err.message,
          consultDay: err.meta,
        });
      }
      throw err;
    }
    const results = await processCsvBatch(rows, {
      randomRows: randomRowsNum,
      produtoId: produtoId ? Number(produtoId) : 28,
      autoAcceptHeadless: String(autoAcceptHeadless).toLowerCase() !== "false",
      stepDelayMs: stepDelayMs != null ? Number(stepDelayMs) : undefined,
      login: loginValue,
      senha: senhaValue,
    });
    const persisted = await saveConsultaPresencaResults(results, {
      loginP: loginValue,
      tipoConsulta: "Em lote",
    });
    const outputFile = saveResultsFile(results);
    const okCount = results.filter((r) => r.final_status === "OK").length;
    return res.json({
      ok: true,
      total: results.length,
      okCount,
      erroCount: results.length - okCount,
      consultDay,
      persisted,
      outputFile,
      results,
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: String(err.message || err) });
  }
});

const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, () => {
  console.log(`API Presenca Node rodando na porta ${PORT}`);
  console.log("POST /api/process/individual");
  console.log("POST /api/process/csv");
});
