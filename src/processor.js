const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");
const { parse } = require("csv-parse/sync");
const { BASE_URL, postWithRetry, login, acceptTermoHeadless } = require("./presencaClient");
const {
  normalizeCpf,
  normalizeNome,
  normalizeTelefone,
  generateRandomTelefone,
  nowFileStamp,
} = require("./utils");

const DEFAULT_LOGIN = process.env.PRESENCA_LOGIN || "40138573832_HDSL";
const DEFAULT_SENHA = process.env.PRESENCA_SENHA || "Presenca@1516";
const DEFAULT_STEP_DELAY_MS = Number(process.env.PRESENCA_STEP_DELAY_MS || 2000);
const REQUIRED_FILE_COLUMNS = ["CPF", "NOME", "TELEFONE"];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitStepDelay(delayMs) {
  if (delayMs > 0) {
    await sleep(delayMs);
  }
}

function ensureOutputsDir() {
  const outDir = path.resolve(process.cwd(), "outputs");
  fs.mkdirSync(outDir, { recursive: true });
  return outDir;
}

function flattenApi5(result) {
  const base = {
    cpf: result.cpf,
    nome: result.nome,
    telefone: result.telefone,
    final_status: result.final_status,
    final_message: result.final_message,
    termo_status: result.termo_status ?? null,
    vinculos_status: result.vinculos_status ?? null,
    margem_status: result.margem_status ?? null,
    tabelas_status: result.tabelas_status ?? null,
  };
  if (result.tabelas_status === 200 && Array.isArray(result.tabelas_body)) {
    return result.tabelas_body.map((item) => ({
      ...base,
      id: item?.id ?? null,
      nome_tabela: item?.nome ?? null,
      prazo: item?.prazo ?? null,
      taxaJuros: item?.taxaJuros ?? null,
      valorLiberado: item?.valorLiberado ?? null,
      tipoCredito: item?.tipoCredito?.name ?? null,
      valorParcela: item?.valorParcela ?? null,
      taxaSeguro: item?.taxaSeguro ?? null,
      valorSeguro: item?.valorSeguro ?? null,
    }));
  }
  return [{ ...base }];
}

class InputFileValidationError extends Error {
  constructor(message) {
    super(message);
    this.name = "InputFileValidationError";
    this.code = "INPUT_FILE_VALIDATION_ERROR";
  }
}

const normalizeHeader = (value) =>
  String(value ?? "")
    .replace(/^\uFEFF/, "")
    .trim()
    .toUpperCase();

function assertRequiredColumns(headers) {
  const normalized = new Set((headers || []).map((h) => normalizeHeader(h)));
  const missing = REQUIRED_FILE_COLUMNS.filter((col) => !normalized.has(col));
  if (missing.length > 0) {
    throw new InputFileValidationError(
      `Arquivo invalido. As colunas obrigatorias sao: ${REQUIRED_FILE_COLUMNS.join(", ")}. Faltando: ${missing.join(", ")}`
    );
  }
}

function mapToClientRows(rawRows) {
  return rawRows.map((row) => {
    const normalized = {};
    Object.keys(row || {}).forEach((k) => {
      normalized[normalizeHeader(k)] = row[k];
    });
    return {
      cpf: normalizeCpf(normalized.CPF || ""),
      nome: normalizeNome(normalized.NOME || ""),
      telefone: normalizeTelefone(normalized.TELEFONE || ""),
    };
  });
}

function parseCsvSemicolon(buffer) {
  const text = buffer.toString("utf8");
  const firstHeaderLine = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .find((line) => line.length > 0);
  if (!firstHeaderLine) return [];
  assertRequiredColumns(firstHeaderLine.split(";"));

  const rows = parse(text, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    delimiter: ";",
    bom: true,
  });
  return mapToClientRows(rows);
}

function parseXlsx(buffer) {
  const wb = XLSX.read(buffer, { type: "buffer" });
  const firstSheetName = wb.SheetNames[0];
  if (!firstSheetName) return [];
  const ws = wb.Sheets[firstSheetName];

  const matrix = XLSX.utils.sheet_to_json(ws, { header: 1, raw: false, defval: "" });
  const headers = Array.isArray(matrix[0]) ? matrix[0] : [];
  assertRequiredColumns(headers);

  const rows = XLSX.utils.sheet_to_json(ws, { raw: false, defval: "" });
  return mapToClientRows(rows);
}

function parseInputRowsFromUpload(file) {
  const originalName = String(file?.originalname || "").toLowerCase();
  const isCsv = originalName.endsWith(".csv");
  const isXlsx = originalName.endsWith(".xlsx");
  if (!isCsv && !isXlsx) {
    throw new InputFileValidationError(
      "Formato de arquivo invalido. Envie .csv (separado por ;) ou .xlsx com colunas CPF, NOME e TELEFONE."
    );
  }
  const rows = isCsv ? parseCsvSemicolon(file.buffer) : parseXlsx(file.buffer);
  if (!rows.length) {
    throw new InputFileValidationError("Arquivo vazio ou sem registros validos.");
  }
  return rows;
}

async function processClient(input, opts = {}) {
  const loginValue = opts.login || DEFAULT_LOGIN;
  const senhaValue = opts.senha || DEFAULT_SENHA;
  const produtoId = Number(opts.produtoId || 28);
  const timeout = Number(opts.timeout || 30000);
  const retries = Number(opts.retries || 2);
  const retryDelayMs = Number(opts.retryDelayMs || 1500);
  const autoAcceptHeadless = opts.autoAcceptHeadless !== false;
  const stepDelayMs = Number(opts.stepDelayMs ?? DEFAULT_STEP_DELAY_MS);

  const logs = [];
  const result = {
    cpf: normalizeCpf(input.cpf),
    nome: normalizeNome(input.nome),
    telefone: normalizeTelefone(input.telefone),
    original: input,
    logs,
  };
  if (!result.telefone) result.telefone = generateRandomTelefone();
  if (!result.cpf) {
    result.final_status = "ERRO";
    result.final_message = "CPF invalido";
    return result;
  }

  const loginResp = await login({ login: loginValue, senha: senhaValue, timeout, retries, retryDelayMs });
  logs.push({ step: "POST /login", request: { login: loginValue }, status: loginResp.status, ok: loginResp.status === 200, response: loginResp.data });
  if (loginResp.status !== 200 || !loginResp.data?.token) {
    result.final_status = "ERRO";
    result.final_message = "Falha login";
    return result;
  }
  await waitStepDelay(stepDelayMs);
  const token = loginResp.data.token;
  const headers = { Authorization: `Bearer ${token}` };

  const termoPayload = { cpf: result.cpf, nome: result.nome, telefone: result.telefone, produtoId };
  const termoResp = await postWithRetry(`${BASE_URL}/consultas/termo-inss`, termoPayload, headers, { timeout, retries, retryDelayMs });
  result.termo_status = termoResp.status;
  logs.push({ step: "POST /consultas/termo-inss", request: termoPayload, status: termoResp.status, ok: termoResp.status === 200, response: termoResp.data });
  if (termoResp.status !== 200 || !termoResp.data?.shortUrl) {
    result.final_status = "ERRO";
    result.final_message = "Falha gerar termo";
    return result;
  }

  if (autoAcceptHeadless) {
    try {
      const acceptResp = await acceptTermoHeadless(termoResp.data.shortUrl, termoResp.data.autorizacaoId, Math.floor(timeout / 1000));
      logs.push({ step: "HEADLESS aceitar termo", request: { shortUrl: termoResp.data.shortUrl }, status: acceptResp.ok ? 200 : 400, ok: acceptResp.ok, response: { calls: acceptResp.calls } });
      if (!acceptResp.ok) {
        result.final_status = "ERRO";
        result.final_message = "Falha aceite headless";
        return result;
      }
    } catch (err) {
      logs.push({ step: "HEADLESS aceitar termo", request: { shortUrl: termoResp.data.shortUrl }, status: 500, ok: false, response: { error: String(err.message || err) } });
      result.final_status = "ERRO";
      result.final_message = "Falha aceite headless";
      return result;
    }
  }
  await waitStepDelay(stepDelayMs);

  const vincPayload = { cpf: result.cpf };
  const vincResp = await postWithRetry(`${BASE_URL}/v3/operacoes/consignado-privado/consultar-vinculos`, vincPayload, headers, { timeout, retries, retryDelayMs });
  result.vinculos_status = vincResp.status;
  logs.push({ step: "POST /v3/operacoes/consignado-privado/consultar-vinculos", request: vincPayload, status: vincResp.status, ok: vincResp.status === 200, response: vincResp.data });
  if (vincResp.status !== 200) {
    result.final_status = "ERRO";
    result.final_message = "Falha consultar-vinculos";
    return result;
  }

  const vinculos = Array.isArray(vincResp.data?.id) ? vincResp.data.id : [];
  const vinculo = vinculos.find((v) => v.elegivel === true) || vinculos[0];
  result.vinculo = vinculo || null;
  if (!vinculo?.matricula || !vinculo?.numeroInscricaoEmpregador) {
    result.final_status = "ERRO";
    result.final_message = "Sem vinculo elegivel";
    return result;
  }
  await waitStepDelay(stepDelayMs);

  const margemPayload = {
    cpf: result.cpf,
    matricula: String(vinculo.matricula),
    cnpj: String(vinculo.numeroInscricaoEmpregador),
  };
  const margemResp = await postWithRetry(`${BASE_URL}/v3/operacoes/consignado-privado/consultar-margem`, margemPayload, headers, { timeout, retries, retryDelayMs });
  result.margem_status = margemResp.status;
  result.margem_data = margemResp.data || null;
  logs.push({ step: "POST /v3/operacoes/consignado-privado/consultar-margem", request: margemPayload, status: margemResp.status, ok: margemResp.status === 200, response: margemResp.data });
  if (margemResp.status !== 200 || !margemResp.data) {
    result.final_status = "ERRO";
    result.final_message = "Falha consultar-margem";
    return result;
  }
  await waitStepDelay(stepDelayMs);

  const m = margemResp.data;
  const tabelasPayload = {
    tomador: {
      cpf: result.cpf,
      nome: result.nome,
      telefone: { ddd: result.telefone.slice(0, 2), numero: result.telefone.slice(2) },
      dataNascimento: m.dataNascimento || "1990-01-01",
      email: "emailmock@mock.com.br",
      sexo: m.sexo || "M",
      nomeMae: m.nomeMae || "NAO INFORMADO",
      vinculoEmpregaticio: {
        cnpjEmpregador: String(m.cnpjEmpregador || vinculo.numeroInscricaoEmpregador),
        registroEmpregaticio: String(m.registroEmpregaticio || vinculo.matricula),
      },
      dadosBancarios: { codigoBanco: null, agencia: null, conta: null, digitoConta: null, formaCredito: null },
      endereco: { cep: "", rua: "", numero: "", complemento: "", cidade: "", estado: "", bairro: "" },
    },
    proposta: {
      valorSolicitado: 0,
      quantidadeParcelas: 0,
      produtoId,
      valorParcela: Number(m.valorMargemDisponivel || 0),
    },
    documentos: [],
  };
  const tabResp = await postWithRetry(`${BASE_URL}/v5/operacoes/simulacao/disponiveis`, tabelasPayload, headers, { timeout, retries, retryDelayMs });
  result.tabelas_status = tabResp.status;
  result.tabelas_body = tabResp.data;
  logs.push({ step: "POST /v5/operacoes/simulacao/disponiveis", request: tabelasPayload, status: tabResp.status, ok: tabResp.status === 200, response: tabResp.data });

  if (tabResp.status === 200) {
    result.final_status = "OK";
    result.final_message = "Fluxo completo OK";
  } else {
    const errs = Array.isArray(tabResp.data?.errors) ? tabResp.data.errors.join(" | ") : "";
    result.final_status = "ERRO";
    result.final_message = errs ? `Falha consultar-tabelas: ${errs}` : `Falha consultar-tabelas: status ${tabResp.status}`;
  }
  return result;
}

function saveResultsFile(results, outputName) {
  const outDir = ensureOutputsDir();
  const fileName = outputName || `resultado_presenca_${nowFileStamp()}.xlsx`;
  const outputPath = path.join(outDir, fileName);

  const resumoRows = results.flatMap((r) => flattenApi5(r));
  const logsRows = [];
  for (const r of results) {
    for (const l of r.logs || []) {
      logsRows.push({
        cpf: r.cpf,
        nome: r.nome,
        step: l.step,
        status: l.status,
        ok: l.ok,
        request_json: JSON.stringify(l.request ?? {}),
        response_json: JSON.stringify(l.response ?? {}),
      });
    }
  }

  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(resumoRows), "resultado");
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(logsRows), "logs");
  XLSX.writeFile(wb, outputPath);
  return outputPath;
}

async function processCsvBatch(rows, opts = {}) {
  let selected = rows;
  const randomRows = Number(opts.randomRows || 0);
  if (randomRows > 0 && rows.length > randomRows) {
    selected = [...rows].sort(() => Math.random() - 0.5).slice(0, randomRows);
  }
  const results = [];
  for (const row of selected) {
    const r = await processClient(row, opts);
    results.push(r);
  }
  return results;
}

module.exports = {
  processClient,
  parseInputRowsFromUpload,
  processCsvBatch,
  saveResultsFile,
  InputFileValidationError,
};
