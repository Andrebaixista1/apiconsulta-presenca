const { processClient } = require("./processor");
const { login } = require("./presencaClient");
const { consumeConsultDay, DEFAULT_TOTAL_LIMIT, ConsultDayLimitError } = require("./consultDayRepo");
const {
  listPendingConsultaPresenca,
  claimPendingConsultaPresencaById,
  markConsultaPresencaStatusById,
  replacePendingConsultaPresencaById,
} = require("./consultaPresencaRepo");

const DEFAULT_LOGIN = process.env.PRESENCA_LOGIN || "40138573832_HDSL";
const DEFAULT_SENHA = process.env.PRESENCA_SENHA || "Presenca@1516";
const DEFAULT_POLL_MS = Number(process.env.PRESENCA_PENDING_POLL_MS || 5000);
const DEFAULT_BATCH_SIZE = Number(process.env.PRESENCA_PENDING_BATCH_SIZE || 20);
const DEFAULT_TOKEN_TTL_MS = Number(process.env.PRESENCA_LOTE_TOKEN_TTL_MS || 20 * 60 * 1000);

let isRunningCycle = false;
const loteTokenCache = new Map();

function buildLoteTokenKey(row, loginValue) {
  const createdAtIso = row?.createdAt instanceof Date ? row.createdAt.toISOString() : String(row?.createdAt || "");
  return `${loginValue}|${row?.tipoConsulta || ""}|${createdAtIso}`;
}

async function getLoteToken(row, loginValue, senhaValue) {
  const cacheKey = buildLoteTokenKey(row, loginValue);
  const cached = loteTokenCache.get(cacheKey);
  if (cached && cached.token && cached.expiresAt > Date.now()) {
    return { token: cached.token, reused: true };
  }

  const loginResp = await login({ login: loginValue, senha: senhaValue });
  if (loginResp.status !== 200 || !loginResp.data?.token) {
    throw new Error(`Falha login lote (status=${loginResp.status})`);
  }
  const token = String(loginResp.data.token);
  loteTokenCache.set(cacheKey, {
    token,
    expiresAt: Date.now() + DEFAULT_TOKEN_TTL_MS,
  });
  return { token, reused: false };
}

async function processPendingRow(row, opts = {}) {
  const loginValue = String(opts.login || row.loginP || DEFAULT_LOGIN);
  const senhaValue = String(opts.senha || DEFAULT_SENHA);

  const consultDay = await consumeConsultDay({
    loginP: loginValue,
    senhaP: senhaValue,
    usedDelta: 1,
    total: DEFAULT_TOTAL_LIMIT,
  });

  const result = await processClient(
    {
      cpf: row.cpf,
      nome: row.nome,
      telefone: row.telefone,
    },
    {
      login: loginValue,
      senha: senhaValue,
      authToken: opts.authToken,
    }
  );

  const persisted = await replacePendingConsultaPresencaById(row, [result], {
    status: "Concluido",
    skipFallbackRow: true,
  });

  return { consultDay, persisted };
}

async function runPendingCycle() {
  if (isRunningCycle) return;
  isRunningCycle = true;

  const stats = {
    found: 0,
    claimed: 0,
    consulted: 0,
    concluded: 0,
    limitErrors: 0,
    errors: 0,
  };

  try {
    const pendingRows = await listPendingConsultaPresenca(DEFAULT_BATCH_SIZE);
    stats.found = pendingRows.length;
    if (stats.found > 0) {
      console.log(`[pending-monitor] pendentes encontrados: ${stats.found}`);
    }

    for (const row of pendingRows) {
      const claimed = await claimPendingConsultaPresencaById(row.id);
      if (!claimed) continue;
      stats.claimed += 1;

      try {
        const loginValue = String(claimed.loginP || DEFAULT_LOGIN);
        const senhaValue = String(DEFAULT_SENHA);
        const tokenInfo = await getLoteToken(claimed, loginValue, senhaValue);
        const processed = await processPendingRow(claimed, {
          login: loginValue,
          senha: senhaValue,
          authToken: tokenInfo.token,
        });
        stats.consulted += 1;
        stats.concluded += 1;
        console.log(
          `[pending-monitor] id=${claimed.id} cpf=${claimed.cpf} consultado: token=${tokenInfo.reused ? "reutilizado" : "novo"} atualizadas=${processed.persisted.updatedRows} inseridas=${processed.persisted.insertedRows} consult_day(usado=${processed.consultDay?.usado}, restantes=${processed.consultDay?.restantes})`
        );
      } catch (err) {
        if (err instanceof ConsultDayLimitError) {
          stats.limitErrors += 1;
          await markConsultaPresencaStatusById(claimed.id, "Limite");
          console.error(`[pending-monitor] limite diario atingido (id=${claimed.id}, cpf=${claimed.cpf}): ${err.message}`);
        } else {
          stats.errors += 1;
          await markConsultaPresencaStatusById(claimed.id, "Erro");
          console.error(`[pending-monitor] erro ao processar id=${claimed.id}, cpf=${claimed.cpf}: ${String(err.message || err)}`);
        }
      }
    }
  } catch (err) {
    stats.errors += 1;
    console.error(`[pending-monitor] falha no ciclo: ${String(err.message || err)}`);
  } finally {
    if (stats.found > 0 || stats.consulted > 0 || stats.errors > 0 || stats.limitErrors > 0) {
      console.log(
        `[pending-monitor] ciclo finalizado: achou=${stats.found} claimed=${stats.claimed} consultou=${stats.consulted} concluiu=${stats.concluded} limite=${stats.limitErrors} erros=${stats.errors}`
      );
    }
    isRunningCycle = false;
  }
}

function startPendingMonitor() {
  console.log(`[pending-monitor] iniciado: intervalo=${DEFAULT_POLL_MS}ms batch=${DEFAULT_BATCH_SIZE}`);
  runPendingCycle().catch((err) => {
    console.error(`[pending-monitor] erro na inicializacao: ${String(err.message || err)}`);
  });
  setInterval(() => {
    runPendingCycle().catch((err) => {
      console.error(`[pending-monitor] erro no agendamento: ${String(err.message || err)}`);
    });
  }, DEFAULT_POLL_MS);
}

module.exports = {
  startPendingMonitor,
};
