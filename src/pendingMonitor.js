const { processClient } = require("./processor");
const { consumeConsultDay, DEFAULT_TOTAL_LIMIT, ConsultDayLimitError } = require("./consultDayRepo");
const { runInConsultationQueue } = require("./consultationQueue");
const {
  listPendingConsultaPresenca,
  claimPendingConsultaPresencaById,
  markConsultaPresencaStatusById,
  replacePendingConsultaPresencaById,
} = require("./consultaPresencaRepo");

const DEFAULT_LOGIN = process.env.PRESENCA_LOGIN || "40138573832_HDSL";
const DEFAULT_SENHA = process.env.PRESENCA_SENHA || "Presenca@1516";
const DEFAULT_POLL_MS = Number(process.env.PRESENCA_PENDING_POLL_MS || 10000);
const DEFAULT_BATCH_SIZE = Number(process.env.PRESENCA_PENDING_BATCH_SIZE || 20);

let isRunningCycle = false;
let schedulerRef = null;
const pauseState = {
  paused: false,
  reason: null,
  pausedAt: null,
  resumedAt: null,
};

function getPendingMonitorState() {
  return {
    paused: pauseState.paused,
    reason: pauseState.reason,
    pausedAt: pauseState.pausedAt,
    resumedAt: pauseState.resumedAt,
    runningCycle: isRunningCycle,
    pollMs: DEFAULT_POLL_MS,
    batchSize: DEFAULT_BATCH_SIZE,
  };
}

function isPendingMonitorPaused() {
  return pauseState.paused;
}

function pausePendingMonitor(reason = "Pausa manual via API") {
  const normalizedReason = String(reason || "Pausa manual via API").trim() || "Pausa manual via API";
  if (!pauseState.paused) {
    pauseState.paused = true;
    pauseState.pausedAt = new Date().toISOString();
  }
  pauseState.reason = normalizedReason;
  console.log(`[pending-monitor] pausado: motivo=${pauseState.reason}`);
  return getPendingMonitorState();
}

function resumePendingMonitor() {
  if (pauseState.paused) {
    pauseState.paused = false;
    pauseState.reason = null;
    pauseState.resumedAt = new Date().toISOString();
    console.log("[pending-monitor] retomado");
  }
  runPendingCycle().catch((err) => {
    console.error(`[pending-monitor] erro ao retomar: ${String(err.message || err)}`);
  });
  return getPendingMonitorState();
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
      stepDelayMs: 2000,
    }
  );

  const finalStatus = result.final_status === "OK" ? "Concluido" : "Erro";
  const persisted = await replacePendingConsultaPresencaById(row, [result], {
    loginP: loginValue,
    mensagem: result.final_message,
    status: finalStatus,
    skipFallbackRow: false,
  });

  return { consultDay, persisted, result };
}

async function runPendingCycle() {
  if (pauseState.paused) return;
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
    while (true) {
      if (pauseState.paused) break;
      const pendingRows = await listPendingConsultaPresenca(DEFAULT_BATCH_SIZE);
      if (!pendingRows.length) break;

      stats.found += pendingRows.length;
      console.log(`[pending-monitor] pendentes encontrados neste lote: ${pendingRows.length}`);

      for (const row of pendingRows) {
        if (pauseState.paused) break;
        const claimed = await claimPendingConsultaPresencaById(row.id);
        if (!claimed) continue;
        stats.claimed += 1;

        if (pauseState.paused) {
          await markConsultaPresencaStatusById(claimed.id, "Pendente");
          console.log(`[pending-monitor] claim revertido por pausa (id=${claimed.id}, cpf=${claimed.cpf})`);
          break;
        }

        try {
          const loginValue = String(claimed.loginP || DEFAULT_LOGIN);
          const senhaValue = String(DEFAULT_SENHA);
          const processed = await runInConsultationQueue(() =>
            processPendingRow(claimed, {
              login: loginValue,
              senha: senhaValue,
            })
          );
          stats.consulted += 1;
          if (processed.result?.final_status === "OK") {
            stats.concluded += 1;
          } else {
            stats.errors += 1;
          }
          console.log(
            `[pending-monitor] id=${claimed.id} cpf=${claimed.cpf} status=${processed.result?.final_status || "N/A"} motivo=${processed.result?.final_message || "N/A"} atualizadas=${processed.persisted.updatedRows} inseridas=${processed.persisted.insertedRows} consult_day(usado=${processed.consultDay?.usado}, restantes=${processed.consultDay?.restantes})`
          );
        } catch (err) {
          if (err instanceof ConsultDayLimitError) {
            stats.limitErrors += 1;
            await markConsultaPresencaStatusById(claimed.id, "Limite", err.message);
            console.error(`[pending-monitor] limite diario atingido (id=${claimed.id}, cpf=${claimed.cpf}): ${err.message}`);
          } else {
            stats.errors += 1;
            await markConsultaPresencaStatusById(claimed.id, "Erro", String(err.message || err));
            console.error(`[pending-monitor] erro ao processar id=${claimed.id}, cpf=${claimed.cpf}: ${String(err.message || err)}`);
          }
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
  if (schedulerRef) return;
  console.log(`[pending-monitor] iniciado: intervalo=${DEFAULT_POLL_MS}ms batch=${DEFAULT_BATCH_SIZE}`);
  runPendingCycle().catch((err) => {
    console.error(`[pending-monitor] erro na inicializacao: ${String(err.message || err)}`);
  });
  schedulerRef = setInterval(() => {
    runPendingCycle().catch((err) => {
      console.error(`[pending-monitor] erro no agendamento: ${String(err.message || err)}`);
    });
  }, DEFAULT_POLL_MS);
}

module.exports = {
  startPendingMonitor,
  pausePendingMonitor,
  resumePendingMonitor,
  getPendingMonitorState,
  isPendingMonitorPaused,
};
