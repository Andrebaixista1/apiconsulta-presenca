const { resetStaleConsultDayCounters } = require("./consultDayRepo");

const DEFAULT_RESET_POLL_MS = Number(process.env.CONSULT_DAY_RESET_POLL_MS || 10000);
const DEFAULT_RESET_ENABLED = String(process.env.CONSULT_DAY_RESET_ENABLED || "false").toLowerCase() === "true";

let isRunningCycle = false;
let intervalRef = null;

async function runConsultDayResetCycle() {
  if (isRunningCycle) return;
  isRunningCycle = true;

  try {
    const rows = await resetStaleConsultDayCounters({
      loginP: process.env.CONSULT_DAY_RESET_LOGIN || null,
      senhaP: process.env.CONSULT_DAY_RESET_SENHA || null,
    });

    if (rows.length > 0) {
      const preview = rows
        .slice(0, 5)
        .map((row) => `${row.loginP}#${row.id} usado:${row.usado_anterior}->${row.usado_atual}`)
        .join(" | ");
      console.log(`[consult-day-reset] resetou ${rows.length} registro(s). ${preview}${rows.length > 5 ? " ..." : ""}`);
    }
  } catch (err) {
    console.error(`[consult-day-reset] erro no ciclo: ${String(err.message || err)}`);
  } finally {
    isRunningCycle = false;
  }
}

function startConsultDayResetMonitor() {
  if (intervalRef) return intervalRef;

  if (!DEFAULT_RESET_ENABLED) {
    console.log(
      `[consult-day-reset] desativado (CONSULT_DAY_RESET_ENABLED=false). Criterio=virada-de-data, intervalo=${DEFAULT_RESET_POLL_MS}ms`
    );
    return null;
  }

  const loginFilter = process.env.CONSULT_DAY_RESET_LOGIN || "*";
  console.log(
    `[consult-day-reset] iniciado: intervalo=${DEFAULT_RESET_POLL_MS}ms, criterio=virada-de-data, login=${loginFilter}`
  );

  runConsultDayResetCycle().catch((err) => {
    console.error(`[consult-day-reset] erro na inicializacao: ${String(err.message || err)}`);
  });

  intervalRef = setInterval(() => {
    runConsultDayResetCycle().catch((err) => {
      console.error(`[consult-day-reset] erro no agendamento: ${String(err.message || err)}`);
    });
  }, DEFAULT_RESET_POLL_MS);

  return intervalRef;
}

function stopConsultDayResetMonitor() {
  if (!intervalRef) return;
  clearInterval(intervalRef);
  intervalRef = null;
}

module.exports = {
  runConsultDayResetCycle,
  startConsultDayResetMonitor,
  stopConsultDayResetMonitor,
};
