const { sql, getPool } = require("./db");

const DEFAULT_TOTAL_LIMIT = Number(process.env.CONSULT_DAY_TOTAL || 1000);

class ConsultDayLimitError extends Error {
  constructor(message, meta = {}) {
    super(message);
    this.name = "ConsultDayLimitError";
    this.code = "CONSULT_DAY_LIMIT_EXCEEDED";
    this.meta = meta;
  }
}

async function consumeConsultDay({ loginP, senhaP, usedDelta, total = DEFAULT_TOTAL_LIMIT }) {
  if (!loginP || !senhaP) {
    throw new Error("loginP e senhaP sao obrigatorios para atualizar consult_day");
  }
  const delta = Number(usedDelta || 0);
  if (delta <= 0) {
    throw new Error("usedDelta deve ser maior que zero");
  }

  const pool = await getPool();
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.SERIALIZABLE);

  try {
    const selectReq = new sql.Request(tx);
    selectReq.input("loginP", sql.VarChar(120), String(loginP));
    selectReq.input("senhaP", sql.VarChar(120), String(senhaP));
    const existing = await selectReq.query(`
      SELECT TOP (1) [id], [total], [usado]
      FROM [presenca].[dbo].[consult_day] WITH (UPDLOCK, HOLDLOCK)
      WHERE [loginP] = @loginP AND [senhaP] = @senhaP
      ORDER BY [id] DESC
    `);

    const configuredTotal = Number(total || DEFAULT_TOTAL_LIMIT);
    let currentId = null;
    let currentUsed = 0;
    let currentTotal = configuredTotal;

    if (existing.recordset.length > 0) {
      const row = existing.recordset[0];
      currentId = Number(row.id);
      currentUsed = Number(row.usado || 0);
      currentTotal = Number(row.total || configuredTotal);
    } else {
      const insertReq = new sql.Request(tx);
      insertReq.input("loginP", sql.VarChar(120), String(loginP));
      insertReq.input("senhaP", sql.VarChar(120), String(senhaP));
      insertReq.input("total", sql.Int, configuredTotal);
      insertReq.input("usado", sql.Int, 0);
      insertReq.input("restantes", sql.Int, configuredTotal);
      await insertReq.query(`
        INSERT INTO [presenca].[dbo].[consult_day]
          ([loginP], [senhaP], [total], [usado], [restantes], [created_at], [updated_at])
        VALUES
          (@loginP, @senhaP, @total, @usado, @restantes, GETDATE(), GETDATE())
      `);

      const reloadReq = new sql.Request(tx);
      reloadReq.input("loginP", sql.VarChar(120), String(loginP));
      reloadReq.input("senhaP", sql.VarChar(120), String(senhaP));
      const reload = await reloadReq.query(`
        SELECT TOP (1) [id], [total], [usado]
        FROM [presenca].[dbo].[consult_day] WITH (UPDLOCK, HOLDLOCK)
        WHERE [loginP] = @loginP AND [senhaP] = @senhaP
        ORDER BY [id] DESC
      `);
      const row = reload.recordset[0];
      currentId = Number(row.id);
      currentUsed = Number(row.usado || 0);
      currentTotal = Number(row.total || configuredTotal);
    }

    const remaining = Math.max(0, currentTotal - currentUsed);
    if (delta > remaining) {
      throw new ConsultDayLimitError("Limite diario de consultas excedido para este login/senha", {
        total: currentTotal,
        usado: currentUsed,
        restantes: remaining,
        solicitado: delta,
      });
    }

    const nextUsed = currentUsed + delta;
    const nextRestantes = Math.max(0, currentTotal - nextUsed);
    const updateReq = new sql.Request(tx);
    updateReq.input("id", sql.Int, currentId);
    updateReq.input("usado", sql.Int, nextUsed);
    updateReq.input("restantes", sql.Int, nextRestantes);
    await updateReq.query(`
      UPDATE [presenca].[dbo].[consult_day]
      SET
        [usado] = @usado,
        [restantes] = @restantes,
        [updated_at] = GETDATE()
      WHERE [id] = @id
    `);

    const resultReq = new sql.Request(tx);
    resultReq.input("loginP", sql.VarChar(120), String(loginP));
    resultReq.input("senhaP", sql.VarChar(120), String(senhaP));
    const finalRow = await resultReq.query(`
      SELECT TOP (1)
        [id],
        [loginP],
        [senhaP],
        [total],
        [usado],
        [restantes],
        [created_at],
        [updated_at]
      FROM [presenca].[dbo].[consult_day]
      WHERE [loginP] = @loginP AND [senhaP] = @senhaP
      ORDER BY [id] DESC
    `);

    await tx.commit();
    return finalRow.recordset[0] || null;
  } catch (err) {
    await tx.rollback();
    throw err;
  }
}

async function resetStaleConsultDayCounters({ loginP = null, senhaP = null } = {}) {
  const pool = await getPool();
  const req = pool.request();
  req.input("loginP", sql.VarChar(120), loginP ? String(loginP) : null);
  req.input("senhaP", sql.VarChar(120), senhaP ? String(senhaP) : null);

  const rs = await req.query(`
    UPDATE [presenca].[dbo].[consult_day]
    SET
      [usado] = 0,
      [restantes] = CASE
        WHEN [total] IS NULL OR [total] < 0 THEN 0
        ELSE [total]
      END,
      [updated_at] = GETDATE()
    OUTPUT
      INSERTED.[id] AS [id],
      INSERTED.[loginP] AS [loginP],
      INSERTED.[senhaP] AS [senhaP],
      DELETED.[usado] AS [usado_anterior],
      INSERTED.[usado] AS [usado_atual],
      INSERTED.[restantes] AS [restantes_atual],
      DELETED.[updated_at] AS [updated_at_anterior],
      INSERTED.[updated_at] AS [updated_at_atual]
    WHERE CAST([updated_at] AS DATE) < CAST(GETDATE() AS DATE)
      AND [usado] > 0
      AND (@loginP IS NULL OR [loginP] = @loginP)
      AND (@senhaP IS NULL OR [senhaP] = @senhaP)
  `);

  return rs.recordset || [];
}

module.exports = {
  consumeConsultDay,
  DEFAULT_TOTAL_LIMIT,
  resetStaleConsultDayCounters,
  ConsultDayLimitError,
};
