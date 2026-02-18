const { sql, getPool } = require("./db");

const onlyDigits = (value) => String(value ?? "").replace(/\D/g, "");
const toBigIntOrNull = (value) => {
  const digits = onlyDigits(value);
  if (!digits) return null;
  return digits;
};
const toVarcharOrNull = (value, max) => {
  if (value == null) return null;
  const text = String(value).trim();
  if (!text) return null;
  return text.slice(0, max);
};

function buildRowsFromResult(result, loginP, tipoConsulta, options = {}) {
  const base = {
    cpf: toBigIntOrNull(result?.cpf),
    nome: toVarcharOrNull(result?.nome, 100),
    telefone: toBigIntOrNull(result?.telefone),
    loginP: toVarcharOrNull(loginP, 50),
    matricula: toVarcharOrNull(result?.vinculo?.matricula, 255),
    numeroInscricaoEmpregador: toVarcharOrNull(result?.vinculo?.numeroInscricaoEmpregador, 255),
    elegivel: result?.vinculo?.elegivel == null ? null : toVarcharOrNull(String(Boolean(result.vinculo.elegivel)), 10),
    valorMargemDisponivel: toVarcharOrNull(result?.margem_data?.valorMargemDisponivel, 20),
    valorMargemBase: toVarcharOrNull(result?.margem_data?.valorMargemBase, 20),
    valorTotalDevido: toVarcharOrNull(result?.margem_data?.valorTotalDevido, 20),
    dataAdmissao: result?.margem_data?.dataAdmissao || null,
    dataNascimento: result?.margem_data?.dataNascimento || null,
    nomeMae: toVarcharOrNull(result?.margem_data?.nomeMae, 100),
    sexo: toVarcharOrNull(result?.margem_data?.sexo, 2),
    tipoConsulta: toVarcharOrNull(tipoConsulta || "Individual", 50),
  };

  const tabelas = Array.isArray(result?.tabelas_body) ? result.tabelas_body : [];
  if (!tabelas.length) {
    if (options.skipFallbackRow) return [];
    return [
      {
        ...base,
        nomeTipo: toVarcharOrNull(null, 150),
        prazo: null,
        taxaJuros: toVarcharOrNull(null, 5),
        valorLiberado: toVarcharOrNull(null, 10),
        valorParcela: toVarcharOrNull(null, 10),
        taxaSeguro: toVarcharOrNull(null, 10),
        valorSeguro: toVarcharOrNull(null, 10),
      },
    ];
  }

  return tabelas.map((t) => ({
    ...base,
    nomeTipo: toVarcharOrNull(t?.nome, 150),
    prazo: t?.prazo ?? null,
    taxaJuros: toVarcharOrNull(t?.taxaJuros, 5),
    valorLiberado: toVarcharOrNull(t?.valorLiberado, 10),
    valorParcela: toVarcharOrNull(t?.valorParcela, 10),
    taxaSeguro: toVarcharOrNull(t?.taxaSeguro, 10),
    valorSeguro: toVarcharOrNull(t?.valorSeguro, 10),
  }));
}

function mapPendingRow(row) {
  const cpfDigits = onlyDigits(row.cpf);
  return {
    id: Number(row.id),
    cpf: cpfDigits ? cpfDigits.padStart(11, "0") : "",
    nome: toVarcharOrNull(row.nome, 100),
    telefone: onlyDigits(row.telefone),
    loginP: toVarcharOrNull(row.loginP, 50),
    tipoConsulta: toVarcharOrNull(row.tipoConsulta, 50),
    createdAt: row.created_at instanceof Date ? row.created_at : new Date(row.created_at),
    status: toVarcharOrNull(row.status, 50),
  };
}

async function insertConsultaPresencaRows(rows, options = {}, exec = {}) {
  if (!rows.length) return 0;
  const pool = await getPool();
  const externalTx = exec.tx;
  const tx = externalTx || new sql.Transaction(pool);
  if (!externalTx) await tx.begin();

  try {
    let inserted = 0;
    for (const row of rows) {
      const req = new sql.Request(tx);
      req.input("cpf", sql.BigInt, row.cpf);
      req.input("nome", sql.VarChar(100), row.nome);
      req.input("telefone", sql.BigInt, row.telefone);
      const loginPValue = toVarcharOrNull(options.loginP, 50);
      req.input("loginP", sql.VarChar(50), loginPValue);
      req.input("matricula", sql.VarChar(255), row.matricula);
      req.input("numeroInscricaoEmpregador", sql.VarChar(255), row.numeroInscricaoEmpregador);
      req.input("elegivel", sql.VarChar(10), row.elegivel);
      req.input("valorMargemDisponivel", sql.VarChar(20), row.valorMargemDisponivel);
      req.input("valorMargemBase", sql.VarChar(20), row.valorMargemBase);
      req.input("valorTotalDevido", sql.VarChar(20), row.valorTotalDevido);
      req.input("dataAdmissao", sql.Date, row.dataAdmissao);
      req.input("dataNascimento", sql.Date, row.dataNascimento);
      req.input("nomeMae", sql.VarChar(100), row.nomeMae);
      req.input("sexo", sql.VarChar(2), row.sexo);
      req.input("nomeTipo", sql.VarChar(150), row.nomeTipo);
      req.input("prazo", sql.BigInt, row.prazo);
      req.input("taxaJuros", sql.VarChar(5), row.taxaJuros);
      req.input("valorLiberado", sql.VarChar(10), row.valorLiberado);
      req.input("valorParcela", sql.VarChar(10), row.valorParcela);
      req.input("taxaSeguro", sql.VarChar(10), row.taxaSeguro);
      req.input("valorSeguro", sql.VarChar(10), row.valorSeguro);
      const tipoConsultaValue = toVarcharOrNull(row.tipoConsulta || options.tipoConsulta || "Em lote", 50);
      req.input("tipoConsulta", sql.VarChar(50), tipoConsultaValue);
      const createdAtValue = options.createdAt || new Date();
      const updatedAtValue = options.updatedAt || createdAtValue;
      const statusValue = toVarcharOrNull(options.status || "Concluido", 50);
      // The table uses SQL Server `datetime` (not datetime2). Use the matching type to avoid precision mismatches.
      req.input("created_at", sql.DateTime, createdAtValue);
      req.input("updated_at", sql.DateTime, updatedAtValue);
      req.input("status", sql.VarChar(50), statusValue);

      await req.query(`
        INSERT INTO [presenca].[dbo].[consulta_presenca]
          ([cpf], [nome], [telefone], [loginP], [created_at], [updated_at], [matricula], [numeroInscricaoEmpregador], [elegivel],
           [valorMargemDisponivel], [valorMargemBase], [valorTotalDevido], [dataAdmissao], [dataNascimento], [nomeMae], [sexo],
           [nomeTipo], [prazo], [taxaJuros], [valorLiberado], [valorParcela], [taxaSeguro], [valorSeguro], [tipoConsulta], [status])
        VALUES
          (@cpf, @nome, @telefone, @loginP, @created_at, @updated_at, @matricula, @numeroInscricaoEmpregador, @elegivel,
           @valorMargemDisponivel, @valorMargemBase, @valorTotalDevido, @dataAdmissao, @dataNascimento, @nomeMae, @sexo,
           @nomeTipo, @prazo, @taxaJuros, @valorLiberado, @valorParcela, @taxaSeguro, @valorSeguro, @tipoConsulta, @status)
      `);
      inserted += 1;
    }
    if (!externalTx) await tx.commit();
    return inserted;
  } catch (err) {
    if (!externalTx) await tx.rollback();
    throw err;
  }
}

async function insertPendingConsultaPresenca(rows, options = {}) {
  const sanitizedRows = (rows || [])
    .map((row) => ({
      cpf: onlyDigits(row?.cpf),
      nome: toVarcharOrNull(row?.nome, 100),
      telefone: onlyDigits(row?.telefone),
    }))
    .filter((row) => row.cpf);
  return insertConsultaPresencaRows(sanitizedRows, {
    ...options,
    status: "Pendente",
    updatedAt: options.createdAt || new Date(),
  });
}

async function listPendingConsultaPresenca(limit = 50) {
  const pool = await getPool();
  const req = pool.request();
  req.input("limit", sql.Int, Number(limit || 50));
  req.input("status", sql.VarChar(50), "Pendente");
  const rs = await req.query(`
    SELECT TOP (@limit)
      [id],
      CAST([cpf] AS VARCHAR(20)) AS cpf,
      [nome],
      CAST([telefone] AS VARCHAR(20)) AS telefone,
      [loginP],
      [tipoConsulta],
      [created_at],
      [status]
    FROM [presenca].[dbo].[consulta_presenca] WITH (READPAST)
    WHERE [status] = @status
    ORDER BY [created_at] ASC, [id] ASC
  `);
  return (rs.recordset || []).map(mapPendingRow);
}

async function claimPendingConsultaPresencaById(id) {
  const pool = await getPool();
  const tx = new sql.Transaction(pool);
  await tx.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);

  try {
    const sel = new sql.Request(tx);
    sel.input("id", sql.Int, Number(id));
    sel.input("status", sql.VarChar(50), "Pendente");
    const current = await sel.query(`
      SELECT TOP (1)
        [id],
        CAST([cpf] AS VARCHAR(20)) AS cpf,
        [nome],
        CAST([telefone] AS VARCHAR(20)) AS telefone,
        [loginP],
        [tipoConsulta],
        [created_at],
        [status]
      FROM [presenca].[dbo].[consulta_presenca] WITH (UPDLOCK, ROWLOCK, READPAST)
      WHERE [id] = @id
        AND [status] = @status
    `);

    if (!current.recordset || !current.recordset.length) {
      await tx.rollback();
      return null;
    }

    const upd = new sql.Request(tx);
    upd.input("id", sql.Int, Number(id));
    upd.input("fromStatus", sql.VarChar(50), "Pendente");
    upd.input("toStatus", sql.VarChar(50), "Processando");
    const updated = await upd.query(`
      UPDATE [presenca].[dbo].[consulta_presenca]
      SET
        [status] = @toStatus,
        [updated_at] = GETDATE()
      WHERE [id] = @id
        AND [status] = @fromStatus
    `);

    if (!updated.rowsAffected || !updated.rowsAffected[0]) {
      await tx.rollback();
      return null;
    }

    await tx.commit();
    return mapPendingRow({
      ...current.recordset[0],
      status: "Processando",
    });
  } catch (err) {
    await tx.rollback();
    throw err;
  }
}

async function markConsultaPresencaStatusById(id, status) {
  const pool = await getPool();
  const req = pool.request();
  req.input("id", sql.Int, Number(id));
  req.input("status", sql.VarChar(50), toVarcharOrNull(status, 50));
  const rs = await req.query(`
    UPDATE [presenca].[dbo].[consulta_presenca]
    SET
      [status] = @status,
      [updated_at] = GETDATE()
    WHERE [id] = @id
  `);
  return Number((rs.rowsAffected && rs.rowsAffected[0]) || 0);
}

async function updateConsultaPresencaRowById(id, row, options = {}, exec = {}) {
  const pool = await getPool();
  const tx = exec.tx || new sql.Transaction(pool);
  if (!exec.tx) await tx.begin();

  try {
    const req = new sql.Request(tx);
    req.input("id", sql.Int, Number(id));
    req.input("cpf", sql.BigInt, row.cpf);
    req.input("nome", sql.VarChar(100), row.nome);
    req.input("telefone", sql.BigInt, row.telefone);
    const loginPValue = toVarcharOrNull(options.loginP, 50);
    const tipoConsultaValue = toVarcharOrNull(options.tipoConsulta || row.tipoConsulta || "Em lote", 50);
    const createdAtValue = options.createdAt || new Date();
    const updatedAtValue = options.updatedAt || new Date();
    const statusValue = toVarcharOrNull(options.status || "Concluido", 50);
    req.input("loginP", sql.VarChar(50), loginPValue);
    req.input("created_at", sql.DateTime, createdAtValue);
    req.input("updated_at", sql.DateTime, updatedAtValue);
    req.input("matricula", sql.VarChar(255), row.matricula);
    req.input("numeroInscricaoEmpregador", sql.VarChar(255), row.numeroInscricaoEmpregador);
    req.input("elegivel", sql.VarChar(10), row.elegivel);
    req.input("valorMargemDisponivel", sql.VarChar(20), row.valorMargemDisponivel);
    req.input("valorMargemBase", sql.VarChar(20), row.valorMargemBase);
    req.input("valorTotalDevido", sql.VarChar(20), row.valorTotalDevido);
    req.input("dataAdmissao", sql.Date, row.dataAdmissao);
    req.input("dataNascimento", sql.Date, row.dataNascimento);
    req.input("nomeMae", sql.VarChar(100), row.nomeMae);
    req.input("sexo", sql.VarChar(2), row.sexo);
    req.input("nomeTipo", sql.VarChar(150), row.nomeTipo);
    req.input("prazo", sql.BigInt, row.prazo);
    req.input("taxaJuros", sql.VarChar(5), row.taxaJuros);
    req.input("valorLiberado", sql.VarChar(10), row.valorLiberado);
    req.input("valorParcela", sql.VarChar(10), row.valorParcela);
    req.input("taxaSeguro", sql.VarChar(10), row.taxaSeguro);
    req.input("valorSeguro", sql.VarChar(10), row.valorSeguro);
    req.input("tipoConsulta", sql.VarChar(50), tipoConsultaValue);
    req.input("status", sql.VarChar(50), statusValue);

    const rs = await req.query(`
      UPDATE [presenca].[dbo].[consulta_presenca]
      SET
        [cpf] = @cpf,
        [nome] = @nome,
        [telefone] = @telefone,
        [loginP] = @loginP,
        [created_at] = @created_at,
        [updated_at] = @updated_at,
        [matricula] = @matricula,
        [numeroInscricaoEmpregador] = @numeroInscricaoEmpregador,
        [elegivel] = @elegivel,
        [valorMargemDisponivel] = @valorMargemDisponivel,
        [valorMargemBase] = @valorMargemBase,
        [valorTotalDevido] = @valorTotalDevido,
        [dataAdmissao] = @dataAdmissao,
        [dataNascimento] = @dataNascimento,
        [nomeMae] = @nomeMae,
        [sexo] = @sexo,
        [nomeTipo] = @nomeTipo,
        [prazo] = @prazo,
        [taxaJuros] = @taxaJuros,
        [valorLiberado] = @valorLiberado,
        [valorParcela] = @valorParcela,
        [taxaSeguro] = @taxaSeguro,
        [valorSeguro] = @valorSeguro,
        [tipoConsulta] = @tipoConsulta,
        [status] = @status
      WHERE [id] = @id
    `);

    if (!exec.tx) await tx.commit();
    return Number((rs.rowsAffected && rs.rowsAffected[0]) || 0);
  } catch (err) {
    if (!exec.tx) await tx.rollback();
    throw err;
  }
}

async function replacePendingConsultaPresencaById(pendingRow, results, options = {}) {
  const rows = [];
  for (const result of results || []) {
    rows.push(
      ...buildRowsFromResult(result, pendingRow?.loginP, pendingRow?.tipoConsulta, {
        skipFallbackRow: options.skipFallbackRow,
      })
    );
  }
  const validRows = rows.filter((r) => r.cpf != null);
  const skippedRows = rows.length - validRows.length;

  const pool = await getPool();
  const tx = new sql.Transaction(pool);
  await tx.begin();

  try {
    let updatedRows = 0;
    let insertedRows = 0;
    const baseOptions = {
      loginP: pendingRow.loginP,
      tipoConsulta: pendingRow.tipoConsulta,
      createdAt: pendingRow.createdAt,
      status: options.status || "Concluido",
    };

    if (validRows.length > 0) {
      // Upsert behavior: update original pending row with the first result.
      updatedRows = await updateConsultaPresencaRowById(pendingRow.id, validRows[0], baseOptions, { tx });
      // Additional results are appended as new rows preserving login/file(created_at+tipoConsulta).
      const extraRows = validRows.slice(1);
      if (extraRows.length > 0) {
        insertedRows = await insertConsultaPresencaRows(extraRows, baseOptions, { tx });
      }
    } else {
      const upd = new sql.Request(tx);
      upd.input("id", sql.Int, Number(pendingRow.id));
      upd.input("status", sql.VarChar(50), toVarcharOrNull(options.status || "Concluido", 50));
      const rs = await upd.query(`
        UPDATE [presenca].[dbo].[consulta_presenca]
        SET
          [status] = @status,
          [updated_at] = GETDATE()
        WHERE [id] = @id
      `);
      updatedRows = Number((rs.rowsAffected && rs.rowsAffected[0]) || 0);
    }

    await tx.commit();
    return { updatedRows, insertedRows, totalRows: updatedRows + insertedRows, skippedRows };
  } catch (err) {
    await tx.rollback();
    throw err;
  }
}

async function saveConsultaPresencaResults(results, { loginP, tipoConsulta, createdAt, status } = {}) {
  const rows = [];
  for (const result of results || []) {
    rows.push(...buildRowsFromResult(result, loginP, tipoConsulta));
  }
  const validRows = rows.filter((r) => r.cpf != null);
  const skippedRows = rows.length - validRows.length;
  const createdAtValue = createdAt || new Date();
  const loginPValue = toVarcharOrNull(loginP, 50);
  const tipoConsultaValue = toVarcharOrNull(tipoConsulta || (validRows[0] && validRows[0].tipoConsulta) || "Em lote", 50);

  const pool = await getPool();
  const tx = new sql.Transaction(pool);
  await tx.begin();

  try {
    // Replace any existing rows for the same batch (loginP + tipoConsulta + created_at).
    // This avoids leaving stale "Pendente" rows and allows persisting multiple rows per CPF
    // (the external API typically returns several table options).
    const del = new sql.Request(tx);
    del.input("loginP", sql.VarChar(50), loginPValue);
    del.input("tipoConsulta", sql.VarChar(50), tipoConsultaValue);
    del.input("created_at", sql.DateTime, createdAtValue);
    await del.query(`
      DELETE FROM [presenca].[dbo].[consulta_presenca]
      WHERE [loginP] = @loginP
        AND [tipoConsulta] = @tipoConsulta
        AND [created_at] = @created_at
    `);

    const insertedRows = await insertConsultaPresencaRows(
      validRows,
      { loginP: loginPValue, tipoConsulta: tipoConsultaValue, createdAt: createdAtValue, status },
      { tx }
    );

    await tx.commit();
    return { insertedRows, skippedRows };
  } catch (err) {
    await tx.rollback();
    throw err;
  }
}

async function getConsultedCpfsTodayByLogin(loginP, cpfs) {
  const normalized = [...new Set((cpfs || []).map((v) => onlyDigits(v)).filter(Boolean))];
  if (!loginP || !normalized.length) return new Set();

  const pool = await getPool();
  const req = pool.request();
  req.input("loginP", sql.VarChar(50), String(loginP));

  const placeholders = normalized.map((cpf, idx) => {
    const name = `cpf${idx}`;
    req.input(name, sql.BigInt, cpf);
    return `@${name}`;
  });

  const rs = await req.query(`
    SELECT DISTINCT CAST([cpf] AS VARCHAR(20)) AS cpf
    FROM [presenca].[dbo].[consulta_presenca]
    WHERE [loginP] = @loginP
      AND CAST([created_at] AS DATE) = CAST(GETDATE() AS DATE)
      AND [cpf] IN (${placeholders.join(", ")})
  `);

  const found = new Set();
  for (const row of rs.recordset || []) {
    const d = onlyDigits(row.cpf);
    if (d) found.add(d.padStart(11, "0"));
  }
  return found;
}

module.exports = {
  saveConsultaPresencaResults,
  insertPendingConsultaPresenca,
  listPendingConsultaPresenca,
  claimPendingConsultaPresencaById,
  markConsultaPresencaStatusById,
  replacePendingConsultaPresencaById,
  getConsultedCpfsTodayByLogin,
};
