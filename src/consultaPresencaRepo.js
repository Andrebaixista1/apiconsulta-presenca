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

function buildRowsFromResult(result, loginP, tipoConsulta) {
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

async function insertConsultaPresencaRows(rows) {
  if (!rows.length) return 0;
  const pool = await getPool();
  const tx = new sql.Transaction(pool);
  await tx.begin();

  try {
    let inserted = 0;
    for (const row of rows) {
      const req = new sql.Request(tx);
      req.input("cpf", sql.BigInt, row.cpf);
      req.input("nome", sql.VarChar(100), row.nome);
      req.input("telefone", sql.BigInt, row.telefone);
      req.input("loginP", sql.VarChar(50), row.loginP);
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
      req.input("tipoConsulta", sql.VarChar(50), row.tipoConsulta);

      await req.query(`
        INSERT INTO [presenca].[dbo].[consulta_presenca]
          ([cpf], [nome], [telefone], [loginP], [created_at], [updated_at], [matricula], [numeroInscricaoEmpregador], [elegivel],
           [valorMargemDisponivel], [valorMargemBase], [valorTotalDevido], [dataAdmissao], [dataNascimento], [nomeMae], [sexo],
           [nomeTipo], [prazo], [taxaJuros], [valorLiberado], [valorParcela], [taxaSeguro], [valorSeguro], [tipoConsulta])
        VALUES
          (@cpf, @nome, @telefone, @loginP, GETDATE(), GETDATE(), @matricula, @numeroInscricaoEmpregador, @elegivel,
           @valorMargemDisponivel, @valorMargemBase, @valorTotalDevido, @dataAdmissao, @dataNascimento, @nomeMae, @sexo,
           @nomeTipo, @prazo, @taxaJuros, @valorLiberado, @valorParcela, @taxaSeguro, @valorSeguro, @tipoConsulta)
      `);
      inserted += 1;
    }
    await tx.commit();
    return inserted;
  } catch (err) {
    await tx.rollback();
    throw err;
  }
}

async function saveConsultaPresencaResults(results, { loginP, tipoConsulta }) {
  const rows = [];
  for (const result of results || []) {
    rows.push(...buildRowsFromResult(result, loginP, tipoConsulta));
  }
  const validRows = rows.filter((r) => r.cpf != null);
  const skippedRows = rows.length - validRows.length;
  const insertedRows = await insertConsultaPresencaRows(validRows);
  return { insertedRows, skippedRows };
}

module.exports = {
  saveConsultaPresencaResults,
};
