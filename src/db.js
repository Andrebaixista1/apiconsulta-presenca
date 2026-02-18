const sql = require("mssql");

let poolPromise;

function getSqlConfig() {
  return {
    user: process.env.MSSQL_USER || "andrefelipe",
    password: process.env.MSSQL_PASSWORD || "899605aA@",
    server: process.env.MSSQL_SERVER || "177.153.62.236",
    database: process.env.MSSQL_DATABASE || "presenca",
    port: Number(process.env.MSSQL_PORT || 1433),
    options: {
      encrypt: String(process.env.MSSQL_ENCRYPT || "false").toLowerCase() === "true",
      trustServerCertificate: String(process.env.MSSQL_TRUST_CERT || "true").toLowerCase() !== "false",
    },
    pool: {
      max: Number(process.env.MSSQL_POOL_MAX || 10),
      min: 0,
      idleTimeoutMillis: 30000,
    },
  };
}

function getPool() {
  if (!poolPromise) {
    const config = getSqlConfig();
    poolPromise = sql.connect(config);
  }
  return poolPromise;
}

module.exports = {
  sql,
  getPool,
};
