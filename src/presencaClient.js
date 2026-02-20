const axios = require("axios");
const { chromium } = require("playwright");

const BASE_URL = "https://presenca-bank-api.azurewebsites.net";

async function postWithRetry(url, payload, headers, { timeout = 30000, retries = 2, retryDelayMs = 1500 } = {}) {
  const attempts = Math.max(1, retries + 1);
  let lastErr = null;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const resp = await axios.post(url, payload, { headers, timeout });
      return { status: resp.status, data: resp.data };
    } catch (err) {
      lastErr = err;
      const hasHttpResponse = Boolean(err.response);
      if (hasHttpResponse) {
        return { status: err.response.status, data: err.response.data };
      }
      if (attempt < attempts) {
        const wait = retryDelayMs * 2 ** (attempt - 1);
        await new Promise((r) => setTimeout(r, wait));
        continue;
      }
    }
  }
  return {
    status: 0,
    data: { error: "Falha de comunicacao com a API", detail: lastErr ? String(lastErr.message || lastErr) : "erro desconhecido" },
  };
}

async function login({ login, senha, timeout = 30000, retries = 2, retryDelayMs = 1500 }) {
  return postWithRetry(
    `${BASE_URL}/login`,
    { login, senha },
    { "Content-Type": "application/json" },
    { timeout, retries, retryDelayMs }
  );
}

async function acceptTermoHeadless(shortUrl, autorizacaoId, timeoutSeconds = 45) {
  const calls = [];
  const timeoutMs = Math.max(10000, Number(timeoutSeconds || 45) * 1000);
  const browser = await chromium.launch({ headless: true });
  try {
    const context = await browser.newContext({
      viewport: { width: 1440, height: 900 },
      geolocation: { latitude: -23.55052, longitude: -46.633308 },
      permissions: ["geolocation"],
    });
    const page = await context.newPage();
    page.on("response", (resp) => {
      const req = resp.request();
      if (["POST", "PUT", "PATCH"].includes(req.method())) {
        calls.push({ method: req.method(), url: resp.url(), status: resp.status() });
      }
    });
    await page.goto(shortUrl, { waitUntil: "domcontentloaded", timeout: timeoutMs });
    await page.waitForLoadState("networkidle", { timeout: Math.min(timeoutMs, 12000) }).catch(() => {});
    await page.waitForTimeout(1200);
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(700);

    let checkbox = page.getByLabel(/autorizo.*dataprev/i);
    if ((await checkbox.count()) === 0) {
      checkbox = page.locator("input[type='checkbox']");
    }
    if ((await checkbox.count()) === 0) {
      throw new Error("Checkbox de autorizacao nao encontrado");
    }
    await checkbox.first().scrollIntoViewIfNeeded().catch(() => {});
    await checkbox.first().check({ force: true, timeout: Math.min(timeoutMs, 15000) });

    let button = page.getByRole("button", { name: /enviar/i });
    if ((await button.count()) === 0) {
      button = page.locator("button:has-text('Enviar')");
    }
    if ((await button.count()) === 0) {
      throw new Error("Botao Enviar nao encontrado");
    }

    const needle = autorizacaoId ? `/consultas/termo-inss/${autorizacaoId}` : "/consultas/termo-inss/";

    const putResponsePromise = page
      .waitForResponse((resp) => resp.request().method() === "PUT" && resp.url().includes(needle), {
        timeout: timeoutMs,
      })
      .catch(() => null);

    await button.first().click({ timeout: Math.min(timeoutMs, 15000) });
    const putResponse = await putResponsePromise;
    await page.waitForTimeout(800);

    const putStatus = putResponse ? putResponse.status() : null;
    const okFromResponse = putStatus != null && putStatus >= 200 && putStatus < 300;
    const okFromCalls = calls.some((c) => c.method === "PUT" && c.status >= 200 && c.status < 300 && c.url.includes(needle));
    const ok = okFromResponse || okFromCalls;

    await context.close();
    return {
      ok,
      calls,
      putStatus,
      reason: ok ? null : putStatus == null ? "PUT de aceite nao identificado dentro do timeout" : `PUT retornou status ${putStatus}`,
    };
  } finally {
    await browser.close();
  }
}

module.exports = {
  BASE_URL,
  postWithRetry,
  login,
  acceptTermoHeadless,
};
