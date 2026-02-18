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
    await page.goto(shortUrl, { waitUntil: "domcontentloaded", timeout: timeoutSeconds * 1000 });
    await page.waitForTimeout(1200);
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(700);

    const checkbox = page.getByLabel(/autorizo.*dataprev/i);
    if ((await checkbox.count()) > 0) {
      await checkbox.first().check({ force: true });
    } else {
      await page.locator("input[type='checkbox']").first().check({ force: true });
    }

    let button = page.getByRole("button", { name: /enviar/i });
    if ((await button.count()) === 0) {
      button = page.locator("button:has-text('Enviar')");
    }
    await button.first().click({ timeout: timeoutSeconds * 1000 });
    await page.waitForTimeout(1500);

    const needle = autorizacaoId ? `/consultas/termo-inss/${autorizacaoId}` : "/consultas/termo-inss/";
    const ok = calls.some((c) => c.method === "PUT" && c.status >= 200 && c.status < 300 && c.url.includes(needle));
    await context.close();
    return { ok, calls };
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
