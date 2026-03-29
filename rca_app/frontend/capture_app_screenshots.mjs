/**
 * Capture screenshots to ../../images (repo root).
 * Requires: `npm run dev` on :5173, RCA API on CAPTURE_API_PORT (default 8003).
 *
 *   cd rca_app/frontend && node capture_app_screenshots.mjs
 */
import { chromium } from 'playwright';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, '../..');
const OUT = path.join(REPO_ROOT, 'images');
const BASE = (process.env.CAPTURE_BASE_URL || 'http://localhost:5173').replace(/\/$/, '');
const API_TARGET = `http://127.0.0.1:${process.env.CAPTURE_API_PORT || '8003'}`;
const VIEW = { width: 1440, height: 900 };

async function shot(page, name, fullPage = true) {
  const p = path.join(OUT, name);
  await page.screenshot({ path: p, fullPage });
  console.log('wrote', p);
}

async function waitReady(page, timeout = 180000) {
  await page.waitForSelector('#root .app-layout', { timeout: 120000 });
  await page.waitForFunction(
    () => !document.querySelector('.loading-container'),
    null,
    { timeout }
  );
}

async function settleCharts(page) {
  await page.waitForTimeout(10000);
  await page.locator('.recharts-surface').first().waitFor({ state: 'visible', timeout: 90000 }).catch(() => {});
  await page.waitForTimeout(6000);
}

async function gotoAndSettle(page, route, afterReady) {
  await page.goto(`${BASE}${route}`, { waitUntil: 'load', timeout: 180000 });
  await waitReady(page);
  if (afterReady) await afterReady(page);
  await settleCharts(page);
}

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({ viewport: VIEW, deviceScaleFactor: 2 });

await context.route(
  (url) => url.href.startsWith(`${BASE}/api/`),
  async (route) => {
    const u = route.request().url().replace(BASE, API_TARGET);
    await route.continue({ url: u });
  }
);

const page = await context.newPage();

await gotoAndSettle(page, '/', async (p) => {
  await p.waitForSelector('.stats-grid .stat-value', { timeout: 120000 });
});

await shot(page, 'app-executive-dashboard.png', true);

await gotoAndSettle(page, '/root-cause', async (p) => {
  await p.getByRole('heading', { name: 'Root Cause Intelligence' }).waitFor({ timeout: 120000 });
  await p.getByText('Failure Patterns by Priority Score').waitFor({ timeout: 60000 });
});

await shot(page, 'app-root-cause-intelligence.png', true);

await gotoAndSettle(page, '/service-risk', async (p) => {
  await p.getByRole('heading', { name: /Service Risk/i }).waitFor({ timeout: 120000 });
});

await shot(page, 'app-service-risk-ranking.png', true);

await gotoAndSettle(page, '/change-correlation', async (p) => {
  await p.getByRole('heading', { name: /Change Correlation/i }).waitFor({ timeout: 120000 });
});

await shot(page, 'app-change-correlation.png', true);

await page.goto(`${BASE}/genie`, { waitUntil: 'load', timeout: 180000 });
await page.waitForSelector('#root .app-layout', { timeout: 120000 });
await page.getByText('Supply Chain Disruption', { exact: false }).waitFor({ timeout: 120000 });
await page.waitForTimeout(10000);
await shot(page, 'app-genie-chat.png', true);

await page.goto(`${BASE}/domain-deep-dive`, { waitUntil: 'load', timeout: 180000 });
await page.waitForSelector('#root .app-layout', { timeout: 120000 });
await page.getByRole('heading', { name: /Domain Deep Dive/i }).waitFor({ timeout: 120000 });
await page.getByRole('button', { name: /^INC-/ }).first().waitFor({ state: 'visible', timeout: 180000 });
await page.waitForTimeout(12000);
await settleCharts(page);
await shot(page, 'app-domain-deep-dive.png', true);

const incBtn = page.getByRole('button', { name: /^INC-/ }).first();
await incBtn.waitFor({ state: 'visible', timeout: 60000 });
await incBtn.click();

await page.getByText(/Incident Payload:/).waitFor({ state: 'visible', timeout: 120000 });
await page.waitForFunction(
  () => !document.querySelector('.loading-container'),
  null,
  { timeout: 120000 }
);

const metricsLoading = page.getByText('Loading metrics...');
await metricsLoading.waitFor({ state: 'visible', timeout: 20000 }).catch(() => {});
await metricsLoading.waitFor({ state: 'hidden', timeout: 180000 });

await page.waitForTimeout(10000);
await settleCharts(page);
await shot(page, 'incident-drawer-metrics.png', true);

await browser.close();
console.log('done');
