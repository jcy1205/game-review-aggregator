const Database = require('better-sqlite3');
const cheerio = require('cheerio');
const { performance } = require('node:perf_hooks');

// 1. Initialize SQLite Database & Telemetry Tables
const db = new Database('games.db');
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS games (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    external_id TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    thumbnail_url TEXT,
    review_summary TEXT,
    review_score REAL,
    total_reviews INTEGER,
    positive_reviews INTEGER,
    negative_reviews INTEGER,
    active_players INTEGER,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(platform, external_id)
  );

  CREATE INDEX IF NOT EXISTS idx_games_title ON games(title COLLATE NOCASE);

  -- Stores timing breakdowns for each database population run
  CREATE TABLE IF NOT EXISTS sync_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,
    items_processed INTEGER NOT NULL,
    fetch_ms REAL NOT NULL,
    db_write_ms REAL NOT NULL,
    total_ms REAL NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );

  -- Stores timing for every search query executed via the Express API
  CREATE TABLE IF NOT EXISTS search_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query TEXT NOT NULL,
    results_count INTEGER NOT NULL,
    execution_ms REAL NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  );
`);

const upsertGameStmt = db.prepare(`
  INSERT INTO games (
    platform, external_id, title, url, thumbnail_url,
    review_summary, review_score, total_reviews,
    positive_reviews, negative_reviews, active_players, updated_at
  ) VALUES (
    @platform, @external_id, @title, @url, @thumbnail_url,
    @review_summary, @review_score, @total_reviews,
    @positive_reviews, @negative_reviews, @active_players, CURRENT_TIMESTAMP
  )
  ON CONFLICT(platform, external_id) DO UPDATE SET
    title = excluded.title,
    thumbnail_url = excluded.thumbnail_url,
    review_summary = excluded.review_summary,
    review_score = excluded.review_score,
    total_reviews = excluded.total_reviews,
    positive_reviews = excluded.positive_reviews,
    negative_reviews = excluded.negative_reviews,
    active_players = excluded.active_players,
    updated_at = CURRENT_TIMESTAMP;
`);

const logSyncStmt = db.prepare(`
  INSERT INTO sync_logs (platform, items_processed, fetch_ms, db_write_ms, total_ms)
  VALUES (@platform, @items_processed, @fetch_ms, @db_write_ms, @total_ms)
`);

const saveGamesBatch = db.transaction((games) => {
  for (const game of games) {
    upsertGameStmt.run(game);
  }
});

const delay = (ms) => new Promise((res) => setTimeout(res, ms));

// Helper to time network fetch vs SQLite insertion and log to sync_logs
async function runTimedSync(platformName, fetchFn) {
  const startTotal = performance.now();

  const startFetch = performance.now();
  const gamesToSave = await fetchFn();
  const fetchMs = Number((performance.now() - startFetch).toFixed(2));

  const startWrite = performance.now();
  saveGamesBatch(gamesToSave);
  const dbWriteMs = Number((performance.now() - startWrite).toFixed(2));

  const totalMs = Number((performance.now() - startTotal).toFixed(2));

  const metric = {
    platform: platformName,
    items_processed: gamesToSave.length,
    fetch_ms: fetchMs,
    db_write_ms: dbWriteMs,
    total_ms: totalMs
  };

  logSyncStmt.run(metric);
  return metric;
}

// 2. Fetch Steam Games + Reviews
async function fetchSteamGames(limit = 15) {
  console.log('Fetching Steam featured games...');
  const res = await fetch('https://store.steampowered.com/api/featuredcategories');
  if (!res.ok) throw new Error(`Steam API failed: ${res.status}`);
  const data = await res.json();

  const rawItems = [
    ...(data.top_sellers?.items || []),
    ...(data.new_releases?.items || [])
  ];
  const uniqueApps = Array.from(new Map(rawItems.map((item) => [item.id, item])).values()).slice(0, limit);
  const games = [];

  for (const app of uniqueApps) {
    await delay(250);
    let reviewData = {};
    try {
      const revRes = await fetch(
        `https://store.steampowered.com/appreviews/${app.id}?json=1&language=all&purchase_type=all&num_per_page=0`
      );
      const revJson = await revRes.json();
      reviewData = revJson.query_summary || {};
    } catch (err) {
      console.warn(`Could not fetch Steam reviews for ${app.name}:`, err.message);
    }

    const total = reviewData.total_reviews || 0;
    const positive = reviewData.total_positive || 0;
    const scorePct = total > 0 ? Number(((positive / total) * 100).toFixed(2)) : null;

    games.push({
      platform: 'steam',
      external_id: String(app.id),
      title: app.name,
      url: `https://store.steampowered.com/app/${app.id}`,
      thumbnail_url: app.large_capsule_image || app.header_image || null,
      review_summary: reviewData.review_score_desc || null,
      review_score: scorePct,
      total_reviews: total,
      positive_reviews: positive,
      negative_reviews: reviewData.total_negative || 0,
      active_players: null
    });
  }
  return games;
}

// 3. Fetch Itch.io Games (RSS) + Scrape JSON-LD Rating
async function fetchItchGames(limit = 10) {
  console.log('Fetching Itch.io top-rated feed...');
  const res = await fetch('https://itch.io/games/top-rated.xml');
  if (!res.ok) throw new Error(`Itch RSS failed: ${res.status}`);
  const xml = await res.text();

  const $ = cheerio.load(xml, { xmlMode: true });
  const items = $('item').toArray().slice(0, limit);
  const games = [];

  for (const el of items) {
    const item = $(el);
    const title = item.find('plainTitle').text() || item.find('title').text();
    const url = item.find('link').text().trim();
    const thumb = item.find('imageurl').text() || null;

    let ratingScore = null;
    let ratingCount = null;
    await delay(300);

    try {
      const pageRes = await fetch(url);
      if (pageRes.ok) {
        const html = await pageRes.text();
        const $page = cheerio.load(html);
        $page('script[type="application/ld+json"]').each((_, scriptEl) => {
          try {
            const parsed = JSON.parse($page(scriptEl).html());
            if (parsed.aggregateRating) {
              ratingScore = Number(parsed.aggregateRating.ratingValue) || null;
              ratingCount = Number(parsed.aggregateRating.ratingCount) || null;
            }
          } catch (_) {}
        });
      }
    } catch (err) {
      console.warn(`Could not fetch Itch page for ${title}:`, err.message);
    }

    games.push({
      platform: 'itch',
      external_id: url,
      title,
      url,
      thumbnail_url: thumb,
      review_summary: ratingScore ? `${ratingScore} / 5 stars` : null,
      review_score: ratingScore,
      total_reviews: ratingCount,
      positive_reviews: null,
      negative_reviews: null,
      active_players: null
    });
  }
  return games;
}

// 4. Fetch Roblox Games via Rolimon's Game List API
async function fetchRobloxGames(limit = 50) {
  console.log("Fetching Roblox games from Rolimon's API...");
  const res = await fetch('https://api.rolimons.com/games/v1/gamelist', {
    headers: { 'User-Agent': 'GameAggregatorPrototype/1.0' }
  });
  if (!res.ok) throw new Error(`Rolimons API failed: ${res.status}`);
  const data = await res.json();

  if (!data.success || !data.games) {
    throw new Error('Unexpected Rolimons payload format');
  }

  const entries = Object.entries(data.games)
    .sort((a, b) => (b[1][1] || 0) - (a[1][1] || 0))
    .slice(0, limit);

  return entries.map(([placeId, info]) => ({
    platform: 'roblox',
    external_id: String(placeId),
    title: info[0],
    url: `https://www.roblox.com/games/${placeId}`,
    thumbnail_url: info[2] || null,
    review_summary: null,
    review_score: null,
    total_reviews: null,
    positive_reviews: null,
    negative_reviews: null,
    active_players: info[1] ?? null
  }));
}

// 5. Run Full Aggregation Pipeline with Timing Breakdown
async function ingestAllPlatforms() {
  const pipelineStart = performance.now();
  const metrics = [];

  try {
    metrics.push(await runTimedSync('steam', () => fetchSteamGames(15)));
    metrics.push(await runTimedSync('itch', () => fetchItchGames(10)));
    metrics.push(await runTimedSync('roblox', () => fetchRobloxGames(50)));

    const pipelineTotalMs = Number((performance.now() - pipelineStart).toFixed(2));

    console.log('\n--- Ingestion Performance Breakdown ---');
    console.table(metrics);
    console.log(`Total Pipeline Duration: ${pipelineTotalMs} ms (${(pipelineTotalMs / 1000).toFixed(2)} s)\n`);
  } catch (error) {
    console.error('Aggregation error:', error);
  }
}

ingestAllPlatforms();