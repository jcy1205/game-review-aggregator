const express = require('express');
const Database = require('better-sqlite3');
const path = require('path');
const { performance } = require('node:perf_hooks');

const app = express();
const PORT = 3000;

// Open in read-write mode so we can log search query performance into search_logs
const db = new Database('games.db');
db.pragma('journal_mode = WAL');

app.use(express.static(path.join(__dirname, 'public')));

const searchGamesStmt = db.prepare(`
  SELECT 
    id, platform, external_id, title, url, thumbnail_url,
    review_summary, review_score, total_reviews, active_players
  FROM games
  WHERE title LIKE @SubstringQuery COLLATE NOCASE
  ORDER BY 
    CASE WHEN title LIKE @PrefixQuery COLLATE NOCASE THEN 0 ELSE 1 END,
    COALESCE(active_players, total_reviews, 0) DESC
  LIMIT 25
`);

const defaultGamesStmt = db.prepare(`
  SELECT 
    id, platform, external_id, title, url, thumbnail_url,
    review_summary, review_score, total_reviews, active_players
  FROM games
  ORDER BY COALESCE(active_players, total_reviews, 0) DESC
  LIMIT 25
`);

const logSearchStmt = db.prepare(`
  INSERT INTO search_logs (query, results_count, execution_ms)
  VALUES (@query, @results_count, @execution_ms)
`);

// GET /api/games/search?q=your_query
app.get('/api/games/search', (req, res) => {
  try {
    const rawQuery = (req.query.q || '').trim();
    const startQuery = performance.now();

    let results;
    if (!rawQuery) {
      results = defaultGamesStmt.all();
    } else {
      const sanitized = rawQuery.replace(/[%_]/g, '\\$&');
      results = searchGamesStmt.all({
        PrefixQuery: `${sanitized}%`,
        SubstringQuery: `%${sanitized}%`
      });
    }

    // High-resolution SQLite query execution time in milliseconds
    const executionMs = Number((performance.now() - startQuery).toFixed(3));

    // Log search performance asynchronously so it doesn't slow down the response
    setImmediate(() => {
      try {
        logSearchStmt.run({
          query: rawQuery || '[DEFAULT_LOAD]',
          results_count: results.length,
          execution_ms: executionMs
        });
      } catch (e) {
        console.warn('Could not write to search_logs:', e.message);
      }
    });

    console.log(`[Search] q="${rawQuery || '*'}" | ${results.length} results | ${executionMs} ms`);

    res.json({
      query: rawQuery,
      count: results.length,
      execution_ms: executionMs,
      results
    });
  } catch (err) {
    console.error('Search error:', err);
    res.status(500).json({ error: 'Failed to search games database.' });
  }
});

// GET /api/metrics - View historical ingestion & search performance
app.get('/api/metrics', (req, res) => {
  try {
    const latestSyncs = db.prepare(`
      SELECT platform, items_processed, fetch_ms, db_write_ms, total_ms, created_at
      FROM sync_logs
      ORDER BY id DESC
      LIMIT 10
    `).all();

    const searchStats = db.prepare(`
      SELECT 
        COUNT(*) as total_searches,
        ROUND(AVG(execution_ms), 3) as avg_search_ms,
        MIN(execution_ms) as fastest_search_ms,
        MAX(execution_ms) as slowest_search_ms
      FROM search_logs
    `).get();

    res.json({ latestSyncs, searchStats });
  } catch (err) {
    res.status(500).json({ error: 'Could not load telemetry metrics.' });
  }
});

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});