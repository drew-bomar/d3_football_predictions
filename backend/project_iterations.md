# D3 Football Predictions — Project History

A narrative history of how this project evolved, including dead ends, bugs, and the *why* behind decisions. Preserved here because the original chat history is no longer accessible.

---

## The full story

### Origins: scraping → API pivot (September 2025)

The project started from the observation that D3 football is a prediction blind spot — no Vegas lines, no ESPN models, just 250 teams and nobody paying attention. The first approach was scraping `d3football.com`, which hit a wall fast: their playoff data was inconsistent/incomplete, which broke cross-season rolling calculations (a team's last few games of 2022 needed to feed into their early 2023 rolling averages, and missing playoff rows poisoned that). After a few frustrating days, the NCAA was discovered to expose a GraphQL endpoint at `sdataprod.ncaa.com`, and the ingestion layer was rebuilt around a proper API client. This pivot became the origin story used in the YC writeup.

### Database schema (September 2025)

Settled on PostgreSQL with a clean relational model: `teams`, `games`, `team_game_stats` (per-team per-game raw stats, two rows per game), and `team_rolling_stats` (computed features, one per team-game). Team resolution was genuinely tricky because the NCAA API doesn't give stable team IDs — `team_manager.py` matches on `seoname` + fallback logic to create/reuse teams. Ended up at ~287 D3 teams, ~4,854 games across 2021–2024, ~9,700 stat records.

### Rolling stats calculator (late September 2025)

The core insight that separates this project from a naive season-averages model: teams in October look nothing like teams in September, so rolling windows (3-week and 5-week) capture form better. The early-season problem was handled elegantly — for weeks 1–3, there aren't enough current-season games yet, so previous-season games contribute with a **decay weight of 0.7**. Around 82 features per game once all the differentials and matchup math was layered on top.

**Known data-quality issue from this era:** roughly 30% of opponent defensive stats were missing from the NCAA API, filled with zeros rather than dropping the rows. Flagged to revisit, never did.

### Logistic regression baseline (October 2025)

First model: calibrated logistic regression (`CalibratedClassifierCV` wrapping `LogisticRegression`). Trained chronologically on 2022–2023, tested on 2024 (hard chronological split to prevent leakage). Results:

- ~82% on 2023 held-out games
- ~74% on the full 2024 season (~950 games)
- ~85% when the model said >80% confidence — calibration was actually decent

The early version was **overconfident**: 90%+ predictions were only correct ~79% of the time. Probability calibration was added to fix this. Playoff weeks (12–15) were noticeably weaker (~71%) than regular season (~83%), probably because playoff teams face stronger, less-familiar opponents.

### The Claude Code detour (early October 2025)

Tried switching from the chat UI to Claude Code for a few days. It went badly — "lost observability" of what was changing in the code. Bailed back to the chat interface and generated a handoff doc on the way out. Worth remembering as a workflow data point: diffs and reasoning need to be visible inline, not delegated.

### Weekly prediction workflow (late October–November 2025)

Built `weekly_predictor.py`, `predict_week.py`, `evaluate_predictions.py`, and added a `predictions` table to the DB so predictions could be stored and later evaluated once games completed.

**Big bug from this phase:** importing **upcoming** games broke because `stats_translator.validate_translated_data` expected exactly 2 team-stat records per game, and upcoming games have zero stats (they haven't been played). The fix was twofold: a `translate_upcoming_game` method that returns the matchup info with `status='scheduled'` and null scores, plus a validator update that short-circuits to "valid" when status is scheduled. Clean solution, no new flag needed.

**Second bug:** `_build_prediction_features` was being called with extra `year`/`week` args it didn't need, because of a refactor that moved the feature construction into `GameDataPrep`. Simple fix but it took a minute to spot.

**Third bug:** a cumulative-vs-per-week bug in the "By Week" accuracy summary — it wasn't filtering by year/week/model properly, so each week's row was showing cumulative totals instead of that week's actual accuracy.

### The ELO detour (early December 2025)

**This is the one to remember clearly, because it has a factual asterisk on it.**

Built `elo_calculator.py` and a `game_elos` table. The plan was to feed ELO-derived features (`current_elo`, `elo_change_3wk`, `elo_change_5wk`, `avg_opp_elo_3wk`, `avg_opp_elo_5wk`) into the model to capture *quality* of wins and strength of schedule — things rolling box-score stats can't see.

**What actually happened:** the ELO calculator runs and stores ratings, but *those ratings are never joined into `team_rolling_stats`, and `data_prep.py`'s feature list doesn't reference them*. This was debated in chat — the first assertion that ELO wasn't wired in got pushed back on because `project.MD` claimed ELO was "integrated into rolling stats." A re-check (grep, schema inspection, feature-list inspection) confirmed: calculated and stored, but not a feature. This was never resolved — it still sits in the repo as an unfinished integration.

**Why this matters:** the YC Combinator blurb drafted during this project tells a story where accuracy hit 65%, defeat set in, a chess-playing epiphany led to ELO, and accuracy jumped to 80%. The 65% → 80% arc is *real* — the logistic regression baseline does get to ~80% on high-confidence picks — but the causal attribution to ELO is not accurate based on what's in the repo. The model gets to 80% from rolling stats and calibration, not from ELO. If the writeup is ever sent anywhere, either wire ELO in for real or rewrite the narrative.

### FastAPI backend (November–early December 2025)

Added a full API layer under `backend/src/api/` with routes for predictions, stats (accuracy, calibration, games-by-bucket), meta (teams, seasons, weeks), and simulate. The simulate endpoint was a mid-frontend feature addition: "can we predict any two teams, not just scheduled games?" Turned out to be easy because `predict_game()` already took arbitrary team IDs — the only change needed was a `_get_latest_rolling_stats` variant that grabbed a team's most recent stats regardless of week.

### React frontend (November–December 2025)

This was the web dev class final project — roughly a 10-hour budget, 20-day timeline. Stack: React + Vite + Tailwind + Recharts, deployed on GitHub Pages. Several visual iterations:

1. First cut: plain purple navbar + two pages (predictions table, model performance with calibration chart)
2. Added a HeroPage with a spinning D3 logo, gradient backgrounds, "Enter Dashboard" CTA
3. Added a DashboardPage unifying stats row, calibration chart with click-to-drill-down, matchup simulator, and predictions table with week selector
4. Polish pass: changed the logo from continuous spin to a hover-tilt, glass-style navbar, fixed the legend order on the calibration chart (though one minor bug stayed unresolved there), searchable team dropdown for all 250 teams (dropdowns of 250 were unusable)
5. Ongoing concern throughout: making it not look "AI-generated."

### Deployment (late December 2025)

Frontend on GitHub Pages, backend + Postgres on Render free tier. The standard set of deployment gotchas:

- **Model paths** broke because relative paths depend on working directory → fixed with `Path(__file__).resolve().parent`
- **Render database URL**: Render gives `postgres://` URLs, SQLAlchemy 2.0 wants `postgresql://` → string replacement in `dependencies.py`
- **GitHub Pages 404s on refresh** because SPA routes aren't real files → `basename="/d3_football_predictions"` on `BrowserRouter`
- **Vite asset paths** broke in build → switched from `/src/assets/...` to ES module imports
- **Database migration**: `pg_dump -Fc` locally → `pg_restore --no-owner --no-privileges` to Render

**Live URLs:**
- Frontend: https://drew-bomar.github.io/d3_football_predictions/
- Backend: https://d3-football-api.onrender.com
- API docs: https://d3-football-api.onrender.com/docs

Render free tier spins down after inactivity so the first request takes 30–50 seconds.

**Real post-deployment bug worth remembering:** after generating Week 13 predictions locally, they didn't appear on the site's stats page. Root cause: the accuracy endpoint filters on `WHERE was_correct IS NOT NULL`, and Week 13 games hadn't been played yet, so `was_correct` was correctly NULL. Not a bug — correct behavior. The predictions page also needed Week 13 added to its available-weeks list manually, which *was* a real oversight.

### Documentation & ops planning (January–February 2026)

Used Claude Code to generate a `project.MD` that covered the full stack end-to-end. Critique pass identified redundancies: the "Key Interactions Summary" duplicated the pipeline diagram, Frontend Architecture was over-detailed for a standard React app, the 92-feature construction was described in three different places, and the "Interactions" bullets on trivial components like `TiltLogo` were padding.

Also sketched a phased plan for operationalization — pipeline status tracking, admin API with key auth, background task runner wrapping the existing `import_week` / `predict_week` / `evaluate` scripts, frontend admin panel, scheduled GitHub Actions runs. No evidence in chat history that any of this actually shipped. Assume it's a TODO.

---

## Tech stack summary

| Layer | Technologies |
|-------|--------------|
| Frontend | React, Vite, Tailwind CSS, React Router, Recharts |
| Backend | Python, FastAPI, SQLAlchemy, Pydantic |
| ML | Scikit-learn (LogisticRegression, CalibratedClassifierCV), Pandas, NumPy, Joblib |
| Database | PostgreSQL |
| Hosting | GitHub Pages (frontend), Render (backend + database) |
| Data source | NCAA GraphQL API (`sdataprod.ncaa.com`) |

---

## Open items

- **ELO is still not wired into the model.** Pick one: wire it in, or rewrite the YC blurb.
- **Scheduled pipeline / admin panel** — planned, not shipped.
- **Render cold starts** — accepted tradeoff for free tier.
- **30% missing opponent defensive stats** — filled with zeros, never revisited.
- **Calibration chart legend order** — minor, acknowledged, never fixed.

---

## Performance benchmarks

- **Baseline (always pick home)**: ~54%
- **Logistic regression (current)**: ~74% on 2024 full season
- **High-confidence picks (>80%)**: ~85% correct
- **Regular season**: ~83%
- **Playoffs (weeks 12–15)**: ~71%
- **Theoretical ceiling**: ~80–85%

---

## Lessons learned

- The interesting problems were in the data pipeline and feature engineering, not in model architecture or hyperparameter tuning.
- Rolling windows beat season averages because team form changes fast.
- Probability calibration matters as much as raw accuracy when the output is going to be displayed to users as a confidence number.
- Chronological train/test splits are the only honest way to evaluate a time-series prediction model.
- Documentation that claims things are integrated should be verified against the actual code, not trusted.
