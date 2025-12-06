# D3 Football Predictions – API Contract

Base URL (dev):
`http://localhost:8000/api`  
(Adjust as needed for your environment.)

This document describes the endpoints required by the **frontend MVP**:
- Predictions Explorer (Home)
- Model Performance

---

## 1. Meta: Seasons & Weeks

### GET `/api/meta/seasons`

Return available seasons, their weeks, and which season/week is current.

#### Query Parameters
_None_

#### Response: 200 OK

```json
{
  "seasons": [
    {
      "year": 2022,
      "weeks": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      "is_current": false
    },
    {
      "year": 2023,
      "weeks": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
      "is_current": false
    },
    {
      "year": 2024,
      "weeks": [1, 2, 3, 4, 5, 6, 7, 8],
      "is_current": true,
      "latest_completed_week": 7
    }
  ]
}

Error Response : 500 - unexpect server error


2. Weekly Predictions
GET /api/predictions/:year/:week

Return all model predictions for a given season/week.
Each item includes matchup, prediction, confidence, and actual result (if completed).

Path Parameters

year (int, required) – e.g. 2024

week (int, required) – e.g. 7

Query Parameters

None (MVP will implement filtering/sorting client-side.)

Response: 200 OK
{
  "year": 2024,
  "week": 7,
  "games_count": 12,
  "correct_count": 8,
  "accuracy": 0.6667,
  "predictions": [
    {
      "game_id": 1234,
      "date": "2024-10-12",

      "status": "completed", 
      // "completed" | "upcoming"

      "home_team": {
        "id": 10,
        "name": "WashU Bears",
        "slug": "washu",
        "conference": "CCIW"
      },
      "away_team": {
        "id": 22,
        "name": "North Central Cardinals",
        "slug": "north-central",
        "conference": "CCIW"
      },

      "predicted_winner": "home", 
      // "home" | "away"

      "home_win_prob": 0.78,
      "away_win_prob": 0.22,
      "confidence": 0.78,
      // usually max(home_win_prob, away_win_prob)

      "confidence_bucket": "70-80",
      // optional – can be computed in backend or frontend

      "key_advantages": [
        "Higher offensive YPP",
        "Better last-3-games margin",
        "Home field advantage"
      ],

      "home_score": 24,
      "away_score": 21,
      // null if upcoming

      "actual_winner": "home",
      // "home" | "away" | null if upcoming

      "was_correct": true,
      // null if upcoming

      "is_upset": false
      // optional – true if completed, was_correct = false, and confidence >= some threshold
    }
  ]
}

Error Responses

400 – invalid year/week

404 – no predictions for given year/week

500 – unexpected server error


3. Accuracy Summary
GET /api/stats/accuracy

Summary of model accuracy overall and by season.

Query Parameters

None

Response: 200 OK
{
  "overall": {
    "games": 3000,
    "correct": 1950,
    "accuracy": 0.65
  },
  "by_season": [
    {
      "year": 2022,
      "games": 1000,
      "correct": 640,
      "accuracy": 0.64
    },
    {
      "year": 2023,
      "games": 1000,
      "correct": 660,
      "accuracy": 0.66
    },
    {
      "year": 2024,
      "games": 1000,
      "correct": 650,
      "accuracy": 0.65
    }
  ],
  "current_season": 2024
}

Error Responses

500 – unexpected server error

4. Calibration / Confidence Buckets
GET /api/stats/calibration

Calibration stats of the model across confidence buckets.

Query Parameters

None

Response: 200 OK
{
  "buckets": [
    {
      "bucket_min": 0.50,
      "bucket_max": 0.60,
      "label": "50-60",

      "games": 400,

      "mean_predicted": 0.55,
      // average predicted win probability in this bucket

      "actual_accuracy": 0.53
      // fraction of predictions correct in this bucket
    },
    {
      "bucket_min": 0.60,
      "bucket_max": 0.70,
      "label": "60-70",
      "games": 500,
      "mean_predicted": 0.65,
      "actual_accuracy": 0.63
    }
    // ...
  ]
}

Error Responses

500 – unexpected server error

5. Games by Confidence Bucket

Used by the Model Performance page to drill down from a confidence bucket (bar) to individual games.

GET /api/stats/games-by-bucket

Return all completed games whose prediction confidence falls in the given bucket range.

Query Parameters

One of:

bucket_min (float, required) – inclusive lower bound, e.g. 0.7

bucket_max (float, required) – exclusive upper bound, e.g. 0.8

OR

label (string, optional) – e.g. "70-80" if you prefer string-based lookup

Backend can support both patterns, but at minimum bucket_min + bucket_max should work.

Example request:

GET /api/stats/games-by-bucket?bucket_min=0.7&bucket_max=0.8

Response: 200 OK
{
  "bucket_min": 0.70,
  "bucket_max": 0.80,
  "label": "70-80",
  "games": [
    {
      "game_id": 1234,
      "year": 2024,
      "week": 7,
      "date": "2024-10-12",

      "home_team": { "id": 10, "name": "WashU Bears", "slug": "washu" },
      "away_team": { "id": 22, "name": "North Central Cardinals", "slug": "north-central" },

      "predicted_winner": "home",
      "confidence": 0.74,
      "home_win_prob": 0.74,
      "away_win_prob": 0.26,

      "home_score": 24,
      "away_score": 21,
      "actual_winner": "home",
      "was_correct": true
    }
  ]
}


Only completed games need to be returned here (upcoming games are not relevant for calibration).

Error Responses

400 – missing or invalid bucket parameters

404 – no games found for this bucket

500 – unexpected server error

6. Notes & Conventions

All probabilities (home_win_prob, away_win_prob, confidence, mean_predicted) are 0.0–1.0 floats.

Timestamps are ISO 8601 strings in UTC (or clearly documented if local).

Any field that is not applicable for upcoming games (home_score, away_score, actual_winner, was_correct) should be null.

confidence_bucket can be:

Precomputed in the backend (recommended), or

Derived in the frontend from confidence. If you choose frontend, that field can be omitted.

Additional fields may be added in responses, but existing fields and semantics should remain stable once the frontend is built against them.