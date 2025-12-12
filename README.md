D3 Football Predictions

D3 Football Predictions is a full-stack web application that uses machine learning to predict NCAA Division III college football game outcomes. The app allows users to explore weekly predictions, analyze model accuracy and calibration, and simulate hypothetical matchups between any two D3 teams.

This project was built as a final project for a web development course and emphasizes API-driven design, interactive UI components, and visual polish, alongside thoughtful domain-specific modeling choices.

🌐 Live Deployment

Frontend (GitHub Pages):
👉 https://drew-bomar.github.io/d3_football_predictions/

The backend is hosted on the free-tier of render.com so please allow a minute or two for the server to spin up the first time you visit.

Backend API (Render):
👉 https://d3-football-api.onrender.com

👉 API docs available at /docs

Note: The backend API is hosted on Render’s free tier.
If the site has not been visited recently, the first API request may take ~30–60 seconds while the service spins up. Subsequent requests are fast.

🧠 Project Overview

The application is powered by a machine learning pipeline trained on ~3,000 NCAA Division III football games across multiple seasons.

Core Features

Weekly game predictions with confidence levels

Accuracy tracking for completed weeks

Model calibration analysis by confidence bucket

Interactive charts with drill-down views

Hypothetical matchup simulator

Responsive, polished dashboard UI

🛠 Tech Stack
Frontend

React + Vite (JavaScript)

Tailwind CSS for styling

Recharts for data visualization

React Router for navigation

Deployed via GitHub Pages

Backend

FastAPI (Python)

PostgreSQL database

Scikit-learn logistic regression model

Hosted on Render (API + managed Postgres)

🖥 Frontend Architecture

The frontend is a single-page React application focused on clarity, interactivity, and visual polish.

Key Pages

Hero Page – Landing page with project overview and CTA

Dashboard – Central hub with:

Model accuracy stats

Calibration chart (clickable confidence buckets)

Matchup simulator

Weekly predictions table

Predictions – Full table view of predictions by season/week

Model Performance – Detailed accuracy and calibration breakdowns

Simulator – Standalone hypothetical matchup simulator

UI Highlights

Dark-mode, purple-themed analytics dashboard

Glassmorphic navbar

Animated probability bars

Interactive charts and tables

Searchable team dropdown (250+ teams)

Hover, click, and loading states throughout

🔌 Backend Architecture

The backend exposes a REST API that serves predictions, statistics, and simulation results.

Database

PostgreSQL schema includes:

teams – team metadata

games – game results and schedules

team_game_stats – per-game stats

team_rolling_stats – rolling performance metrics

predictions – model outputs and evaluation results

Machine Learning Model

The predictive engine is built around a logistic regression model trained on engineered team-level features derived from historical game data.

Custom ELO Rating System (Key Innovation)

Division III football has extreme disparities in team strength compared to higher divisions — perennial powerhouses and first-year or rebuilding programs often coexist within the same season. Standard statistical features alone struggle to capture these long-term strength differences.

To address this, the project implements a custom ELO-based rating system tailored specifically for NCAA Division III football:

Every team is assigned a dynamic ELO rating that updates after each game.

Rating adjustments account for:

Opponent strength

Game outcome

Margin of victory

Strong wins against elite opponents produce larger gains, while expected wins yield smaller changes.

These ELO ratings are used as core input features to the machine learning model, alongside rolling performance metrics.

Incorporating the custom ELO system increased the model’s predictive accuracy by approximately 12% compared to an otherwise identical model that did not include ELO-based features.

This improvement highlights the importance of domain-aware feature engineering, especially in environments with large skill gaps and unbalanced competition.

Model Evaluation

Overall accuracy: ~65% baseline, with higher accuracy in high-confidence buckets

Confidence calibration is tracked and visualized in the frontend

Accuracy and calibration statistics are recomputed as new games are added

🔁 Frontend ↔ Backend Integration

The frontend communicates exclusively via HTTP requests to the FastAPI backend.

API base URL is configured via environment variables.

CORS is enabled to allow frontend access.

All interactive components (tables, charts, simulator) are powered by live API data.

🎯 Course Rubric Alignment
✅ Visual Design

Consistent color palette and typography

Clean card layouts and spacing

Responsive dashboard design

Custom animations and UI components

✅ Interactivity

Dropdown filters (season, week, teams)

Clickable charts with drill-down views

Hover and transition effects

Matchup simulator with real-time predictions

✅ API Usage

Custom REST API (FastAPI)

Multiple endpoints consumed by the frontend

Dynamic data fetching and rendering

Proper loading and error handling

🚀 Running Locally (Optional)
Backend
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload


Requires:

Python 3.10+

PostgreSQL

Environment variables for DATABASE_URL

Frontend
npm install
npm run dev


👤 Author

Drew Bomar
Final Project – Web Development Course
