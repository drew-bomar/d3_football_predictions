// src/pages/DashboardPage.jsx

import { useState, useEffect } from 'react'
import { getAccuracy, getCalibration, getGamesByBucket } from '../api/stats'
import { getMeta, simulateMatchup } from '../api/simulate'
import { getPredictions } from '../api/predictions'

import { 
  BarChart, 
  Bar, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer
} from 'recharts'

import TeamSearchDropdown from '../components/TeamSearchDropdown'
import ProbabilityBar from '../components/ProbabilityBar'

function DashboardPage() {
  // ============ STATE ============
  // Core data
  const [accuracy, setAccuracy] = useState(null)
  const [calibration, setCalibration] = useState(null)
  const [teams, setTeams] = useState([])
  const [predictions, setPredictions] = useState(null)
  const [meta, setMeta] = useState(null)
  
  // Loading states
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  
  // Simulator state
  const [homeTeam, setHomeTeam] = useState(null)
  const [awayTeam, setAwayTeam] = useState(null)
  const [simResult, setSimResult] = useState(null)
  const [simLoading, setSimLoading] = useState(false)
  const [simError, setSimError] = useState(null)
  
  // Predictions filter state
  const [selectedYear, setSelectedYear] = useState(2025)
  const [selectedWeek, setSelectedWeek] = useState(null)

  // Bucket drill-down state
  const [selectedBucket, setSelectedBucket] = useState(null)
  const [bucketGames, setBucketGames] = useState(null)
  const [bucketLoading, setBucketLoading] = useState(false)

  // ============ DATA FETCHING ============
  useEffect(() => {
    async function loadDashboardData() {
      try {
        setLoading(true)
        
        const [accuracyData, calibrationData, metaData] = await Promise.all([
          getAccuracy(),
          getCalibration(),
          getMeta()
        ])
        
        setAccuracy(accuracyData)
        setCalibration(calibrationData)
        setMeta(metaData)
        setTeams(metaData.teams)
        
        const currentSeason = metaData.seasons.find(s => s.year === 2025)
        if (currentSeason && currentSeason.weeks_with_predictions.length > 0) {
          const latestWeek = Math.max(...currentSeason.weeks_with_predictions)
          setSelectedWeek(latestWeek)
          
          const predictionsData = await getPredictions(2025, latestWeek)
          setPredictions(predictionsData)
        }
        
        setError(null)
      } catch (err) {
        setError(err.message)
      } finally {
        setLoading(false)
      }
    }
    
    loadDashboardData()
  }, [])

  async function loadPredictions(year, week) {
    try {
      const data = await getPredictions(year, week)
      setPredictions(data)
    } catch (err) {
      console.error('Failed to load predictions:', err)
    }
  }

  function handleWeekChange(week) {
    setSelectedWeek(week)
    loadPredictions(selectedYear, week)
  }

  // ============ BUCKET DRILL-DOWN ============
  async function handleBarClick(data) {
    if (!data || !data.payload) return
    
    const bucket = data.payload
    setSelectedBucket(bucket)
    setBucketLoading(true)
    setBucketGames(null)
    
    try {
      const games = await getGamesByBucket(bucket.bucket_min, bucket.bucket_max)
      setBucketGames(games)
    } catch (err) {
      console.error('Failed to fetch bucket games:', err)
    } finally {
      setBucketLoading(false)
    }
  }

  function closeBucketView() {
    setSelectedBucket(null)
    setBucketGames(null)
  }

  // ============ SIMULATOR ============
  async function runSimulation() {
    if (!homeTeam || !awayTeam) return

    setSimLoading(true)
    setSimResult(null)
    setSimError(null)

    try {
      const result = await simulateMatchup(homeTeam.id, awayTeam.id)
      setSimResult(result)
    } catch (err) {
      console.error('Simulation failed:', err)

      if (err.status === 404) {
        setSimError(
          'The model does not have enough recent data for one or both of these teams yet.'
        )
      } else {
        setSimError('Something went wrong while simulating this matchup. Please try again.')
      }
    } finally {
      setSimLoading(false)
    }
  }

  const canSimulate = homeTeam && awayTeam && homeTeam.id !== awayTeam.id

  // ============ RENDER ============
  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-slate-400 text-xl">Loading dashboard...</div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-red-400 text-xl">Error: {error}</div>
      </div>
    )
  }

  const availableWeeks = meta?.seasons.find(s => s.year === selectedYear)?.weeks_with_predictions || []

  return (
    <div className="relative min-h-screen -mt-20 pt-20">
      
      {/* ===== BACKGROUND LAYER ===== */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        {/* Base gradient - subtle purple tint */}
        <div className="absolute inset-0 bg-gradient-to-br from-purple-950/40 via-transparent to-transparent" />
        
        {/* Corner accent - top left */}
        <div className="absolute -top-32 -left-32 w-96 h-96 rounded-full bg-purple-600/10 blur-3xl" />
        
        {/* Corner accent - bottom right */}
        <div className="absolute -bottom-32 -right-32 w-96 h-96 rounded-full bg-fuchsia-600/[0.08] blur-3xl" />
        
        {/* Subtle center glow for depth */}
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[600px] rounded-full bg-violet-500/5 blur-3xl" />
        
        {/* Subtle grid overlay - very faint */}
        <div 
          className="absolute inset-0 opacity-[0.015]"
          style={{
            backgroundImage: `linear-gradient(rgba(139, 92, 246, 0.5) 1px, transparent 1px),
                              linear-gradient(90deg, rgba(139, 92, 246, 0.5) 1px, transparent 1px)`,
            backgroundSize: '60px 60px'
          }}
        />
      </div>

      {/* ===== CONTENT LAYER ===== */}
      <div className="relative z-10 p-6">
        <div className="max-w-7xl mx-auto">
          
          {/* Header */}
          <div className="mb-8">
            <h1 className="text-3xl font-bold text-white">Dashboard</h1>
            <p className="text-slate-400 mt-1">D3 Football Predictions Overview</p>
          </div>

          {/* Stats Row */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
            <div className="card">
              <p className="text-slate-400 text-sm">Overall Accuracy</p>
              <p className="text-3xl font-bold text-violet-400">
                {(accuracy.overall.accuracy * 100).toFixed(1)}%
              </p>
            </div>
            <div className="card">
              <p className="text-slate-400 text-sm">Games Predicted</p>
              <p className="text-3xl font-bold text-white">
                {accuracy.overall.games}
              </p>
            </div>
            <div className="card">
              <p className="text-slate-400 text-sm">Correct Picks</p>
              <p className="text-3xl font-bold text-emerald-400">
                {accuracy.overall.correct}
              </p>
            </div>
            <div className="card">
              <p className="text-slate-400 text-sm">Current Season</p>
              <p className="text-3xl font-bold text-white">
                {accuracy.current_season}
              </p>
            </div>
          </div>

          {/* Main Grid - Chart and Simulator */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-6">
            
            {/* Calibration Chart */}
            <div className="card">
              <h2 className="text-lg font-semibold text-white mb-4">
                Model Calibration
              </h2>
              <p className="text-slate-400 text-sm mb-4">
                Click a bar to see games in that confidence range
              </p>
              <ResponsiveContainer width="100%" height={250}>
                <BarChart data={calibration.buckets}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                  <XAxis 
                    dataKey="label" 
                    tick={{ fill: '#94a3b8', fontSize: 12 }}
                    axisLine={{ stroke: '#374151' }}
                  />
                  <YAxis 
                    domain={[0, 1]} 
                    tickFormatter={(value) => `${(value * 100).toFixed(0)}%`}
                    tick={{ fill: '#94a3b8', fontSize: 12 }}
                    axisLine={{ stroke: '#374151' }}
                  />
                  <Tooltip 
                    formatter={(value) => `${(value * 100).toFixed(1)}%`}
                    contentStyle={{ 
                      backgroundColor: '#1a1528', 
                      border: '1px solid #374151',
                      borderRadius: '8px'
                    }}
                    labelStyle={{ color: '#fff' }}
                  />
                  <Bar 
                    dataKey="mean_predicted" 
                    name="Predicted" 
                    fill="#7c3aed"  /* Darker purple - was #8b5cf6 */
                    radius={[4, 4, 0, 0]}
                    cursor="pointer"
                    onClick={handleBarClick}
                  />
                  <Bar 
                    dataKey="actual_accuracy" 
                    name="Actual" 
                    fill="#bf22ceff"  /* Light pink/fuchsia - was #10b981 */
                    radius={[4, 4, 0, 0]}
                    cursor="pointer"
                    onClick={handleBarClick}
                  />
                </BarChart>
              </ResponsiveContainer>

              {/* Bucket Games Table (appears when bucket is clicked) */}
              {selectedBucket && (
                <div className="mt-6 pt-4 border-t border-slate-700">
                  <div className="flex items-center justify-between mb-3">
                    <div>
                      <h3 className="text-white font-medium">
                        {selectedBucket.label}% Confidence Games
                      </h3>
                      <p className="text-slate-400 text-sm">
                        {selectedBucket.games} games · Actual: {(selectedBucket.actual_accuracy * 100).toFixed(1)}%
                      </p>
                    </div>
                    <button
                      onClick={closeBucketView}
                      className="text-slate-400 hover:text-white text-sm"
                    >
                      ✕ Close
                    </button>
                  </div>

                  {bucketLoading && (
                    <p className="text-slate-400">Loading games...</p>
                  )}

                  {bucketGames && !bucketLoading && (
                    <div className="max-h-64 overflow-y-auto">
                      <table className="table-dark text-sm">
                        <thead>
                          <tr>
                            <th>Matchup</th>
                            <th>Confidence</th>
                            <th>Result</th>
                          </tr>
                        </thead>
                        <tbody>
                          {bucketGames.games.map((game) => (
                            <tr key={game.game_id}>
                              <td className="text-slate-300">
                                {game.away_team.name} @ {game.home_team.name}
                              </td>
                              <td className="text-violet-400">
                                {(game.confidence * 100).toFixed(0)}%
                              </td>
                              <td>
                                <span className={game.was_correct ? 'text-emerald-400' : 'text-red-400'}>
                                  {game.was_correct ? '✓' : '✗'}
                                </span>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              )}
            </div>

            {/* Matchup Simulator - in DashboardPage.jsx */}
            <div className="card">
              <h2 className="text-lg font-semibold text-white mb-1">
                Matchup Simulator
              </h2>
              <p className="text-slate-400 text-sm mb-6">
                Predict any hypothetical matchup
              </p>
              
              {/* Team Selection Area */}
              <div className="mb-6">
                {/* Labels row */}
                <div className="mb-2 grid grid-cols-[1fr_auto_1fr] items-end">
                  <span className="text-sm text-slate-400">Home Team</span>
                  <span />
                  <span className="text-sm text-slate-400 text-right">Away Team</span>
                </div>

                {/* Inputs + VS row */}
                <div className="flex items-center gap-4">
                  {/* Home input */}
                  <div className="flex-1">
                    <TeamSearchDropdown
                      teams={teams}
                      selectedTeam={homeTeam}
                      onSelect={setHomeTeam}
                      /* no label here – labels are above */
                    />
                  </div>

                  {/* VS badge – now centered to inputs only */}
                  <div className="flex-shrink-0 flex items-center justify-center">
                    <div className="relative flex items-center justify-center">
                      {/* outer ring — 48x48 */}
                      <div className="h-12 w-12 rounded-full bg-gradient-to-br from-purple-500 to-fuchsia-500 shadow-[0_0_0_1px_rgba(248,250,252,0.15)] shadow-purple-500/40" />
                      {/* inner dark chip — 38x38 */}
                      <div className="absolute h-9 w-9 rounded-full bg-black/80 flex items-center justify-center">
                        <span className="text-[11px] font-semibold tracking-[0.18em] text-slate-100">
                          VS
                        </span>
                      </div>
                    </div>
                  </div>

                  {/* Away input */}
                  <div className="flex-1">
                    <TeamSearchDropdown
                      teams={teams}
                      selectedTeam={awayTeam}
                      onSelect={setAwayTeam}
                      /* no label here – labels are above */
                    />
                  </div>
                </div>
              </div>
              
              {/* Error Message */}
              {homeTeam && awayTeam && homeTeam.id === awayTeam.id && (
                <p className="mb-4 text-red-400 text-sm text-center">
                  Select two different teams
                </p>
              )}
              
              {/* Simulate Button */}
              <button
                  onClick={runSimulation}
                  disabled={!canSimulate || simLoading}
                  className={`group relative mt-1 inline-flex w-full items-center justify-center overflow-hidden rounded-full border text-sm font-semibold transition-all duration-200 py-3 sm:py-3.5${
                    canSimulate && !simLoading
                      ? 'border-purple-400/60 bg-gradient-to-r from-purple-500 via-fuchsia-500 to-purple-500 text-white shadow-[0_18px_45px_rgba(0,0,0,0.55)] hover:shadow-[0_22px_55px_rgba(0,0,0,0.75)] hover:-translate-y-[1px] active:translate-y-0 '
                      : 'border-slate-700 bg-slate-900/60 text-slate-500 cursor-not-allowed'
                  }`}
                >
                  {/* subtle shine on hover */}
                  {canSimulate && !simLoading && (
                    <span className="pointer-events-none absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-200 bg-gradient-to-r from-white/10 via-transparent to-white/5" />
                  )}

                  <span className="relative flex items-center gap-2">
                    {simLoading ? (
                      <>
                        <svg className="h-4 w-4 animate-spin" viewBox="0 0 24 24">
                          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
                          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                        </svg>
                        Simulating…
                      </>
                    ) : (
                      'Simulate Matchup'
                    )}
                  </span>
                </button>
              
              {simError && (
                <p className="mt-3 text-xs text-rose-300/90">
                  {simError}
                </p>
              )}

              {/* Results Area */}
              {simResult && (
                <div className="mt-6 pt-6 border-t border-white/10">
                  <ProbabilityBar
                    homeTeam={simResult.home_team.name}
                    awayTeam={simResult.away_team.name}
                    homeWinProb={simResult.home_win_prob}
                    awayWinProb={simResult.away_win_prob}
                  />
                  <p className="mt-4 text-center text-slate-400">
                    Predicted Winner:{' '}
                    <span
                      className={`font-semibold ${
                        simResult.predicted_winner === 'home'
                          ? 'text-violet-300'   // matches home bar
                          : 'text-slate-300'    // matches away bar
                      }`}
                    >
                      {simResult.predicted_winner === 'home'
                        ? simResult.home_team.name
                        : simResult.away_team.name}
                    </span>
                  </p>

                </div>
              )}
            </div>
          </div>

          {/* Predictions Table */}
          <div className="card">
            <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4 mb-4">
              <div>
                <h2 className="text-lg font-semibold text-white">
                  Week {selectedWeek} Predictions
                </h2>
                {predictions && (
                  <p className="text-slate-400 text-sm">
                    {predictions.games_count} games · {(predictions.accuracy * 100).toFixed(1)}% accuracy
                  </p>
                )}
              </div>
              
              <div className="flex items-center gap-2">
                <label className="text-slate-400 text-sm">Week:</label>
                <select
                  value={selectedWeek || ''}
                  onChange={(e) => handleWeekChange(Number(e.target.value))}
                  className="bg-slate-800 border border-slate-600 rounded px-3 py-2 text-white text-sm focus:border-violet-500 focus:outline-none"
                >
                  {availableWeeks.map((week) => (
                    <option key={week} value={week}>
                      {week}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            {predictions && (
              <div className="overflow-x-auto">
                <table className="table-dark">
                  <thead>
                    <tr>
                      <th>Matchup</th>
                      <th>Prediction</th>
                      <th>Confidence</th>
                      <th>Result</th>
                    </tr>
                  </thead>
                  <tbody>
                    {predictions.predictions.map((game) => (
                      <tr key={game.game_id}>
                        <td className="text-slate-300">
                          {game.away_team.name} @ {game.home_team.name}
                        </td>
                        <td className="text-white">
                          {game.predicted_winner === 'home'
                            ? game.home_team.name
                            : game.away_team.name}
                        </td>
                        <td>
                          <span className={`font-medium ${
                            game.confidence >= 0.8 ? 'text-violet-400' :
                            game.confidence >= 0.6 ? 'text-slate-300' :
                            'text-slate-500'
                          }`}>
                            {(game.confidence * 100).toFixed(0)}%
                          </span>
                        </td>
                        <td>
                          {game.status === 'completed' ? (
                            <span className={game.was_correct ? 'text-emerald-400' : 'text-red-400'}>
                              {game.was_correct ? '✓ Correct' : '✗ Wrong'}
                            </span>
                          ) : (
                            <span className="text-slate-500">Upcoming</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

        </div>
      </div>
    </div>
  )
}

export default DashboardPage