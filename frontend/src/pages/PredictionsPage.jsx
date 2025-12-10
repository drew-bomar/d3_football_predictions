//src/pages/PredictionsPage.jsx
import { useState, useEffect} from 'react'
import { getPredictions } from '../api/predictions'

//hardcode the available options until we get /api/meta/seasons running
const AVAILABLE_YEARS = [2025]
const AVAILABLE_WEEKS = [5,6,7,8,9,10,11,12,13] //complete predictions for more weeks to add more weeks

function PredictionsPage(){
    //filter states
    const [selectedYear, setSelectedYear] = useState(2025)
    const [ selectedWeek, setSelectedWeek] = useState(11)

    //data states
    const [predictions, setPredictions] = useState(null) //actual data
    const [loading, setLoading] = useState(true) // currently loading the page
    const [error, setError] = useState(null) //encountered an error

    
    async function loadPredictions() {
      try {
        setLoading(true)
        const data = await getPredictions(selectedYear, selectedWeek)
        setPredictions(data)
        setError(null)
      } catch (err) {
        setError(err.message)
        setPredictions(null)
      } finally {
        setLoading(false)
      }
    }

    useEffect(() => {
        loadPredictions()
    }, [])

  //show the data 
  return(
    <div>
        <h1 className = "text-2xl font-bold text-gray-800">
            Predictions Explorer
        </h1>

        {/*Filter Bar*/}
        <div className="mt-4 flex gap-4">
            {/*Year Select*/}
            <div>
                <label className = "block text-sm text-gray-600 mb-1">Season</label>
                <select
                    value={selectedYear}
                    onChange={(e) => setSelectedYear(Number(e.target.value))}
                    className = "border border-gray-300 rounder px-3 py-2 bg-white"
                    >
                    {AVAILABLE_YEARS.map((year) => (
                        <option key={year} value={year}>
                            {year}
                        </option>
                    ))} 
                </select>
            </div>
            
        {/*Week Select */}
        <div>
            <label className = "block text-sm text-gray-600 mb-1">Week</label>
            <select
                value={selectedWeek}
                onChange={(e) => setSelectedWeek(Number(e.target.value))}
                className = "border border-gray-300 grounded px-3 py-2 bg-white"
            >
                {AVAILABLE_WEEKS.map((week) => (
                   <option key={week} value={week}>
                        Week {week}
                   </option> 
                ))}
                </select>
            </div>

        {/*Find Button*/}
        <div className = "flex items-end">
            <button
                onClick={loadPredictions}
                className= "bg-purple-600 text-white px-6 py-2 rounded hover:bg-purple-700"
            >
                FIND
            </button>
        </div>
     </div>

    {/*Loading State*/}
    {loading && ( 
        <div className ="mt-6 text-gray-600"> Loading predictions...</div>
    )}
    {/*Error State*/}
    {error && ( 
        <div className ="mt-6 text-gray-600"> Error: {error}</div>
    )}

   {/* Data Table */}
      {predictions && !loading && (
        <>
          <p className="mt-6 text-gray-600">
            {predictions.games_count} games, {' '}
            {(predictions.accuracy * 100).toFixed(1)}% accuracy
          </p>

          <table className="mt-4 w-full border-collapse">
            <thead>
              <tr className="border-b-2 border-gray-300">
                <th className="text-left p-2">Matchup</th>
                <th className="text-left p-2">Prediction</th>
                <th className="text-left p-2">Confidence</th>
                <th className="text-left p-2">Result</th>
              </tr>
            </thead>
            <tbody>
              {predictions.predictions.map((game) => (
                <tr key={game.game_id} className="border-b border-gray-200">
                  <td className="p-2">
                    {game.away_team.name} @ {game.home_team.name}
                  </td>
                  <td className="p-2">
                    {game.predicted_winner === 'home'
                      ? game.home_team.name
                      : game.away_team.name}
                  </td>
                  <td className="p-2">
                    {(game.confidence * 100).toFixed(0)}%
                  </td>
                  <td className="p-2">
                    {game.status === 'completed' ? (
                      <span className={game.was_correct ? 'text-green-600' : 'text-red-600'}>
                        {game.was_correct ? '✓ Correct' : '✗ Wrong'}
                      </span>
                    ) : (
                      <span className="text-gray-400">Upcoming</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  )
}

export default PredictionsPage