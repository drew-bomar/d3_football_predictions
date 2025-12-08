import { useState, useEffect} from 'react'
import { getPredictions } from '../api/predictions'

function PredictionsPage(){
    //possible states for the page
    const [predictions, setPredictions] = useState(null) //actual data
    const [loading, setLoading] = useState(true) // currently loading the page
    const [error, setError] = useState(null) //encountered an error

    useEffect(() => {
    async function loadPredictions() {
      try {
        setLoading(true)
        const data = await getPredictions(2025, 11)
        setPredictions(data)
        setError(null)
      } catch (err) {
        setError(err.message)
        setPredictions(null)
      } finally {
        setLoading(false)
      }
    }

    loadPredictions()
  }, [])

  if (loading){
    return <div className = "text-gray-600"> Loading Predictions ...</div>
  }

  if (error){
    return <div className = "text-red-600"> Error: {error}</div>
  }

  //show the data 
  return(
    <div>
        <h1 className = "text-2xl font-bold text-gray-800">
            Predictions Explorer
        </h1>

        <p className = "mt-2 text-gray-600">
            Week {predictions.week}, {predictions.year} - {predictions.games_count} games, {' '}
            {(predictions.accuracy * 100).toFixed(1)}% accuracy
        </p>

        {/*Basic Table */}
        <table className="mt-6 w-full border-collapse">
            <thead>
                <tr className="border-b-2 border-gray-300">
                    <th className = "text-left p-2"> Matchup</th>
                    <th className = "text-left p-2"> Prediction</th>
                    <th className = "text-left p-2"> Confidence</th>
                    <th className = "text-left p-2"> Result</th>
                </tr>
            </thead>

            <tbody>
                {predictions.predictions.map((game) => (
                    <tr key={game.game_id} className = "border-b border-gray-200">
                        <td className = "p-2">
                            {game.away_team.name} @ {game.home_team.name}
                        </td>
                        <td>
                            {game.predicted_winner ==='home'
                                ? game.home_team.name
                                :game.away_team.name}        
                        </td>
                        <td className = "p-2">
                            {(game.confidence *100).toFixed(1)}%
                        </td>
                        <td className = "p-2">
                            {game.status ==='completed'
                                ?(
                                    <span className={game.was_correct 
                                                        ? 'text-green-600'
                                                        : 'text-red-600'}>
                                        {game.was_correct 
                                        ? '✓ Correct'
                                        : '✗ Wrong'} 
                                    </span>
                                ) 
                                : (
                                    <span className= "text-gray-400"> Upcoming </span>
                                )}
                        </td>
                    </tr>
                ))}
            </tbody>
        </table>
    </div>
  )
}

export default PredictionsPage