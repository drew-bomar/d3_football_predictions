import { useState, useEffect} from 'react'
import { getAccuracy, getCalibration, getGamesByBucket } from '../api/stats'

import { 
  BarChart, 
  Bar, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  Legend, 
  ResponsiveContainer 
} from 'recharts'

function ModelPerformancePage(){
    //data states
    const[accuracy, setAccuracy] = useState(null)
    const[calibration, setCalibration] = useState(null)
    const[loading, setLoading] = useState(true)
    const[error, setError] = useState(null)

    //bucket data states
    const [selectedBucket, setSelectedBucket] = useState(null)
    const [bucketGames, setBucketGames] = useState(null)
    const [bucketLoading, setBucketLoading] = useState(false)

    //fetch data on page load
    useEffect(() => {
        async function loadData(){
            try{
                setLoading(true)
                const [accuracyData, calibrationData] = await Promise.all([
                    getAccuracy(),
                    getCalibration()
                    ])
                setAccuracy(accuracyData)
                setCalibration(calibrationData)
                setError(null)
            } catch(err){
                setError(err.message)
            }finally {
                setLoading(false)
            }
        }
        loadData()
    }, [])

    async function handleBarClick(data) {
        if (!data || !data.payload) return

        const bucket = data.payload
        setSelectedBucket(bucket)
        setBucketLoading(true)

        try {
            const games = await getGamesByBucket(bucket.bucket_min, bucket.bucket_max)
            setBucketGames(games)
        } catch (err) {
            console.error('Failed to fetch bucket games:', err)
            setBucketGames(null)
        } finally {
            setBucketLoading(false)
        }
    }


    if(loading){
        return <div className="text-gray-600"> Loading model stats...</div>
    }

    if(error){
        return <div className="text-red-600"> Error: {error}</div>
    }

    return(
        <div>
            <h1 className="text-2xl font-bold text-gray-600">
                Model Performance
            </h1>

            {/*Summary Cards */}
            <div className="mt-6 grid grid-cols-3 gap-4">
                {/*Overall Accuracy Card*/}
                <div className = "bg-white rounded-lg shadow p-4">
                    <p className="text-sm text-gray-500">Overall Accuracy</p>
                    <p className="text-3xl font-bold text-purple-600">
                        {(accuracy.overall.accuracy * 100).toFixed(1)}%
                    </p>
                    <p className= "text-m text-gray-500 mt-1">
                        {accuracy.overall.correct}/{accuracy.overall.games} games
                    </p>
                </div>

                {/*Current Season Card*/}
                <div className = "bg-white rounded-lg shadow p-4">
                    <p className = "text-sm text-gray-500"> {accuracy.current_season} Season</p>
                    <p className = "text-3xl font-bold text-purple-600">
                        {(accuracy.by_season[0].accuracy * 100).toFixed(1)}%
                    </p>
                    <p className="text-sm text-gray-500 mt-1">
                        {accuracy.by_season[0].correct}/{accuracy.by_season[0].games} games
                    </p>
                </div>

                {/*Calibration Card*/}
                <div className= "bg-white rounded-lg shadow p-4">
                    <p className="text-sm text-gray-500">Confidence Buckets</p>
                    <p className="text-3xl font-bold text-purple-600">
                        {calibration.buckets.length}
                    </p>
                    <p className="text-sm text-gray-500 mt-1">
                        Click chart bars to explore
                    </p>
                </div>
            </div>

            {/*Calibration Chart*/}
            <div className="mt-8 bg-white rounded-lg shadow p-4">
                <h2 className="text-lg font-semibold text-gray-800 mb-4">
                    Calibration: Predicted vs Actual Accuracy
                </h2>
                <ResponsiveContainer width="100%" height={300}>
                    <BarChart data={calibration.buckets}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="label"/>
                    <YAxis 
                        domain={[0, 1]} 
                        tickFormatter={(value) => `${(value * 100).toFixed(0)}%`}
                    />
                    {/* <Tooltip 
                        formatter={(value) => `${(value * 100).toFixed(1)}%`}
                    /> */}
                    <Legend 
                        payload={[
                            { value: 'Actual', type: 'square', color: '#10b981' },
                            { value: 'Predicted', type: 'square', color: '#8b5cf6' }
                        ]}
                        />
                    <Bar 
                        dataKey="mean_predicted" 
                        name="Predicted" 
                        fill="#8b5cf6" 
                        cursor = "pointer"
                        onClick={(data) => handleBarClick(data)}
                    />
                    <Bar 
                        dataKey="actual_accuracy" 
                        name="Actual" 
                        fill="#10b981" 
                        cursor = "pointer"
                        onClick={(data) => handleBarClick(data)}
                    />
                    </BarChart>
                </ResponsiveContainer>
            </div>

            {/* Bucket Games Table */}
            {selectedBucket && (
            <div className="mt-8 bg-white rounded-lg shadow p-4">
                <h2 className="text-lg font-semibold text-gray-800 mb-2">
                    Games in {selectedBucket.label}% Confidence Bucket
                </h2>
                <p className="text-gray-600 mb-4">
                    Predicted: {(selectedBucket.mean_predicted * 100).toFixed(1)}% | 
                    Actual: {(selectedBucket.actual_accuracy * 100).toFixed(1)}% | 
                    {selectedBucket.games} games
                </p>
                
                {bucketLoading && (
                <p className="text-gray-600">Loading games...</p>
                )}
                
                {bucketGames && !bucketLoading && (
                <table className="w-full border-collapse">
                    <thead>
                    <tr className="border-b-2 border-gray-300">
                        <th className="text-left p-2">Matchup</th>
                        <th className="text-left p-2">Predicted Winner</th>
                        <th className="text-left p-2">Confidence</th>
                        <th className="text-left p-2">Result</th>
                    </tr>
                    </thead>
                    <tbody>
                    {bucketGames.games.map((game) => (
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
                            <span className={game.was_correct ? 'text-green-600' : 'text-red-600'}>
                            {game.was_correct ? '✓ Correct' : '✗ Wrong'}
                            </span>
                        </td>
                        </tr>
                    ))}
                    </tbody>
                </table>
                )}
            </div>
            )}
        </div>
    )
}

export default ModelPerformancePage