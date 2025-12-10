import { useState, useEffect } from 'react'
import { getMeta, simulateMatchup } from '../api/simulate'
import TeamSearchDropdown from '../components/TeamSearchDropdown'
import ProbabilityBar from '../components/ProbabilityBar'

function SimulatePage(){
    // Teams data from API
    const [teams, setTeams] = useState([])
    const [teamsLoading, setTeamsLoading] = useState(true)

    // Selected teams
    const [homeTeam, setHomeTeam] = useState(null)
    const [awayTeam, setAwayTeam] = useState(null)

    // Simulation results
    const [result, setResult] = useState(null)
    const [simLoading, setSimLoading] = useState(false)
    const [error, setError] = useState(null)

    // Fetch all teams on page load
    useEffect(() => {
        async function loadTeams() {
            try {
                const data = await getMeta()
                setTeams(data.teams)
            } catch (err) {
                setError('Failed to load teams')
            } finally {
                setTeamsLoading(false)
            }
        }
        loadTeams()
    }, [])

    //fecth simulateMatchup data from api/simulate if teams are selected
    async function runSimulation() {
        if (!homeTeam || !awayTeam) return
        
        setSimLoading(true)
        setError(null)
        setResult(null)
        
        try {
            const data = await simulateMatchup(homeTeam.id, awayTeam.id)
            setResult(data)
        } catch (err) {
            setError('Simulation failed. Please try again.')
        } finally {
            setSimLoading(false)
        }
    }

    // Can only simulate if both teams are selected (and they're different)
    const canSimulate = homeTeam && awayTeam && homeTeam.id !== awayTeam.id

    if (teamsLoading) {
        return <div className="text-gray-600">Loading teams...</div>
    }

    return(
            <div>
                <h1 className="text-2xl font-bold text-gray-800">
                    Matchup Simulator
                </h1>
                <p className="text-gray-600 mt-1">
                    Select any two teams to see the predicted outcome
                </p>
                
                {/* Team Selection */}
                <div className="mt-6 bg-white rounded-lg shadow p-6">
                    <div className="grid grid-cols-2 gap-8">
                        {/* Home Team */}
                        <div>
                            <TeamSearchDropdown
                                teams={teams}
                                selectedTeam={homeTeam}
                                onSelect={setHomeTeam}
                                label="Home Team"
                            />
                        </div>
                        
                        {/* Away Team */}
                        <div>
                            <TeamSearchDropdown
                                teams={teams}
                                selectedTeam={awayTeam}
                                onSelect={setAwayTeam}
                                label="Away Team"
                            />
                        </div>
                    </div>

                    {/* Simulate Button */}
                    <div className="mt-6 text-center">
                        <button
                            onClick={runSimulation}
                            disabled={!canSimulate || simLoading}
                            className={`px-8 py-3 rounded font-semibold ${
                                canSimulate && !simLoading
                                    ? 'bg-purple-600 text-white hover:bg-purple-700'
                                    : 'bg-gray-300 text-gray-500 cursor-not-allowed'
                            }`}
                        >
                           {simLoading ? 'Simulating...' : 'Simulate Matchup'}
                        </button>
                    </div> 

                    {/* Same team warning */}
                    {homeTeam && awayTeam && homeTeam.id === awayTeam.id && (
                        <p className="mt-2 text-center text-red-500 text-sm">
                            Please select two different teams
                        </p>
                    )}
                </div>

                {/* Error State */}
                {error && (
                    <div className="mt-6 bg-red-50 border border-red-200 rounded-lg p-4 text-red-700">
                        {error}
                    </div>
                )}
                {/* Results */}
                {result && (
                    <div className="mt-6 bg-white rounded-lg shadow p-6">
                        <h2 className="text-lg font-semibold text-gray-800 mb-4">
                            Prediction Result
                        </h2>
                        
                        <ProbabilityBar
                            homeTeam={result.home_team.name}
                            awayTeam={result.away_team.name}
                            homeWinProb={result.home_win_prob}
                            awayWinProb={result.away_win_prob}
                        />
                        
                        {/* Winner callout */}
                        <div className="mt-6 text-center">
                            <p className="text-gray-600">Predicted Winner</p>
                            <p className={`text-2xl font-bold ${
                                result.predicted_winner === 'home' 
                                    ? 'text-blue-600' 
                                    : 'text-red-600'
                            }`}>
                                {result.predicted_winner === 'home' 
                                    ? result.home_team.name 
                                    : result.away_team.name}
                            </p>
                            <p className="text-gray-500 mt-1">
                                Confidence: {(result.confidence * 100).toFixed(1)}%
                            </p>
                        </div>
                    </div>
                )}
            </div>
        )
    }

export default SimulatePage