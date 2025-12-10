import { fetchAPI } from './client'

export async function getMeta(){
    return fetchAPI('/meta')
}

/**
 * Simulates a matchup between two teams.
 * @param {number} homeTeamId - The home team's ID
 * @param {number} awayTeamId - The away team's ID
 * Returns: { home_team, away_team, predicted_winner, home_win_prob, away_win_prob, ... }
 */
export async function simulateMatchup(homeTeamId, awayTeamId) {
    return fetchAPI(`/simulate?home_team_id=${homeTeamId}&away_team_id=${awayTeamId}`)
}