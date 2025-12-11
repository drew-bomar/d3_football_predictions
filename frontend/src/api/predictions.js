//src/api/predictions.js

import { fetchAPI } from './client'

export async function getPredictions(year, week){
    return fetchAPI(`/predictions/${year}/${week}`)
}