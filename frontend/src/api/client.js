//api/client.js

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL

export async function fetchAPI(endpoint){
  const url = `${API_BASE_URL}${endpoint}`

  const response = await fetch(url)

  if (!response.ok) {
    const error = new Error(`API error: ${response.status}`)
    error.status = response.status
    throw error
  }

  return response.json()
}
