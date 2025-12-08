const API_BASE_URL = import.meta.env.VITE_API_BASE_URL

export async function fetchAPI(endpoint){
    const url = `${API_BASE_URL}${endpoint}`

    const response = await fetch(url)

    if(!response.ok){
        throw new Error(`API error: ${response.status}`)
    }

    return response.json()
}