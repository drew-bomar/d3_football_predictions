import { fetchAPI} from './client'

export async function getAccuracy(){
    return fetchAPI('/stats/accuracy')
}
export async function getCalibration(){
    return fetchAPI('/stats/calibration')
}
export async function getGamesByBucket(bucketMin, bucketMax){
    return fetchAPI(`/stats/games-by-bucket?bucket_min=${bucketMin}&bucket_max=${bucketMax}`)
}