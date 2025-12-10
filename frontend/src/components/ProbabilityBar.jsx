import { useState, useEffect } from 'react'

function ProbabilityBar({ homeTeam, awayTeam, homeWinProb}){
    //start at 50% for each
    const[displayedHomeProb, setDisplayedHomeProb] = useState(50)

    useEffect(() => {
        //small delay before animation begins
        const timer = setTimeout(() => {
            setDisplayedHomeProb(homeWinProb *100)
        },300 )

        return () => clearTimeout(timer)
    }, [homeWinProb])

    const displayedAwayProb = 100- displayedHomeProb

    return(
        <div className="w-full">
            {/* Team names and percentages */}
            <div className="flex justify-between mb-2">
                <div className="text-left">
                    <span className="font-semibold text-blue-600">{homeTeam}</span>
                    <span className="ml-2 text-gray-600">
                        {displayedHomeProb.toFixed(1)}%
                    </span>
                </div>
                <div className="text-right">
                    <span className="text-gray-600">
                        {displayedAwayProb.toFixed(1)}%
                    </span>
                    <span className="ml-2 font-semibold text-red-600">{awayTeam}</span>
                </div>
            </div>

            {/* the actual bar*/}
            <div className="h-8 w-full flex rounded overflow-hidden">
                {/* Home team (blue) - left side */}
                <div 
                    className="bg-blue-500 transition-all duration-1400 ease-out"
                    style={{ width: `${displayedHomeProb}%` }}
                />
                {/* Away team (red) - right side */}
                <div 
                    className="bg-red-500 transition-all duration-1400 ease-out"
                    style={{ width: `${displayedAwayProb}%` }}
                />
            </div>

            {/* Labels below bar */}
            <div className="flex justify-between mt-1 text-sm text-gray-500">
                <span>Home</span>
                <span>Away</span>
            </div>
        </div>
    )
}

export default ProbabilityBar