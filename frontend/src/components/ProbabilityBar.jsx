import { useState, useEffect } from 'react'

function ProbabilityBar({ homeTeam, awayTeam, homeWinProb }) {
  const [displayedHomeProb, setDisplayedHomeProb] = useState(50)

  useEffect(() => {
    const timer = setTimeout(() => {
      setDisplayedHomeProb(homeWinProb * 100)
    }, 200)
    return () => clearTimeout(timer)
  }, [homeWinProb])

  const displayedAwayProb = 100 - displayedHomeProb

  return (
    <div className="w-full">
      
      {/* Team labels */}
      <div className="flex justify-between mb-2 text-xs sm:text-sm">
        <div>
          <span className="font-semibold text-violet-300">{homeTeam}</span>
          <span className="ml-2 text-slate-400 tabular-nums">
            {displayedHomeProb.toFixed(1)}%
          </span>
        </div>
        <div className="text-right">
          <span className="text-slate-400 tabular-nums">
            {displayedAwayProb.toFixed(1)}%
          </span>
          <span className="ml-2 font-semibold text-fuchsia-300">{awayTeam}</span>
        </div>
      </div>

      {/* Bar container */}
      <div className="h-3.5 w-full flex rounded-full overflow-hidden bg-white/5">
        
        {/* Home bar */}
        <div
          className="h-full transition-[width] duration-700 ease-out
                     bg-gradient-to-r from-violet-600 to-violet-400"
          style={{ width: `${displayedHomeProb}%` }}
        />

        {/* Away bar */}
        <div
          className="h-full transition-[width] duration-700 ease-out
                     bg-gradient-to-r from-slate-600 to-slate-400/70"
          style={{ width: `${displayedAwayProb}%` }}
        />
      </div>

      {/* Bottom labels */}
      <div className="flex justify-between mt-1 text-[10px] uppercase tracking-[0.15em] text-slate-500">
        <span>Home</span>
        <span>Away</span>
      </div>
    </div>
  )
}

export default ProbabilityBar
