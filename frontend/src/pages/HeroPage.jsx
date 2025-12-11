// src/pages/HeroPage.jsx

import { useNavigate } from 'react-router-dom'
import TiltLogo from '../components/TiltLogo'

function HeroPage() {
  const navigate = useNavigate()

  return (
    //negative margin -m-16 to pull up the hero background behind the navbar
    <div className="relative min-h-screen w-full overflow-hidden -mt-20 pt-16">
      
      {/* Background decorative shapes - now extend behind navbar */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div className="absolute top-[-20%] right-[-5%] w-[800px] h-[800px] rounded-full bg-purple-600/20 blur-3xl"></div>
        <div className="absolute bottom-[-30%] left-[-10%] w-[600px] h-[600px] rounded-full bg-fuchsia-600/20 blur-3xl"></div>
        <div className="absolute top-[20%] right-[25%] w-[400px] h-[400px] rounded-full bg-violet-500/10 blur-2xl"></div>
        
        {/* Diagonal accent line */}
        <div className="absolute top-0 right-[35%] w-[2px] h-[150%] bg-gradient-to-b from-transparent via-purple-500/30 to-transparent rotate-[25deg]"></div>
      </div>

      {/* Content container - unchanged from here down */}
      <div className="relative z-10 max-w-7xl mx-auto px-6">
        
        {/* Main hero content - side by side */}
        <div className="flex items-center justify-between gap-12 py-16">
          
          {/* Left side - Text content */}
          <div className="flex-1 max-w-xl translate-x-4">
            <div className="max-w-2xl space-y-7">
            {/* Eyebrow */}
            <p className="text-xs font-semibold tracking-[0.28em] uppercase text-slate-400">
              NCAA Division III • Machine Learning
            </p>

            {/* H1 */}
            <h1 className="text-6xl md:text-7xl font-semibold leading-[1.05] text-white">
              <span className="block text-purple-400">D3 Football</span>
              <span className="block">Predictions</span>
            </h1>

            {/* Solid accent bar */}
            <div className="h-[3px] w-24 rounded-full bg-purple-500" />

            {/* Intro copy */}
            <p className="text-lg md:text-xl text-slate-300 max-w-xl">
              Machine learning predictions for NCAA Division III football games, powered by
              rolling team statistics and historical performance data.
            </p>
          </div>


            {/* Metric Chip */}
            <div className="inline-flex flex-col items-start px-5 py-4 rounded-xl bg-black/30 border border-white/10 backdrop-blur-sm mt-4 mb-4">
              <div className="flex items-baseline gap-2">
                <span className="text-4xl font-bold text-white">{79.95}%</span>
                <span className="text-sm font-semibold text-slate-300">
                  Prediction Accuracy
                </span>
              </div>
              <span className="text-xs text-slate-400 mt-1">
                Across {838}+ games
              </span>
            </div>


            <button 
              onClick={() => navigate('/dashboard')}
              className="mt-2 px-6 py-3 rounded-full bg-purple-600 hover:bg-purple-700 transition-all text-white font-medium shadow-lg shadow-purple-600/30 flex items-center gap-2"
              >
                Enter Dashboard
                <span className="text-lg">→</span>
              </button>
          </div>

          {/* Right side - Logo */}
          <div className="flex-shrink-0 relative translate-x-14">
            <div className="absolute inset-0 bg-purple-500/30 rounded-full blur-3xl scale-100"></div>
            <TiltLogo size={620} />
          </div>
        </div>

        {/* About the Model Section */}
        <div className="border-t border-purple-500/20 mt-12 pt-12 pb-16">
          <h2 className="text-2xl font-bold text-white mb-8 text-center">
            About the Model
          </h2>
          
          <div className="grid grid-cols-3 gap-6">
            <div className="card">
              <h3 className="text-lg font-semibold text-violet-400 mb-3">
                Predictive Features
              </h3>
              <p className="text-slate-400 leading-relaxed">
                Our logistic regression model uses rolling team statistics including 
                yards per play differential, turnover margin, scoring efficiency, 
                and historical performance metrics.
              </p>
            </div>

            <div className="card">
              <h3 className="text-lg font-semibold text-violet-400 mb-3">
                ELO Rating System
              </h3>
              <p className="text-slate-400 leading-relaxed">
                Teams are rated using an ELO-based system that adjusts after each game. 
                Wins against stronger opponents yield larger rating gains. Produced a ~12% increase in predictive accuracy
              </p>
            </div>

            <div className="card">
              <h3 className="text-lg font-semibold text-violet-400 mb-3">
                Coming Soon
              </h3>
              <p className="text-slate-400 leading-relaxed">
                We're expanding to include XGBoost and ensemble methods for model 
                comparison, with confidence intervals and head-to-head tracking.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}

export default HeroPage