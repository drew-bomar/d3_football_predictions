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
          <div className="flex-1 max-w-xl">
            <h1 className="text-6xl font-extrabold leading-tight mb-6">
              <span className="text-gradient">D3 Football</span>
              <br />
              <span className="text-white">Predictions</span>
            </h1>
            
            <p className="text-xl text-slate-400 mb-8 leading-relaxed">
              Machine learning predictions for NCAA Division III football games. 
              Powered by rolling team statistics and historical performance data.
            </p>

            {/* Accuracy stat */}
            <div className="flex items-baseline gap-3 mb-8">
              <span className="text-5xl font-extrabold text-violet-400">79.9%</span>
              <div className="flex flex-col">
                <span className="text-white font-semibold">Prediction Accuracy</span>
                <span className="text-slate-500 text-sm">Across 800+ games</span>
              </div>
            </div>

            <button 
              onClick={() => navigate('/dashboard')}
              className="btn-primary text-lg px-8 py-4"
            >
              Enter Dashboard →
            </button>
          </div>

          {/* Right side - Logo */}
          <div className="flex-shrink-0 relative">
            <div className="absolute inset-0 bg-purple-500/30 rounded-full blur-3xl scale-150"></div>
            <TiltLogo size={650} />
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
                Wins against stronger opponents yield larger rating gains.
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