// src/components/layout/Navbar.jsx

import { Link, useLocation } from 'react-router-dom'

function Navbar() {
  const location = useLocation()
  
  const isActive = (path) => location.pathname === path

  return (
    // Outer wrapper: adds padding around the floating navbar
    <div className="w-full px-6 pt-4 pb-2 relative z-50">
      {/* The floating pill container */}
      <nav className="mx-auto max-w-5xl rounded-2xl bg-white/[0.03] backdrop-blur-sm border border-white/[0.08] shadow-lg shadow-black/10">
        <div className="flex items-center justify-between px-6 py-3">
          
          {/* Logo - bolder, with subtle hover */}
          <Link 
            to="/" 
            className="text-lg font-semibold tracking-tight text-white hover:text-purple-300 transition-colors"
          >
            D3 Predictions
          </Link>
          
          {/* Nav links */}
          <div className="flex items-center gap-8">
            <NavLink to="/dashboard" active={isActive('/dashboard')}>
              Dashboard
            </NavLink>
            <NavLink to="/predictions" active={isActive('/predictions')}>
              Predictions
            </NavLink>
            <NavLink to="/simulate" active={isActive('/simulate')}>
              Simulator
            </NavLink>
            <NavLink to="/model-performance" active={isActive('/model-performance')}>
              Model
            </NavLink>
          </div>
        </div>
      </nav>
    </div>
  )
}

// Extracted link component for cleaner code
function NavLink({ to, active, children }) {
  return (
    <Link
      to={to}
      className="relative py-1 text-sm tracking-wide transition-colors duration-200 group"
    >
      {/* Text color: white if active, gray if not */}
      <span className={active ? 'text-white' : 'text-slate-400 group-hover:text-white'}>
        {children}
      </span>
      
      {/* Underline indicator - shows on active, animates on hover */}
      <span 
        className={`absolute -bottom-1 left-0 h-[2px] bg-purple-400 transition-all duration-200 ${
          active 
            ? 'w-full' 
            : 'w-0 group-hover:w-full'
        }`}
      />
    </Link>
  )
}

export default Navbar