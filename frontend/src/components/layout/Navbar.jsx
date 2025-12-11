// src/components/layout/Navbar.jsx

import { Link, useLocation } from 'react-router-dom'

function Navbar() {
  const location = useLocation()
  
  // Helper to check if link is active
  const isActive = (path) => location.pathname === path
  
  const linkClass = (path) => `transition-colors ${
    isActive(path) 
      ? 'text-white font-semibold' 
      : 'text-purple-200 hover:text-white'
  }`

  return (
    <nav className="bg-purple-700 text-white p-4">
      <div className="flex items-center justify-between max-w-7xl mx-auto">
        <Link to="/" className="text-xl font-bold">
          D3 Football Predictions
        </Link>
        <div className="flex gap-6">
          <Link to="/dashboard" className={linkClass('/dashboard')}>
            Dashboard
          </Link>
          <Link to="/predictions" className={linkClass('/predictions')}>
            Predictions
          </Link>
          <Link to="/simulate" className={linkClass('/simulate')}>
            Simulator
          </Link>
          <Link to="/model-performance" className={linkClass('/model-performance')}>
            Model
          </Link>
        </div>
      </div>
    </nav>
  )
}

export default Navbar