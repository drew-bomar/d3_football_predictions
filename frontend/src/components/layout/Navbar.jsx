import { Link } from 'react-router-dom'

function Navbar() {
  return (
    <nav className="bg-purple-700 text-white p-4">
      <div className="flex items-center justify-between max-w-6xl mx-auto">
        <Link to="/" className="text-xl font-bold">
          D3 Football Predictions
        </Link>
        <div className="flex gap-6">
          <Link to="/" className="hover:text-purple-200">
            Predictions
          </Link>
          <Link to="/simulate" className="hover:text-purple-200">
            Matchup Simulator
          </Link>
          <Link to="/model-performance" className="hover:text-purple-200">
            Model Performance
          </Link>
        </div>
      </div>
    </nav>
  )
}

export default Navbar