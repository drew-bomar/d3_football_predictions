// src/App.jsx

import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Navbar from './components/layout/Navbar'
import HeroPage from './pages/HeroPage'
import DashboardPage from './pages/DashboardPage'
import PredictionsPage from './pages/PredictionsPage'
import ModelPerformancePage from './pages/ModelPerformancePage'
import SimulatePage from './pages/SimulatePage'

function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen">
        <Navbar />
        <Routes>
          <Route path="/" element={<HeroPage />} />
          <Route path="/dashboard" element={<DashboardPage />} />
          <Route path="/predictions" element={<PredictionsPage />} />
          <Route path="/model-performance" element={<ModelPerformancePage />} />
          <Route path="/simulate" element={<SimulatePage />} />
        </Routes>
      </div>
    </BrowserRouter>
  )
}

export default App