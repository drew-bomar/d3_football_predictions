import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Navbar from './components/layout/Navbar'
import PredictionsPage from './pages/PredictionsPage'
import ModelPerformancePage from './pages/ModelPerformancePage'
import SimulatePage from './pages/SimulatePage'

function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-gray-100">
        <Navbar />
        <main className="max-w-6xl mx-auto p-6">
          <Routes>
            <Route path="/" element={<PredictionsPage />} />
            <Route path="/model-performance" element={<ModelPerformancePage />} />
            <Route path="/simulate" element={<SimulatePage />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}

export default App