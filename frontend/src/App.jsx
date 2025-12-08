import { BrowserRouter, Routes, Route} from 'react-router-dom'
import Navbar from './components/layout/Navbar'
import PredictionsPage from './pages/PredictionsPage'
import ModelPerformancePage from './pages/ModelPerformancePage'

function App() {
  return (
    <BrowserRouter>
    <div className="min-h-screen bg-gray-100">
      <Navbar />
      <main className="max-w-6xl mx-auto p-6">
        <Routes>
          <Route path = "/" element = {<PredictionsPage />} />
          <Route path = "/model-performance" element = {<ModelPerformancePage />} />
        </Routes>
      </main> 
    </div>
    </BrowserRouter>
  )
}

export default App
