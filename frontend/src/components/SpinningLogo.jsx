// src/components/SpinningLogo.jsx

import './SpinningLogo.css'

function SpinningLogo({ size = 480 }) {
  return (
    <div 
      className="logo-container"
      style={{ width: size, height: size }}
    >
      <div className="logo-flipper">
        <img 
          src="/src/assets/d3-logo.png" 
          alt="NCAA D3 Logo"
          className="logo-face logo-front"
        />
        <img 
          src="/src/assets/d3-logo.png" 
          alt="NCAA D3 Logo"
          className="logo-face logo-back"
        />
      </div>
    </div>
  )
}

export default SpinningLogo