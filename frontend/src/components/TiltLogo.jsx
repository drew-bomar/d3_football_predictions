// src/components/TiltLogo.jsx

import { useState, useRef } from 'react'

function TiltLogo({ size = 620 }) {
  const [rotation, setRotation] = useState({ x: 0, y: 0 })
  const containerRef = useRef(null)

  function handleMouseMove(e) {
    if (!containerRef.current) return
    
    const rect = containerRef.current.getBoundingClientRect()
    const xRelative = (e.clientX - rect.left) / rect.width - 0.5
    const yRelative = (e.clientY - rect.top) / rect.height - 0.5
    
    const maxTilt = 50
    setRotation({
      x: -yRelative * maxTilt,
      y: xRelative * maxTilt
    })
  }

  function handleMouseLeave() {
    setRotation({ x: 0, y: 0 })
  }

  return (
    <div
      ref={containerRef}
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
      className="cursor-pointer rounded-full"  // Added rounded-full
      style={{
        width: size,
        height: size,
        perspective: '1000px',
      }}
    >
      <img
        src="/src/assets/d3-logo.png"
        alt="NCAA D3 Logo"
        className="w-full h-full object-contain transition-transform duration-200 ease-out"
        style={{
          transform: `rotateX(${rotation.x}deg) rotateY(${rotation.y}deg)`,
        }}
      />
    </div>
  )
}

export default TiltLogo