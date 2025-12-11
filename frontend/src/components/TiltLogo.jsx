// src/components/TiltLogo.jsx

import { useState, useRef } from 'react'

function TiltLogo({ size = 480, hitAreaScale = 0.75 }) {
  const [rotation, setRotation] = useState({ x: 0, y: 0 })
  const containerRef = useRef(null)

  function handleMouseMove(e) {
    if (!containerRef.current) return
    
    const rect = containerRef.current.getBoundingClientRect()
    const xRelative = (e.clientX - rect.left) / rect.width - 0.5
    const yRelative = (e.clientY - rect.top) / rect.height - 0.5
    
    const maxTilt = 45
    setRotation({
      x: -yRelative * maxTilt,
      y: xRelative * maxTilt
    })
  }

  function handleMouseLeave() {
    setRotation({ x: 0, y: 0 })
  }

  // Calculate the hit area size and offset
  const hitAreaSize = size * hitAreaScale
  const hitAreaOffset = (size - hitAreaSize) / 2

  return (
    // Outer wrapper maintains the visual size
    <div
      style={{
        width: size,
        height: size,
        perspective: '1000px',
        position: 'relative',
      }}
    >
      {/* The image - full size, transforms applied here */}
      <img
        src="/src/assets/d3-logo.png"
        alt="NCAA D3 Logo"
        className="w-full h-full object-contain transition-transform duration-200 ease-out pointer-events-none"
        style={{
          transform: `rotateX(${rotation.x}deg) rotateY(${rotation.y}deg)`,
        }}
      />
      
      {/* Invisible hit area - smaller circle centered over the logo */}
      <div
        ref={containerRef}
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
        className="absolute rounded-full cursor-pointer"
        style={{
          width: hitAreaSize,
          height: hitAreaSize,
          top: hitAreaOffset,
          left: hitAreaOffset,
          // Uncomment this to visualize the hit area while debugging:
          //backgroundColor: 'rgba(255, 0, 0, 0.2)',
        }}
      />
    </div>
  )
}

export default TiltLogo