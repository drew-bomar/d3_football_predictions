// src/components/TeamSearchDropdown.jsx

import { useState, useRef, useEffect } from 'react'

function TeamSearchDropdown({ teams, selectedTeam, onSelect, label }) {
    const [searchText, setSearchText] = useState('')
    const [isOpen, setIsOpen] = useState(false)
    const wrapperRef = useRef(null)

    useEffect(() => {
        function handleClickOutside(event) {
            if (wrapperRef.current && !wrapperRef.current.contains(event.target)) {
                setIsOpen(false)
            }
        }

        document.addEventListener('mousedown', handleClickOutside)
        return () => document.removeEventListener('mousedown', handleClickOutside)
    }, [])

    const filteredTeams = searchText.length > 0
        ? teams.filter(team =>
            team.name.toLowerCase().startsWith(searchText.toLowerCase())
          )
        : []

    const showDropdown = isOpen && filteredTeams.length > 0

    function handleSelect(team) {
        onSelect(team)
        setSearchText('')
        setIsOpen(false)
    }

    return (
        <div className="relative" ref={wrapperRef}>
            <label className="block text-sm text-slate-400 mb-1">
                {label}
            </label>

            {selectedTeam ? (
                <div className="bg-slate-800 border border-slate-600 rounded px-3 py-2 flex justify-between items-center">
                    <span className="text-white">{selectedTeam.name}</span>
                    <button
                        onClick={() => onSelect(null)}
                        className="text-slate-400 hover:text-white"
                    >
                        ✕
                    </button>
                </div>
            ) : (
                <input
                    type="text"
                    value={searchText}
                    onChange={(e) => {
                        setSearchText(e.target.value)
                        setIsOpen(true)
                    }}
                    placeholder="Type to search..."
                    className="w-full bg-slate-800 border border-slate-600 rounded px-3 py-2 text-white placeholder-slate-500 focus:border-violet-500 focus:outline-none"
                />
            )}

            {showDropdown && (
                <ul className="absolute z-10 w-full bg-slate-800 border border-slate-600 rounded mt-1 max-h-48 overflow-y-auto shadow-lg">
                    {filteredTeams.map(team => (
                        <li
                            key={team.id}
                            onClick={() => handleSelect(team)}
                            className="px-3 py-2 hover:bg-violet-600 cursor-pointer text-slate-300 hover:text-white"
                        >
                            {team.name}
                        </li>
                    ))}
                </ul>
            )}
        </div>
    )
}

export default TeamSearchDropdown