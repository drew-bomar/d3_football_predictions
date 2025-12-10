import { useState, useRef, useEffect } from 'react'

function TeamSearchDropdown({ teams, selectedTeam, onSelect, label }){
    const[searchText, setSearchText] = useState('')
    const[isOpen, setIsOpen] = useState(false)
    const wrapperRef= useRef(null) 

    useEffect(() => {
        function handleClickOutside(event) {
            if (wrapperRef.current && !wrapperRef.current.contains(event.target)){
                setIsOpen(false)
            }
        }

        document.addEventListener('mousedown', handleClickOutside)
        return () => document.removeEventListener('mousedown',handleClickOutside)
    }, [])

    //filter teams based on search text, only show results if user has typed something
    const filteredTeams = searchText.length > 0
        ? teams.filter(team =>
            team.name.toLowerCase().startsWith(searchText.toLowerCase())
        )
        :[]
    
    const showDropdown = isOpen && filteredTeams.length > 0

    function handleSelect(team) {
        onSelect(team)              //tell parent which team is selected
        setSearchText('')           //clear the search
        setIsOpen(false)            //close dropdown
    }

    return(
        <div className = "relative" ref ={wrapperRef}>
            <label className="block text-sm text-gray-600 mb-1">
                {label}
            </label>

            {/*show selected team or the search input*/}
            {selectedTeam ?(
                <div className="border border-gray-300 rounded px-3 py-2 bg-white flex justify-between items-center">
                    <span> {selectedTeam.name}</span>
                    <button 
                        onClick={()=> onSelect(null)}
                        className="text-gray-400 hover:text-gray-600"
                    >
                        ✕
                    </button>
                </div>
            ):(
                <input
                    type="text"
                    value={searchText}
                    onChange={(e) => {
                        setSearchText(e.target.value)
                        setIsOpen(true) //open dropdown when typing
                    }}
                    placeholder="Type to search..."
                    className="border border-gray-300 rounded px-3 py-2 w-full"
                />
            )}

            {/*Dropdown List */}
            {showDropdown && (
                <ul className="absolute z-10 w-full bg-white border border-gray-300 rounded max-h-60 overflow-y-auto shadow-lg">
                    {filteredTeams.map(team => (
                        <li 
                            key = {team.id}
                            onClick={() => handleSelect(team)}
                            className="px-3 py-2 hover:bg-purple-100 cursor-pointer"
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