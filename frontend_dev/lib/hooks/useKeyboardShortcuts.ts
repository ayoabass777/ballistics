import { useEffect, useRef } from "react"

interface KeyboardShortcut {
  key: string
  ctrlKey?: boolean
  metaKey?: boolean
  shiftKey?: boolean
  handler: (e: KeyboardEvent) => void
  description?: string
}

/**
 * Custom hook for registering keyboard shortcuts
 * Helps improve power user experience
 */
export function useKeyboardShortcuts(shortcuts: KeyboardShortcut[], enabled: boolean = true) {
  const shortcutsRef = useRef(shortcuts)

  // Update ref when shortcuts change
  useEffect(() => {
    shortcutsRef.current = shortcuts
  }, [shortcuts])

  useEffect(() => {
    if (!enabled) return

    const handleKeyDown = (e: KeyboardEvent) => {
      for (const shortcut of shortcutsRef.current) {
        const keyMatches = e.key === shortcut.key
        const ctrlMatches = shortcut.ctrlKey === undefined || e.ctrlKey === shortcut.ctrlKey
        const metaMatches = shortcut.metaKey === undefined || e.metaKey === shortcut.metaKey
        const shiftMatches = shortcut.shiftKey === undefined || e.shiftKey === shortcut.shiftKey

        if (keyMatches && ctrlMatches && metaMatches && shiftMatches) {
          // Prevent default for all registered shortcuts
          e.preventDefault()
          shortcut.handler(e)
          break
        }
      }
    }

    window.addEventListener("keydown", handleKeyDown)
    return () => window.removeEventListener("keydown", handleKeyDown)
  }, [enabled])
}
