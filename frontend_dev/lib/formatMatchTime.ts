import {
  differenceInHours,
  differenceInDays,
  isToday,
  isTomorrow,
  isPast,
  format,
} from "date-fns"

export interface MatchTimeDisplay {
  label: string
  isUrgent: boolean
  isPast: boolean
}

export function formatMatchTime(dateString: string): MatchTimeDisplay {
  const matchDate = new Date(dateString)
  const now = new Date()

  if (Number.isNaN(matchDate.getTime())) {
    return { label: "TBD", isUrgent: false, isPast: false }
  }

  if (isPast(matchDate)) {
    return { label: "Played", isUrgent: false, isPast: true }
  }

  const hoursUntil = differenceInHours(matchDate, now)
  const daysUntil = differenceInDays(matchDate, now)
  const timeFormat = format(matchDate, "h:mm a")

  if (hoursUntil < 2 && hoursUntil >= 0) {
    return { label: "Starting soon", isUrgent: true, isPast: false }
  }

  if (isToday(matchDate)) {
    return { label: `Today, ${timeFormat}`, isUrgent: true, isPast: false }
  }

  if (isTomorrow(matchDate)) {
    return { label: `Tomorrow, ${timeFormat}`, isUrgent: false, isPast: false }
  }

  if (daysUntil <= 6) {
    const dayName = format(matchDate, "EEEE")
    return { label: `${dayName}`, isUrgent: false, isPast: false }
  }

  return { label: format(matchDate, "MMM d"), isUrgent: false, isPast: false }
}
