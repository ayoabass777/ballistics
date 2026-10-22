"use client"

interface SectionHeaderProps {
  title: string
  count: number
  itemName?: string
}

export function SectionHeader({ title, count, itemName = "streak" }: SectionHeaderProps) {
  return (
    <div className="text-center sm:text-left animate-fade-in">
      <h2 className="text-2xl sm:text-3xl font-bold text-slate-900 dark:text-white mb-2 transition-all duration-300">
        {title}
      </h2>
      <p className="text-sm text-slate-600 dark:text-slate-400 transition-all duration-300">
        <span className="inline-block transition-transform duration-200 hover:scale-110">{count}</span> active {itemName}
        {count !== 1 ? "s" : ""} tracked
      </p>
    </div>
  )
}
