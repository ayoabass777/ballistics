import { z } from 'zod';
export const preferencesSchema = z.object({ data: z.object({
    timezone_override: z.string().refine(zone => { try { new Intl.DateTimeFormat('en', {timeZone: zone}); return true; } catch { return false; } }).nullable(),
    usage_events_enabled: z.boolean().optional(),
    created_at: z.string().nullable(), updated_at: z.string().nullable(),
}) });
export function dayInZone(zone: string, offset = 0, now = new Date()) {
    const parts = new Intl.DateTimeFormat('en', { timeZone: zone, year: 'numeric', month: '2-digit', day: '2-digit' }).formatToParts(now);
    const part = (type: string) => parts.find(p => p.type === type)!.value;
    const day = new Date(`${part('year')}-${part('month')}-${part('day')}T12:00:00Z`);
    day.setUTCDate(day.getUTCDate() + offset);
    return day.toISOString().slice(0, 10);
}
