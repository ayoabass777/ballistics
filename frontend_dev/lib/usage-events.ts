import { z } from 'zod';

// Beta usage events (D164). The account API holds the allow-list; these schemas only bound the proxy.
export const usageEventBatchSchema = z.object({ events: z.array(z.object({
    event_id: z.string().uuid(), session_id: z.string().uuid(), event_name: z.string().min(1).max(40),
    props: z.record(z.union([z.string().max(100), z.number(), z.boolean()])), occurred_at: z.string().max(40),
}).strict()).min(1).max(20) }).strict();
export const usageEventsResponseSchema = z.object({ data: z.object({ accepted: z.number().int().min(0), enabled: z.boolean() }) });
export const usageEventsSettingSchema = z.object({ enabled: z.boolean() }).strict();
export const usageEventsSettingResponseSchema = z.object({ data: z.object({ usage_events_enabled: z.boolean() }) });

type Props = Record<string, string | number | boolean>;
type Event = { event_id: string; session_id: string; event_name: string; props: Props; occurred_at: string };

const SESSION_KEY = 'ballistics:usage:session';
const IDLE_MS = 30 * 60 * 1000;
// null until the member's preference loads; nothing is sent before then.
let enabled: boolean | null = null;
let queue: Event[] = [];
let timer: ReturnType<typeof setTimeout> | null = null;
let fallbackSession: { id: string; at: number } | null = null;

export function setUsageEventsEnabled(value: boolean | null) {
    enabled = value;
    if (value === false) queue = [];
    if (value) flush();
}

function routeTemplate(path: string) {
    return path.toLowerCase()
        .replace(/^\/(match|fixture)\/[^/]+/, '/$1/[id]')
        .replace(/^\/(team|league)\/[^/]+/, '/$1/[slug]')
        .replace(/[^a-z0-9\-/[\]]/g, '').slice(0, 100) || '/';
}

// Same-origin API path with ids collapsed, so no query string or record id is sent.
export function apiTemplate(url: string) {
    let path = url;
    try { path = new URL(url, window.location.origin).pathname; } catch { /* Keep the raw path. */ }
    return path.toLowerCase().split('/').map(part => /^\d+$|^[0-9a-f-]{32,36}$/.test(part) ? '[id]' : part).join('/')
        .replace(/[^a-z0-9\-/[\]]/g, '').slice(0, 94);
}

function session(now: number) {
    let current = fallbackSession;
    try { const raw = sessionStorage.getItem(SESSION_KEY); if (raw) current = JSON.parse(raw); } catch { /* Keep the in-memory session. */ }
    const fresh = !current || now - current.at > IDLE_MS;
    const next = { id: fresh ? crypto.randomUUID() : current!.id, at: now };
    fallbackSession = next;
    try { sessionStorage.setItem(SESSION_KEY, JSON.stringify(next)); } catch { /* In-memory session still works. */ }
    return { id: next.id, fresh };
}

export function discardQueued(name: string) {
    queue = queue.filter(event => event.event_name !== name);
}

export function track(name: string, props: Props = {}) {
    if (enabled === false || typeof window === 'undefined' || !window.crypto?.randomUUID) return;
    const now = Date.now(), { id, fresh } = session(now), at = new Date(now).toISOString();
    const make = (event_name: string, p: Props): Event => ({ event_id: crypto.randomUUID(), session_id: id, event_name, props: p, occurred_at: at });
    if (fresh) queue.push(make('session_started', {
        entry_route: routeTemplate(window.location.pathname),
        device: window.matchMedia?.('(max-width: 767px)').matches ? 'mobile' : 'desktop',
    }));
    queue.push(make(name, props));
    if (queue.length > 100) queue = queue.slice(-100);
    // Speed metrics arrive as the tab hides, after the hide-time flush has run.
    if (enabled && (queue.length >= 20 || document.visibilityState === 'hidden')) flush();
    else if (!timer) timer = setTimeout(flush, 5000);
}

export function flush() {
    if (timer) { clearTimeout(timer); timer = null; }
    if (!enabled) return;
    while (queue.length) {
        const events = queue.splice(0, 20);
        // Tracking must never block or break a page, so failures are dropped.
        fetch('/api/account/events', { method: 'POST', keepalive: true, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ events }) })
            .then(response => response.ok ? response.json() : null)
            .then(body => { if (body?.data?.enabled === false) setUsageEventsEnabled(false); })
            .catch(() => {});
    }
}

if (typeof document !== 'undefined')
    document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'hidden') flush(); });

export function trackRequestFailure(url: string, method: string, status: number, reason: 'http' | 'network' | 'invalid') {
    if (!url.startsWith('/api/') && !url.startsWith(`${window.location.origin}/api/`)) return;
    const api = apiTemplate(url);
    if (api === '/api/account/events') return;   // never report the reporter
    track('request_failed', { api, method: method.toUpperCase(), status, reason });
}

// Observe every same-origin API call once, rather than at each call site. The
// response passes through untouched; aborted requests are not failures.
if (typeof window !== 'undefined' && !(window as { __ballisticsFetch?: boolean }).__ballisticsFetch) {
    (window as { __ballisticsFetch?: boolean }).__ballisticsFetch = true;
    const original = window.fetch.bind(window);
    window.fetch = async (input, init) => {
        const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
        const method = init?.method || (input instanceof Request ? input.method : 'GET');
        try {
            const response = await original(input, init);
            if (!response.ok) trackRequestFailure(url, method, response.status, 'http');
            return response;
        } catch (error) {
            if (!(error instanceof DOMException && error.name === 'AbortError')) trackRequestFailure(url, method, 0, 'network');
            throw error;
        }
    };
    window.addEventListener('error', () => track('client_error', { route: routeTemplate(window.location.pathname), kind: 'error' }));
    window.addEventListener('unhandledrejection', () => track('client_error', { route: routeTemplate(window.location.pathname), kind: 'unhandled_rejection' }));
}

export function trackSpeed(name: string, value: number, rating: string) {
    const metric = name.toLowerCase();
    if (!['lcp', 'inp', 'ttfb', 'fcp'].includes(metric)) return;
    track('page_speed', { route: routeTemplate(window.location.pathname), metric, value_ms: Math.min(120000, Math.max(0, Math.round(value))), rating });
}
