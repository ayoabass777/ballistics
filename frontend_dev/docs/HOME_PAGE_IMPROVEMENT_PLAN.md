# Home Page Improvement Plan

## Overview
This document outlines a comprehensive plan for improving the Ballistics home page, focusing on user experience, performance, accessibility, and visual design.

---

## 1. Component Architecture Improvements

### ✅ Completed
- [x] Extracted HeroCTA component
- [x] Extracted QuickStats component
- [x] Extracted SectionHeader component
- [x] Extracted ExploreMoreCTA component
- [x] Moved page-specific components to `components/home/` folder
- [x] Refactored sports-streaks.tsx to use extracted components

### 🔄 Recommended Next Steps
- [ ] Create a dedicated `components/home/TopStreaksSection.tsx` to encapsulate the entire streaks section
- [ ] Extract league filtering logic to a custom hook (`useLeagueFilter`)
- [ ] Extract search logic to a custom hook (`useStreakSearch`)
- [ ] Create a `lib/hooks/useStreakData.ts` for data fetching logic
- [ ] Implement proper TypeScript types in a separate `types/` directory

---

## 2. Performance Optimizations

### Critical
- [ ] **Implement React.memo for StreakCard** - Prevent unnecessary re-renders when parent state changes
- [ ] **Add useMemo for filteredStreaks** - Cache filtered results to avoid recomputing on every render
- [ ] **Lazy load streak cards** - Use intersection observer to load cards as user scrolls
- [ ] **Add image optimization** - Use Next.js Image component for team/league logos with proper sizes
- [ ] **Implement virtualization** - Use `react-window` or `react-virtual` for large lists (when 50+ streaks)

### Medium Priority
- [ ] Add service worker for offline support
- [ ] Implement proper cache strategy for API responses (SWR or React Query)
- [ ] Optimize bundle size by code splitting the explore page
- [ ] Add prefetching for league-specific pages on hover

### Low Priority
- [ ] Add web vitals monitoring
- [ ] Implement progressive image loading with blur placeholders
- [ ] Optimize CSS delivery with critical CSS

---

## 3. User Experience Enhancements

### Search & Filtering
- [ ] **Debounce search input** - Add 300ms debounce to reduce re-renders
- [ ] **Add search suggestions** - Show dropdown with matching teams/leagues as user types
- [ ] **Add filter chips** - Display active filters as removable chips below search
- [ ] **Add "Clear all filters"** - Quick way to reset search and league selection
- [ ] **Add URL parameters** - Persist search/filter state in URL for shareability
- [ ] **Add empty state illustrations** - Better visuals when no results found

### Navigation & Flow
- [ ] **Add breadcrumbs** - Show current location in app hierarchy
- [ ] **Add "Back to top" button** - Appears after scrolling down
- [ ] **Improve mobile navigation** - Add bottom navigation bar for key actions
- [ ] **Add keyboard shortcuts** - Support "/" for search, ESC to clear, etc.
- [ ] **Add streak detail modal** - Quick preview without leaving the page

### Data Visualization
- [ ] **Add streak trends chart** - Small sparkline showing streak history
- [ ] **Add league comparison view** - Compare streaks across different leagues
- [ ] **Add streak calendar view** - Visual calendar showing match dates
- [ ] **Add team head-to-head preview** - Show historical matchup data
- [ ] **Add streak strength indicator** - Visual gauge showing streak quality

---

## 4. Accessibility Improvements

### Critical (WCAG 2.1 AA Compliance)
- [ ] **Add proper ARIA labels** - Label all interactive elements
- [ ] **Fix heading hierarchy** - Ensure h1 → h2 → h3 logical flow
- [ ] **Add focus indicators** - Visible focus states for keyboard navigation
- [ ] **Add skip links** - "Skip to main content" link
- [ ] **Test with screen readers** - NVDA, JAWS, VoiceOver compatibility
- [ ] **Fix color contrast** - Ensure 4.5:1 ratio for text (especially in light mode)

### Medium Priority
- [ ] Add reduced motion support (respect `prefers-reduced-motion`)
- [ ] Add high contrast mode
- [ ] Support keyboard navigation for dropdowns
- [ ] Add live regions for dynamic content updates
- [ ] Add descriptive alt text for all images

---

## 5. Mobile Experience Refinements

### Layout & Touch
- [ ] **Increase touch targets** - Ensure minimum 44x44px for all buttons
- [ ] **Add swipe gestures** - Swipe to navigate between streaks
- [ ] **Optimize mobile grid** - Single column on small phones, 2 on tablets
- [ ] **Add pull-to-refresh** - Native-feeling refresh mechanism
- [ ] **Fix header on scroll** - Collapse/minimize header when scrolling down

### Performance
- [ ] Reduce initial bundle size for mobile
- [ ] Optimize images for mobile (responsive images)
- [ ] Add touch-optimized animations (shorter durations)
- [ ] Test on low-end devices (throttle testing)

---

## 6. Animation & Interaction Patterns

### Micro-interactions
- [ ] **Add loading skeletons** - Smooth transitions from loading to content
- [ ] **Add toast notifications** - Success/error feedback for actions
- [ ] **Add optimistic UI updates** - Show changes immediately before API confirms
- [ ] **Enhance hover states** - More engaging interactions on desktop
- [ ] **Add success animations** - Celebrate user actions (e.g., saving favorites)

### Page Transitions
- [ ] Add route change progress bar
- [ ] Add shared element transitions between pages
- [ ] Add smooth scroll behavior
- [ ] Add fade-in animations for new content

---

## 7. Data & API Integration

### When Connecting Real API
- [ ] **Add error boundaries** - Graceful error handling
- [ ] **Add retry logic** - Auto-retry failed requests
- [ ] **Add request cancellation** - Cancel outdated requests
- [ ] **Add loading states** - Granular loading for each section
- [ ] **Add real-time updates** - WebSocket or polling for live scores
- [ ] **Add data caching** - Implement SWR or React Query
- [ ] **Add pagination** - Load more streaks on demand
- [ ] **Add optimistic updates** - Instant UI feedback

### Data Quality
- [ ] Add data validation with Zod
- [ ] Add error logging (Sentry or similar)
- [ ] Add analytics tracking (Google Analytics, Plausible)
- [ ] Add A/B testing framework

---

## 8. Visual Design Enhancements

### Color & Typography
- [ ] **Add color mode sync** - Sync with OS theme preference
- [ ] **Refine cream theme** - Fine-tune light mode colors for better readability
- [ ] **Add accent colors** - Team-specific colors for streak cards
- [ ] **Improve typography hierarchy** - Better font size/weight scale
- [ ] **Add custom fonts** - Consider Inter or Geist for modern look

### Layout & Spacing
- [ ] Add consistent spacing system (4px grid)
- [ ] Improve responsive breakpoints
- [ ] Add container max-widths for readability
- [ ] Refine card shadows and depth
- [ ] Add subtle gradients for visual interest

### Illustrations & Graphics
- [ ] Add custom illustrations for empty states
- [ ] Add team badge/crest graphics
- [ ] Add league branding elements
- [ ] Add decorative background patterns
- [ ] Consider adding animated SVG icons

---

## 9. SEO & Metadata

- [ ] Add proper meta tags (title, description, OG tags)
- [ ] Add JSON-LD structured data for streaks
- [ ] Add canonical URLs
- [ ] Add proper heading structure for SEO
- [ ] Add sitemap generation
- [ ] Add robots.txt
- [ ] Optimize for Core Web Vitals

---

## 10. Testing Strategy

### Unit Tests
- [ ] Test custom hooks (useStreakData, useLeagueFilter)
- [ ] Test utility functions (formatMatchDate, generateHeadline)
- [ ] Test component rendering (StreakCard, QuickStats)

### Integration Tests
- [ ] Test search and filter functionality
- [ ] Test league selection flow
- [ ] Test theme toggling
- [ ] Test navigation

### E2E Tests
- [ ] Test complete user journeys
- [ ] Test mobile responsive behavior
- [ ] Test accessibility with automated tools

---

## 11. Documentation

- [ ] Add component documentation (Storybook)
- [ ] Add API documentation
- [ ] Add contribution guidelines
- [ ] Add design system documentation
- [ ] Add performance budgets

---

## Priority Matrix

### High Impact, Low Effort (Do First)
1. Add debounced search
2. Implement React.memo for StreakCard
3. Add proper loading skeletons
4. Fix accessibility issues (ARIA labels, focus states)
5. Add URL parameters for filters

### High Impact, High Effort (Plan Carefully)
1. Implement data fetching with React Query/SWR
2. Add real-time updates via WebSocket
3. Build streak detail modal with charts
4. Add comprehensive testing suite
5. Implement lazy loading and virtualization

### Low Impact, Low Effort (Quick Wins)
1. Add "Back to top" button
2. Add toast notifications
3. Improve empty states
4. Add keyboard shortcuts
5. Add subtle animations

### Low Impact, High Effort (Revisit Later)
1. Custom illustrations
2. Advanced analytics
3. A/B testing framework
4. Multi-language support

---

## Implementation Phases

### Phase 1: Foundation (Week 1-2)
- Extract remaining components
- Add custom hooks for data/filtering
- Implement performance optimizations (memo, useMemo)
- Fix critical accessibility issues

### Phase 2: UX Polish (Week 3-4)
- Add debounced search
- Implement URL parameters
- Add loading states and skeletons
- Improve mobile experience
- Add animations

### Phase 3: Data Integration (Week 5-6)
- Set up React Query/SWR
- Connect to real API
- Add error handling
- Implement caching strategy

### Phase 4: Advanced Features (Week 7-8)
- Add streak detail modal
- Implement charts and visualizations
- Add real-time updates
- Build comparison features

### Phase 5: Testing & Optimization (Week 9-10)
- Write comprehensive tests
- Optimize bundle size
- Improve Core Web Vitals
- Add monitoring and analytics

---

## Success Metrics

### Performance
- First Contentful Paint < 1.5s
- Time to Interactive < 3.5s
- Lighthouse score > 90

### User Experience
- Search response time < 300ms
- Zero accessibility violations
- Mobile usability score > 95

### Engagement
- Bounce rate < 40%
- Average session duration > 2 minutes
- Click-through rate to detail pages > 30%

---

## Notes
- This plan is iterative - priorities may shift based on user feedback
- Each phase should include user testing and feedback collection
- Performance budgets should be monitored throughout
- Accessibility should be a continuous focus, not a one-time fix
