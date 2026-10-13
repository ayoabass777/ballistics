# Prototype links and versions

Desktop review variants: [1440px](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1226-62085&starting-point-node-id=1226%3A62085) · [1280px](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1235-62127&starting-point-node-id=1235%3A62127) · [1024px](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1235-62521&starting-point-node-id=1235%3A62521).

2026-10-12 — Desktop follow-up QA completed for review variants: 1440px `1226:62085`, 1280px `1235:62127`, 1024px `1235:62521`. Narrow view stacks Context subcolumns and evidence sections; 1280/1440 retain Patterns left and Strength/Goals right. Fixed Context scope controls changing width between states, misleading fixed bold styling on the All venues pattern label, and excessive fixed-height gaps in the 1024px layout. Added wrapping for Context team names and the attack/defence pair label. Browser clicked both Context scopes and both Pattern scopes in both directions; confirmed their state independence, venue unavailable message, restoration of all-venues records, See all, and bottom Show fewer returning to Patterns. Separate synthetic QA cards cover long names with twelve expanded rows (`1240:62211`), one-sided Results without Also (`1240:62397`), and empty categories (`1240:62538`); visually checked at the narrower 560px pattern-card width. Synthetic QA records are layout fixtures, not qualified football evidence. Figma-only; these are separate breakpoint frames, not responsive frontend implementation. External sidebar/detail navigation still retains legacy routes and is not fully regression-tested. Additional isolated long-name checks passed for the 736px match header (`1243:62211`) and 408px Strength card (`1243:62241`); Strength wraps to two lines without overlap.

2026-10-12 — First [desktop Match details review](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1226-62085&starting-point-node-id=1226%3A62085) built at 1226:62085 using current mobile cards and existing desktop shell. Context across top; Patterns 680px left; Strength/Goals stacked right. Nine qualified patterns preserved, one limitations block. Browser verified expansion, bottom collapse returning to Patterns, and Home/Away unavailable state; Goals remains in place during expansion. Screenshot checked at 1440px. Narrower layouts, long-name/list stress cases and exhaustive external navigation remain pending. Inherited sidebar and external detail links retain existing prototype destinations, including older/mobile flows. Figma-only, no frontend implementation.

2026-10-12 — Team strength review 1196:62157: added lime underline beneath Big dog team name; replaced Reliable here with Big dogs usually win in this league and supporting Based on results across several seasons. Hid redundant reliability dots to keep the claim explicit. No numerical league count restored. Figma-only; source/canonical card retained. Structural text-fit check passed.

2026-10-12 — Owner rejected the extra probability-above-league trial. Active review uses only the existing Bayes display eligibility gate; observed records and league baselines inform presentation. Keep Results Winless/Unbeaten and Attack/defence pair. AEK Over 1.5 returns to Also alongside Atromitos Under 3.5; three records remain expanded. Explicit More/Less often than the league average wording retained; Atromitos Under 4.5 uses Around the league average. Nine unique records total. The 80% trial and diagnostic files are historical, not active requirements. Figma-only; no production algorithm changed.

2026-10-12 — Existing review 1196:62044 uses explicit league-average wording. Other-fixture trial is documented in patterns-two-stage-review.md; no new prototype routes created.

2026-10-12 — Two-stage trial requested: retain nine display-eligible records, use P(theta > fixed league base)=1-I_base(alpha,beta) >=0.80 for prominence. Saved rounded Beta parameters give Results Winless/Unbeaten 0.8717; attack/defence and Under 3.5 0.8293. Four remaining records expand; no second gate on expansion. Figma 1196:62044 now has four main records, one Also and four expanded, without duplication. Neutral comparison on AEK Over 1.5 and Under 4.5; common-league wording on Atromitos Under 4.5 does not assert statistical equivalence. Trial assumes fixed baseline and is not a validated production rule. Evidence: mock/patterns-111915-two-stage-trial.json. Canvas structure checked; backend unchanged.

2026-10-12 — Review 1196:62044 now displays nine Bayes-qualified patterns, with four additional records behind See all patterns. All three former 3/6 rows and the unqualified Lost/Won pair are hidden. Source snapshot recorded in implementation status.

2026-10-12 — Team patterns wording updated in review 1196:62044: More often than usual versus Around the league average; see expanded Under 4.5 and Atromitos Over 2.5 rows for typical examples.

2026-10-12 — [Inline patterns expansion review](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1196-62044&starting-point-node-id=1196%3A62044): Patterns → See all patterns. Nine additional rows expand inline; bottom Show fewer returns to the heading. Browser verified.

2026-10-12 — [Team patterns venue-toggle review](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1196-62044&starting-point-node-id=1196%3A62044): tap Patterns, then Home / Away or All venues. Current sample defaults overall and explains immature venue samples.

2026-10-12 — Corrected [Strength and patterns review](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1196-62044&starting-point-node-id=1196%3A62044): owner Strength styling restored, no league count/disclosure CTA, goal-total patterns returned to See all patterns.

2026-10-12 — [Updated Strength and Team patterns review](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1196-62044&starting-point-node-id=1196%3A62044): See league evidence opens historical count; Over 1.5 and Under 4.5 appear before See all patterns.

2026-10-12 — [Team strength evidence preview](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1196-62044&starting-point-node-id=1196%3A62044). Tap Strength. Separate comparison with clearer league claim and 160/214 historical record; current canonical design unchanged.

2026-10-12 — [Discover shared date scope review](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1169-62199&starting-point-node-id=1169%3A62199). Next 7 days / Today / Tomorrow / Date. Scope shared across category and All standouts review clones; original canonical flow preserved.

2026-10-12 — [Calendar preview](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1155-62270&starting-point-node-id=1155%3A62115): tap date for month calendar. Replaces three-button picker in compact review; old panel 1158:62177 preserved.

2026-10-12 — [Compact Matches date preview](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1155-62115&starting-point-node-id=1155%3A62115): previous/next and date picker above league scope, bounded to 9–11 October.

2026-10-12 — D117 [Compact Matches with real saved fixtures](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1155-62115&starting-point-node-id=1155%3A62115). Five saved 10 Oct examples in kickoff order; Your leagues shows Serbia, Search/Filters offers a Riga example. Separate review; original flow unchanged.

2026-10-12 — D118 [Current Discover](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=592-54464&starting-point-node-id=1030%3A57675). Teaser sign-in and reachable onboarding completion now converge here; legacy Discover designs preserved outside the active flow.

2026-10-12 — D117: [Teaser detail info preview](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1033-57869&starting-point-node-id=1030%3A57675). Tap muted ⓘ beside Save for the short explanation; Read full Methodology opens existing methodology.

2026-10-12 — D116 [Match-detail onboarding preview](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1030-57675&starting-point-node-id=1030%3A57675): Open Explore a sample match from the owner-edited teaser. First-visit tip appears below the section menu; Got it dismisses. Restart prototype to see it again.

2026-10-11 — D112 saved-detail correction: [Teaser entry](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1030-57675&starting-point-node-id=1030%3A57675) now opens the established saved-detail layout at 1033:57869 for Strength. Supersedes custom 1030:57748 on this route.

2026-10-11 — D112 [Explorable teaser review](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=1030-57675&starting-point-node-id=1030%3A57675). Three sample cards → corresponding evidence → membership for Save/Browse more. Strength includes section jumps. Original D111 preserved; separate mobile review, pending owner approval.

2026-10-11 — D111 (prototype for review): Compact teaser 1016:57666 fits 390×844, replacing large statistics and separate examples with one fixture and three evidence summaries from the existing frozen Radnicki/Crvena sample. Two Get started entries reuse updated onboarding; original teaser 329:47955 preserved. Canonical Discover 592:54464 and review 990:57646 now use a muted lime context panel and equal-width 44px Explore/outlined Save controls; saved label shortened to Saved, existing state/routes retained. Discover still defaults to All leagues. Figma screenshots visually verified; production persistence/filtering unchanged.

2026-10-11 — D110 (user-authorized integration, Figma-only): Refined D109 Discover content integrated into canonical 592:54464 (content 1007:57941); previous screen preserved as 1007:57650. Teaser 329:47955 preview cards/Get started now enter refreshed explainer 989:57617, followed by multi-league picker, account/plan mocks and completion. Completion 989:57766 now enters canonical Discover with the dismissible onboarding tip. Standalone review 990:57646 retained. Existing main navigation destinations remain stable. Multi-league downstream filtering/account persistence still pending; no backend delivery claimed.

2026-10-11 — D109 visual refinement (Figma review only): Discover review 990:57646 now uses a stronger Standouts heading and supporting subtitle, grouped category heading/tabs/count with extra separation, 292px featured cards with side-by-side 44px actions, and compact category cards without repeated category/reason labels. End prompt reads Explore more, reserving 5/5 for the fifth fixture. Existing context copy retained because no new comparative evidence was supplied. Main Discover unchanged.

2026-10-11 — D109 review refinement: [Multi-league picker](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=989-57638&starting-point-node-id=989%3A57592); [Discover hierarchy and carousel end prompt](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=990-57646&starting-point-node-id=989%3A57592). End CTA 997:57681 → existing full list 536:54144, browser verified. Selection UI is interactive, downstream multi-league filtering remains pending.

2026-10-11 — D108 [Onboarding review start](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=989-57592&starting-point-node-id=989%3A57592). Welcome 989:57592 → explainer 989:57617 → leagues 989:57638 → phone 989:57674 / email 989:57703 → plans 989:57731 → completion 989:57758 → Discover review 990:57646. Tips: Discover 990:57634, detail 990:57640. Detail remains shared 841:57932. Preview/sign-in entry points retain the existing flows.

2026-10-09 — D107: [Shared-component first Discover detail](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=841-57932&starting-point-node-id=592%3A54464). Enter through current Discover 592:54464 for Strength highlight. Component IDs and instance mapping: mock/match-detail-components-2026-10-09.json. Four previous v2 examples and original first-fixture detail preserved.

2026-10-08 — Navigation repair: traversed 569 reachable destination nodes from current Discover/Saved/v2 roots, with queue exhausted. Retargeted old Saved 325:47880 to 605:54814 and old mobile Discover destinations to their current category screens throughout reachable reactions, including conditional actions. Matches, Forecast Record and v2 league-table principal roots have zero references to the four replaced destinations. Browser verified Matches → current Saved; other routes structurally checked. Historical layouts preserved. This is a navigation repair, not the remaining 78-fixture v2 layout rollout or replacement of September Matches sample data. Evidence: mock/navigation-repaired-2026-10-08.jpg.

2026-10-08 — D106: Existing v2 frames now connect to current Discover/Saved: Zlin 668:57306, Inter/Parma 698:57344, RAAL/Club Brugge 699:57371, Naples/Richmond 701:57399. Current Inter/RAAL full-list and Saved entries open the matching v2 fixture. Remaining sample fixtures retain their existing details.

2026-10-08 — D105: [Integrated mobile detail](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=661-54827&starting-point-node-id=592%3A54464), reached from current Discover 592:54464 and Saved 605:54814. All 80 mobile mappings: [route inventory](mock/integrated-detail-routes-2026-10-07.json). Established reference 512:54321 and original simplified details preserved; separate D102–D104 v2 is unchanged.

2026-10-07 — D100: D99 is superseded by owner correction. [Established match-details identity review](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=512-54321&starting-point-node-id=512%3A54321); [existing Zlin/Slavia interactive flow](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=346-52558&starting-point-node-id=346%3A52558). Current first standout links restored to fixture-correct D80 detail; integration with established layout pending.

2026-10-07 — D99: [Match details · evidence panels review](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=652-54826&starting-point-node-id=592%3A54464). Radnicki 1923 / FK Crvena Zvezda first-fixture trial. Other 79 details unchanged; original first detail preserved.

2026-10-07 — D98: [Current Discover flow](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=592-54464&starting-point-node-id=592%3A54464) now shares Save with All standouts 536:54144 and all 80 linked details. [Detail Save evidence](detail-save-test-2026-10-07.jpg). Representative Save/View/remove browser checks pass; see interaction-handoff.md for implementation boundaries.

2026-10-07 — D97: [Current Discover with repaired navigation and league draft](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=592-54464&starting-point-node-id=592%3A54464). Edit opens origin-specific picker; Strength picker 632:55127, Goals 632:55165, Patterns 632:55203. Follow/Unfollow, Done/Cancel and cross-category session count tested; Matches return route tested. Original two picker frames preserved.

2026-10-07 — D96: [Tested Discover prototype](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=592-54464&starting-point-node-id=592%3A54464). Show more now expands in place; Save click conflict repaired. [Browser test report and screenshot](interaction-test-2026-10-07.md). Earlier expanded frames and pressed-state design board are preserved; board pressed state is specification-only.

2026-10-07 — D95: [Discover hover review](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=592-54464&starting-point-node-id=592%3A54464). Page-level hover reactions now supplement Save hover across nine Discover states. Initial visual check only; browser hover validation pending.

2026-10-07 — D94: [Save interaction states](https://www.figma.com/design/dLgWAaafmzxQNcp2Rb9P7u?node-id=612-55217) compares Default, Hover and Pressed/touch. Applied to current Discover Save controls; board visually checked, runtime hover verification pending.

2026-10-07 — D93: [Discover Save flow](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=592-54464&starting-point-node-id=592%3A54464). Save on cards/carousel → Saved link or bottom tab → session shortlist 605:54814 → Remove. Save/remove and multi-fixture persistence across category navigation browser-tested. Prototype-session only; restarting may reset selections.

2026-10-07 — D92: [Discover](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=592-54464&starting-point-node-id=592%3A54464) now has shorter standout copy and Your leagues · 1 / Edit routes. [League picker demo](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=599-55422&starting-point-node-id=599%3A55422) supports only Serbia/Super Liga; followed/empty samples, not full persistence or directory implementation.

2026-10-07 — D91: [Discover seven-day design](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=592-54464&starting-point-node-id=592%3A54464). Date picker replaced by window label across all category/expanded states; cards include dates. Frozen 10 October examples explicitly labelled, not a live seven-day cohort.

2026-10-07 — D90: [Current Discover review](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=592-54464&starting-point-node-id=592%3A54464) now shows date strip and league scope controls without the Discover heading. Visual proposal; date/league interactions pending. Existing carousel and show-more routes retained.

2026-10-07 — D89: [Discover with Show more](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=592-54464&starting-point-node-id=592%3A54464). Switch among three categories, scroll past five cards and select Show more matches. Sample totals 9/10/10. Structural route checks and initial Strength visual check; browser scroll continuity unverified.

2026-10-07 — [Longer Team strength list, original standouts restored](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=590-54448&starting-point-node-id=590%3A54448). Nine saved-sample strength fixtures below the unchanged top-five carousel. Supersedes the misunderstood long-name test for this request. Layout checked; browser scroll unverified.

2026-10-07 — [Long team-name / strength-copy test](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=588-54432&starting-point-node-id=588%3A54432). Illustrative first fixture, disabled illustrative CTA and category tabs; layout review only. Source carousel preserved.

2026-10-07 — D88: [Top-five carousel](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=586-54384&starting-point-node-id=586%3A54384). Horizontal swipe/scroll configured; All standouts opens full list. Goals 586:54548 / Patterns 586:54714. Initial Strength screenshot checked, browser swipe unverified; pending review.

2026-10-07 — D87: [Pale-lime feature trial](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=584-54336&starting-point-node-id=584%3A54336). Linked Goals/Patterns 584:54439 / 584:54544. Final Strength screenshot checked; prior dark version preserved. Pending review.

2026-10-07 — D86: [Featured-match alternative](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=582-54288&starting-point-node-id=582%3A54288). Worth a closer look above tabs, Explore match and All standouts wired; Goals 582:54399 / Patterns 582:54512. Strength screenshot checked, pending review. D85 white-card comparison preserved.

2026-10-07 — D85: [White-card comparison](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=580-54240&starting-point-node-id=580%3A54240), with linked Goals/Patterns states 580:54342 / 580:54446. Compare with [green-card version](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=576-54479&starting-point-node-id=576%3A54479). Screenshot-checked; theme and standouts remain pending review.

2026-10-07 — D84: [Discover review](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=576-54479&starting-point-node-id=576%3A54479) now uses a deep-green standout panel with lime accents across all three category states. Earlier pale version archived; still pending review.

2026-10-07 — D83 first combined mobile review: [Discover v1](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=576-54479&starting-point-node-id=576%3A54479). Switch Team strength / Goals outlook / Team patterns; use View all 80 or View match to enter existing destinations. Roots 576:54479 / 576:54589 / 576:54701. Screenshots checked; browser interactions untested. Fixed-date, fixed-scope sample; standouts remains pending review.

2026-10-07 — D82: **Biggest standouts is pending review.** The D79–D80 Discover, full-list and detail links below remain available for review; prototype delivery is separate from design approval.

2026-10-06 — D80: [Discover](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-359&starting-point-node-id=16%3A359) and full lists now have paired logo/name rows plus View match links. Example mobile destinations: first standout 542:54144, Riga 542:54206; desktop 542:54175 / 542:54235. Enter through a card for the Back interaction. Complete mapping in mock/standout-detail-routes-2026-10-06.json. Mobile first-card forward/back tested; all 178 routes audited.

2026-10-06 — D79 applied standouts: [Discover mobile](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-359&starting-point-node-id=16%3A359), [Discover desktop](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-354&starting-point-node-id=16%3A354). View all 80 opens 536:54144 / 536:54871. Back requires entering from Discover. Three top cards shown without category quotas; full fixed-date list contains every qualifying 10 October fixture. Mobile entry, scroll and Back tested; older D71 proposal retained separately.

2026-10-06 — D78 status: 15-point standout cutoff is agreed and implemented locally in engine selection. The separate D71 preview remains historical: it has not been refreshed with the new cutoff, uncapped results or D77 pattern rules.

2026-10-06 — D76 main-flow crests: [Discover mobile](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-359&starting-point-node-id=16%3A359), [Matches mobile](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-360&starting-point-node-id=16%3A360). Desktop roots 16:354 / 16:355 also updated. All three Discover sections and populated Matches date/filter states carry team crests, with league/country text retained. Four screen screenshots checked; Matches card reactions preserved.

2026-10-06 — D75 identity review: [match details](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=512-54321&starting-point-node-id=512%3A54321), [standings](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=512-54498&starting-point-node-id=512%3A54498), [country/league picker](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=512-54758&starting-point-node-id=512%3A54758). Separate mobile visual previews, screenshot-checked; match/table forward/back wired. Picker retains earlier demo interactions targeting original states, so icon propagation through its entire flow is not claimed.

2026-10-05 D71 Biggest standouts PROPOSAL (Figma, not in flow): [section](https://www.figma.com/design/dLgWAaafmzxQNcp2Rb9P7u?node-id=493-54285) on page 16:353. Mobile Discover clone 493:54286 with a members-only standouts strip above the section tabs (Sat 10 Oct, real Oct 5 run data), all five cards laid out at 493:54625, review note 493:54679. Strength cards use the pooled Big dog rate pending the per-league record. Original screens unchanged; cards not linked.

2026-10-05 — D73 historical-pattern trial: [Barcelona](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=482-54496&starting-point-node-id=482%3A54496), [Kairat](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=482-54259&starting-point-node-id=482%3A54259). Focused mobile reviews, six historical records each, no 75% rating gate. Back returns to the original full match; original screens preserved. Full-length comparison panels 483:54285 / 483:54375. Not a full live-data implementation.

2026-10-05 — D70 separate crest review: [teaser](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=467-54203&starting-point-node-id=467%3A54203) and [match header](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=467-54275&starting-point-node-id=467%3A54275). Mobile only, screenshot-verified. Original current screens preserved; wider rollout not applied.

2026-10-05 — D69: [mobile teaser](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=329-47955&starting-point-node-id=329%3A47955), [desktop teaser](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=334-51169&starting-point-node-id=334%3A51169). Try [mobile Zlin match](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=346-52558&starting-point-node-id=346%3A52558) or [desktop Zlin match](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=348-52917&starting-point-node-id=348%3A52917), then scroll to standings/H2H. Table destinations 457:55601 / 457:55860 supersede the empty review pages in current links. All 13 fixtures have dedicated destinations and populated H2H; Brooklyn/Detroit table is unavailable pending conference grouping. Mobile entry/back/scroll/expansion tested; wider state coverage structurally audited only.

2026-10-05 D68 hierarchy review: Goals mobile 22:642 / desktop 22:452, Patterns mobile 22:722 / desktop 22:547. All six main Discover category tabs use the new names. Four screen screenshots checked; desktop clipping repaired and rechecked. Existing card navigation preserved; browser click-through not tested.

2026-10-05 D67: current saved-data match-detail pattern panels now show qualitative league comparisons instead of numeric league baselines, including [Botafogo mobile](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=42-635&starting-point-node-id=42%3A635). Records and observed percentages remain. 536 labels updated; mobile panel screenshot checked; historical illustrative designs preserved.

2026-10-05 dedicated standings review: [mobile](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=441-54169&starting-point-node-id=441%3A54169) / [desktop](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=441-54363&starting-point-node-id=441%3A54363). Linked from Zlin/Slavia detail roots 346:52558 / 348:52917. Back targets the corresponding match with scroll reset disabled. Unavailable-data state only; H2H unchanged inline. Screenshots checked; browser scroll restoration unverified.

2026-10-04 D65: fixture pairing is now bold above the stronger team on the 32 revised cards. Supporting sections added to 138 detail states. Open [Zlin/Slavia mobile detail](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=346-52558&starting-point-node-id=346%3A52558) and scroll below the main analysis for League standings and Head-to-head. Both headers toggle open/closed; saved-data contents currently unavailable. Component state review: 427:54486. Screenshot and reaction checks only; no browser click verification.

2026-10-04 D64 onboarding and Strength hierarchy: [mobile welcome](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=324-47839&starting-point-node-id=324%3A47839), [desktop welcome](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=334-50895&starting-point-node-id=334%3A50895). Get started reaches the existing number explainer with league-context copy. Teaser roots 329:47955 / 334:51169 and Discover Strength roots 16:359 / 16:354 now lead with the stronger team and a Big dog badge. Review root 393:53374 matches. Screenshots and 32 card-link targets checked; browser click-through remains unverified.

2026-10-04 Goals league context added: [desktop Discover Goals](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=22-452&starting-point-node-id=22%3A452) / [mobile Discover Goals](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=22-642&starting-point-node-id=22%3A642). Both saved-data cards now include qualitative league comparisons. [Mobile teaser](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=329-47955&starting-point-node-id=329%3A47955) / [desktop teaser](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=334-51169&starting-point-node-id=334%3A51169) retain the existing Typical-for-league goals example. Screenshot-verified 4 Oct; no new browser interaction test.

2026-10-04 simplified Strength review: [Discover](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=393-53374&starting-point-node-id=393%3A53374), [match details](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=393-53527&starting-point-node-id=393%3A53527), [expanded explanation](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=399-53941&starting-point-node-id=399%3A53941). First Discover card opens the revised Zlin/Slavia detail; disclosure opens/closes the numeric rating explanation; Back returns to this review list. Removed visible illustrative W/D/L and numeric gaps from all ten review list cards. Previous group-record review archived at 399:53505. Screenshot and reaction-target checks completed; browser click-through not verified. This focused mobile review does not replace every existing Discover/detail state.

2026-10-03 high-volume Forecast record: [mobile](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=287-37762&starting-point-node-id=287%3A37762), [desktop](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=287-37698&starting-point-node-id=287%3A37698). Homepage Forecast record links target this flow. Try Settled → Brooklyn → 3 forecasts → Correction history; Search teams → Brooklyn; country-first league selection; or scroll to Load 25 more fixtures. Supersedes the six-record list as the current volume review; old screens remain preserved. Mock-only, with three leagues and two preset weeks.

2026-10-02 Strength: Discover Strength [desktop](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-354&starting-point-node-id=16%3A354) / [mobile](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-359&starting-point-node-id=16%3A359) show five engine-shortlist cards (preview-only, not clickable). Real strength labels in Brooklyn (42:715 / 42:800) and Naples (42:880 / 42:965) details. Supersedes the 2026-09-30 notes about the illustrative Naples Big dog card.

2026-10-02 navigation update: Forecast record is now in the primary navigation on all desktop/mobile Discover category screens, not the footer. Start [mobile homepage](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-359&starting-point-node-id=16%3A359) or [desktop homepage](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-354&starting-point-node-id=16%3A354), then Forecast record. Existing record entry IDs are unchanged.

2026-10-02 Forecast record: [desktop](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=269-36732&starting-point-node-id=269%3A36732), [mobile](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=269-36733&starting-point-node-id=269%3A36733). (Footer placement superseded the same day by D34: Forecast record now sits in primary navigation; How it works stays in the footer.) Try Settled then Brooklyn's Correction history. Lists: All 269:36732/36733, Settled 36734/36735, Pending 36736/36737, Other 36738/36739, Empty 36740/36741. Detail pairs 36742–36753 cover Hit, Miss, Pending, Void, Unavailable, Excluded respectively (all prefix 269:). Two demo weeks only; all records synthetic and non-live. Native BACK is wired from details to the originating list. This supersedes older notes saying Forecast record has no current-flow access.

2026-10-02 Discover footer review: [mobile Discover](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-359&starting-point-node-id=16%3A359), [desktop Discover](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-354&starting-point-node-id=16%3A354). Scroll to How it works below the content; mobile places it below Browse all matches. Opens Methodology, with Back returning to the originating Discover screen. Added across Strength, Goals and Patterns in both viewports; mobile Strength entry/return browser-tested.

2026-10-01 Methodology prototype: [mobile overview](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=239-36044&starting-point-node-id=239%3A36044), [desktop overview](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=239-36036&starting-point-node-id=239%3A36036). Eight states per viewport, named Methodology · D/M · topic=0..7. Calibration desktop 239:36041 / mobile 239:36049. Contextual entry from saved-data details: How these numbers work. Back preserves the detail origin after topic changes; Matches shortcut opens the default list. Mobile entry/topic/return browser-tested. Screens are draft content, not validated performance evidence or production implementation.

2026-10-01 integrated Matches supersedes the separate-picker limitation below. Start [mobile Matches](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-360&starting-point-node-id=16%3A360) or [desktop Matches](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=16-355&starting-point-node-id=16%3A355). All 352 date/search/league combinations now use the collapsible country picker. Selected leagues can combine Serie B and both US leagues. Match details return to the originating filtered list via Matches/All matches. Mobile browser-tested examples: 29 Sep + Serie B 161:20303; 30 Sep + Brooklyn + Serie B/USL Championship 178:38884; same filters on 7 Oct empty state 178:46186. Demo limitations remain 11 dates, clickable example search and three selectable leagues. Use these integrated entry links rather than the older focused review entries.

2026-09-30 accordion update: same league-picker review entries (mobile 166:21177, desktop 166:20022). Country groups now start collapsed; existing overlay entry 166:21734 retained. Mobile US-expanded example 171:23303. Selected leagues remain visible above country list. One country expanded at a time in this prototype.

2026-09-30 scalable league-picker review: mobile entry 166:21177, desktop 166:20022; mobile both-leagues applied 166:21594; mobile selected sheet 166:22082. Separate focused review, not yet propagated into the 176 date/search states. Open Leagues, tap the country-search example, toggle USL Championship and USL League One, then Show matches. Eight applied screens and sixteen draft/search overlays; Close discards overlay changes. Other directory entries illustrative and disabled. Existing browsing flow preserved.

2026-09-30 expanded browsing supersedes the four-date limit below: entry desktop 16:355 / mobile 16:360; Brooklyn query mobile 161:21233; Brooklyn + Serie B no-results mobile 161:21535; Naples filtered to USL League One 161:30338. October calendar example 162:20602. Root names encode viewport/day/league/query; day 0 is 28 Sep, day 10 is 8 Oct. Calendar opens from the selected-date control. Filters persist across dates; selected mobile league is shown first. Prototype only: 11 dates and one clickable search query, no free typing.

2026-09-30 date browsing: Matches default 30 Sep remains desktop 16:355 / mobile 16:360. Other dates: 29 Sep 155:4471 / 155:4798; 1 Oct empty 155:4569 / 155:4883; 7 Oct 155:4667 / 155:4968. Discover Strength 16:354 / 16:359 now shows an explicitly illustrative Naples/Richmond strength example linking to 42:880 / 42:965, superseding the empty-state presentation documented below. Four fixed sample dates, not a live date picker.

2026-09-30 Team patterns parity: existing Botafogo detail entry nodes 42:550/42:635 and Naples 42:880/42:965 now include both-team tabs and venue/Overall pattern dropdowns. Overall comparison entry nodes 71:724/71:997 and 71:1806/71:2079 retain their identities. New linked state names encode fixture, comparison scope, pattern side and pattern scope. Example mobile Richmond open menu 145:8833; Botafogo Overall patterns 145:4877. Earlier notes saying only Brooklyn has this interaction are superseded.

2026-09-30 real-data flow: Strength entry nodes 16:354/16:359 now show a source-backed shortlist-empty state. Goals 22:452/22:642 shows Brooklyn and Naples saved forecasts and links to snapshot detail nodes. Matches 16:355/16:360 now lists the same three real fixtures used by Patterns. The six old Arsenal/PSV/Benfica detail roots are preserved but no longer destinations of visible active-list cards. Start with Goals 22:452 (desktop) / 22:642 (mobile) for populated forecast examples.

2026-09-30 consistency baseline: use this working page, not original page 0:1. Enter from Discover Strength desktop 16:354 / mobile 16:359, switch to Goals or Patterns using the category tabs. Current pattern format has no percentage-point difference row. D06 is now applied: league averages remain in details only. Linked older illustrative details share the event-first hierarchy but do not have Brooklyn/Detroit's complete team/scope interactions; no data was invented to fill those gaps. Original design is historical, not an implementation reference.

Dropdown revision (2026-09-29): closed entry points remain desktop `80:982` and mobile `80:1312`. Representative open Matches menus: desktop `86:3938`, mobile `86:4590`. Open Compare menus: desktop `86:4101`, mobile `86:4738`. Select an option to return to the corresponding closed state, or click the trigger again to close. All Brooklyn/Detroit team/scope combinations updated; scope data semantics unchanged.

Overall-pattern extension (2026-09-29): Brooklyn desktop `80:982`, mobile `80:1312`; Detroit desktop `80:1612`, mobile `80:1942`. Corresponding states with Overall attack/defence comparison: Brooklyn desktop `80:1147`, mobile `80:1462`; Detroit desktop `80:1777`, mobile `80:2092`. Original venue-pattern states remain linked via the panel's At home/Away toggle. Team and comparison toggles preserve pattern scope.

Team-pattern tabs added 2026-09-29 for Brooklyn vs Detroit City. Start desktop `42:715` or mobile `42:800`, then select Detroit City under Team patterns. Away-tab states: desktop `74:878`, mobile `74:1320`; with Overall comparison: desktop `74:1099`, mobile `74:1526`. Scope switching preserves the selected pattern team. Each tab shows four selected historical records. Other fixtures have not received this exploration yet.

Recorded: 2026-09-29 from the last verified Figma work in this conversation.

- [Figma project: Ballistics 2.0](https://www.figma.com/files/team/1373888400229971892/folder/662168365)
- [Design file](https://www.figma.com/design/dLgWAaafmzxQNcp2Rb9P7u)
- [Current Patterns — desktop](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=22-547&starting-point-node-id=22%3A547)
- [Current Patterns — mobile](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=22-722&starting-point-node-id=22%3A722)

## Preserved design

Page `0:1`, **Ballistics · Prototype**, contains the original eight screens. Preserve it; do not overwrite merely to advance the current design.

| Original screen | Node |
| --- | --- |
| Explorer | 2:256 |
| Evidence | 2:257 |
| Forecast record | 2:258 |
| Methodology | 2:259 |
| Saved | 2:260 |
| Mobile explorer / evidence / record | 2:261 / 2:262 / 2:263 |

## Current working page

Page `16:353`, **Discover · Football first**.

| Flow | Desktop | Mobile |
| --- | --- | --- |
| Strength advantages | 16:354 | 16:359 |
| Goals outlook | 22:452 | 22:642 |
| Consistent patterns, saved-data mocks | 22:547 | 22:722 |
| Matches, earlier illustrative fixtures | 16:355 | 16:360 |
| Botafogo SP vs Ponte Preta, saved-data detail | 42:550 | 42:635 |
| Brooklyn vs Detroit City, saved-data detail | 42:715 | 42:800 |
| Naples vs Richmond Kickers, saved-data detail | 42:880 | 42:965 |
| Botafogo SP vs Ponte Preta, Overall comparison | 71:724 | 71:997 |
| Brooklyn vs Detroit City, Overall comparison | 71:1265 | 71:1538 |
| Naples vs Richmond Kickers, Overall comparison | 71:1806 | 71:2079 |

2026-09-29: saved-data details now include both teams' four context metrics. Home/Away and Overall link in both directions. The selected pattern remains separate. Direct comparison previews: [desktop](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=42-715&starting-point-node-id=42%3A715), [mobile](https://www.figma.com/proto/dLgWAaafmzxQNcp2Rb9P7u?node-id=42-800&starting-point-node-id=42%3A800). Desktop full-layout and mobile comparison visual checks completed; native editable layers, no image-filled UI. This does not resolve the outstanding changes below.

The saved-data detail screens are separate from the older Arsenal, PSV and Benfica examples, which remain preserved. Pattern cards link to the saved-data screens, whose Discover return links lead back to Patterns.

Last visual checks covered desktop/mobile pattern compositions, the desktop unavailable-forecast state and a populated mobile detail screen. Font and prototype destination checks passed. This is not a claim that every interaction was browser-tested or that the latest agreed navigation changes are complete.

Current outstanding differences from agreed design (updated 2026-10-02): D06, Methodology access and Forecast record access are done in Figma (see 2026-09-30 and 2026-10-02 entries above). Saved still has no screen or entry in the current flow; only the original frame 2:260 exists. The earlier note listing league averages and all three destinations as outstanding is superseded.
