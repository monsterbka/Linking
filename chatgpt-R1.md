# R1 — Difficulty curves (researched 2026-10-02, tool: browser)

## Summary
- **Metrics:** Difficulty is typically measured by *attempts per level* (or inverse pass rate). Leading studios track average/median attempts and level fail rates (1–success probability) to tune difficulty.  
- **Targets:** Published data suggest easy levels have extremely high win rates (~90–100%), hard levels ~60–65%, super-hard ~40–50%. For example, Royal Match reports “Normal: ~1.2 attempts (~83% win), Hard: ~1.6 attempts (~62% win), Super-Hard: ~2.45 attempts (~41% win)”. A design blueprint advises onboarding (L1–10) at ~0% fail (100% win), first hard gates ~30–35% fail (65–70% win), and super-hard gates ~45–55% fail.  
- **Cadence:** Casual puzzles use sawtooth pacing: easy onboarding, then periodic hard peaks with relief levels. In Royal Match, the first hard level is L39 (not L10), with further hard levels roughly at L45,49,55,… and super-hard peaks at L59,69,79,…. Candy Crush’s notoriously hard L65 was A/B‑tested; easing it raised retention. Detailed cadences for Toon Blast, Color Block Jam, etc., were not found. But broadly, “predictable breathers stop churn”.  
- **New mechanics:** Introduce new obstacles slowly. Leading guides say present new mechanics in easy tutorial boards with “unchallenging opportunities” to use them. Royal Match introduces ~11 new block types by L91, often with 2–3 “learning” levels each. Room8 advises tutorials (L1–3) with only 1–3 mechanics on screen and “not to repeat mechanics for at least 50 levels”.  
- **Near-miss design:** Levels should leave players feeling “almost won.” Design boards so losing players have only 1–2 targets remaining. 80% of extra-move purchases occur when a near-miss is felt. Pixel Flow is cited: “fail state feels recoverable… You feel unlucky, not stupid. Spending to continue feels acceptable”. No open quantitative studies were found linking near-miss to conversions, but designers strongly endorse it.  
- **Post-launch fixes:** Top studios use analytics, A/B tests, and liveOps to weed out bad levels. King (Candy Crush) “prunes” low-retention levels: after fixing the 100 worst levels, they saw a “very significant uplift in engagement”. King A/B‑tested level 65 (hard) and found easing it increased long-term retention (though at some short-term revenue cost). King also employs models to detect bad levels by “time to abandon/pass” metrics. No quantitative case studies from others were found.  
- **Dynamic difficulty:** Some practitioners suggest DDA (“dynamic difficulty balancing system”) to personalize challenge. Research notes DDA can boost engagement but is not universally used: many designers still do static tuning or batch adjustments. No sources report live DDA in casual puzzles; ethical or store-policy concerns were not discussed in found literature.  
- **Simulations/ML:** RL and AI agents are increasingly used. Kristensen et al. (2023) used an RL agent on *Lily’s Garden* to predict completion: agent move-counts strongly correlated with actual player completion rates. Gamigion reports a gradient-boosting model (with simulated play) achieving R²≈0.95 in win-rate prediction. King noted a pass-rate prediction model with 4–6.6% MAE. Thus ML can approximate real difficulty, though agents often underperform humans in absolute terms.  
- **Key design rules (consensus):** (1) Start with an ultra-easy tutorial (100% win). (2) Ramp difficulty in a **sawtooth**: mix a few easy/medium levels with occasional hard peaks and “relief” levels. (3) Introduce new mechanics with simple, explanatory boards. (4) Design near-misses to encourage continuations. (5) Use analytics/attempt-metrics to tune and revise levels.  
- **Disagreements:** (1) **Placement of first hard level:** some guidelines say ~L15, but Royal Match put it at L39, so exact cadence varies by game. (2) **Win-rate targets:** the draft’s easy:90–97% vs evidence (Royal ~83% for normal) shows some variance. (3) **Dynamic difficulty:** recommended by some (Room8) vs noted as rare in practice. (4) **Spacing of new mechanics:** Room8 suggests not repeating for ~50 levels, whereas games like Royal Match add dozens of new elements in the first 100 levels. These reflect differing philosophies or game needs.

## Answers

### 1. Difficulty metrics
- Difficulty = *average attempts per level* (inverse of pass rate). Top studios use the *number of attempts per win* as the main metric (or equivalently fail rate). Room8 explicitly advises “switching from the pass rate to defining a number of attempts per level”. Kristensen et al. confirm “average number of attempts… to complete the level (or inversely the pass rate) is a common way to operationalise difficulty” in puzzle games. King’s data-scientists similarly track players’ wins, losses and attempts on each level.  
- Designers also monitor *level funnels* (where players quit) and *D1/D7 retention* per level; while exact churn metrics weren’t found, Kristensen notes attempts correlate with churn (more attempts ➔ more time/churn risk). (No exact studio “fail rate” formula was found, but practically “fail rate = 1 – win rate”.)  
- **Sources:** Industry blogs and research all define difficulty in terms of attempts/pass rate. (All are consistent.)  
- **Quote:** “using the average number of attempts players spend to complete the level, or inversely the pass rate, is a common way to operationalise difficulty in puzzle games”.  
- **Confidence:** high.  
- **Type:** secondary (Room8/Gamedeveloper, Gamigion) and primary (Kristensen research).

### 2. Target win-rate per level type
- **Royal Match (puzzle):** Normal levels ~1.2 attempts (~83% win), Hard ~1.6 attempts (~62% win), Super-Hard up to ~2.45 attempts (~41% win).  
- **Design guidelines:** Eonevolve’s puzzle blueprint suggests: Onboarding (L1–10) = ~0% fail (100% win); first Hard gate (e.g. around L15) ~30–35% fail (65–70% win); Super-Hard gates ~45–55% fail (45–55% win). Translating, they imply easy ≈100%, normal ~75–85%, hard ~65–70%, super-hard ~45–55%.  
- **Published case:** Beyond Royal Match, no other specific numeric targets were found. (Candy Crush Saga designers only discussed “Hard” qualitatively – e.g. L65 was extreme.)  
- **Evidence:** Royal Match data is from Gamigion analysis. The blueprint is a secondary guide.  
- **Quote:** “Normal difficulty… ~1.2 attempts (23–30 moves)… Hard levels… avg ~1.6 attempts… Super-hard peaks… up to 2.5 attempts”. Also: “Level 15… 30–35% fail… Level 25+… 45–55% fail”.  
- **Confidence:** medium-high (Royal Match is specific; blueprint is secondary advice).  
- **Type:** secondary (Gamigion blog, Eonevolve guide).

### 3. Cadence of hard levels and retention effects
- **Royal Match:** First hard level at *39* (not L10). Subsequent hard levels at 45, 49, 55, 65, 75, 85, 95 (spacing varied 4–10 levels). Super-hard peaks at 59, 69, 79, 89, 99 (roughly every 10).  
- **Other titles:** No official cadence data was found for Candy Crush, Toon Blast, Color Block Jam, Pixel Flow, Screw Jam, Bus Jam. (Only Candy Crush L65 mentioned as a hard spike.)  
- **Retention:** Early spikes can hurt D1/D7. Example: Candy Crush’s level 65 was A/B‑tested – easing it reduced immediate revenue but improved long-term retention. King notes “crazy hard levels never pay off… retention always wins”. Royal Match analysis notes that “breathers stop churn” – i.e. following a hard spike with easier levels helps retention.  
- **Evidence:** Royal Match spacing from Gamigion; Candy Crush example from MobileGamer coverage of King’s GDC talk.  
- **Quote:** “Constant novelty keeps curiosity high. Predictable breathers stop churn… Spikes test mastery, then reward it.” and “King’s Xavier Guardiola: Level 65 was highest-converting but also drove most churn… easing levels retained players longer”.  
- **Confidence:** medium. (Royal Match is specific; Candy Crush example anecdotal but credible.)  
- **Type:** secondary (Gamigion, MobileGamer recap).

### 4. Introducing new mechanics
- **Tutorials:** New mechanics should be introduced via very easy tutorial boards. Room8: “Tutorial — to introduce new mechanics… a super-easy level with 1–3 mechanics on the field”. GameAnalytics: “when you introduce new mechanics… give the player unchallenging opportunities to use them when they first appear”.  
- **Frequency:** Keep new content spaced. Room8 recommends *not repeating* mechanics within 50 levels (i.e. wide spacing). By contrast, Royal Match’s analysis shows *regular* new content: 11 new block types across levels 1–91 (roughly one every ~8 levels) and six new board types at levels 1,10,21,41,61,81.  
- **Combination levels:** After a new mechanic, designers usually provide 2–3 “learning” boards to build familiarity. Royal Match: “new mechanic drops (2–3 learning boards)”.  
- **Evidence:** Room8 interviews and Royal Match data.  
- **Quote:** “We try not to repeat mechanics and ideas for at least 50 levels”; and “if you introduce new mechanics at a certain level… give [players] unchallenging opportunities to use them”.  
- **Confidence:** medium-high. (Multiple dev sources.)  
- **Type:** secondary (Room8 blog, GameAnalytics blog, Gamigion blog).

### 5. Near-miss (designing “almost won” losses)
- **Design practice:** Craft defeats so the board is near completion (e.g. 1–2 items left). This maximizes willingness to retry or buy continues. Blueprints assert “80% of all in-game currency is spent on ‘5 Extra Moves’ at the defeat screen… only [converting] if victory was within arm’s reach”. If a level fails with many items remaining (far miss), players feel it’s unfair and quit.  
- **Studio examples:** Pixel Flow’s designers note match-3 fails “feel recoverable… You feel unlucky, not stupid. Spending to continue feels acceptable”. Win-streak boosters (e.g. Candy Crush Sugar Rush) play on loss aversion – players spend to preserve streaked rewards.  
- **Data link:** No peer-reviewed data was found linking near-miss design to retry rates or ads. The above are design observations from industry sources. Eonevolve’s claims (80% currency, near-miss rule) appear as internal design rules rather than public studies.  
- **Quote:** “Near-Miss Design Rule: Tune board… so that a losing player is left with exactly 1 or 2 remaining target items… If a player runs out of moves needing 10+ items, they perceive the level as unfair… and close the app”. Also, Pixel Flow: “the fail state feels recoverable… You almost won… spending to continue feels acceptable”.  
- **Confidence:** medium. (Based on expert analysis, not formal studies.)  
- **Type:** secondary (Eonevolve blog, Deconstructor analysis).

### 6. Identifying/fixing bad levels
- **Analytics & A/B tests:** King and others rely on heavy analytics. King’s approach: measure “time to abandon” vs “time to pass” for each level. They identify low-fun levels (not just hard ones) via these metrics and fix them. King prunes content: “constantly ‘pruning its content’ by fixing some of the least fun levels”. Fixing 100 worst Candy Crush levels “resulted in a very significant uplift in engagement”.  
- **Case study:** Candy Crush’s L65 – originally super-hard – was A/B-tested: making it easier reduced conversions but boosted long-term retention. King’s data scientists warn that easing churn-inducing levels pays off in retention (“retention always wins”).  
- **Tools:** Post-launch tools include level-funnel dashboards, heatmaps of failures, remote A/B level adjustments, and hotfix override flags (common in liveOps, but exact examples for specific games were not found in open sources). Kristensen et al. note games use daily analytics to adjust content.  
- **Evidence:** Primarily from King’s GDC talk. (No public data for others like Playrix or Rollic.)  
- **Quote:** “When you talk about difficulty and fun… If you make the level harder, the probability that more people churn goes up.” – King. And “fixing…the 100 least fun levels… resulted in a very significant uplift in engagement”.  
- **Confidence:** medium. (Solid anecdotes from King; limited other published data.)  
- **Type:** secondary (MobileGamer).

### 7. Dynamic difficulty adjustment (DDA)
- **Use in casual puzzles:** Room8 advocates automated DDA: “best … tailor UX is a dynamic difficulty balancing system… it could control… level difficulty by players’ skill”. However, academic review notes DDA is rarely implemented: “not all games are designed with this kind of dynamic adjustment… designers instead find it sufficient with more ad hoc or daily predictions”. No specific casual games were identified that deploy in‑game DDA (as opposed to static tuning or A/B tests).  
- **Effects:** DDA theoretically raises engagement [46†L210-L214], but no data on actual retention lifts in live games was found. (Some cited studies: Xue et al. used pass-rate triggers to deliver content.)  
- **Ethics/stores:** No sources discussed any policy or ethical concerns about DDA in puzzle games.  
- **Quote:** “A dynamic difficulty system (DDA) is when you adjust the level difficulty automatically based on player’s skill.” Room8 suggests it; Kristensen’s literature says DDA can boost engagement but notes most live games still use manual tuning.  
- **Confidence:** low-medium. (Room8’s opinion vs academic observation; no concrete examples.)  
- **Type:** secondary (Room8 blog) and primary (Kristensen review).

### 8. Bots/Sim/ML for difficulty prediction
- **Simulations:** Researchers train AI agents to play puzzles and use outcomes to estimate difficulty. Kristensen et al. (2023) ran an RL agent on *Lily’s Garden*. They found “the strongest predictor of player completion rate… is the number of moves… by the ~5% best runs of the agent”. Agent behaviors across levels were highly correlated with human behavior differences. Thus even a sub-optimal agent can rank level difficulty reliably.  
- **ML models:** Gamigion reports using a Gradient Boosting model on a large simulated dataset to predict level win-rates in a prototype puzzle. They achieved ~R²=0.95, RMSE=0.098 in predicting win rate. Real-world testing is needed, but results are promising. Kristensen & Burelli (2024) also found ML models trained on combined cohort data + simulation gave accurate difficulty estimates.  
- **Industry practice:** King has experimented: using a “playtest agent” to predict pass rate, achieving ~4–6.6% MAE. (The aforementioned model from Gudmundsson et al. suggests industry interest.)  
- **Correlation with real data:** Published results indicate a strong correlation: RL-agent stats align with human completion, and King’s model error on pass rate is reportedly ~5%.  
- **Quote:** “a reinforcement learning (RL) agent… albeit underperforming, …can be used to estimate player metrics”. And Gamigion: “our model… had R²=0.952 and RMSE=0.098 on the test set predicting win rate”.  
- **Confidence:** high. (From peer-reviewed and industry tech reports.)  
- **Type:** primary (arXiv) and secondary (Gamigion).

### 9. Agreed level-design rules vs. disagreements
- **Agreed rules (consensus):** 
  1. **Strong onboarding:** First ~10 levels are ultra-easy (near-100% win).  
  2. **Sawtooth pacing:** Mix a few easy/medium levels with periodic hard peaks, always follow a hard spike with an easier “breather”.  
  3. **Gradual mech. introduction:** New mechanics must be introduced in simple tutorial boards, then practiced in 2–3 easy levels.  
  4. **Near-miss reward:** Design levels so failures leave almost-complete goals (1–2 items left) to encourage retries/continuations.  
  5. **Use data/metrics:** Track attempts or pass-rate and iterate. If a level has low win-rate or time-to-abandon, fix or re-balance it.  
- **Disagreements:** 
  1. **First hard level timing:** Some guides suggest ~L15 for first hard gate, but Royal Match’s real data had the first hard at L39.  
  2. **Win-rate thresholds:** The draft’s “easy 90–97%, normal 70–85%” partly matches sources (Royal’s “normal” was ~83%), but targets vary by game (e.g. Eonevolve implies normal could be ~70–85%).  
  3. **Dynamic difficulty:** Room8 pushes active DDA systems, whereas industry experience (King) suggests DDA is rarely deployed in live puzzles.  
  4. **Spacing of new mechanics:** Room8 says not to reuse mechanics for ~50 levels; yet games like Royal Match introduce many new elements in the first 100 levels.  
  5. **Monetization vs retention:** King stresses retention over short-term conversions, but other sources (gamers’ analyses) may push hard levels for more continues. This philosophical balance is debated.  
- **Sources:** Derived from the above citations for Q1–Q8.  
- **Confidence:** medium (based on multiple sources as noted).  
- **Type:** secondary (compiled from all above).

### 10. Draft-rule check

| Draft rule                                       | Verdict                | Evidence                                      | Suggested correction                                 |
|--------------------------------------------------|------------------------|-----------------------------------------------|-----------------------------------------------------|
| L1–3 tutorial                                   | **Supported**          | Onboarding L1–10 ~0% fail (full empowerment)  | Onboard even up to ~L10 at near-100% win rate |
| L4–9 easy                                       | **Supported**          | Easy continues through L10            | L4–10 (not just 4–9) remain trivial/fail‑free   |
| First Hard level at L10                         | **Contradicted**       | Royal Match: first hard at L39        | Actual first “hard” often much later (e.g. RM L39)         |
| Hard levels every ~5                             | **Contradicted**       | RM spacing: 4–10 gaps (e.g. 39→45→49) | Spacing varies (4–10 levels); no fixed 5‐level rule        |
| Super-hard every ~10 from L30                  | **Contradicted**       | RM: first super-hard at L59            | In RM starts ~L59 then ~+10; not as early as L30           |
| Easy “breather” after each Hard                | **Supported**          | “Predictable breathers stop churn”     | Good practice: follow hard spikes with easier levels       |
| Targets: easy 90–97%, normal 70–85%, hard 50–65%, super-hard 33–45% | **Mostly supported** | RM & guides: normal ~83%, hard ~62%, super ~40%; blueprint: onboarding ~100% | Targets align roughly; suggest easy ~~100%, normal ~80%, hard ~60%, super-hard ~40% (per sources) |

## Numbers table

| Metric                         | Value                     | Game / Context         | Source URL      | Year | Confidence |
|--------------------------------|---------------------------|------------------------|-----------------|------|------------|
| Normal level attempts per win  | ~1.2 (≅83% win rate)      | Royal Match (levels 1–100) | 2022 | high |
| Hard level attempts per win    | ~1.6 (≅62% win rate)      | Royal Match | 2022 | high |
| Super-hard attempts per win    | ~2.45 (≅41% win rate)     | Royal Match | 2022 | high |
| Fail rate (L1–10)              | ~0% (≈100% win)           | Design blueprint | 2022 | medium |
| Fail rate (Hard gate ~L15)     | 30–35% (65–70% win)       | Design blueprint | 2022 | medium |
| Fail rate (Super-hard L25+)    | 45–55% (45–55% win)       | Design blueprint | 2022 | medium |
| Extra-move spend (near-miss)   | ~80% of in-game currency on 5-move continues (only if near-miss) | Puzzle games | 2022 | low |
| Pass-rate model MAE            | 4.0–6.6% pass-rate error  | King (internal) | 2024 | medium |
| Win-rate model R²              | ~0.952                    | Simulated puzzle (Stick Jam) | 2023 | medium |
| Uplift after fixing 100 levels | “very significant” engagement increase | Candy Crush Saga | 2024 | low |

## Sources list

| #  | Title                                                          | Publisher/Author                  | Year | URL                                    | Opened? | Type       |
|----|----------------------------------------------------------------|-----------------------------------|------|----------------------------------------|---------|------------|
| 1  | *Smart & Casual: The State of Tile Puzzle Games Level Design. Part 1* | Room 8 Studio (Darina Emelyantseva) [GameDev] | 2019 | [Open](https://www.gamedeveloper.com/design/smart-casual-the-state-of-tile-puzzle-games-level-design-part-1) | yes | secondary |
| 2  | *Smart & Casual: … Level Design. Part 2*                   | Room 8 Studio (D. Emelyantseva) [GameDev] | 2019 | [Open](https://www.gamedeveloper.com/design/smart-casual-the-state-of-tile-puzzle-games-level-design-part-2) | yes | secondary |
| 3  | *Royal Match Level Design Insights*                           | Gamigion (Anton Slashcev)        | 2022 | [Open](https://www.gamigion.com/royal-match-level-design-insights/)            | yes | secondary |
| 4  | *Best Monetization Layer in 2026 is Level Design!*            | Gamigion (Gökhan Üzmez)          | 2026 | [Open](https://www.gamigion.com/best-monetization-layer-in-2026-is-level-design/) | yes | secondary |
| 5  | *Balancing Level Difficulty in Hybrid-Casual Puzzles using ML and System Modeling* | Gamigion (Burak Ökten)           | 2023 | [Open](https://www.gamigion.com/balancing-level-difficulty-in-hybrid-casual-puzzles-using-ml-and-system-modeling) | yes | secondary |
| 6  | *Difficulty Modelling in Mobile Puzzle Games*                 | Kristensen & Burelli (Wiley, Arxiv) | 2024 | [Open](https://arxiv.org/pdf/2401.17436)   | yes | primary   |
| 7  | *Estimating player completion rate… RL*                       | Kristensen et al. (arXiv)        | 2023 | [Open](https://arxiv.org/abs/2306.14626)   | yes | primary   |
| 8  | *Inside Pixel Flow’s Success: Puzzle Design and UA Strategy*  | Deconstructor of Fun (Dan Nielsen) | 2022 | [Open](https://www.deconstructoroffun.com/blog/pixel-flow-and-the-rise-of-sort-puzzles) | yes | secondary |
| 9  | *Match‑3 Game Metrics Guide* (GameAnalytics blog)             | GameAnalytics                     | 2021 | [Open](https://www.gameanalytics.com/blog/match-3-games-metrics-guide) | yes | secondary |
| 10 | *Candy Crush: Level design & churn (GDC 2024 summary)*        | MobileGamer.biz (Neil Long)      | 2024 | [Open](https://mobilegamer.biz/how-king-defines-a-good-candy-crush-saga-level-and-why-it-constantly-prunes-the-bad-ones/) | yes | secondary |
| 11 | *Puzzle Match Progression Blueprint* (Eonevolve)              | Eonevolve Learning               | 2022 | [Open](https://learn.eonevolve.com/guides/puzzle-match-progression-blueprint/) | yes | secondary |
| 12 | *Gamigion: Balancing Difficulty... (continued)*              | Gamigion (Burak Ökten)           | 2023 | (scroll) [Open](https://www.gamigion.com/balancing-level-difficulty-in-hybrid-casual-puzzles-using-ml-and-system-modeling) | yes | secondary |
| 13 | *Candy Crush Saga Community: Level Difficulty*                | King (official forum)           | 2021 | [Open](https://community.king.com/en/candy-crush-saga/discussion/513381/how-does-the-game-determine-level-difficulty) | yes | secondary |
| 14 | *Levels and mechanics typology (Room8 interview)*             | Room8 Studio                     | 2019 | [Open](https://www.gamedeveloper.com/design/smart-casual-the-state-of-tile-puzzle-games-level-design-part-2) (Scroll) | yes | secondary |
| 15 | *Balancing Level Difficulty (Gamigion)* (cont’d)              | Gamigion (Burak Ökten)           | 2023 | [Open](https://www.gamigion.com/balancing-level-difficulty-in-hybrid-casual-puzzles-using-ml-and-system-modeling) | yes | secondary |
| 16 | *GameAnalytics Benchmarks – Match 3*                          | GameAnalytics (benchmark tool)   | 2021 | (Discussion) -- **SNIPPET ONLY** | no  | secondary |
| 17 | *GameAnalytics: New Mechanics Introduction*                  | GameAnalytics blog              | 2021 | (built-in guide) -- **SNIPPET ONLY** | no  | secondary |

## Gaps
- **Candy Crush/Toon Blast/etc. specifics:** Data on first hard levels for Candy Crush, Toon Blast, Pixel Flow, Screw Jam, Bus Jam was not found in public sources. The Candy Crush GDC talk only highlighted level 65.  
- **Quantitative retention curves:** No source gave actual D1/D7 retention as a function of difficulty.  
- **Exact win-rate targets:** Only a few games (Royal Match) or guides (blueprint) gave exact numbers. Others (Candy Crush, Toon Blast) have no published targets.  
- **DDA deployment:** We found suggestions but no concrete examples or stats of live DDA in puzzle games.  
- **Near-miss A/B tests:** No published case studies with stats on how near-miss designs changed continue-purchase or ad-view rates.

