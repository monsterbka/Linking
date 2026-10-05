# P3 — Cheap A/B testing of playable ads (researched Oct 05 2026, tool: browser)  

## Summary  
- **Controlled vs algorithmic testing:** Meta Ads (Facebook/Instagram) and Google App Campaigns support true A/B split tests (Meta’s **Experiments** splits audiences; Google’s directional experiments evenly split traffic among video assets).  Other networks like Unity Ads and AppLovin MAX rely on their own algorithms: Unity recommends using separate “test” campaigns to hold creatives constant, and AppLovin’s Axon engine automatically deprioritizes weaker variants.  
- **Sample size:** Statistical formulas show thousands of impressions are needed.  For example, detecting a jump from 1.0% IPM to 1.5% IPM (~IPM 10 → 15) at 95% confidence/80% power requires on the order of 7,700 impressions per variant.  (The A/B test formula is *n = (Z<sub>α</sub>+Z<sub>β</sub>)²·[p₁(1–p₁)+p₂(1–p₂)]/(p₂–p₁)²*.)  In general, baselines of 0.5–2% convert rates demand thousands of events; for a 5%→7.5% lift in CVR, ~1,500 installs per group are needed.  
- **Cost benchmarks:**  Tier-1 markets (US/EU) see high CPMs and CPIs (playable CPI ~~$1.50–$4 in 2026).  Emerging markets are far cheaper: for example, an analysis found Android playable CPI ~$1.41 (vs $3.66 iOS), implying sub-$1 CPIs in India/SEA.  Cheap geos often have CPMs ~$1–3.  Thus achieving ~10k impressions might cost on the order of \$10–\$30 per variant in those regions.  
- **Key metrics:**  The primary metric to compare creatives is **IPM** (installs per 1,000 impressions) (directly tied to CPI via CPI=CPM/IPM).  **CPI** (cost per install) and **Install Rate** (installs after engagement) measure efficiency.  Secondary “diagnostic” metrics include **Interaction Rate** (the % of views with user engagement), **Completion Rate** of the playable, and **D1 retention** of installs.  Ultimately choose the creative with higher IPM (or lower CPI) and acceptable D1/D7 retention.  
- **In-playable event tracking:**  Networks generally do *not* report granular in-ad events by default; they only count impressions, clicks/CTAs, and installs.  Facebook’s playable format requires an explicit API call (`FbPlayableAd.onCTAClick()`) to register a CTA tap.  Unity Ads requires using `mraid.open(url)` on the CTA so the tap is captured.  Events like “first touch” or “level reached” must be tracked by the ad’s own code (e.g. firing MRAID tracking events or custom analytics), since network dashboards won’t show them (NOT FOUND in public docs).  
- **Playable vs video/end-card tests:**  Use controlled methods to avoid algorithm bias. If testing playable *vs.* video, run them in separate test campaigns or use an official A/B tool.  Avoid intermixing in one campaign without control – the network will quickly shift spend to the better-performing format.  Likewise for “end-card only” vs full playable: treat them as separate variants.  Let each creative run long enough to exit learning (Unity suggests ~10k impressions) and don’t judge before ~48h.  Monitor for **creative fatigue**: Unity advises running new creatives ~10–14 days post-learning and dropping any with <10% impression share.  
- **Stop/decision criteria:**  A practical rule is: stop when one variant’s IPM is ~2× the other’s (with ≥500 impressions) or when both have ~5,000 impressions and their IPMs are within ~10%.  Pick the winner by higher IPM (and lower CPI); validate that D1/D7 quality is not worse.  If no clear lift emerges, assume no significant creative difference.  
- **Test plan outline (solo dev):** Run tests in a low-cost geo (e.g. India/SEA) using a network that allows traffic split or paired campaigns (Meta experiments, Google directional video test, Mintegral AB, or Unity Ads with parallel “test” campaigns).  Use modest budget (e.g. \$20–50 per variant) and aim for several thousand impressions each.  Run for ~3–7 days (min 2 days).  Monitor IPM/CPI daily; stop when criteria met.  

## Answers

### 1. Traffic-splitting vs algorithmic networks  
- **Meta (Facebook/Instagram Ads):** Supports true A/B split testing via the Experiments tool.  Meta *randomly and exclusively* assigns each user to one variant so that creative groups have non-overlapping audiences.  (By default splits 50/50.)  This ensures a fair comparison of CTR/CVR/IPM by audience.  *Force fair test:* Use Meta’s A/B Experiments, which automatically split the audience.  
- **Google App Campaigns (UAC):** Offers “Directional experiments” for video assets.  Up to 20 videos can be tested head-to-head, and Google *aims to give each test group an equal share of impressions*.  This yields a controlled A/B environment for video ads.  *Force fair test:* Use a Google App Campaign experiment (separate “test” campaign) to evenly split traffic.  
- **Mintegral:** Provides a built-in **Creative A/B Testing** feature.  Advertisers can define 1 control + up to 4 test variants in one campaign setup.  The platform automatically divides traffic to each group.  *Force fair test:* Use the Creative A/B tool (in Mintegral’s Delivery dashboard); it requires identical offer settings and uses traffic allocation to each variant.  
- **Unity Ads:** No native A/B split. Unity uses algorithmic optimization (SOV-based).  Unity’s documentation explicitly advises creating *separate “Creative Testing” campaigns* to compare creatives without interference.  In practice, you’d duplicate your campaign (same targeting), each with one creative, to simulate a controlled test.  
- **AppLovin/MAX:** Uses its Axon algorithm to choose top-performing creatives.  There is no manual split; the system automatically spends on winners.  (As one guide notes, you should *not* manually pause “losers” because Axon will deprioritize them anyway.)  *Force fair test:* The only way is to run two separate campaigns or control budgets manually; in a shared campaign Axon biases in favor of the currently best CTR/CVR creative.  
- **Snapchat, TikTok, others:** (No specific public info found.)  Snapchat does have “Split Campaigns” but details are limited.  TikTok’s Business Center allows A/B tests, but no known docs for playables.  In general, if a network offers a split-test tool (like Meta/Google/Mintegral), use it; otherwise run parallel campaigns for each variant to approximate a 50/50 split. (No quotes found for these networks.)  

*Sources:* Networks’ official docs and guides.  *Confidence:* 0.9.  (Meta/Google/Mintegral facts are from official sources; Unity/AppLovin info from vendor guidance.)  

### 2. Sample size for IPM/CVR differences  
- **Formula:**  For two variants with conversion rates *p₁* and *p₂*, the required sample per group is  
  > *n ≈ (Z<sub>α</sub>+Z<sub>β</sub>)²·[p₁(1–p₁)+p₂(1–p₂)]/(p₂–p₁)²*,  
  where Z<sub>α</sub>=1.96 (95% CI) and Z<sub>β</sub>=0.84 (80% power).  (This assumes equal-sized groups and two-tailed test.)  
- **Example (IPM):**  Suppose baseline IPM=10 (1.0% CVR) and variant =15 (1.5%).  Then p₁=0.010, p₂=0.015.  Plugging into the formula gives ~7,700 impressions per variant (calc from).  Thus, ~15,400 total imps would be needed to detect this 50% lift with 95% confidence (Power 80%).  (Smaller lifts or higher variability require more impressions.)  
- **Example (CVR):**  For click-to-install CVR: if p₁=5% and p₂=7.5% (change of +2.5p), the same formula yields ~1,500 installs per variant.  (In other words, you’d need ~1.5k installs in each group to detect that CVR change.)  
- **General Rule:**  These calculations imply that aiming for *several thousand impressions* (or installs) per creative is common.  For instance, one example found ~31,200 per variant for a 5%→5.5% lift.  In practice, ~5k–10k impressions per creative is a useful benchmark for IPM in the 5–15 range.  

*Source:* Standard A/B test sample size formula.  *Quote:* “n = (Z<sub>α</sub> + Z<sub>β</sub>)²·[p₁(1–p₁)+p₂(1–p₂)]/(p₂–p₁)²”.  *Confidence:* 0.9 (formula from expert blog; examples calculated from it).  

### 3. Cost benchmarks (CPM/CPI and budget)  
- **Tier-1 markets (US/EU):**  Playable ad CPI is relatively high.  One benchmark (2026) cites casual game playable CPI ≈\$1.50–4.00 in top markets.  Video ads are even costlier (~\$2–6 CPI).  Corresponding CPMs are on the order of \$20–40 (implied by CPI=CPM/IPM and typical IPMs of ~1–2%).  
- **Low-cost geos:**  Countries like India, Southeast Asia, Eastern Europe, and LATAM have much lower rates.  For example, a 2022 report found **Android** playable CPI ≈\$1.41 (vs \$3.66 on iOS).  That suggests India/SEA CPIs **well under \$1** for playables, and CPMs often ~\$1–5.  (E.g. at \$1 CPM, 10k impressions cost only \$10.)  
- **Budget per variant:**  Combining the above, a creative test with a few thousand impressions might cost only tens of dollars in cheap geos.  For example, 7,700 imps at \$1 CPM ≈\$7.70 per variant.  Even at \$3 CPM, 10k imps is \$30.  The major cost driver is CPI: at \$0.50–1.00 CPI, even hundreds of installs cost only \$250–\$500.  

*Sources:* Industry benchmarks.  *Quotes:* “Casual (game) playable CPI: \$1.50–4.00 (tier-1, 2026)”; “Android playable CPI \$1.41 vs iOS \$3.66.”  *Confidence:* 0.7 (available data is limited to some sources; actual values vary by campaign).  

### 4. Key metrics (which decide vs diagnostic)  
- **IPM (Installs per Mille):** Installs per 1,000 impressions.  *Primary success metric.* “IPM…installs per 1,000 impressions. The primary volume signal”.  It captures the net effect of CTR and CVR on installs.  Compare creatives by IPM (higher = better).  
- **Interaction Rate:** Percent of impressions in which the user performs the first engagement (tap, swipe, etc).  Reflects how “sticky” the creative is. Defined as “% of impressions where user engages with the mechanic”.  (In video ads, this is akin to CTR; in playables, it is like engagement rate.)  Use it to diagnose *initial interest*.  
- **Install Rate (post-engagement CVR):** Of users who engaged (saw CTA), what % clicked to install.  Listed as “Install Rate: % tapped CTA after engaging”.  This is the funnel conversion after initial engagement.  
- **CPI (Cost per Install):** Dollars spent per install.  The ultimate efficiency metric, determined by *CPM/IPM*.  As noted, **CPI = CPM ÷ IPM**.  Lower CPI is better.  If you have a fixed budget, you’d choose the variant giving the lowest CPI.  
- **Retention/Quality (D1/D7):** Post-install quality metrics (e.g. D1 or D7 retention) should be checked after the test.  A creative that drives installs but yields very poor retention may be a false positive.  We use D1/D7 to verify quality, not to choose the winner directly in the A/B.  
- **Other metrics:** CTR (for video ads) and “Completion Rate” (fraction of users who finish the playable or video) are secondary. They help diagnose whether people lose interest, but do not directly decide the winner.  

*Sources:* Playable-ads analytics guide.  *Quotes:* “IPM…primary volume signal”; “Interaction Rate: % of impressions where user engages”; “CPI = CPM ÷ IPM”.  *Confidence:* 0.9.  (Metrics from an industry guide; interpretations are common practice.)  

### 5. Tracking in-playable events  
- **Network support:**  Almost no major UA platform directly reports fine-grained in-playable events to advertisers.  By default, networks only report ad **impressions**, **CTA clicks** (view-to-store clicks), and resulting **installs**.  For example, Facebook’s playable ad format does not log “first hit” or “level completion” in Ads Manager.  (We found no documentation of any network auto-tracking such events.)  
- **Facebook/Meta:** For playables on Facebook/Audience Network, the only special API is `FbPlayableAd.onCTAClick()`.  The PlayCanvas dev guide notes you must call this function on the CTA tap so Facebook registers the store click.  Aside from that, no further events (like first-play or end-card) are surfaced.  
- **Unity Ads:** Requires use of MRAID.  Unity injects `mraid.js`, and your HTML must call `mraid.open(url)` on the CTA tap.  This lets the SDK count a click/install event.  Unity does *not* report intermediate events (like “level reached 2”) to the campaign console.  
- **Custom events:**  If you need to track in-ad events (first touch, level reached, etc), you must implement them *inside the playable creative* (e.g. using MRAID `fireTrackingEvent` or sending a network request to your analytics when an event occurs).  These can be captured in your own analytics or MMP, but they **won’t appear** in the ad network’s dashboard.  

*Sources:* Network docs and guides.  *Quotes:* “Have you added…`FbPlayableAd.onCTAClick()` as part of your CTA callback?”; “creative must call `mraid.open(url)` on CTA tap”.  *Confidence:* 0.8.  (The CTA APIs are documented; absence of other events is implied by lack of sources.)  

### 6. Methodology and pitfalls (playable vs video, end-card)  
- **Controlled setup:** To compare playable vs video (or end-card vs full playable), keep all other variables constant (audience, budget, etc).  Best is to use a dedicated A/B test or separate campaigns for each variant.  *Pitfall:* If you simply add multiple creatives to one campaign and let the optimizer run (creative optimization), it will allocate most spend to the initially better ad, starving others and invalidating the test.  
- **Algorithm bias / learning phase:**  Machine-learned bidding systems require time to calibrate.  Playables often under-deliver initially.  One guide warns: *“Don’t stop tests during the first 48 hours: ML bidding systems deprioritize new creatives… which artificially depresses IPM”*.  Unity’s docs likewise say each creative needs ~10,000 impressions (learning phase) before stable results.  *Pitfall:* Judging a creative “loser” too early is risky.  
- **Creative fatigue:**  Over time an ad can burn out.  Unity recommends continuing to run a creative for about 10 days after the learning phase; if it fails to reach ~10% of impressions in that time, drop it.  *Pitfall:* A creative that started strong may decay (users see it repeatedly); conversely, a new creative might perform poorly at first but improve, so give equal test duration.  
- **Version variants (end card vs full):**  When testing an interactive end-card vs a full playable, treat them like separate creatives.  The end card alone often has higher click-through but may lower engagement.  Ensure the test isolates just that change.  *Pitfall:* Combining video+playable vs playable-only can confound results, as the funnels differ.  
- **Stop rules:** Follow objective rules.  One recommended rule: stop when one variant’s IPM is ~2× the other’s **and** it has ≥500 impressions, or when each has ~5,000+ impressions and IPM difference is <10%.  Don’t terminate a test prematurely because the algorithm hasn’t fully explored each creative.  

*Sources:* Unity and marketing guides.  *Quotes:* “Don’t stop tests during the first 48 hours: ML bidding…deprioritize new creatives”; “Once creative packs are live, they enter a learning phase…10,000 impressions …before the learning phase is complete”; “After 10 days…if a creative does not scale… remove it”.  *Confidence:* 0.9.  (Industry best-practices and vendor guidance.)  

## Sample size and budget table

| Metric compared    | Baseline                 | Detectable difference      | Impressions or installs per variant | Budget per variant (cheap geo) | Source                           |
|--------------------|--------------------------|----------------------------|-------------------------------------|-------------------------------|----------------------------------|
| **IPM (imps→installs)**  | 10 IPM (1.0%)           | +5 IPM (to 15, +50%)       | ~7,700 impressions                 | ~\$8 (at \$1 CPM)             |  (formula)         |
| **IPM (imps→installs)**  | 5 IPM (0.5%)            | +2 IPM (to 7, +40%)        | ~23,500 impressions                | ~\$24 (at \$1 CPM)            |                   |
| **CVR (post-click)**| 30%                     | +10% (to 40%)              | ~350 installs                      | ~\$350 (@ \$1/install)        |  (formula)         |

*Notes:* Values are illustrative using the sample-size formula.  “Budget” assumes a cheap CPM~\$1 (impression-based campaigns) or \$1 CPI (install-based); adjust for your actual CPM/CPI.  

## Recommended test plan  

1. **Choose network/approach:** Use a platform that allows controlled tests.  For example, run a Meta Ads **A/B Experiment** or Google UAC directional experiment if using videos.  If using Unity or AppLovin, create separate test campaigns for each variant (with identical targeting/settings) to split traffic manually.  Alternatively, use Mintegral’s built-in Creative AB tool.  
2. **Select geos:** Target low-cost countries (e.g. India, Indonesia, Vietnam, Brazil).  These have high volume at low CPM/CPI.  Exclude these geos from your main campaign to avoid overlap.  
3. **Set budget:** Allocate on the order of a few tens of dollars per variant.  Estimate from sample needs: e.g. for ~10k impressions at \$1–2 CPM, budget ~$10–20.  (Ensure each variant can get at least a few thousand impressions.)  
4. **Launch test:**  Start campaigns simultaneously.  Run for a short “learning” period (~48h) with no changes.  Then continue until each variant has at least a few thousand impressions (e.g. 3–7 days total).  Monitor IPM daily.  
5. **Stop rule:** End the test when one variant’s IPM ≥2× the other’s *and* it has ≥500 impressions, or once each has ~5,000+ impressions with <10% IPM gap.  Do not stop earlier due to initial ML bias.  
6. **Decision:** Choose the creative with higher IPM (lower CPI).  Confirm that install quality (D1/D7) is similar; if the cheaper CPI creative yields much worse retention, reconsider.  Document results for next round.  

*Sources:* Mintegral test guidelines, Unity Ads best practices, and statistical planning.  *Confidence:* 0.8.  

## Sources list  

| #  | Title                                                  | Publisher            | Year  | URL                                       | Opened | Primary/Secondary |
|----|--------------------------------------------------------|----------------------|-------|-------------------------------------------|--------|-------------------|
| 1  | Creative A/B Testing (Mintegral Support)               | Mintegral            | 2023  | https://mtjinternational.zendesk.com/...  | yes    | Primary           |
| 2  | How to test ad creatives on AppLovin in 2026 (blog)    | AppLovin (Rocketship)| 2026  | https://rocketshiphq.com/guides/…         | yes    | Secondary         |
| 3  | Scaling Ad Creatives (Unity Ads guide)                 | Unity                | 2025  | https://unity.com/resources/...           | yes    | Primary           |
| 4  | Playable ad metrics guide (“5 metrics that matter”)    | PlayableAdsMaker blog| 2026  | https://playableadsmaker.com/...          | yes    | Secondary         |
| 5  | Unity Ads Playable End Cards (best practices)          | PlayableAdsMaker blog| 2026  | https://playableadsmaker.com/...          | yes    | Secondary         |
| 6  | Liftoff Mobile Gaming Benchmark (blog)                 | Marketing Interactive| 2022  | https://marketing-interactive.com/...     | yes    | Secondary         |
| 7  | A/B Testing in Meta Ads (Experiments)                 | Clarigital Codex     | 2026  | https://www.clarigital.com/...            | yes    | Secondary         |
| 8  | How to Calculate A/B Sample Size                       | ABTesting.ai (blog)  | 2026  | https://www.abtesting.ai/...              | yes    | Secondary         |
| 9  | Facebook Playable Ads (PlayCanvas Dev docs)            | PlayCanvas / Meta    | 2019  | https://developer.playcanvas.com/...      | yes    | Primary           |
| 10 | How to Lower CPI (PlayableAdsMaker guide)              | PlayableAdsMaker blog| 2026  | https://playableadsmaker.com/...          | yes    | Secondary         |
| 11 | Directional experiments (Google Ads Help)              | Google Ads Help      | 2020  | https://support.google.com/...            | yes    | Primary           |
| 12 | Playable Ads Overview (AppsFlyer blog)                 | AppsFlyer            | 2023  | https://www.appsflyer.com/blog/tips/...   | no     | Secondary         |

## Gaps  
- **Network event tracking:** We found no official documentation listing any built-in “first interaction” or “level reached” events.  Major networks do not natively report these; they only track impressions/clicks/installs (as noted). This remains a gap (no source explicitly states it, but absence of mention implies it).  
- **TikTok/Snap info:**  Specific details on TikTok or Snapchat A/B testing for playables were not uncovered. These platforms have general A/B tools, but their treatment of playables is undocumented (NOT FOUND).  
- **Precise geo CPM data:** Hard data on CPM/CPI in specific cheap countries (India, SEA, LATAM) was not available. We inferred from tier benchmarks. A systematic source for 2025-26 regional rates would improve accuracy.