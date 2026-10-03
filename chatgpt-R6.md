# R6 — Game feel, VFX, sound (researched 2026-10-03, tool: ChatGPT)

## Summary  
- **Fast feedback on every action:** e.g. when a player taps or selects a piece, immediately play a subtle scale or color feedback (e.g. scale to ~1.1–1.2× over ~100–150 ms with an ease-out). Even for “invalid” taps play a negative sound (a short buzzer) and a very quick no-resize hit-stop, to ensure no tap ever feels ignored.  
- **Piece movement animation:** smooth easing (typically cubic out) for drag and drop; snap pieces into grid with a quick ease out bounce or small “squash/stretch” (e.g. overshoot to 1.1× size then back). Use a brief scale-down on drop (e.g. to 0.8× width, 1.2× height over ~50 ms) then back to normal over ~100–150 ms (inferred).  
- **Match/Clear effects:** On a successful match, animate pieces exploding or fading out with particle bursts. Typical particle counts are modest (e.g. 10–30 particles per clear, higher for combos). Combine an “explosion” of little sparkles or stars with a mild screen shake. For chain clears, add extra flair (e.g. bigger shake or additional particles) for each extra line. Clear animations are usually very quick: the pieces may scale up ~1.2–1.5× over ~80–120 ms then pop or fade out.  
- **Combo/pitch ladders:** For multi-row clears or combos, layer a rising-pitch effect: e.g. Candy Crush plays the same clear sound at a semitone higher for each successive row. This “pitch ladder” gives a sense of build-up. Similarly, incremental musical chimes or dribble sounds can ascend in pitch or volume for each combo link (a classic “pat on back” escalation).  
- **Level Complete/Rewards:** When the level is won, show celebratory animations: objects (coins, stars) flying to the score, plus confetti particles. Animate coins or stars at ~1.5× scale quickly (80–100 ms) then shrink to normal as they “collect,” with slow ease-in-out movement from piece to UI. Play a distinct “success” sound (e.g. a rising arpeggio/chime or UINotification feedback) and a gentle *heavy* impact/vibration. Reward coins should have a bright “ping” or tinkling tone with a quick attack/decay.  
- **Fail animation:** On level fail, use a softer, downwards musical cue (Candy Crush uses a falling piano figure) and a brief low-frequency bass *thud*, not a harsh buzzer. A small screen shake is optional, but if used keep it subtle (avoid nausea). Grayout or shake the grid and play a short “disappointed sigh” sound or error tone. Use UINotification “error” haptics on iOS to signal failure but allow quick retry.  
- **Sound mixing and frequency:** Keep SFX relatively loud (feedback is critical) and music softer. Emphasize mid/high frequencies (phone speakers lose bass). For every UI or game event, play a sound (tap, clear, open menu). Layer sounds – combine an oscillator tone with a bit of noise for “pop” or “click” effects. Use short envelopes (fast attack, moderate release) so sounds are punchy, and pitch variation (+/– a few percent) to prevent repetition fatigue.  
- **Haptics:** Map actions to standard haptic patterns. On iOS, use **UISelectionFeedback** for tile selection or rolling through options, **UIImpactFeedback** (light/medium) for placing or clearing a piece, and **UINotificationFeedback** (success/warning) for matches versus misses. On Android, use `VibrationEffect.EFFECT_CLICK` for taps and small impacts, and `EFFECT_DOUBLE_CLICK` or longer waveforms for matches. In general, have brief, low-latency vibrations on every good action; allow players to disable them.  
- **Avoid excess:** Too much shake/noise can hide gameplay and cause motion sickness. Do not let VFX obscure pieces. Cap overlapping sounds (e.g. mute redundant identical SFX). Avoid full-screen flashy overlays or strobing effects (WCAG limits flashing), and provide options to reduce motion.  
- **Performance:** Keep effects cheap. Reuse particle instances (object-pool particles) and batch draws to minimize CPU/GPU work. A 60 fps budget is ~16.7 ms, so profile VFX to avoid frame drops. Cull off-screen or obscured effects, and pre-bake camera shake (short perlin offset instead of expensive post-process). On low-end devices, limit particles (e.g. <50 total), avoid large semitransparent screen quads (minimal overdraw), and prefer simple sprite animations to heavy shaders.

## 1. Selection and Move  
- **Selection highlight:** When the user taps a tile or UI element, scale it slightly up (e.g. 1.1× to 1.2×) and back with an ease-out over ~0.1–0.15 s (inferred). Coupled with a click/pop sound, this confirms input.  
- **Move/drag:** Dragged pieces should follow finger with a little ease (linear or slight cubic), and optionally a slight scale down (~0.9×) for “active” state. On drop or cancel, animate back to original position with a bounce easing (like `easeOutCubic`). For invalid moves, play a negative “thud” sound and a tiny shake.  
- *Source:* Candy Crush’s polished UI shows sounds on every tap; game feel literature suggests easing that adds momentum (Swink) though exact values are inferred.

## 2. Snap/Place  
- **Snapping pieces:** When a piece snaps into its target, use a “punch” animation: e.g. squash to 0.8× width and 1.2× height in ~0.05 s, then stretch to 1.2× width and 0.8× height in next 0.05 s, then return to normal in ~0.1 s (values inferred). This simulates elasticity.  
- **Sound:** Play a “place” sound on snap (e.g. a soft click or chime).  
- *Source:* UI best-practices cite instant feedback for inputs, and “squash-and-stretch” bounce is a known method for punchiness (Swink). Exact numbers are developer-defined (inferred).

## 3. Match/Clear  
- **Clear animation:** Matched tiles should “explode” outwards or fade with particles. A common approach: instantly remove the sprite and emit a burst of 10–30 particles (stars, sparkles) flying radially or upwards. Particles live ~0.2–0.5 s, fade out or shrink. Add a quick screen shake (low magnitude, ~2–3% of screen, ~0.1 s) for emphasis.  
- **Animation timing:** The core “pop” should be fast: e.g. scale matched gems to ~1.2× over ~60–100 ms then hide. Particles and shake continue slightly longer.  
- **Visual style:** Use bright colors (white, gold) and add a short bloom/glow flash at origin.  
- *Source:* Jonasson/Purho’s “Juice” talk and Vlambeer’s screenshake emphasize screen shake and particles for impact. Candy Crush’s clears are accompanied by a pleasant sound and minimal delay.

## 4. Combos and Chains  
- **Incremental effects:** For multi-row clears, chain the feedback. E.g. if a single move clears two rows, do the first clear with sound+shake, then after ~80 ms do the second clear with another. Raise the pitch on the second sound by a semitone. Each extra cleared row adds another rising tone.  
- **Visual cues:** Possibly color the second (combo) row effect differently (e.g. brighter/contrasting). Some games add a numeric combo label with a scale-up tween.  
- *Source:* Candy Crush explicitly raises pitch for each row, describing it as “feeling of elevation” in the write-up. This pitch ladder is a documented best-practice for combos.

## 5. Level Complete / Reward  
- **Victory animation:** Explode all tiles, emit confetti or particles across screen for ~0.5–1 s. Animate score numbers or coins flying to UI. E.g. spawn 5–10 coin sprites that arc to the coin counter, scaling and fading.  
- **Sound:** Play a distinct “success” jingle or glissando. In iOS, trigger UINotificationFeedbackGenerator with the *success* pattern (three short taps).  
- **Reward collection:** For each coin collected, play a bright “ting” sound with a short envelope (inferred: attack ~10 ms, decay ~100 ms, high frequency). Possibly layer a chime plus a click. Animate coins scaling from 0→1.2× in ~100 ms then settling.  
- *Source:* The Microsoft XAG 117 guideline shows HyperDot’s “Animate Background” toggle for UIs, and general advice is to reinforce good actions with rewarding sound and haptics.

## 6. Level Fail  
- **Fail animation:** Flash a “Level Failed” UI. Grey out board or shake lightly. Use a brief downward camera offset or quick shake (~0.1 s, small magnitude). Avoid strong camera motion (motion sickness risk).  
- **Sound:** Play the “fail” audio cue: Candy Crush uses a descending piano theme and a soft crying sound. Use a minor-key or low-pitched sound (e.g. a short gliss / “uh-oh” tone). On iOS, use UINotificationFeedbackGenerator with the *error* pattern.  
- *Source:* Candy Crush intentionally has minimal negative sounds (only error beep and fail melody) to avoid discouragement. The XAG guideline recommends allowing players to disable shake/motion and minimize jarring feedback.

## 7. Sound Design (SFX)  
- **SFX layers:** Split sounds into layers: UI (clicks/taps), small actions, and big actions. Use short WAVs for UI, and simple synth hits for actions. Keep SFX louder than background music.  
- **Pitch variation & layering:** For pops/clicks, combine a quick sine or square oscillator burst (attack <10 ms, decay 50–100 ms) with filtered noise. For example, a “click” can be a 1 kHz sine shot with a 50 ms decay plus a bit of white noise. A “pop” might be a short upward pitch sweep. Vary pitch slightly per play.  
- **Mixing:** Allow separate SFX and music volume (accessibility). Leave headroom to prevent clipping. Use ducking if voiceover/dialogue plays over music. Cap overlapping instances to avoid muddiness.  
- **Phone speakers:** Emphasize mids (1–3 kHz) and highs; attenuate sub-bass (<100 Hz) since small speakers lack it. Avoid very long decays or low drones (fatigue). On mobile, dynamic range can be large (games often allow peaks), but balance so that UI clicks are audible above any ambient loop. Many casual players may play muted or use device vibration only – ensure UI clarity in all modes.  
- *Source:* Game Developer analysis notes Candy Crush’s rich, positive sound design and separate UI audio on every action. Egmatic advises audio priority: uncompressed short effects vs streamed music and mixing tiers. Mobile-mix guidance warns that phone speakers distort low bass and emphasize high frequencies. 

## 8. Adaptive Music  
- **Background loop:** Use a calm, simple loop in a major key. Keep instrumentation sparse (e.g. pad, gentle piano).  
- **Layers:** Prepare 2–3 layers (e.g. drums, strings, melody). Bring in additional layers on bigger events: e.g. add drum hit on board wobble or a more intense chord on combos.  
- **Transitions:** Cross-fade smoothly. On tension (combo streaks or time running out), gradually increase volume or add percussion, then revert if things calm.  
- **Non-fatiguing:** Avoid jarring changes or very repetitive motifs. Loop length should be long enough (~15–30 s). Use subtle variations each cycle (small random twists) or interactive filters to avoid obvious looping.  
- *Source:* While specific academic sources on puzzle game music are scarce here, standard practice is to treat music as ambience (lowest tier), and adapt instrumentation rather than tempo for puzzles. We inferred these guidelines from general game audio (Egmatic) and casual game practice.

## 9. Event–Feedback Table  

| Event            | Visual                                | Sound                                 | Haptic                      | Timing (source)       | Sourced?             |
|------------------|---------------------------------------|---------------------------------------|-----------------------------|-----------------------|----------------------|
| **Select tile/UI**   | Scale up to ~1.1× (ease-out)         | Soft “click” (short, high-pitched)   | Light *selection* tap (iOS)  | ~100–150 ms        | Inferred/known UX |
| **Drag move**        | Smooth follow; snap with small bounce | None (or quick slide whoosh)         | (none or light)            | Move duration (inferred) | Inferred         |
| **Place/Snap**       | Quick squash/stretch then settle (~0.1s) | “Pop” sound (click + small thud)     | Light impact (iOS)     | ~100–150 ms        | Inferred (UX practice) |
| **Match/Clear**      | Explode pieces + particle burst; tiny screen-shake | “Clear” ding; increase pitch on combos  | Brief moderate pulse on each row | ~80–120 ms reveal; particles ~300–500 ms decay | Partial (pitch, rest inferred) |
| **Combo Continuation** | Extra sparkles; repeating above effects | Same clear sound raised by semitone; *laugh* or higher tone for big chain | Repeated taps or longer rumble  | Staggered by ~80 ms per extra row | Sourced (pitch) |
| **Level Complete**   | Full-screen confetti; star/coin fly-in  | Cheerful flourish; success chime; coins “ping” | Strong *success* taps (iOS)  | ~0.2–0.5 s for animations | Inferred (general practice) |
| **Reward (coin)**    | Coin sprite pop+fly to HUD              | Coin “tink” (high sine burst)        | Light double-tap          | ~100 ms pop; travel ~0.5–1s | Inferred           |
| **Error tap**       | Quick tile wiggle or flash red          | Short “buzz” or thud (error tone)    | Small jolt (*warning*) (iOS) | <100 ms feedback  | Inferred           |
| **Level Fail**     | Board greyed/shaken slightly            | Downward piano + soft “womp”; fail jingle | One *error* buzz (iOS)    | ~0.5 s fade-out jingle | Inferred/known (Candy Crush) |

*(“Sourced?”: whether parameters come from a cited source vs. general practice.)*

## Code-ready Recipes (pseudo-code/WebAudio snippets)  

- **UI Click (e.g. on select):**  
  ```js
  let osc = audioCtx.createOscillator();
  let gain = audioCtx.createGain();
  osc.type = 'square'; osc.frequency.value = 1000;
  gain.gain.setValueAtTime(0, now);
  gain.gain.linearRampToValueAtTime(0.5, now + 0.01);
  gain.gain.exponentialRampToValueAtTime(0.001, now + 0.1);
  osc.connect(gain).connect(audioCtx.destination);
  osc.start(now); osc.stop(now + 0.15);
  ```  
  *Inferred:* fast attack (10 ms) to half volume, decay to near zero by 150 ms.

- **Match Clear (oscillator + noise burst):**  
  ```js
  // Sine sweep
  let osc = audioCtx.createOscillator();
  let gain = audioCtx.createGain();
  osc.type = 'sine'; osc.frequency.setValueAtTime(600, now);
  osc.frequency.exponentialRampToValueAtTime(200, now + 0.1);
  gain.gain.setValueAtTime(0, now);
  gain.gain.linearRampToValueAtTime(0.7, now + 0.02);
  gain.gain.exponentialRampToValueAtTime(0.001, now + 0.15);
  osc.connect(gain).connect(audioCtx.destination);
  osc.start(now); osc.stop(now + 0.15);
  // Noise burst (for crisp edge)
  let bufferSize = audioCtx.sampleRate * 0.1;
  let buffer = audioCtx.createBuffer(1, bufferSize, audioCtx.sampleRate);
  let data = buffer.getChannelData(0);
  for (let i=0; i<bufferSize; i++) data[i] = (Math.random()*2 -1);
  let noise = audioCtx.createBufferSource();
  noise.buffer = buffer;
  let noiseGain = audioCtx.createGain();
  noiseGain.gain.setValueAtTime(0.5, now);
  noiseGain.gain.exponentialRampToValueAtTime(0.001, now + 0.1);
  noise.connect(noiseGain).connect(audioCtx.destination);
  noise.start(now); noise.stop(now + 0.1);
  ```  
  *Derived:* a pitched sine dropping in frequency (600→200 Hz), plus a decaying white noise burst. Envelope: ~20 ms attack, ~150 ms decay.

- **Coin Collect (bell/chime):**  
  ```js
  let osc = audioCtx.createOscillator();
  let gain = audioCtx.createGain();
  osc.type = 'triangle'; osc.frequency.setValueAtTime(800, now);
  gain.gain.setValueAtTime(0, now);
  gain.gain.linearRampToValueAtTime(0.4, now + 0.02);
  gain.gain.exponentialRampToValueAtTime(0.001, now + 0.5);
  osc.connect(gain).connect(audioCtx.destination);
  osc.start(now); osc.stop(now + 0.6);
  ```  
  *Inferred:* a single medium-high tone with a longer tail (~0.5 s) for a “zing”.

- **Miss/Error (subdued buzzer):**  
  ```js
  let osc = audioCtx.createOscillator();
  let gain = audioCtx.createGain();
  osc.type = 'triangle'; osc.frequency.setValueAtTime(100, now);
  gain.gain.setValueAtTime(0, now);
  gain.gain.linearRampToValueAtTime(0.8, now + 0.01);
  gain.gain.exponentialRampToValueAtTime(0.001, now + 0.3);
  osc.connect(gain).connect(audioCtx.destination);
  osc.start(now); osc.stop(now + 0.3);
  ```  
  *Inferred:* a low tone with quick attack for an unhappy note (aligns with Candy Crush’s downscale effect).

*(All code examples use WebAudio scheduling. Actual implementation should manage contexts, nodes pooling to avoid GC.)*

## Sources list  
| #  | Title / Content Summary                                       | Author / Speaker       | Year | URL                                            | Opened? |
|---|--------------------------------------------------------------|------------------------|------|---------------------------------------------|---------|
| 1 | *Why Candy Crush Saga is so Engaging – An Audio Breakdown* (blog) – analysis of Candy Crush audio, UI sounds, positive reinforcement | PJ Belcher (blog post) | 2013 | https://www.gamedeveloper.com/audio/why-candy-crush-saga-is-so-engaging-an-audio-breakdown | Yes |
| 2 | *Game Audio and Sound Design: A Practical 2026 Guide* – layering SFX/music, mixing tips, mistakes in game audio (Egmatic blog) | Vladislav Kovnerov (Egmatic) | 2026 | https://egmatic.com/blog/game-audio-sound-design-guide | Yes |
| 3 | *Optimize 2D Game Performance: A Practical Guide to 60 FPS* (Egmatic blog) – profiling, batching, pooling recommendations | Vladislav Kovnerov (Egmatic) | 2026 | https://egmatic.com/blog/optimize-2d-game-performance | Yes |
| 4 | *Xbox Accessibility Guideline 117: Visual distractions and motion settings* – guidelines to pause/disable blink/motion, avoid camera shake | Microsoft XAG | 2023 | https://learn.microsoft.com/en-us/xbox/accessibility/xbox-accessibility-guidelines/117 | Yes |
| 5 | *Mix and master game audio for mobile devices* – phone speaker EQ (bass loss, high emphasis), headphone vs phone considerations | 5argon (Sound Designer blog) | 2022 | https://gametorrahod.com/mix-and-master-game-audio-for-mobile-devices/ | Yes |
| 6 | *How (And When) To Use Haptic Feedback for a Better iOS App* (blog) – Apple’s UIFeedbackGenerator usage (selection, impact, notification) | Basam Alasaly (Medium) | 2021 | https://medium.com/cracking-swift/how-and-when-to-use-haptic-feedback-for-a-better-ios-app-9bcfcc97393a | Yes |
| 7 | **(Reddit)** *Juice it or lose it* reference – links to Martin Jonasson & Petri Purho talk and similar (screenshake) | user comments | 2013 | https://www.reddit.com/r/gamedesign/comments/201o3l/juice_it_or_lose_it_a_talk_by_martin_jonasson/ | Yes |
| 8 | **(GitHub)** *The art of screenshake* – mention of Jan Willem Nijman’s Vlambeer talk, demo references | colinbellino (repo) | 2022 | https://github.com/colinbellino/screenshake | Yes |
| 9 | *Game Developer Collective – Casual Game Audio Resources* (references via Giantic’s GDC Summit note) – on layering, pitch ladders (Audio Summit reference) | Giantic (GDC audio talks) | 2024 | [GDC Vault Audio Summit notes] (not opened) | No |
| 10 | *Why Candy Crush Saga is so Engaging* (Gamasutra/Dev) – narrative breakdown (as above) | PJ Belcher | 2013 | duplicate content | opened in [1] |
| 11 | Game feel: general sources (Swink book, talk videos) – background info | Swink (book), Jonasson (GDC talk), etc. | 2008/2012 | - (various) | - |

**Open content:** [18], [50], [72], [67], [76], [77], [87] are open and cited above.

## Gaps  
We found ample advice on audio mixing and feedback (Candy Crush, Egmatic, mobile audio blogs) and on performance/accessibility guidelines. Harder to find were specific numerical animation parameters (e.g. exact scale values or durations for effects) – these are often proprietary. Many “juice” talks are on video only (no transcripts) so we relied on summarizing their known content (e.g. screenshake, particles, squash/stretch) without direct quotes. Detailed recipes for procedural SFX (exact oscillator types or filter values) are rarely published; we inferred typical synthesizer setups from common practice. Similarly, generative/adaptive music strategies are described only in general terms, as few sources detail algorithms for puzzle games. We cite best practices where available and note inferred values for specifics like timing and intensities.