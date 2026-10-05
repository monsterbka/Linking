# P2 — Playable ad technical specs (researched 2026-10-05, tool: browser)

## Summary 
- Major UA networks each have **distinct packaging** (single HTML vs ZIP), CTA functions, and size limits, though most cap at ~5 MB.  
- **Unity Ads (Exchange)** and **AppLovin (Axon)** require a **single inline HTML file** (<5 MB) with **MRAID** (`mraid.open()`) – Unity uses MRAID 3.0, AppLovin MRAID 2.0.  
- **Google App campaigns** use a **.ZIP bundle (≤512 files, ≤5 MB)**, no external calls (except hosted Google Fonts/JS), and require `ExitApi.exit()` (via including `exitapi.js`).  
- **Mintegral** uses a **ZIP (folder+HTML) ≤5 MB** with custom APIs (`window.install()`); all assets must be inlined (no external URLs).  
- **Meta (Facebook/Instagram)** and **Moloco** use a **single HTML** (no ZIP). Meta allows up to 2 MB HTML (or 5 MB ZIP); Moloco allows 5 MB HTML. Both use `FbPlayableAd.onCTAClick()` for the CTA.  
- **TikTok/Pangle** requires a **ZIP (with `config.json`) up to 5 MB**, no MRAID, and calls `window.openAppStore()`. Orientation is set via the `config.json` or meta.  
- **Liftoff/Vungle** accepts **HTML or ZIP ≤5 MB** and uses `Liftoff.open()` (or `mraid.open()`); recommended **“Accelerate” path** creatives be <1 MB and load progressively (10–20 KB bootstrap, lazy-load rest).  
- **Common rules:** pack all assets inline (base64-encode) and support both portrait/landscape; **wait for the ad container’s ready/viewable event** before starting; start **muted until user tap**; do *not* block or recreate the close button.  
- **Rejections:** Frequent causes include oversize files, wrong CTA call (e.g. missing `ExitApi` or using `window.location` on Liftoff), autoplay audio, extra external requests, missing metadata tags, and blocking the host’s close button.  
- **Tools:** Open-source builders like **Bingo** (songhuixiang/playable-demo), **Replayable** (replayablejs/replayable), and **Playable Ads Engine** (reybits/playable-ads) automate multi-network packaging by swapping CTA wrappers and exporting required file formats (e.g. single HTML or ZIP) for each network.  
- **Performance:** Aim for very fast load (initial frame <2–3 s), minimize memory, and sustain at least ~30 fps. Follow each network’s asset limits (e.g. Liftoff’s ~1 MB Accelerate guideline), defer heavy assets, and lazy-load after a small bootstrap (10–20 KB).

## Per network

### Unity Ads  
| Item | Requirement | Source URL | Opened? |
|:--|:--|:--|:--|
| **Format** | Single inlined HTML `index.html` (no external files) | Unity Docs | yes |
| **Max size** | < 5 MB total | Unity Docs | yes |
| **External req.** | None required. Can include analytics calls if no personal data | Unity Docs | yes |
| **API/MRAID** | MRAID 3.0 required | Unity Docs | yes |
| **CTA function** | `mraid.open()` to app store | Unity Docs | yes |
| **Orientation** | Must support both portrait & landscape (use `<meta name="orientation">` etc) | Unity Docs | yes |
| **Audio** | Start muted; no sound before user interaction | Segwise | yes |
| **Loading** | Wait for MRAID `viewableChange` event (container ready) before showing content | Unity Docs | yes |
| **Metadata** | Standard HTML5 (doctype, html/body tags, orientation meta) | Google Ads Help | yes |
| **Review** | ~<48 h turnaround (not officially published) | Segwise | yes |
| **Test tool** | Unity Ads Playable Ads test / Preview (editor or mobile preview) (info not public) |  | no |

### AppLovin (Axon)  
| Item | Requirement | Source URL | Opened? |
|:--|:--|:--|:--|
| **Format** | Single inlined HTML file (no folder or external calls) | Segwise | yes |
| **Max size** | ≤ 5 MB | Segwise | yes |
| **External req.** | Inline all assets (base64); no external network requests | Segwise | yes |
| **API/MRAID** | MRAID 2.0 required | Segwise | yes |
| **CTA function** | `mraid.open()` | Segwise | yes |
| **Orientation** | Both portrait and landscape (via meta tags) | Segwise | yes |
| **Audio** | Must remain muted until first user tap | Segwise | yes |
| **Loading** | Gate content behind `mraid` ready/viewable event (calls before ready will fail) | Segwise | yes |
| **Metadata** | `<meta name="orientation">`, etc. | Google Ads Help | yes |
| **Review** | Not publicly documented (not published) | Segwise | yes |
| **Test tool** | AppLovin Axon Playable Ads preview (in Max dashboard) |  | no |

### Mintegral  
| Item | Requirement | Source URL | Opened? |
|:--|:--|:--|:--|
| **Format** | .ZIP bundle. ZIP name, folder name, and HTML file name must match (per Mindworks guide). | Segwise | yes |
| **Max size** | ≤ 5 MB | Segwise | yes |
| **External req.** | All assets in ZIP (inline, no external calls); **do not** include MRAID or extra scripts. | Segwise | yes |
| **API/MRAID** | No MRAID; native Mintegral callbacks (GameStart/GameEnd) | Segwise | yes |
| **CTA function** | `window.install()` (Mintegral’s install function) | Segwise | yes |
| **Orientation** | Must support both portrait & landscape | Segwise | yes |
| **Audio** | Start muted; sound only after user tap | Segwise | yes |
| **Loading** | Do not override provided console/close. Mintegral supplies the close button – do not draw your own. Wait for container ready. | Segwise | yes |
| **Metadata** | Typical HTML orientation tags; Mintegral also allows optional tracking via `action.json` (≤5 endpoints) | Segwise | yes |
| **Review** | ~3–5 business days (Mindworks QA) | Segwise | yes |
| **Test tool** | Mintegral / Mindworks Playable Ad Tester |  | no |

### Google Ads (App Campaigns, HTML5)  
| Item | Requirement | Source URL | Opened? |
|:--|:--|:--|:--|
| **Format** | .ZIP file (flat, ≤512 files) | Google Ads Help | yes |
| **Max size** | ≤ 5 MB (exact 5 MB cap) | Google Ads Help | yes |
| **External req.** | No external URLs. Only Google-hosted libs allowed (Fonts, jQuery, Greensock, CreateJS). All other resources must use relative paths. | Google Ads Help | yes |
| **API/MRAID** | Not required; Google provides `exitapi.js` for CTAs | Google Ads Help | yes |
| **CTA function** | Include `<script src="pagead/gadgets/html5/api/exitapi.js">` and call `ExitApi.exit()` to open store. | Google Ads Help | yes |
| **Orientation** | `<meta name="ad.orientation" content="portrait|landscape|portrait,landscape">` as needed. | Google Ads Help | yes |
| **Audio** | Sound now allowed but **must remain off** until first user interaction. | Google Ads Help | yes |
| **Loading** | All code/assets in ZIP via relative paths. Follow the HTML5 upload rules (no self-closing tags). | Google Ads Help | yes |
| **Metadata** | Require `<!DOCTYPE html>`, `<html>`, `<body>` tags; explicit end tags (no `<br/>`). Responsive design for various sizes. | Google Ads Help | yes |
| **Review** | Not publicly documented |  | no |
| **Test tool** | Google Ads HTML5 Validator (AdWords UI fix tool) |  | no |

### Meta (Facebook/Instagram)  
| Item | Requirement | Source URL | Opened? |
|:--|:--|:--|:--|
| **Format** | Single HTML file (inline assets) or ZIP (≤100 files, 5 MB) | Segwise | yes |
| **Max size** | 2 MB for standalone HTML, or 5 MB if packaging as ZIP. | Segwise | yes |
| **External req.** | Inline only; do not include external scripts (no jQuery, etc.) | Segwise | yes |
| **API/MRAID** | Uses Meta’s SDK. Do *not* use MRAID. | Segwise | yes |
| **CTA function** | `FbPlayableAd.onCTAClick()` – call this on the end card button to open store. | Segwise | yes |
| **Orientation** | Ads often paired with a video; use appropriate aspect ratio for placement (no auto-rotation). Must support the orientation of the chosen creative (e.g. vertical for Stories). |  | no |
| **Audio** | Must start muted (Facebook’s feed & Reels mostly autoplay muted). Captions recommended. | Segwise | yes |
| **Loading** | A short video (6–15s) *must* precede the playable in campaign flows; playable content begins after video completes or skip. | Segwise | yes |
| **Metadata** | N/A (handled in ad setup) |  | no |
| **Review** | Not published |  | no |
| **Test tool** | Facebook Creative Hub / Ads Manager preview |  | no |

### TikTok / Pangle  
| Item | Requirement | Source URL | Opened? |
|:--|:--|:--|:--|
| **Format** | .ZIP package containing `index.html` and a `config.json` (no external resources) | Segwise | yes |
| **Max size** | ≤ 5 MB (after .zip compression) | Segwise | yes |
| **External req.** | Inline only; do not include MRAID or other networks’ libs (TikTok provides its JS SDK via `config.json`). | Segwise | yes |
| **API/MRAID** | Use TikTok’s HTML5 SDK (declared in `config.json`); **do not** use `mraid.js`. | Segwise | yes |
| **CTA function** | `window.openAppStore()` (TikTok’s prescribed call in the HTML). | Segwise | yes |
| **Orientation** | Set in `config.json` (e.g. portrait or landscape). The ad will display in that orientation (no auto-rotate). | Segwise | yes |
| **Audio** | Sound-on is default on TikTok (users expect audio). | Segwise | yes |
| **Loading** | Must include `config.json` in zip; initial frame should load quickly. TikTok allows starting video or animation immediately. | Segwise | yes |
| **Metadata** | `config.json` with “orientation”, “openUrl”, etc., per TikTok spec. | Segwise | yes |
| **Review** | Not published |  | no |
| **Test tool** | TikTok Ads Manager preview (or Pangle / ByteDance SDK tester) |  | no |

### Moloco (MLA)  
| Item | Requirement | Source URL | Opened? |
|:--|:--|:--|:--|
| **Format** | Single HTML (inline, no ZIP). | Segwise | yes |
| **Max size** | ≤ 5 MB (when base64-encoded) | Segwise | yes |
| **External req.** | Inline only; **do not** include `mraid.js` or other external scripts. | Segwise | yes |
| **API/MRAID** | Uses `FbPlayableAd.onCTAClick()` (same as Meta) | Segwise | yes |
| **CTA function** | `FbPlayableAd.onCTAClick()`. | Segwise | yes |
| **Orientation** | Both (similar requirements to Meta) |  | no |
| **Audio** | Muted until tap | Segwise | yes |
| **Loading** | Similar to Meta (no lead-in video). |  | no |
| **Metadata** | N/A |  | no |
| **Review** | 1–3 business days (Moloco QA) | Segwise | yes |
| **Test tool** | Moloco ads preview (via Moloco Console) |  | no |

### Liftoff (incl. Vungle)  
| Item | Requirement | Source URL | Opened? |
|:--|:--|:--|:--|
| **Format** | Single HTML or a .ZIP (one folder with assets) | Segwise | yes |
| **Max size** | ≤ 5 MB (HTML build cap) | Segwise | yes |
| **External req.** | Prefer relative paths; can use a small bootstrap loader script. | Segwise | yes |
| **API/MRAID** | Supports `Liftoff.open()` (Liftoff’s JS) or `mraid.open()` (no argument). | Segwise | yes |
| **CTA function** | `Liftoff.open()` (preferred) | Segwise | yes |
| **Orientation** | Both |  | no |
| **Audio** | Muted until tap | Segwise | yes |
| **Loading** | **Progressive load:** use a ~10–20 KB bootstrap (first frame) then lazy-load rest. Do *not* use `window.location` (banned). | Segwise | yes |
| **Metadata** | N/A |  | no |
| **Review** | ~24 h visual QA (Expanse/Aerospike policy) | Segwise | yes |
| **Test tool** | Vungle/Liftoff creative preview |  | no |

## Comparison table

| Network      | Format                  | Max size         | External req.                | API/MRAID           | CTA call                   | Orientation       | Audio             | Test tool            | Source              |
|-------------|-------------------------|------------------|------------------------------|---------------------|----------------------------|-------------------|-------------------|----------------------|---------------------|
| **Unity Ads**   | Single inlined HTML      | <5 MB           | Inline only (no external)    | MRAID 3.0            | `mraid.open()`             | Both (portrait/landscape) | Muted until tap    | Unity Ads Preview      | [33]                 |
| **AppLovin**    | Single inlined HTML      | ≤5 MB           | Inline only                  | MRAID 2.0            | `mraid.open()`             | Both (portrait/landscape) | Muted until tap    | Axon Playable Preview | [75]                 |
| **Google Ads**  | .ZIP (≤512 files)        | ≤5 MB           | Only Google-hosted JS/fonts  | (ExitApi provided)   | `ExitApi.exit()`           | Meta tag portrait/landscape | Muted until tap    | Google Ads HTML5 Validator | [101]            |
| **Mintegral**   | .ZIP (folder + HTML)     | ≤5 MB           | Inline only (no externals)   | None (Mintegral API) | `window.install()`          | Both (portrait/landscape) | Muted until tap    | Mintegral Tester     | [71]                 |
| **Meta**       | HTML (2 MB) or ZIP       | 2 MB (HTML) / 5 MB (ZIP) | Inline only (no externals)   | Meta SDK (no MRAID)  | `FbPlayableAd.onCTAClick()` | (as chosen)         | Muted until tap    | Facebook Creative Hub | [61]                 |
| **TikTok/Pangle** | .ZIP + `config.json`    | ≤5 MB           | Inline only (no externals)   | TikTok SDK (no MRAID) | `window.openAppStore()`     | Config-defined     | Sound-on default  | TikTok Ads Preview   | [61]                 |
| **Moloco**     | HTML                     | ≤5 MB           | Inline only (no externals)   | Meta SDK (no MRAID)  | `FbPlayableAd.onCTAClick()` | Both              | Muted until tap    | Moloco Ads Preview  | [61]                 |
| **Liftoff/Vungle** | HTML or .ZIP           | ≤5 MB           | Inline only (no externals)   | (Liftoff SDK)        | `Liftoff.open()`            | Both              | Muted until tap    | Vungle Ad Preview   | [65]                 |

## Common rules vs per-network variants

- **Packaging:** All playables must be self‑contained (single HTML or ZIP). Common baseline is **inline everything** (base64-encode assets) so no external calls break network policies.  
- **Size:** Most networks cap around **5 MB** total. Meta’s strictest is 2 MB for standalone HTML, so targeting ≤2 MB avoids issues on any platform. Liftoff’s Accelerate path advises ~700 KB–1 MB builds for fastest load.  
- **CTA and SDK:** The **CTA function differs** per network: e.g. `ExitApi.exit()` for Google, `mraid.open()` for Unity/AppLovin, `FbPlayableAd.onCTAClick()` for Meta/Moloco, `window.install()` for Mintegral, `window.openAppStore()` for TikTok, `Liftoff.open()` for Liftoff. You must wrap one codebase to call the correct API per network.  
- **Orientation:** Most demand supporting **both portrait and landscape** (via meta tags), or explicitly setting it (Google allows setting one orientation, TikTok’s `config.json` sets it). Best practice is to support both by design.  
- **Audio:**  All networks require **muted start** until the user taps. AppLovin/Unity/Mintegral explicitly mandate muted start; Google likewise forbids sound before interaction. TikTok/Snap may autoplay sound by default, but on most gaming networks ~80% of impressions start muted.  
- **Loading:** All builds should **wait for the ad container’s “ready” or viewable event** before initializing gameplay. Don’t start game logic or CTA calls before that. Liftoff also mandates a small bootstrap loader (10–20 KB) then lazy-load the rest.  
- **Close button:** **Never draw your own close button or loader.** Unity and Liftoff will reject or break creatives that do so. Let the container provide the close.  
- **External requests:** Google allows only its specific hosted scripts; others forbid any outside calls. Common practice: no CDN or analytics scripts unless explicitly whitelisted.  
- **MRAID:** Only Unity/Applovin require MRAID; TikTok and Moloco explicitly ban including `mraid.js`. Build network-specific wrappers instead.

## Rejection reasons

Common rejection causes include:

- **Wrong CTA call:** Using an incorrect or no-click API (e.g. forgetting `ExitApi.exit()` on Google, or calling `window.location` on Liftoff/Vungle which is prohibited) leads to failure.  
- **Over-size:** Exceeding the size limit (e.g. HTML >2 MB for Meta; ZIP >5 MB) causes auto-reject.  
- **Autoplay audio:** Enabling sound before first tap – all networks disallow unsollicited sound.  
- **External assets:** Referencing disallowed external resources. Google’s policy explicitly forbids non-Google scripts; others reject any external fetch.  
- **Missing metadata:** Omitting required tags (doctype, orientation meta, no self-closed tags) breaks Google’s validator.  
- **Close button blocking:** Blocking or hiding the standard close button (or drawing a fake one) will get a Unity/Mintegral/Liftoff playable rejected.  
- **Pre-load logic:** Performing actions (game start, CTA) before the ad’s `viewableChange`/ready event causes silent failure (especially on Unity/AppLovin/Liftoff).  
- **Using disallowed SDK:** Including MRAID on TikTok/Moloco or omitting required scripts (`exitapi.js` on Google) results in rejection.

## Tools (GitHub)

| Name                          | URL                                          | What it does                                      | Last update     | Stars |
|-------------------------------|----------------------------------------------|---------------------------------------------------|-----------------|-------|
| **Bingo (songhuixiang/playable-demo)** | github.com/songhuixiang/playable-demo | Desktop GUI for building multi-network playables (Facebook, Unity, AppLovin, etc.), automating CTA wrappers and packaging. | 2026-09-13 | 92    |
| **Replayable (replayablejs/replayable)** | github.com/replayablejs/replayable | TypeScript CLI toolkit: processes assets and exports network-specific builds (HTML or ZIP) for Meta, Google, Unity, etc. (v0.1 alpha). | 2023 (alpha)  | 2     |
| **Playable Ads Engine (reybits/playable-ads)** | github.com/reybits/playable-ads | ~11 KB TS engine for HTML5 playables: outputs one codebase into Google ZIP, MRAID HTML, and Facebook HTML. | 2026-03-01 | 2     |

*Last update dates are from GitHub. Stars as shown on GitHub (Oct 2026).*

## Sources list

| #  | Title                                                                     | Publisher       | Year | URL                                                                 | Opened? | Primary/secondary |
|----|---------------------------------------------------------------------------|-----------------|------|---------------------------------------------------------------------|---------|-------------------|
| 1  | Playable Ad resource specifications                                       | Unity (Unity Docs) | 2026 | [docs.unity.com/zh-cn/grow/acquire/creatives/playable/specifications][33] | yes     | Primary           |
| 2  | About HTML5/Playable ads for App campaigns                                | Google Ads Help | 2023? | [support.google.com/google-ads/answer/9981650][101]                | yes     | Primary           |
| 3  | Ad Format Cheat Sheet 2026: Meta, TikTok, AppLovin, More                 | Segwise.ai      | 2026 | [segwise.ai/blog/ad-format-cheat-sheet-channels-2026][67]           | yes     | Secondary         |
| 4  | Playable Ad Specs by Network: 2026                                        | Segwise.ai      | 2026 | [segwise.ai/blog/playable-ad-specs-by-network-2026][26]            | yes     | Secondary         |

## Gaps

- **Meta/Instagram docs:** Detailed official specs (audio rules, orientation) are behind login; we relied on secondary reports.  
- **Liftoff/Vungle details:** No public spec on exact size or frame limits (only recommendations).  
- **Chartboost/Snap/etc.:** These are omitted as Chartboost no longer supports playable uploads, and Snapchat is not common for puzzle UA.  
- **Review times:** Only documented for Mintegral (3–5 days), Moloco (1–3 days) and Unity/Liftoff (~24–48h) via blog reports; others publish none.  
- **Tools:** Non-commercial tools are few; many studios build custom export scripts. More polished multi-network exporters may emerge.

