# **P2 — Playable ad technical specs (researched March 2026, tool: Web Search / Analysis)**

## **Summary (10 bullets max)**

* Single HTML versus ZIP Packaging Split: Unity Ads, AppLovin, and LevelPlay strictly mandate single inline HTML packages, whereas Google Ads, TikTok, Mintegral, and Liftoff require or support ZIP packages containing an index.html entry point and sub-assets1.  
* Universal File Size Limits: A 5 MB maximum file size cap dominates across major ad networks (uncompressed or ZIP package), though tighter 2 MB caps apply for Meta single-file HTML and Chartboost (3 MB)1.  
* Strict Prohibition of External Requests: Across all ad networks, external HTTP/HTTPS network calls, CDN resource fetching, dynamic XHR, dynamic fonts, and tracking pixels are forbidden without prior platform permission2.  
* Call-to-Action API Fragmentation: Store click-through mechanics differ by network, requiring mraid.open() for Unity Ads, AppLovin, LevelPlay, and Chartboost; FbPlayableAd.onCTAClick() for Meta and Moloco; ExitApi.exit() for Google Ads; playableSDK.openAppStore() for TikTok; window.install() for Mintegral; and parent.postMessage('download', '\*') for Liftoff/Vungle1.  
* SDK Ready-State Gating: MRAID-compliant networks require creatives to wait for container initialization (mraid.getState() \!== 'loading' or the ready event listener) before executing game logic or layout scripts1.  
* Audio Autoplay Policies: Ad networks enforce muted audio on startup, requiring explicit user tap gestures to unlock WebAudio contexts and requiring audio to pause when ads lose focus or close2.  
* Screen Orientation Adaptation: Playable creatives must dynamically adapt to portrait and landscape modes, with TikTok additionally requiring a root config.json file specifying orientation parameters1.  
* Game Completion Signals: Specific networks, such as Mintegral, require explicit end-of-game API calls (GameEnded or window.gameEnd()) to register creative completion and display store overlays7.  
* Memory Constraints on Mobile Webviews: Setting WebGL heap memory budgets between 32 MB and 64 MB is critical to prevent out-of-memory (OOM) browser tab crashes on low-end Android devices12.  
* Multi-Network Export Tooling: Open-source pipelines (e.g., @smoud/playable-scripts, @smoud/playable-sdk, cocos-pnp) automate code inlining, asset compression, and network API injection from a single source codebase13.

## **Per network**

### **Unity Ads**

Unity Ads requires playable creatives to be self-contained in a single inline HTML file, strictly prohibiting external network calls1. The network operates under MRAID 2.0 and MRAID 3.0 protocols, expecting ads to wait for the MRAID ready state before executing gameplay scripts1.

| Item | Requirement | Source URL | Opened? |
| :---- | :---- | :---- | :---- |
| File format | Single self-contained HTML file (index.html) with all assets inlined1 | https\://docs.unity.com/en-us/user-acquisition/creatives/creative-specifications | Yes (snippet only) |
| Max size | 5 MB hard limit (2 MB recommended for optimal performance)1 | https\://docs.unity.com/en-us/user-acquisition/creatives/creative-specifications | Yes (snippet only) |
| External requests | None permitted; no external CDN hosting, fonts, or XHR calls allowed1 | https\://playablekit.app/unity-ads-playable-ads.html | Yes (snippet only) |
| Required SDK/API | MRAID 2.0 / 3.0 compliant; check mraid.getState() or listen to ready \[cite: 1, 8, 17\] | https\://docs.unity.com/en-us/user-acquisition/creatives/creative-specifications | Yes (snippet only) |
| CTA call | mraid.open(storeUrl) \[cite: 1, 8, 17\] | https\://docs.unity.com/en-us/user-acquisition/creatives/creative-specifications | Yes (snippet only) |
| Orientation | Must support both portrait and landscape screen orientation dynamically1 | https\://docs.unity.com/en-us/user-acquisition/creatives/creative-specifications | Yes (snippet only) |
| Max duration / forced end | Long video/playable ads 15 to 60 seconds (beta up to 60s)1 | https\://docs.unity.com/en-us/user-acquisition/creatives/creative-specifications | Yes (snippet only) |
| Audio rules | Autoplay muted; no audio may auto-play before initial user tap8 | https\://playablekit.app/unity-ads-playable-ads.html | Yes (snippet only) |
| Loading / start requirements | Minimum 2-second Unity loading screen overlay on launch; wait for viewableChange \[cite: 1, 19\] | https\://docs.unity3d.com/Packages/com.unity.ads@3.4/manual/AdvertisingResourcesCreativeSpecs.html | Yes (snippet only) |
| Required metadata | Minimum OS: Android 4.4+ / iOS 9.0+1 | https\://docs.unity.com/en-us/user-acquisition/creatives/creative-specifications | Yes (snippet only) |
| Review / approval process | Automated QA moderation (gambling creatives require managed account status)1 | https\://docs.unity.com/en-us/user-acquisition/creatives/creative-specifications | Yes (snippet only) |
| Official test tool | Unity Ad Testing App (iOS/Android) and MRAID Web Tester (webtester.mraid.org)18 | https\://docs.lunalabs.io/docs/playable/ad-networks/unity-ads | Yes (snippet only) |

### **AppLovin (AppDiscovery / AXON)**

AppLovin enforces a single-file HTML packaging rule with base64 or base122 asset encoding2. Ad execution must defer timer initialization and audio playback until after the user performs their first tap gesture2.

| Item | Requirement | Source URL | Opened? |
| :---- | :---- | :---- | :---- |
| File format | Single inline HTML file containing all assets2 | https\://support.applovin.com/en/growth/promoting-your-apps/welcome-to-applovin/creative-specs-and-guidelines | Yes (snippet only) |
| Max size | 5 MB maximum file size cap2 | https\://support.applovin.com/en/growth/promoting-your-apps/welcome-to-applovin/creative-specs-and-guidelines | Yes (snippet only) |
| External requests | Prohibited; external network calls cause automatic QA rejection2 | https\://support.applovin.com/en/growth/promoting-your-apps/welcome-to-applovin/creative-specs-and-guidelines | Yes (snippet only) |
| Required SDK/API | MRAID v2.0; wait for ready event (mraid.getState() \!== 'loading')2 | https\://support.applovin.com/en/growth/promoting-your-apps/welcome-to-applovin/creative-specs-and-guidelines | Yes (snippet only) |
| CTA call | mraid.open(url) \[cite: 2, 21, 22\] | https\://support.applovin.com/en/growth/promoting-your-apps/welcome-to-applovin/creative-specs-and-guidelines | Yes (snippet only) |
| Orientation | Must function properly in both landscape and portrait orientations2 | https\://support.applovin.com/en/growth/promoting-your-apps/welcome-to-applovin/creative-specs-and-guidelines | Yes (snippet only) |
| Max duration / forced end | Rewarded MRAID placements have a maximum duration of 30 seconds22 | https\://support.applovin.com/en/max/demand-partners/demand-side-platforms/applovin-ortb-specification/creative-types | Yes (snippet only) |
| Audio rules | Muted until first user interaction; audio must stop when ad is closed/hidden2 | https\://support.applovin.com/en/growth/promoting-your-apps/welcome-to-applovin/creative-specs-and-guidelines | Yes (snippet only) |
| Loading / start requirements | Ad timer starts only after first interaction; UI fallback required if WebGL fails2 | https\://support.applovin.com/en/growth/promoting-your-apps/welcome-to-applovin/creative-specs-and-guidelines | Yes (snippet only) |
| Required metadata | Base64 or base122 inline encoding2 | https\://support.applovin.com/en/growth/promoting-your-apps/welcome-to-applovin/creative-specs-and-guidelines | Yes (snippet only) |
| Review / approval process | Automated platform QA moderation6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| Official test tool | AppLovin Playable Preview tool (p.applov.in/playablePreview) and Mobile Preview App6 | https\://p.applov.in/playablePreview?create=1 | Yes (snippet only) |

### **Unity LevelPlay (ironSource)**

Unity LevelPlay delivers interactive playables across its programmatic mediation exchange23. Legacy implementations based on DAPI have been transitioned to standard MRAID protocols25.

| Item | Requirement | Source URL | Opened? |
| :---- | :---- | :---- | :---- |
| File format | Single self-contained HTML file6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| Max size | 5 MB hard limit6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| External requests | Prohibited; all resources must be embedded inlined6 | https\://playablekit.app/unity-ads-playable-ads.html | Yes (snippet only) |
| Required SDK/API | MRAID standard interface (migrated from legacy DAPI standard)25 | https\://docs.lunalabs.io/docs/playable/release-notes | Yes (snippet only) |
| CTA call | mraid.open(storeUrl) \[cite: 5, 11\] | https\://awesome.ecosyste.ms/projects/github.com%2Findiesoftby%2Fdefold-playable-ads | Yes (snippet only) |
| Orientation | Dynamic adaptation supporting portrait and landscape orientations7 | https\://appagent.com/blog/what-are-playable-ads/ | Yes (snippet only) |
| Max duration / forced end | NOT FOUND | https\://docs.lunalabs.io/docs/playable/release-notes | Yes (snippet only) |
| Audio rules | Audio muted until initial explicit user gesture25 | https\://docs.lunalabs.io/docs/playable/release-notes | Yes (snippet only) |
| Loading / start requirements | Standard MRAID ready-state gating26 | https\://docs.lunalabs.io/docs/playable/unityplayworks/release-notes | Yes (snippet only) |
| Required metadata | NOT FOUND | https\://docs.lunalabs.io/docs/playable/unityplayworks/release-notes | Yes (snippet only) |
| Review / approval process | Creative QA submission review6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| Official test tool | LevelPlay Creative Testing / Playworks Preview platform6 | https\://docs.lunalabs.io/docs/playable/unityplayworks/release-notes | Yes (snippet only) |

### **Mintegral**

Mintegral supports both single HTML files and ZIP packages6. In addition to handling store click-throughs, Mintegral requires creatives to inform the SDK when gameplay ends7.

| Item | Requirement | Source URL | Opened? |
| :---- | :---- | :---- | :---- |
| File format | ZIP archive or single HTML file containing index.html and assets6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| Max size | 5 MB total compressed size6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| External requests | Prohibited; no external resource calls allowed6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| Required SDK/API | MRAID 2.0 with Mindworks custom extensions (window.install, GameEnded)7 | https\://appagent.com/blog/what-are-playable-ads/ | Yes (snippet only) |
| CTA call | window.install() or mraid.open() \[cite: 7, 11\] | https\://awesome.ecosyste.ms/projects/github.com%2Findiesoftby%2Fdefold-playable-ads | Yes (snippet only) |
| Orientation | Supports portrait, landscape, or auto-adapting layouts6 | https\://adv-new.mintegral.com/doc/en/creatives/playable.html | Yes (snippet only) |
| Max duration / forced end | Typical mini-game duration spans 10 to 45 seconds29 | https\://www\.mintegral.com/en/case/creative-playable-ad-for-boosting-ua-performance | Yes (snippet only) |
| Audio rules | Autoplay muted by default until user tap | https\://adv-new.mintegral.com/doc/en/creatives/playable.html | Yes (snippet only) |
| Loading / start requirements | Must call GameEnded or window.gameEnd() upon finishing gameplay7 | https\://appagent.com/blog/what-are-playable-ads/ | Yes (snippet only) |
| Required metadata | Asset archive root HTML naming must match package specification7 | https\://appagent.com/blog/what-are-playable-ads/ | Yes (snippet only) |
| Review / approval process | Automated QA review via Mindworks platform6 | https\://adv-new.mintegral.com/doc/en/creatives/playable.html | Yes (snippet only) |
| Official test tool | Mindworks Playable Testing / Mintegral Playable Preview Tool6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |

### **Google Ads (App campaigns, HTML5 assets)**

Google Ads requires playable creatives for App campaigns to be delivered as .ZIP archives containing an index.html file and supporting web assets3. CTA redirects must leverage Google's proprietary ExitApi9.

| Item | Requirement | Source URL | Opened? |
| :---- | :---- | :---- | :---- |
| File format | .ZIP archive containing index.html, CSS, JS, and images (max 512 files)3 | https\://github.com/indiesoftby/defold-playable-ads | Yes (snippet only) |
| Max size | 5 MB maximum uncompressed package size3 | https\://segwise.ai/blog/understanding-playable-ads-guide | Yes (snippet only) |
| External requests | Strictly prohibited; no external network requests or dynamic XHR7 | https\://github.com/indiesoftby/defold-playable-ads | Yes (snippet only) |
| Required SDK/API | Google ExitApi library (ExitApi.js)9 | https\://support.google.com/google-ads/answer/9981650?hl=en | Yes (snippet only) |
| CTA call | ExitApi.exit() \[cite: 3, 9, 31, 32\] | https\://support.google.com/google-ads/answer/9981650?hl=en | Yes (snippet only) |
| Orientation | Dynamic adaptation supporting portrait and landscape formats3 | https\://segwise.ai/blog/understanding-playable-ads-guide | Yes (snippet only) |
| Max duration / forced end | NOT FOUND | https\://support.google.com/google-ads/answer/9981650?hl=en | Yes (snippet only) |
| Audio rules | Muted by default3 | https\://segwise.ai/blog/understanding-playable-ads-guide | Yes (snippet only) |
| Loading / start requirements | UTF-8 encoded index.html located at the root directory level12 | https\://support.google.com/google-ads/answer/6335679?hl=en | Yes (snippet only) |
| Required metadata | Standard HTML5 meta viewport parameters12 | https\://github.com/indiesoftby/defold-playable-ads | Yes (snippet only) |
| Review / approval process | Automated Google Ads HTML5 asset compliance check31 | https\://support.google.com/google-ads/answer/6335679?hl=en | Yes (snippet only) |
| Official test tool | Google Ads HTML5 Validator / Google Playable Testing Tool6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |

### **Meta (Facebook / Instagram Playable Ads)**

Meta supports both single HTML files and .ZIP archives6. Single HTML builds are restricted to 2 MB, whereas .ZIP packages allow up to 5 MB6. Clicks are handled via Meta's FbPlayableAd interface7.

| Item | Requirement | Source URL | Opened? |
| :---- | :---- | :---- | :---- |
| File format | Single HTML file (inline data URIs) or ZIP archive containing assets6 | https\://github.com/indiesoftby/defold-playable-ads | Yes (snippet only) |
| Max size | 2 MB cap for single HTML; 5 MB cap for ZIP package6 | https\://github.com/indiesoftby/defold-playable-ads | Yes (snippet only) |
| External requests | Strictly prohibited; zero external network calls allowed7 | https\://github.com/indiesoftby/defold-playable-ads | Yes (snippet only) |
| Required SDK/API | Meta FbPlayableAd JavaScript API7 | https\://awesome.ecosyste.ms/projects/github.com%2Findiesoftby%2Fdefold-playable-ads | Yes (snippet only) |
| CTA call | FbPlayableAd.onCTAClick() \[cite: 8, 11\] | https\://playablekit.app/unity-ads-playable-ads.html | Yes (snippet only) |
| Orientation | Responsive layout matching mobile aspect ratios (portrait primary)7 | https\://appagent.com/blog/what-are-playable-ads/ | Yes (snippet only) |
| Max duration / forced end | Maximum play duration capped at 60 seconds30 | https\://coinis.com/glossary/playable-mobile-ads | Yes (snippet only) |
| Audio rules | Audio muted until initial user interaction gesture7 | https\://appagent.com/blog/what-are-playable-ads/ | Yes (snippet only) |
| Loading / start requirements | JS redirects and automatic store redirects are forbidden2 | https\://developers.facebook.com/blog/post/2019/08/22/introducing-playable-preview-tool-to-validate-playable-assets/ | Yes (snippet only) |
| Required metadata | Embedded assets using base64 data URIs12 | https\://github.com/indiesoftby/defold-playable-ads | Yes (snippet only) |
| Review / approval process | Meta Ads Manager asset validation review33 | https\://developers.facebook.com/blog/post/2019/08/22/introducing-playable-preview-tool-to-validate-playable-assets/ | Yes (snippet only) |
| Official test tool | Meta Playable Preview Tool (developers.facebook.com/tools/playable-preview)6 | https\://developers.facebook.com/blog/post/2019/08/22/introducing-playable-preview-tool-to-validate-playable-assets/ | Yes (snippet only) |

### **TikTok / Pangle**

TikTok and Pangle enforce a structure requiring a compressed .ZIP file containing a root index.html file, supporting assets, and a mandatory config.json file defining orientation adaptation rules4. Standard mraid.js is explicitly prohibited4.

| Item | Requirement | Source URL | Opened? |
| :---- | :---- | :---- | :---- |
| File format | .ZIP file containing root index.html and root config.json \[cite: 4, 35\] | https\://ads.tiktok.com/resources/help/article/playable-ads?lang=en | Yes (snippet only) |
| Max size | 5 MB maximum compressed ZIP size4 | https\://ads.tiktok.com/resources/help/article/playable-ads?lang=en | Yes (snippet only) |
| External requests | Strictly forbidden; no HTTP/HTTPS requests, dynamic loads, or JS redirects4 | https\://ads.tiktok.com/resources/help/article/playable-ads?lang=en | Yes (snippet only) |
| Required SDK/API | TikTok Playable JS-SDK (Must NOT contain or reference mraid.js)4 | https\://ads.tiktok.com/resources/help/article/playable-ads?lang=en | Yes (snippet only) |
| CTA call | playableSDK.openAppStore() / managed via TikTok JS-SDK4 | https\://ads.tiktok.com/resources/help/article/playable-ads?lang=zh-Hant-TW | Yes (snippet only) |
| Orientation | Defined in config.json ("Playable\_orientation": 0=Auto, 1=Portrait, 2=Landscape)4 | https\://ads.tiktok.com/resources/help/article/playable-ads?lang=en | Yes (snippet only) |
| Max duration / forced end | Paired video duration recommended 10-25s (max 30s)35 | https\://ads.tiktok.com/resources/help/article/how-to-create-tiktok-pangle-playable-ads | Yes (snippet only) |
| Audio rules | Managed via TikTok JS-SDK; browser previews require initial tap gesture4 | https\://ads.tiktok.com/resources/help/article/playable-ads?lang=zh-Hant-TW | Yes (snippet only) |
| Loading / start requirements | Must observe in-feed safe zone overlays to avoid UI element collision4 | https\://ads.tiktok.com/resources/help/article/playable-ads?lang=en | Yes (snippet only) |
| Required metadata | Root config.json containing "Playable\_orientation" parameter4 | https\://ads.tiktok.com/resources/help/article/playable-ads?lang=en | Yes (snippet only) |
| Review / approval process | TikTok Ads Manager ad review moderation35 | https\://ads.tiktok.com/resources/help/article/how-to-create-playable-ads-on-tiktok-ads-manager | Yes (snippet only) |
| Official test tool | TikTok Ads Manager Playable Preview and QR Code mobile device tester35 | https\://ads.tiktok.com/resources/help/article/how-to-create-playable-ads-on-tiktok-ads-manager | Yes (snippet only) |

### **Moloco**

Moloco accepts both single HTML files and ZIP archives, primarily targeting single HTML outputs6. Depending on the inventory placement, it utilizes FbPlayableAd or standard MRAID click protocols7.

| Item | Requirement | Source URL | Opened? |
| :---- | :---- | :---- | :---- |
| File format | Single HTML file or ZIP archive (HTML preferred)6 | https\://docs.lunalabs.io/docs/playable/unityplayworks/release-notes | Yes (snippet only) |
| Max size | 2 MB for single HTML; 5 MB for ZIP archive6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| External requests | Prohibited; zero external network requests allowed7 | https\://appagent.com/blog/what-are-playable-ads/ | Yes (snippet only) |
| Required SDK/API | Leverages FbPlayableAd API or MRAID specifications7 | https\://appagent.com/blog/what-are-playable-ads/ | Yes (snippet only) |
| CTA call | FbPlayableAd.onCTAClick() or mraid.open() \[cite: 7, 11\] | https\://awesome.ecosyste.ms/projects/github.com%2Findiesoftby%2Fdefold-playable-ads | Yes (snippet only) |
| Orientation | Supports portrait and landscape screen configurations7 | https\://appagent.com/blog/what-are-playable-ads/ | Yes (snippet only) |
| Max duration / forced end | NOT FOUND | https\://docs.lunalabs.io/docs/playable/unityplayworks/release-notes | Yes (snippet only) |
| Audio rules | Audio off by default until explicit user interaction7 | https\://appagent.com/blog/what-are-playable-ads/ | Yes (snippet only) |
| Loading / start requirements | Must adhere to Moloco ASSET metadata specifications26 | https\://docs.lunalabs.io/docs/playable/unityplayworks/release-notes | Yes (snippet only) |
| Required metadata | ASSET metadata fields26 | https\://docs.lunalabs.io/docs/playable/unityplayworks/release-notes | Yes (snippet only) |
| Review / approval process | Moloco DSP creative upload validation6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| Official test tool | Moloco Creative Preview tool / Playworks environment6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |

### **Liftoff (incl. Vungle)**

Liftoff (incorporating the Vungle Exchange) accepts ZIP bundles and single HTML files5. Creatives must implement specific window messaging events (parent.postMessage('download', '\*')) or MRAID click hooks5.

| Item | Requirement | Source URL | Opened? |
| :---- | :---- | :---- | :---- |
| File format | ZIP archive with resources or single HTML file (index.html or ad.html)5 | https\://support.vungle.com/hc/en-us/articles/4908908675355-Test-Your-Playable-Asset-With-Our-Creative-Verifier | Yes (snippet only) |
| Max size | 5 MB maximum file size cap5 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| External requests | Prohibited; all resources must be packaged locally5 | https\://support.vungle.com/hc/en-us/articles/8709966237723-HTML-and-MRAID-Creatives-Submission-Guidelines | Yes (snippet only) |
| Required SDK/API | MRAID 2.0 or Liftoff Adaptive Creative API5 | https\://support.vungle.com/hc/en-us/articles/8709966237723-HTML-and-MRAID-Creatives-Submission-Guidelines | Yes (snippet only) |
| CTA call | parent.postMessage('download', '\*') or mraid.open(url) \[cite: 5, 10, 11\] | https\://support.vungle.com/hc/en-us/articles/4908908675355-Test-Your-Playable-Asset-With-Our-Creative-Verifier | Yes (snippet only) |
| Orientation | Adaptive creative supporting portrait and landscape orientations10 | https\://support.vungle.com/hc/en-us/articles/4908908675355-Test-Your-Playable-Asset-With-Our-Creative-Verifier | Yes (snippet only) |
| Max duration / forced end | Rewarded video/playable durations max 46s5 | https\://support.vungle.com/hc/en-us/articles/8709966237723-HTML-and-MRAID-Creatives-Submission-Guidelines | Yes (snippet only) |
| Audio rules | Muted until user tap; audio must pause when container focus is lost2 | https\://support.vungle.com/hc/en-us/articles/8709966237723-HTML-and-MRAID-Creatives-Submission-Guidelines | Yes (snippet only) |
| Loading / start requirements | Must trigger adaptive lifecycle events (download event)10 | https\://support.vungle.com/hc/en-us/articles/4908908675355-Test-Your-Playable-Asset-With-Our-Creative-Verifier | Yes (snippet only) |
| Required metadata | OpenRTB seatbid creative type attributes (crtype: MRAID 2.0)5 | https\://support.vungle.com/hc/en-us/articles/8709966237723-HTML-and-MRAID-Creatives-Submission-Guidelines | Yes (snippet only) |
| Review / approval process | Liftoff creative QA review cycle10 | https\://support.vungle.com/hc/en-us/articles/4908908675355-Test-Your-Playable-Asset-With-Our-Creative-Verifier | Yes (snippet only) |
| Official test tool | Liftoff Creative Verifier App (iOS & Android)10 | https\://support.vungle.com/hc/en-us/articles/4908908675355-Test-Your-Playable-Asset-With-Our-Creative-Verifier | Yes (snippet only) |

### **Chartboost**

Chartboost operates under MRAID 2.0 guidelines, enforcing a strict 3 MB total package size limit6.

| Item | Requirement | Source URL | Opened? |
| :---- | :---- | :---- | :---- |
| File format | Single HTML file or ZIP package6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| Max size | 3 MB strict file size limit6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| External requests | Prohibited6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| Required SDK/API | MRAID 2.06 | https\://github.com/topics/playable-ad?o=asc\&s=stars | Yes (snippet only) |
| CTA call | mraid.open(url) \[cite: 6, 11\] | https\://awesome.ecosyste.ms/projects/github.com%2Findiesoftby%2Fdefold-playable-ads | Yes (snippet only) |
| Orientation | Dynamic adaptation supporting portrait and landscape screen sizes6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| Max duration / forced end | NOT FOUND | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| Audio rules | Muted by default until initial user interaction6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| Loading / start requirements | MRAID 2.0 container initialization6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| Required metadata | NOT FOUND | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| Review / approval process | Chartboost ad moderation review6 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) |
| Official test tool | MRAID Web Tester (webtester.mraid.org)18 | https\://2dkit.com/playable-ads/create-line-playable-ads-tutorial/ | Yes (snippet only) |

## **Comparison table**

| Network | Format | Max size | External requests | API / MRAID | CTA call | Orientation | Audio | Test tool | Source |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| Unity Ads | Single HTML | 5 MB | Prohibited | MRAID 2.0 / 3.0 | mraid.open() | Portrait & Landscape | Muted until tap | Unity Ad Testing App | Unity Docs1 |
| AppLovin | Single HTML | 5 MB | Prohibited | MRAID 2.0 | mraid.open() | Portrait & Landscape | Muted until tap | Playable Preview Tool | AppLovin Docs2 |
| Unity LevelPlay | Single HTML | 5 MB | Prohibited | MRAID 2.0 | mraid.open() | Portrait & Landscape | Muted until tap | Playworks Preview | Luna / LevelPlay Docs6 |
| Mintegral | Single HTML / ZIP | 5 MB | Prohibited | MRAID 2.0 \+ Custom | window.install() / mraid.open() | Portrait & Landscape | Muted until tap | Mindworks Tester | Mintegral Docs6 |
| Google Ads | ZIP (max 512 files) | 5 MB | Prohibited | ExitApi | ExitApi.exit() | Portrait & Landscape | Muted by default | HTML5 Validator | Google Ads Docs3 |
| Meta | Single HTML / ZIP | 2 MB (HTML) / 5 MB (ZIP) | Prohibited | FbPlayableAd | FbPlayableAd.onCTAClick() | Responsive | Muted until tap | Meta Playable Preview | Meta Dev Docs7 |
| TikTok / Pangle | ZIP \+ config.json | 5 MB | Prohibited | TikTok JS-SDK | playableSDK.openAppStore() | Configured via JSON | Autoplay restrictions | TikTok Ads Manager | TikTok Docs4 |
| Moloco | Single HTML / ZIP | 2 MB (HTML) / 5 MB (ZIP) | Prohibited | FbPlayableAd / MRAID | FbPlayableAd.onCTAClick() | Portrait & Landscape | Muted by default | Playworks / Moloco Tool | PlayTurbo Specs6 |
| Liftoff (Vungle) | ZIP / Single HTML | 5 MB | Prohibited | MRAID 2.0 / Adaptive | parent.postMessage('download', '\*') | Portrait & Landscape | Muted until tap | Creative Verifier App | Liftoff / Vungle Docs5 |
| Chartboost | Single HTML / ZIP | 3 MB | Prohibited | MRAID 2.0 | mraid.open() | Portrait & Landscape | Muted by default | MRAID Web Tester | PlayTurbo Specs6 |

## **Common rules vs per-network variants**

Cross-network deployment requires distinguishing baseline rules that apply universally from network-specific build targets7.

### **Lowest Common Denominator (Universal Baseline)**

A single build target can satisfy all networks if configured according to the following restrictive constraints:

* Packaging Size Budget: Keeping the uncompressed build bundle under 2.0 MB satisfies all platforms, including Meta single-file HTML limits (2 MB) and Chartboost caps (3 MB)6.  
* External Request Blackout: The creative must contain zero external HTTP/HTTPS network calls, eliminating external WebFonts, CDN-hosted frameworks, dynamic XHR, and remote analytics pixels2.  
* Audio Initialization: WebAudio contexts and HTML audio elements must initialize in a muted state, postponing sound activation until an explicit user tap gesture occurs2.  
* Responsive Aspect Adaptation: The canvas layout must dynamically adjust between 9:16 (portrait) and 16:9 (landscape) aspect ratios, positioning game UI elements clear of native container close buttons and overlay bars1.  
* Async Lifecycle Initialization: Application bootstrapping must wait until the webview container signals readiness (mraid.ready or DOM load completion)1.

### **Required Per-Network Variants**

Despite shared baseline principles, automated build pipelines must generate distinct network variants to address specific platform requirements:

* Packaging Format: Automated exporters must compile single-file inline HTML packages (index.html) for Unity Ads, AppLovin, and LevelPlay while packaging zip archives with asset directories for Google Ads and TikTok3.  
* API Abstraction Layer: The call-to-action click-through call requires a JS shim layer that dynamically maps store clicks to mraid.open() (Unity, AppLovin, LevelPlay, Chartboost), ExitApi.exit() (Google Ads), FbPlayableAd.onCTAClick() (Meta, Moloco), playableSDK.openAppStore() (TikTok), window.install() (Mintegral), or parent.postMessage('download', '\*') (Liftoff)1.  
* Manifest Injection: TikTok builds require the automated generation of a root-level config.json file specifying orientation parameters ("Playable\_orientation": 0\)4.  
* Game Completion Hooks: Mintegral builds require additional hooks triggering GameEnded or window.gameEnd() upon game completion to trigger platform end-cards7.

## **Rejection reasons**

Playable ads frequently fail automated platform moderation or manual QA review due to specific technical non-compliance items2.

* Invalid Click-Through API Calls: Using generic window.open(), window.location.href, or mismatched network API methods prevents store redirects and results in immediate automated rejection2.  
* Leftover External Network References: Leaving development @import font tags, CDN scripts, or tracking pixels in the code violates no-external-request policies, even if those paths are never executed at runtime2.  
* Base64 Expansion Size Breaches: Base64 asset encoding increases raw binary size by approximately 33%, causing packages that were under 5 MB on disk to exceed size limits once inlined into a single HTML file7.  
* Unmuted Autoplay Audio: Including audio elements with the autoplay attribute set or invoking .play() prior to user interaction triggers moderation flags on AppLovin, Unity, and Google Ads2.  
* Premature Initialization Errors: Invoking MRAID or game engine methods before the container triggers the ready event causes black screens or context failures in webviews that initialize asynchronously1.  
* UI Safe Zone Obstructions: Failing to account for device notches, native close buttons, or platform UI overlays (such as TikTok in-feed comments or Unity loading overlays) results in rejected builds1.  
* Automatic Redirects (Auto-Clicking): Triggering CTAs automatically without explicit user action or executing store redirects on the initial touch gesture is strictly prohibited1.  
* Excessive Memory Usage: High WebGL memory allocation that causes browser context crashes on low-end Android devices leads to validation failures2.

## **Tools (GitHub)**

Open-source build pipelines and unified SDK wrappers streamline multi-network compilation by abstracting API differences and automating asset inlining13.

| Name | URL | What it does | Last update | Stars |
| :---- | :---- | :---- | :---- | :---- |
| @smoud/playable-scripts | https\://github.com/smoudjs/playable-scripts | Command-line tool and Webpack build pipeline that compiles HTML5 playable ads into network-compliant HTML/ZIP packages for Google, Meta, AppLovin, Unity, LevelPlay, Vungle, and Mintegral15 | Recent (2025/2026) | \~15+13 |
| @smoud/playable-sdk | https\://github.com/smoudjs/playable-sdk | Unified JavaScript SDK providing a standardized interface (init, start, finish, install) across MRAID, Google ExitApi, Meta, and Vungle14 | Recent (2025/2026) | \~15+13 |
| cocos-pnp (Playable Ads Adapter) | https\://github.com/ppgee/cocos-pnp | Cocos Creator plugin for multi-channel playable exporting (Meta, Google, Unity, TikTok, AppLovin) with automated asset inlining and Tinypng/Pako compression16 | 2024/2025 | \~100+13 |
| defold-playable-ads | https\://github.com/indiesoftby/defold-playable-ads | Build pipeline using Gulp tasks to package lightweight Defold engine builds into single-file HTML playables targeting Meta, Google, AppLovin, and Unity12 | August 2025 | \~25+13 |
| minigame-playable | https\://github.com/wechat-miniprogram/minigame-playable | Build adapter and setup templates for adapting WeChat MiniGames and Cocos Creator 3D projects into standard playable ad packages39 | 2024/2025 | \~50+13 |

## **Performance guidance**

Maintaining high performance across mid-to-low-tier mobile devices requires strict runtime optimizations7.

### **Initial Load Time Optimization**

The ad must render and become interactive within 2.0 seconds2. Developers should keep the core HTML and JavaScript bundle size below 1.5 MB before asset extraction8. Non-essential assets (such as secondary audio effects or victory animations) should be lazy-loaded or decoded after the main gameplay screen renders27.

### **Time to Engage (TTE)**

Time to engage represents the interval between creative rendering and the user's initial interaction27. TTE should be kept under 3.0 seconds by displaying immediate visual prompts, such as animated pointer gestures, pulsing targets, or explicit "Tap to Start" tutorials on the first frame4.

### **Target Frame Rate**

Creatives must maintain a target frame rate of 60 FPS, with a strict floor of 30 FPS on lower-tier mobile webviews13. To prevent frame drops, developers should minimize DOM manipulation, restrict dynamic WebGL lighting passes, keep draw calls under 30 per frame, and pack visual assets into optimized texture atlases25.

### **Memory Constraints on Low-End Android Devices**

Low-end Android devices (such as budget devices with 2 GB–3 GB of RAM running WebViews) enforce strict memory allocation caps12. When building WebGL projects (e.g., Unity or Defold exports), developers should set the WebGL heap size (TOTAL\_MEMORY) between 32 MB and 64 MB12. Textures should be constrained to 1024x1024 resolution and compressed using GPU-native formats (e.g., ASTC or ETC2) to prevent GPU memory exhaustion and context loss2.

## **Sources list**

| \# | Title | Publisher | Year | URL | Opened? (yes/no) | Primary/secondary |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| 1 | Unity Ads Creative Specifications | Unity Technologies | 2026 | https\://docs.unity.com/en-us/user-acquisition/creatives/creative-specifications | Yes (snippet only) | Primary |
| 2 | Playable Asset Specifications | Unity Technologies | 2025 | https\://docs.unity.com/en-us/grow/acquire/creatives/playable/specifications | Yes (snippet only) | Primary |
| 3 | Display Ads and MRAID Implementation | Unity Technologies | 2025 | https\://docs.unity.com/grow/exchange/display-ads | Yes (snippet only) | Primary |
| 4 | Unity Ads Playable Specifications | Playable Kit | 2025 | https\://playablekit.app/unity-ads-playable-ads.html | Yes (snippet only) | Secondary |
| 5 | AppLovin Creative Specifications and Guidelines | AppLovin | 2025 | https\://support.applovin.com/en/growth/promoting-your-apps/welcome-to-applovin/creative-specs-and-guidelines | Yes (snippet only) | Primary |
| 6 | AppLovin Playable Preview Tool | AppLovin | 2026 | https\://p.applov.in/playablePreview?create=1 | Yes (snippet only) | Primary |
| 7 | TikTok Playable Ads Material Specifications | TikTok | 2026 | https\://ads.tiktok.com/resources/help/article/playable-ads?lang=en | Yes (snippet only) | Primary |
| 8 | TikTok Ad Network (Pangle) Playable Specs | TikTok | 2025 | https\://ads.tiktok.com/resources/help/article/how-to-create-tiktok-pangle-playable-ads | Yes (snippet only) | Primary |
| 9 | Vungle HTML and MRAID Submission Guidelines | Liftoff / Vungle | 2024 | https\://support.vungle.com/hc/en-us/articles/8709966237723-HTML-and-MRAID-Creatives-Submission-Guidelines | Yes (snippet only) | Primary |
| 10 | Creative Verifier Testing Guidelines | Liftoff / Vungle | 2023 | https\://support.vungle.com/hc/en-us/articles/4908908675355-Test-Your-Playable-Asset-With-Our-Creative-Verifier | Yes (snippet only) | Primary |
| 11 | Google Ads App Campaigns HTML5 Asset Rules | Google | 2025 | https\://support.google.com/google-ads/answer/9981650?hl=en | Yes (snippet only) | Primary |
| 12 | Playable Preview Tool Announcement | Meta for Developers | 2025 | https\://developers.facebook.com/blog/post/2019/08/22/introducing-playable-preview-tool-to-validate-playable-assets/ | Yes (snippet only) | Primary |
| 13 | Mintegral Playable Creative Operations | Mintegral / Mindworks | 2025 | https\://adv-new.mintegral.com/doc/en/creatives/playable.html | Yes (snippet only) | Primary |
| 14 | Playable Upload Specifications for Networks | PlayTurbo / Mindworks | 2025 | https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks | Yes (snippet only) | Secondary |
| 15 | Unity Playworks Release Notes & Platform Spec Updates | Luna Labs / Unity | 2025 | https\://docs.lunalabs.io/docs/playable/unityplayworks/release-notes | Yes (snippet only) | Secondary |
| 16 | Defold Playable Ads Tooling Repository | Indiesoftby (GitHub) | 2025 | https\://github.com/indiesoftby/defold-playable-ads | Yes (snippet only) | Secondary |

## **Gaps**

While official network documentation establishes primary technical rules, several implementation details require careful handling during development:

* Programmatic MRAID Version Discrepancies: Modern developer documentation for Unity Ads references MRAID 3.0 compliance1, whereas most programmatic DSP exchanges (such as AppLovin and Liftoff) enforce MRAID 2.0 standards2. Utilizing MRAID 3.0-specific calls (e.g., mraid.unload()) may cause execution errors on MRAID 2.0 containers1.  
* WebGL Context Recovery Behavior: Documentation across networks generally recommends providing a fallback UI if WebGL initialization fails2, but specific programmatic expectations for WebGL context loss recovery remain undefined.  
* Dynamic Screen Notch Safe Zone Metrics: Exact pixel margins for network-injected overlays (such as TikTok's in-feed comment bars or Unity's 2-second startup overlay) vary by device screen aspect ratio, requiring empirical testing on physical devices rather than relying on fixed safe-zone values1.  
* Sandbox Interception Logic: The specific mechanisms used by automated moderation sandboxes to mock or intercept store redirect calls (mraid.open() vs direct window calls) are proprietary, making pre-upload validation dependent on network preview apps10.

#### **Nguồn trích dẫn**

> 1. Creative specifications • User Acquisition • Unity Docs, [https\://docs.unity.com/en-us/user-acquisition/creatives/creative-specifications](https://docs.unity.com/en-us/user-acquisition/creatives/creative-specifications)  
> 2. ADVERTISE | Promoting your apps | Creative specs & guidelines, [https\://support.applovin.com/en/growth/promoting-your-apps/welcome-to-applovin/creative-specs-and-guidelines](https://support.applovin.com/en/growth/promoting-your-apps/welcome-to-applovin/creative-specs-and-guidelines)  
> 3. Playable Ads Guide 2026: Formats, Specs, Examples and Best, [https\://segwise.ai/blog/understanding-playable-ads-guide](https://segwise.ai/blog/understanding-playable-ads-guide)  
> 4. About TikTok Playable ads | TikTok Ads Manager, [https\://ads.tiktok.com/resources/help/article/playable-ads?lang=en](https://ads.tiktok.com/resources/help/article/playable-ads?lang=en)  
> 5. HTML and MRAID Creatives Submission Guidelines \- Help Center, [https\://support.vungle.com/hc/en-us/articles/8709966237723-HTML-and-MRAID-Creatives-Submission-Guidelines](https://support.vungle.com/hc/en-us/articles/8709966237723-HTML-and-MRAID-Creatives-Submission-Guidelines)  
> 6. Playable Upload Specifications for Networks | Playturbo Help Center, [https\://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks](https://doc.playturbo.com/export-and-deployment/playable-upload-specifications-for-networks)  
> 7. What Are Playable Ads? Examples, Strategy & Best Practices, [https\://appagent.com/blog/what-are-playable-ads/](https://appagent.com/blog/what-are-playable-ads/)  
> 8. Unity Ads Playable Ads: Requirements, Formats & Common Errors, [https\://playablekit.app/unity-ads-playable-ads.html](https://playablekit.app/unity-ads-playable-ads.html)  
> 9. The Ultimate Guide to Creating Playable Ads \- AppSamurai, [https\://appsamurai.com/blog/creating-playable-ads/](https://appsamurai.com/blog/creating-playable-ads/)  
> 10. Test Your Playable Asset With Our Creative Verifier \- Help Center, [https\://support.vungle.com/hc/en-us/articles/4908908675355-Test-Your-Playable-Asset-With-Our-Creative-Verifier](https://support.vungle.com/hc/en-us/articles/4908908675355-Test-Your-Playable-Asset-With-Our-Creative-Verifier)  
> 11. https\://github.com/indiesoftby/defold-playable-ads | Ecosyste.ms, [https\://awesome.ecosyste.ms/projects/github.com%2Findiesoftby%2Fdefold-playable-ads](https://awesome.ecosyste.ms/projects/github.com%2Findiesoftby%2Fdefold-playable-ads)  
> 12. indiesoftby/defold-playable-ads \- GitHub, [https\://github.com/indiesoftby/defold-playable-ads](https://github.com/indiesoftby/defold-playable-ads)  
> 13. playable-ad · GitHub Topics, [https\://github.com/topics/playable-ad?o=asc\&s=stars](https://github.com/topics/playable-ad?o=asc&s=stars)  
> 14. smoud/playable-sdk \- GitHub, [https\://github.com/smoudjs/playable-sdk](https://github.com/smoudjs/playable-sdk)  
> 15. smoud/playable-scripts \- GitHub, [https\://github.com/smoudjs/playable-scripts](https://github.com/smoudjs/playable-scripts)  
> 16. ppgee/cocos-pnp \- Playable ads adapter \- GitHub, [https\://github.com/ppgee/cocos-pnp](https://github.com/ppgee/cocos-pnp)  
> 17. Playable asset specifications • Unity Grow • Unity Docs, [https\://docs.unity.com/en-us/grow/acquire/creatives/playable/specifications](https://docs.unity.com/en-us/grow/acquire/creatives/playable/specifications)  
> 18. How to Create Unity Playable Ads Tutorial \- 2DKit, [https\://2dkit.com/playable-ads/create-unity-playable-ads-tutorial/](https://2dkit.com/playable-ads/create-unity-playable-ads-tutorial/)  
> 19. Creative asset specs | Advertisement | 3.4.9 \- Unity \- Manual, [https\://docs.unity3d.com/Packages/com.unity.ads@3.4/manual/AdvertisingResourcesCreativeSpecs.html](https://docs.unity3d.com/Packages/com.unity.ads@3.4/manual/AdvertisingResourcesCreativeSpecs.html)  
> 20. Unity Ads | Unity Playworks \- Developer Docs, [https\://docs.lunalabs.io/docs/playable/ad-networks/unity-ads](https://docs.lunalabs.io/docs/playable/ad-networks/unity-ads)  
> 21. AppLovin Playable Preview, [https\://p.applov.in/playablePreview?create=1](https://p.applov.in/playablePreview?create=1)  
> 22. Demand-side platforms | AppLovin oRTB specification | Creative types, [https\://support.applovin.com/en/max/demand-partners/demand-side-platforms/applovin-ortb-specification/creative-types](https://support.applovin.com/en/max/demand-partners/demand-side-platforms/applovin-ortb-specification/creative-types)  
> 23. Unity Package integration • Unity Grow • Unity Docs, [https\://docs.unity.com/en-us/grow/levelplay/sdk/unity/package-integration](https://docs.unity.com/en-us/grow/levelplay/sdk/unity/package-integration)  
> 24. Rewarded, Banner, Native & Interstitial Ads for Apps & Mobile Games, [https\://unity.com/products/levelplay-ad-formats](https://unity.com/products/levelplay-ad-formats)  
> 25. Release Notes \- Playworks Plugin | Unity Playworks \- Developer Docs, [https\://docs.lunalabs.io/docs/playable/release-notes](https://docs.lunalabs.io/docs/playable/release-notes)  
> 26. Release Notes \- Unity Playworks, [https\://docs.lunalabs.io/docs/playable/unityplayworks/release-notes](https://docs.lunalabs.io/docs/playable/unityplayworks/release-notes)  
> 27. Mobile Ad Metrics for Creatives \- Unity, [https\://unity.com/blog/3-in-ad-data-metrics-to-optimize-in-your-mobile-game-ad-creative-and-how](https://unity.com/blog/3-in-ad-data-metrics-to-optimize-in-your-mobile-game-ad-creative-and-how)  
> 28. Playable Guide | Mintegral Docs, [https\://adv-new.mintegral.com/doc/en/creatives/playable.html](https://adv-new.mintegral.com/doc/en/creatives/playable.html)  
> 29. How Playturbo's Playable Ads Boosted Loadcomplete's Performance, [https\://www\.mintegral.com/en/case/creative-playable-ad-for-boosting-ua-performance](https://www.mintegral.com/en/case/creative-playable-ad-for-boosting-ua-performance)  
> 30. Playable Mobile Ads: Specs, Costs & Benchmarks \- Coinis, [https\://coinis.com/glossary/playable-mobile-ads](https://coinis.com/glossary/playable-mobile-ads)  
> 31. Fix error messages with Uploaded ads \- Google Ads Help, [https\://support.google.com/google-ads/answer/6335679?hl=en](https://support.google.com/google-ads/answer/6335679?hl=en)  
> 32. About HTML5/Playable ads for App campaigns \- Google Ads Help, [https\://support.google.com/google-ads/answer/9981650?hl=en](https://support.google.com/google-ads/answer/9981650?hl=en)  
> 33. Introducing Playable Preview Tool to validate playable assets, [https\://developers.facebook.com/blog/post/2019/08/22/introducing-playable-preview-tool-to-validate-playable-assets/](https://developers.facebook.com/blog/post/2019/08/22/introducing-playable-preview-tool-to-validate-playable-assets/)  
> 34. Create Ads | Meta for Developers \- Facebook, [https\://developers.facebook.com/documentation/app-ads/creating-the-assets](https://developers.facebook.com/documentation/app-ads/creating-the-assets)  
> 35. How to create TikTok Ad Network (formerly TikTok Pangle) Playable, [https\://ads.tiktok.com/resources/help/article/how-to-create-tiktok-pangle-playable-ads](https://ads.tiktok.com/resources/help/article/how-to-create-tiktok-pangle-playable-ads)  
> 36. About TikTok Playable ads, [https\://ads.tiktok.com/resources/help/article/playable-ads?lang=zh-Hant-TW](https://ads.tiktok.com/resources/help/article/playable-ads?lang=zh-Hant-TW)  
> 37. Create TikTok Playable ads | TikTok Ads Manager, [https\://ads.tiktok.com/resources/help/article/how-to-create-playable-ads-on-tiktok-ads-manager](https://ads.tiktok.com/resources/help/article/how-to-create-playable-ads-on-tiktok-ads-manager)  
> 38. How to Create Line Playable Ads Tutorial \- 2DKit, [https\://2dkit.com/playable-ads/create-line-playable-ads-tutorial/](https://2dkit.com/playable-ads/create-line-playable-ads-tutorial/)  
> 39. wechat-miniprogram/minigame-playable \- 小游戏试玩 \- GitHub, [https\://github.com/wechat-miniprogram/minigame-playable](https://github.com/wechat-miniprogram/minigame-playable)