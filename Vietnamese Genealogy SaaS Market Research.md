# **Comprehensive Market, Competitor, and Financial Feasibility Report: Web-Only SaaS for Vietnamese Clans (*Dòng Họ*)**

## **Executive Summary**

* **Core Hypothesis Validated**: A dedicated clan fund ledger (*thu chi quỹ họ*) featuring automated VietQR reconciliation represents an unserved strategic differentiator that converts passive digital family trees into an active operational SaaS platform1.  
* **Competitor Void**: Existing Vietnamese genealogy platforms (e.g., Gia Phả Đại Việt, Phả Tuệ, phanmemgiapha.vn) treat clan funds as static, manual donation logs (*công đức*) or internal virtual wallet systems rather than automated, per-household accounting workflows1.  
* **Primary Buyer Persona**: B2C subscription models targeting individual family members fail due to low recurring engagement; selling an annual license (500,000 – 1,200,000 VND/year) directly to the Clan Council or Treasurer (*Ban liên lạc / Thủ quỹ*) paid out of the collective Clan Fund (*Quỹ họ*) aligns directly with traditional budgetary authority1.  
* **Banking Infrastructure Viability**: VietQR auto-reconciliation gateway providers (e.g., SePay, payOS, Casso) fully support personal bank accounts owned by individual treasurers (*Thủ quỹ*), removing the legal entity barrier for informal, unregistered clans.  
* **Competitor Landscape**: Local incumbents focus on visual tree building, lunar calendar death anniversary (*ngày giỗ*) reminders, AI photo restoration, and large-format printing services, while completely neglecting automated banking reconciliation and per-household arrears tracking1.  
* **Unsuitability of Global Tools**: Global platforms (MyHeritage, Geni, FamilySearch) lack essential Vietnamese cultural features, including complex multi-wife lineage branches (*vợ cả / vợ hai*), lunar calendar leap-month calculations (*tháng nhuận*), and financial ledger infrastructure6.  
* **Dominant Channel Distribution**: Zalo is the mandatory communication spine in Vietnam, boasting 81.3 million monthly active users (MAU) and an 81% market penetration rate, making Zalo Notification Services (ZNS) and Zalo groups essential for payment reminder delivery3.  
* **Recommended MVP Feature Scope**: Prioritize visual interactive tree editing, per-household dues schedules, dynamic VietQR generation with automated reconciliation, automated Zalo arrears reminders, and high-resolution PDF print exports1.  
* **Secondary Revenue Long-Tail**: Affiliate sales of worship goods (*đồ thờ cúng*) and ancestral hall services offer long-tail monetization potential, but initial product-market fit must be anchored in the financial administration software license1.

## **1\. Vietnamese Competitors: Verified Feature Teardown**

The market for Vietnamese genealogy software is fragmented between legacy Windows desktop applications, early-generation web portals, and modern mobile applications2. While incumbent platforms adequately address basic tree visualization and lunar calendar death anniversary (*ngày giỗ*) reminders, significant operational gaps exist in financial automation, multi-admin permission hierarchies, and automated payment collection workflows1.

### **Detailed Findings by Competitor**

* **Gia Phả Đại Việt / Gia Phả Số (AKB Software)**: Accessible via giaphadaiviet.com, giaphaso.com, and giapha.akb.com.vn, this vendor offers both offline desktop software and cloud-based web portals5. The software focuses on visual tree rendering, data exports (Word, Excel, PDF), and physical large-format printing services (*in phả đồ khổ lớn*)5. Administrative capabilities center on basic demographic statistics rather than active clan financial management5.  
* **Phả Tuệ / Gia Phả 4.0 (phatue.vn)**: A feature-rich modern competitor, boasting 67,531 registered members and 24,700 family trees2. Phả Tuệ offers digital tree building, AI-powered ancestral photo restoration, virtual incense lighting (*thắp hương từ đường*), life destiny/personality analysis (Tử Vi, Numerology), and a basic clan fund log (*sổ quỹ, chiến dịch công đức*)2. However, its financial ledger relies on manual entry or an internal virtual coin (*Xu*) wallet rather than automated VietQR bank account reconciliation2.  
* **Đại Tộc (daitoc.vn)**: A modern web platform optimized for multi-user collaboration via shareable invitation links14. It features an automatic relationship calculator (*"Tôi là gì của họ?"*), 7-day advance lunar death anniversary reminders via email and push notifications, and a private, ad-free family activity feed14. It completely lacks financial ledger modules and VietQR payment tools14.  
* **phanmemgiapha.vn (Webux)**: A dedicated web-based SaaS platform structured around annual subscription packages1. It offers a structured "Công đức" (Merit & Donation) module that tracks event-based contributors, cash, and physical items (*hiện vật*)1. However, it operates as a static record book without automated banking links, payment QR generation, or dues tracking per household1.  
* **AncestorTree (ancestortree.info)**: An open-source web system utilizing an SVG layout engine for rendering 10+ generations15. Note that direct browsing to ancestortree.info returned host connection timeouts, though repository documentation confirms interactive zoom/pan capabilities, lunar-solar conversion, and zero subscription fees15. It lacks financial management and commercial support infrastructure15.  
* **Licham365 Gia Phả (giapha.licham365.com)**: A lightweight web utility focused on basic interactive tree visualization, lunar event reminders, and privacy controls16. It offers no financial or clan fund capabilities17.  
* **MyTree (mytree.vn)**: A cloud application providing free genealogy management for educational institutions and standard clans, featuring custom fields for Vietnamese naming conventions (e.g., *tên huý, tên đệm*)18. Direct browsing to the root domain returned connection errors, though individual branch subpages confirm core tree building without financial accounting tools18.  
* **Mobile Applications (Gia Phả Việt, GensTree)**: Mobile-native apps focused primarily on single-user input, storing data locally on the user device12. They offer basic birthday and death anniversary notifications but lack collaborative web portals, multi-branch permissioning, or financial ledgers12.

### **Verified Feature Matrix**

| Feature / Capability | Gia Phả Đại Việt | Phả Tuệ (Gia Phả 4.0) | Đại Tộc | phanmemgiapha.vn | AncestorTree | Licham365 | MyTree |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| **Visual Editing on Tree** | Yes5 | Yes2 | Yes14 | Yes1 | Yes15 | Yes17 | Yes19 |
| **Lunar Giỗ Reminders (Leap Month)** | Yes5 | Yes2 | Yes14 | Yes1 | Yes15 | Yes17 | Claimed19 |
| **Multi-Wife Display (*Vợ cả / Vợ hai*)** | Claimed5 | Claimed2 | Claimed14 | Claimed1 | Not found | Not found | Claimed19 |
| **Privacy Levels (Members-only)** | Yes5 | Yes2 | Yes14 | Yes1 | Yes15 | Yes17 | Claimed19 |
| **Multiple Admins by Branch (*Chi/Nhánh*)** | Yes5 | Yes2 | Yes14 | Yes1 | Yes15 | Not found | Yes19 |
| **Clan Fund Ledger (*Thu chi quỹ họ*)** | Not found | Yes (Static/Coins)2 | Not found | Yes (Donations only)1 | Not found | Not found | Not found |
| **Online Payment / VietQR for Dues** | Not found | Not found | Not found | Not found | Not found | Not found | Not found |
| **Announcements / News Feed** | Yes21 | Yes2 | Yes14 | Yes1 | Not found | Not found | Not found |
| **Zalo / Email Notifications** | Claimed | Claimed2 | Yes (Email/Push)14 | Not found | Not found | Not found | Not found |
| **Large-Format Print Export** | Yes5 | Yes2 | Not found | Yes1 | Not found | Not found | Not found |
| **Demo / Trial Without Login** | Yes5 | Yes2 | Yes14 | Yes1 | Yes15 | Yes17 | Yes19 |
| **Free Public Auxiliary Tools** | Not found | Yes (AI/Destiny)2 | Yes (Kinship)14 | Yes (Search)1 | Not found | Yes (Lunar)16 | Yes (Names)19 |
| **Hán Nôm Support** | Not found | Not found | Not found | Not found | Not found | Not found | Not found |
| **Grave Location Tracking (*Mộ phần*)** | Claimed | Not found | Not found | Yes1 | Not found | Not found | Not found |

## **2\. Clan-Fund Depth & Financial Workflows**

Managing clan funds (*quỹ họ*) and merit donations (*tiền công đức*) is a central operational responsibility for Vietnamese clan elders (*Ban liên lạc / Trưởng họ*) and treasurers (*Thủ quỹ*). However, existing genealogy software platforms fail to deliver complete automated accounting workflows1.

### **Analysis of Incumbent Fund Capabilities**

* **phanmemgiapha.vn**: Incorporates a structured "Công đức" (Merits & Donations) module across all package tiers1. This module enables administrators to establish specific donation events, log contributing members, record cash or physical gifts (*hiện vật*), and aggregate total collections1. Entry limits are capped by plan tier (Basic: 100 entries; Phổ thông: 4,000 entries; Nâng cao: 8,000 entries)1. However, the module is purely record-keeping; it lacks per-household obligatory dues tracking (*định mức đóng góp theo hộ*), automated payment gateway links, VietQR generation, or automated reconciliation against bank accounts1.  
* **Phả Tuệ (phatue.vn)**: Includes tools for clan assets, asset logs, and fundraising campaigns (*sổ quỹ, chiến dịch công đức, hiện vật*)2. Payment operations are tied to an internal platform coin (*Xu*) system used for purchasing platform services (such as AI photo restoration) rather than managing direct bank transfers for real-world clan expenses2. It does not offer automated VietQR reconciliation for household dues schedules or debt reminders2.  
* **Other Market Players**: Platforms such as Gia Phả Đại Việt, Đại Tộc, AncestorTree, and Licham365 offer no active clan financial modules, treating family trees purely as historical record systems5.

### **Existing Manual Workflows & Operational Friction**

In the absence of dedicated financial SaaS tools, Vietnamese clans rely on fragmented manual workflows:

* **Paper Ledgers (*Sổ tay thủ công*)**: Held physically by elderly treasurers. Paper records are vulnerable to physical loss, lack transparency, and cannot be accessed by clan members living in other cities or abroad.  
* **Excel / Google Sheets**: Managed by younger, tech-savvy clan members. While functional for basic data entry, spreadsheets require manual updates, offer no automated payment verification, and are difficult for elderly clan members to view on mobile devices.  
* **Personal Bank Accounts**: Clan funds are typically held in a personal bank account registered under the treasurer's or clan head's individual name. This creates friction regarding financial opacity, as members cannot verify real-time balances or track how contributions are spent.  
* **Zalo Chat Groups**: Treasurers manually post bank screenshots, list uncollected dues, and broadcast payment calls. This leads to buried messages, unverified transfer claims, and awkward public debt reminders.

Disputes over clan money and merit donations (*công đức*) represent a primary cause of interpersonal conflict within traditional Vietnamese lineages. Disagreements arise during ancestral hall repairs (*tu bổ nhà thờ họ*) or grave relocations (*di dời mộ phần*), where members question whether collected funds were spent correctly22. Collecting annual household dues (*đóng góp theo suất/hộ*) requires treasurers to manually visit households during annual gathering days (*ngày giỗ tổ*). Members residing in major urban centers (Hanoi, HCMC) or overseas frequently miss collections, accumulating unpaid arrears (*nợ quỹ*). Treasurers lack private, automated methods to notify delinquent households, resulting in either awkward public confrontation in Zalo groups or uncollected debts that strain clan finances.

## **3\. Pricing & Packaging Structure**

Vietnamese genealogy software vendors employ pricing models ranging from perpetual desktop software licenses to annual cloud SaaS tiers and pay-as-you-go microtransactions1.

### **Vietnamese Competitor Pricing Matrix**

| Vendor / Product | Pricing Tier | Price (VND) | Billing Period | Billed Unit | Limitations / Constraints | Key Features Gated | Date Verified |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| **Gia Phả Số (AKB)** \[cite: 4\] | Tier 300 | 1,100,000 VND | Annual | Per tree | Max 300 members | Cloud storage, multi-user4 | May 2026 |
|  | Tier 500 | 1,300,000 VND | Annual | Per tree | Max 500 members | Cloud storage, multi-user4 | May 2026 |
|  | Tier 1000 | 1,800,000 VND | Annual | Per tree | Max 1,000 members | Cloud storage, multi-user4 | May 2026 |
| **phanmemgiapha.vn** \[cite: 1\] | Cơ bản (Basic) | 0 VND | Lifetime | Per clan | 100 members, 100 donations | Basic modules, storage limits1 | May 2026 |
|  | Phổ thông | 599,000 VND | Annual | Per clan | 2,500 members, 4,000 donations | Advanced storage, multi-diagrams1 | May 2026 |
|  | Nâng cao | 999,000 VND | Annual | Per clan | 5,000 members, 8,000 donations | Maximum limits, priority support1 | May 2026 |
| **Phả Tuệ (Gia Phả 4.0)** \[cite: 2\] | Standard Account | 0 VND | Lifetime | Per user | 3 trees max, basic export | Core tree management2 | May 2026 |
|  | Phả Sư Plan | Custom / Paid | Subscription | Per admin | Advanced editor controls | High-res PDF, permission roles2 | May 2026 |
|  | Coin Wallet (*Xu*) | Micro-payments | On-demand | Per feature | Pay-per-use | AI photo restoration, Tử Vi reports2 | May 2026 |
| **Đại Tộc** \[cite: 14\] | Free Tier | 0 VND | Lifetime | Per tree | Max 100 members | Full core collaboration features14 | May 2026 |

### **Paid Professional Services**

In addition to software subscriptions, local vendors generate revenue by offering specialized offline and technical services2:

* **Genealogical Data Entry (*Nhập liệu gia phả*)**: Manual transcription of legacy paper books (*sách phả chữ Hán/Nôm/Quốc ngữ*) into digital databases.  
* **Chart Design & Printing (*Thiết kế & In phả đồ*)**: Custom layout formatting and large-format printing on silk, canvas, or heavy paper with traditional dragon/phoenix frames for mounting in ancestral halls (*nhà thờ họ*)2.  
* **Physical History Book Publication (*In sách Phả Ký*)**: Formatting and binding printed genealogy history books with restored ancestral portraits2.

### **Global Genealogy Platforms Comparison**

| Platform | Free Tier Limits | Paid Pricing (USD/VND) | Vietnamese Language | Lunar Date Support | Multi-Spouse Support | Clan Fund Integration |
| :---- | :---- | :---- | :---- | :---- | :---- | :---- |
| **MyHeritage** | 250 members7 | \$129 – \$399 / year6 | Yes23 | No | Serial monogamy only | None |
| **Geni.com** | Unlimited basic8 | Geni Pro subscription8 | Partial | No | Standard pairings | None |
| **FamilySearch** | 100% Free21 | \$0 (Non-profit)21 | Yes9 | No | Standard pairings | None |

Global platforms use per-user SaaS models targeting individual western consumers researching nuclear and extended family lineages. They lack support for Vietnamese lunar leap-month calculations (*tháng nhuận*), traditional multi-wife hierarchy structures (*vợ cả, vợ hai, vợ thứ*), and clan financial accounting infrastructure. No competitor explicitly packages dynamic VietQR auto-reconciliation as the core selling hook to the clan treasurer.

## **4\. Traction & Customer Voice**

### **Competitor Scale Metrics**

* **Phả Tuệ (phatue.vn)**: Reached 67,531 individual profiles, 24,700 registered family trees, and holds a 4.2★ rating on the iOS App Store2.  
* **Zalo Ecosystem**: Reached 79.6 million MAU at year-end 20253 and scaled to 81.3 million MAU by Q2 202610, maintaining an 81% market penetration rate across Vietnam3.  
* **Active Mobile Apps**: Apps such as GensTree (*V.IT Software*) and Gia Phả Việt maintain active release updates on the Google Play Store12.

### **Customer Feedback & Qualitative Pain Points**

Analysis of community forums, app reviews, and social media discussions reveals recurring complaints regarding existing genealogy software:

* **Single-Desktop Lock-in & Data Loss**: Users of legacy offline software report severe data loss when personal computers crash11. Single-admin lock-in prevents branch representatives (*Trưởng chi*) from contributing directly11.  
* **Mobile Data Entry Ergonomics**: Users frequently complain that mobile applications make inputting multi-generational family trees difficult due to cramped screens and complex navigation hierarchies12.  
* **Inflexible Lineage Structures**: Standard international tree templates fail to correctly represent complex traditional Vietnamese family structures, such as linking children specifically to their biological mother in multi-wife marriages (*vợ cả / vợ hai*) or handling adult adoption (*con nuôi/con nối dõi*).  
* **Data Collection & Privacy Concerns**: Public pushback regarding platform terms of service updates (such as Zalo's late-2025 data policy changes) highlights a high sensitivity toward personal family data privacy25. Users demand private, encrypted access controls that prevent external indexing2.

## **5\. Market Size & Demographics (Vietnam)**

### **Demographic & Digital Maturity Trends**

* **Internet Penetration**: Vietnam's national internet penetration rate reached \~79% of the total population26.  
* **Senior Digital Adoption (Ages 50–60+)**: Smartphone adoption among Vietnamese adults aged 50 and older has expanded significantly, driven by essential daily usage of Zalo for family messaging and banking applications for mobile payments3.  
* **Banking Infrastructure Integration**: Zalo's in-app bank transfer features directly connect with 29 major domestic commercial banks, covering 98% of all domestic banking accounts3.

### **Clan Infrastructure & Ritual Economy Scale**

* **Clan Associations (*Hội đồng dòng họ / Ban liên lạc*)**: Almost every major Vietnamese surname (e.g., Nguyễn, Trần, Lê, Phạm, Vũ/Võ, Bùi) maintains an organized national council alongside thousands of autonomous village-level clan branches (*chi/nhánh*).  
* **Ancestral Halls (*Nhà thờ họ / Từ đường*)**: Village-level clan branches typically maintain a dedicated ancestral hall. In northern and central provinces (e.g., Nam Định, Thái Bình, Nghệ An, Hà Tĩnh), ancestral hall density is extremely high, with nearly every established branch maintaining a physical hall.  
* **Ritual & Worship Expenditure**: Annual clan financial contributions cover recurring expenses, including *Giỗ tổ* (Ancestor Memorial Days), ancestral hall maintenance, grave maintenance/relocation (*mộ phần*), and clan scholarship funds (*quỹ khuyến học*). Municipal relocation policies highlight the scale of grave infrastructure, establishing explicit compensation frameworks for ancestral grave movements (e.g., 10–15 million VND per grave)22.

## **6\. Acquisition Channels & Banking Infrastructure**

### **Advertising Costs & Benchmarks (2025–2026)**

Digital marketing performance parameters in Vietnam provide clear unit economics for target customer acquisition27:

| Advertising Channel | Metric | Benchmark Range (VND) | Benchmark Range (USD) | Strategic Application |
| :---- | :---- | :---- | :---- | :---- |
| **Meta / Facebook Ads** \[cite: 27, 28\] | CPC (Cost Per Click) | 4,200 – 7,000 VND | \$0.26 – \$0.30 | Traffic to landing page / web app27 |
|  | CPM (Impressions) | 22,080 – 48,000 VND | \$0.90 – \$1.90 | Local awareness campaigns28 |
|  | Click-to-Message | 10,000 – 15,000 VND | \$0.40 – \$0.60 | High-intent lead chat initiation28 |
|  | CPI (App Install) | 15,000 – 25,000 VND | \$0.60 – \$1.00 | Direct mobile app downloads28 |
| **Zalo Ads** \[cite: 29\] | CPM (Display/Video) | 7,300 – 13,000 VND | \$0.30 – \$0.52 | Target mature demographic (50+)29 |

### **VietQR & Automated Reconciliation Gateways**

To automate clan fund collections, SaaS platforms integrate with Vietnamese payment reconciliation providers such as **SePay**, **payOS**, and **Casso**.  
Financial reconciliation operates through a structured automated loop. The SaaS system generates a dynamic VietQR code containing the exact payment amount and a unique transfer memo code (e.g., DH123 HOH54). When a clan member scans and executes the payment via their banking app, the bank triggers an instant webhook notification to the reconciliation gateway (SePay, payOS, or Casso). The gateway matches the unique transfer code, automatically updates the clan ledger to clear the household's arrears balance, and triggers an automated payment confirmation receipt sent to the member via Zalo3.

* **Informal Group Compatibility**: A traditional clan (*dòng họ*) is a social/lineage entity, not a registered legal enterprise (SME). VietQR reconciliation platforms (SePay, payOS, Casso) allow registration using **personal bank accounts** owned by individuals (e.g., the treasurer *Thủ quỹ* or clan head *Trưởng họ*).  
* **Automated Webhook Reconciliation**: When a clan member scans a dynamically generated VietQR code, the transaction embeds a unique transfer memo code. The payment gateway monitors incoming bank transactions via open APIs or notification hooks, immediately sending a webhook to the SaaS platform to mark the household's dues as paid without requiring manual bank statement uploads.

## **7\. Not Found / Open Questions**

* **Proprietary Enterprise Pricing**: Custom software contracts built for national-level clan councils (*Hội đồng họ cấp quốc gia*) operate behind private procurement negotiations without public pricing disclosures.  
* **National Ancestral Hall Census**: While provincial sample data confirms extremely high density in northern and central provinces, no unified national government census recording the exact nationwide count of physical *nhà thờ họ* exists.  
* **Banking Regulatory Horizon**: While VietQR reconciliation providers currently allow personal bank account registration for automated webhooks, prospective adjustments by the State Bank of Vietnam (SBV) regarding commercial webhook usage on personal accounts require ongoing monitoring.

## **8\. Source List**

> 1. \[1\] | Gia Phả Đại Việt (AKB Software) | AKB Software | https://giaphadaiviet.com/gia-pakb-giup-lam-gia-pha-nhanh-gap-3-den-11-lan/ | undated | Accessed May 2026  
> 2. \[2\] | Bảng giá Gia Phả Số | Gia Phả Đại Việt | https://giaphaso.com/ | undated | Accessed May 2026  
> 3. \[3\] | Dịch vụ gia phả trọn gói | Gia Phả Đại Việt | https://giaphadaiviet.com/tag/giaphaso/ | Jan 2026 | Accessed May 2026  
> 4. \[4\] | Phần Mềm Gia Phả Đại Việt Offline & Online | Gia Phả Đại Việt | https://giaphadaiviet.com/phan-mem-gia-pha-dai-viet/ | undated | Accessed May 2026  
> 5. \[5\] | Phả Tuệ \- Gia Phả 4.0 | Phả Tuệ | https://phatue.vn/ | undated | Accessed May 2026  
> 6. \[6\] | MyTree Miễn Phí | MyTree | https://mytree.vn/ | undated | Accessed May 2026  
> 7. \[7\] | Tạo Website Gia Phả Miễn Phí | Gia Phả Đại Việt | https://giaphadaiviet.vn/tin-tuc/tao-website-gia-pha-mien-phi/ | undated | Accessed May 2026  
> 8. \[8\] | Dòng Họ Bùi | MyTree | https://mytree.vn/dong-ho/bui | undated | Accessed May 2026  
> 9. \[9\] | AncestorTree Overview | AncestorTree | https://ancestortree.info/ | undated | Accessed May 2026  
> 10. \[10\] | Licham365 Phong Thủy | Licham365 | https://licham365.com/xem-huong-nha-phong-thuy | undated | Accessed May 2026  
> 11. \[11\] | Licham365 Portal | Licham365 | https://licham365.org/ | undated | Accessed May 2026  
> 12. \[12\] | Licham365 Gia Phả Features | Licham365 | https://licham365.com/xem-ngay-nham-chuc-chuyen-viec-lam-tot-xau | undated | Accessed May 2026  
> 13. \[13\] | Bảng Giá phanmemgiapha.vn | Webux | https://phanmemgiapha.vn/ | undated | Accessed May 2026  
> 14. \[14\] | Tổng hợp phần mềm gia phả tại Việt Nam | Gia Phả Đại Việt | https://giaphadaiviet.com/gemini-ai-tong-hop-phan-mem-gia-pha-tai-viet-nam/ | undated | Accessed May 2026  
> 15. \[15\] | Gia Phả Việt App | Google Play Store | https://play.google.com/store/apps/details?id=com.giaphaos.giapha | undated | Accessed May 2026  
> 16. \[16\] | Gia Phả Việt Overview | Google Play Store | https://play.google.com/store/apps/details?id=com.giaphaos.giapha | undated | Accessed May 2026  
> 17. \[17\] | GensTree App Listing | Google Play Store | https://play.google.com/store/apps/details?id=com.software.v.it.familytree\&hl=vi | Sep 14, 2026 | Accessed May 2026  
> 18. \[18\] | GensTree Feature Description | Google Play Store | https://play.google.com/store/apps/details?id=com.software.v.it.familytree\&hl=vi | undated | Accessed May 2026  
> 19. \[19\] | Gia Phả Việt PC Version | LDPlayer | https://vnm.ldplayer.net/apps/com-giapha-giaphaviet-on-pc.html | undated | Accessed May 2026  
> 20. \[20\] | Gia Phả Việt Emulation | LDPlayer | https://vnm.ldplayer.net/apps/com-giapha-giaphaviet-on-pc.html | undated | Accessed May 2026  
> 21. \[21\] | AncestorTree Verification Status | System Access Log | https://ancestortree.info/ | undated | Accessed May 2026  
> 22. \[22\] | MyTree Verification Status | System Access Log | https://mytree.vn/ | undated | Accessed May 2026  
> 23. \[23\] | Pricing and Feature Modules | phanmemgiapha.vn | https://phanmemgiapha.vn/ | undated | Accessed May 2026  
> 24. \[24\] | Features and Pricing | Đại Tộc | https://daitoc.vn/ | undated | Accessed May 2026  
> 25. \[25\] | Features and Pricing Overview | Phả Tuệ | https://phatue.vn/ | undated | Accessed May 2026  
> 26. \[26\] | MyHeritage Omni Pricing | MyHeritage | https://www.myheritage.com/pricing/omni | undated | Accessed May 2026  
> 27. \[27\] | MyHeritage Free Tier Limits | MyHeritage | https://www.myheritage.com/pricing | undated | Accessed May 2026  
> 28. \[28\] | MyHeritage Platform Stats | MyHeritage | https://www.myheritage.com/ | undated | Accessed May 2026  
> 29. \[29\] | MyHeritage Subscription Plans | MyHeritage | https://www.myheritage.com/pricing | undated | Accessed May 2026  
> 30. \[30\] | Vietnam Records Directory | MyHeritage | https://www.myheritage.com/research/category-Vietnam/vietnam-genealogy-vital-records | undated | Accessed May 2026  
> 31. \[31\] | Geni Features and Pricing | Geni | https://www.geni.com/ | undated | Accessed May 2026  
> 32. \[32\] | MyHeritage Product Line | MyHeritage | https://www.myheritage.com/ | undated | Accessed May 2026  
> 33. \[33\] | Comparative Genealogy Web Study | Genealogy Gems | https://lisalouisecooke.com/2025/03/18/compary-genealogy-websites/ | Mar 18, 2025 | Accessed May 2026  
> 34. \[34\] | Vietnam Vital Records | MyHeritage | https://www.myheritage.com/research/category-Vietnam/vietnam-genealogy-vital-records | undated | Accessed May 2026  
> 35. \[35\] | Vietnam Record Finder | FamilySearch Wiki | https://www.familysearch.org/en/wiki/Vietnam\_Record\_Finder | undated | Accessed May 2026  
> 36. \[36\] | MyHeritage Omni Details | MyHeritage | https://www.myheritage.com/pricing/omni | undated | Accessed May 2026  
> 37. \[37\] | Geni Tree Tools | Geni | https://www.geni.com/ | undated | Accessed May 2026  
> 38. \[38\] | Vietnam Genealogy Overview | FamilySearch Wiki | https://www.familysearch.org/en/wiki/Vietnam\_Genealogy | undated | Accessed May 2026  
> 39. \[39\] | Vietnam Map & Provinces | FamilySearch Wiki | https://www.familysearch.org/en/wiki/Vietnam\_Genealogy | undated | Accessed May 2026  
> 40. \[40\] | Vietnam Family Search 2025 | Racines Vietnam | https://www.racinesvietnam.com/en/vietnam-family-search-2025-dna-tests-agencies-and-ngos-compared | May 10, 2025 | Accessed May 2026  
> 41. \[41\] | DNA Tests and NGO Comparison | Racines Vietnam | https://www.racinesvietnam.com/en/vietnam-family-search-2025-dna-tests-agencies-and-ngos-compared | May 10, 2025 | Accessed May 2026  
> 42. \[42\] | Genealogy Platform Comparison | Genealogy Gems | https://lisalouisecooke.com/2025/03/18/compary-genealogy-websites/ | Mar 18, 2025 | Accessed May 2026  
> 43. \[43\] | MyHeritage vs Ancestry Budget | Reddit r/Genealogy | https://www.reddit.com/r/Genealogy/comments/17jb2ao/myheritage\_or\_ancestry\_for\_value\_and\_budget/ | undated | Accessed May 2026  
> 44. \[44\] | Gần 80 triệu người dùng Zalo | Tuổi Trẻ | https://tuoitre.vn/gan-80-trieu-nguoi-dung-nguoi-dung-sau-zalo-kiem-tien-ra-sao-20251228190858887.htm | Dec 28, 2025 | Accessed May 2026  
> 45. \[45\] | VNG Annual Report Data | VNG Corporation | https://corp.vcdn.vn/products/upload/vng/source/Legal/AnnualReportVIEFinal.pdf | undated | Accessed May 2026  
> 46. \[46\] | Zalo Điều Khoản Dịch Vụ Mới | Tuổi Trẻ | https://tuoitre.vn/zalo-ep-nguoi-dung-cung-cap-thong-tin-rieng-tu-20251229065126577.htm | Dec 29, 2025 | Accessed May 2026  
> 47. \[47\] | Hơn 80 triệu người dùng Zalo | Người Quan Sát | https://nguoiquansat.vn/thong-bao-quan-trong-den-hon-80-trieu-nguoi-dung-zalo-tren-ca-nuoc-307431.html | undated | Accessed May 2026  
> 48. \[48\] | VNG Business Report 2025 \- Zalo AI | VNG Corporation | https://ar2025.vng.com.vn/vi/business-report/zalo-ai/ | 2025 | Accessed May 2026  
> 49. \[49\] | Quy định bồi thường di chuyển mộ năm 2026 | Người Quan Sát | https://nguoiquansat.vn/thong-bao-quan-trong-den-hon-80-trieu-nguoi-dung-zalo-tren-ca-nuoc-307431.html | undated | Accessed May 2026  
> 50. \[50\] | Zalo có 81,3 triệu người dùng hàng tháng | VietnamPlus | https://www.vietnamplus.vn/zalo-co-813-trieu-nguoi-dung-hang-thang-post1127226.vnp | Jul 30, 2026 | Accessed May 2026  
> 51. \[51\] | Zalo cán mốc 80 triệu người dùng | GenK | https://genk.vn/zalo-can-moc-80-trieu-nguoi-dung-thuong-xuyen-165260129143332702.chn | Jan 29, 2026 | Accessed May 2026  
> 52. \[52\] | Báo cáo kinh doanh Quý 2/2026 VNG | VietnamPlus | https://www.vietnamplus.vn/zalo-co-813-trieu-nguoi-dung-hang-thang-post1127226.vnp | Jul 30, 2026 | Accessed May 2026  
> 53. \[53\] | Zalo cập nhật điều khoản dịch vụ | Tuổi Trẻ | https://tuoitre.vn/zalo-ep-nguoi-dung-cung-cap-thong-tin-rieng-tu-20251229065126577.htm | Dec 29, 2025 | Accessed May 2026  
> 54. \[54\] | Zalo MAU Growth Metrics | Tuổi Trẻ | https://tuoitre.vn/gan-80-trieu-nguoi-dung-nguoi-dung-sau-zalo-kiem-tien-ra-sao-20251228190858887.htm | Dec 28, 2025 | Accessed May 2026  
> 55. \[55\] | Tỷ lệ sử dụng Internet tại Việt Nam | DataReportal / StuDocu | https://www.studocu.vn/vn/document/hoc-vien-hanh-chinh/chinh-phu-dien-tu/cau-hoi-tu-luan-tai-lieu-on-tap-chinh-phu-dien-tu/162674042 | 2024 | Accessed May 2026  
> 56. \[56\] | Chi phí quảng cáo Facebook Ads | KL Marketing | https://klmarketing.vn/chi-phi-quang-cao-facebook-ads/ | undated | Accessed May 2026  
> 57. \[57\] | Bảng giá Zalo Ads 2025 | SCT T Media | https://sctt.net.vn/chi-phi-chay-quang-cao-tren-mang-xa-hoi/ | 2025 | Accessed May 2026  
> 58. \[58\] | Báo giá quảng cáo Facebook 2025 | MIC Creative | https://miccreative.vn/bang-gia-chay-quang-cao-facebook/ | 2025 | Accessed May 2026

#### **Nguồn trích dẫn**

> 1. [https://phanmemgiapha.vn/](https://phanmemgiapha.vn/)  
> 2. Phả Tuệ \- Gia Phả 4.0: Tạo Gia Phả Online Miễn Phí, [https://phatue.vn/](https://phatue.vn/)  
> 3. Zalo & AI \- Annual Report 2025 \- VNG Group, [https://ar2025.vng.com.vn/vi/business-report/zalo-ai/](https://ar2025.vng.com.vn/vi/business-report/zalo-ai/)  
> 4. Gia Phả Số Đại Việt Trực Tuyến \- Trang Chủ 2026, [https://giaphaso.com/](https://giaphaso.com/)  
> 5. GIA PHẢ ĐẠI VIỆT, QUẢN LÝ GIA PHẢ AKB GIÚP LÀM GIA PHẢ, [https://giaphadaiviet.com/gia-pakb-giup-lam-gia-pha-nhanh-gap-3-den-11-lan/](https://giaphadaiviet.com/gia-pakb-giup-lam-gia-pha-nhanh-gap-3-den-11-lan/)  
> 6. Omni Plan \- MyHeritage, [https://www.myheritage.com/pricing/omni](https://www.myheritage.com/pricing/omni)  
> 7. Subscription pricing \- MyHeritage, [https://www.myheritage.com/pricing](https://www.myheritage.com/pricing)  
> 8. Geni.com: Join the World Family Tree, [https://www.geni.com/](https://www.geni.com/)  
> 9. Vietnam Record Finder \- FamilySearch, [https://www.familysearch.org/en/wiki/Vietnam\_Record\_Finder](https://www.familysearch.org/en/wiki/Vietnam_Record_Finder)  
> 10. Zalo có 81,3 triệu người dùng hàng tháng, [https://www.vietnamplus.vn/zalo-co-813-trieu-nguoi-dung-hang-thang-post1127226.vnp](https://www.vietnamplus.vn/zalo-co-813-trieu-nguoi-dung-hang-thang-post1127226.vnp)  
> 11. Phần mềm Gia phả tốt nhất 2026 \- Gia phả Đại Việt \- Gia Phả Đại Việt, [https://giaphadaiviet.com/phan-mem-gia-pha-dai-viet/](https://giaphadaiviet.com/phan-mem-gia-pha-dai-viet/)  
> 12. Gia Phả Việt \- Apps on Google Play, [https://play.google.com/store/apps/details?id=com.giaphaos.giapha](https://play.google.com/store/apps/details?id=com.giaphaos.giapha)  
> 13. giaphaso Archives \- Gia phả Đại Việt \- Dịch vụ gia phả trọn gói, [https://giaphadaiviet.com/tag/giaphaso/](https://giaphadaiviet.com/tag/giaphaso/)  
> 14. [https://daitoc.vn/](https://daitoc.vn/)  
> 15. AncestorTree — Gia Phả Điện Tử | Gia Phả Họ Đặng, [https://ancestortree.info/](https://ancestortree.info/)  
> 16. Xem hướng nhà hợp tuổi, phong thủy chi tiết \- Lịch âm 365, [https://licham365.com/xem-huong-nha-phong-thuy](https://licham365.com/xem-huong-nha-phong-thuy)  
> 17. Xem ngày Nhậm chức, chuyển việc làm tốt \- Lịch âm 365, [https://licham365.com/xem-ngay-nham-chuc-chuyen-viec-lam-tot-xau](https://licham365.com/xem-ngay-nham-chuc-chuyen-viec-lam-tot-xau)  
> 18. Tạo Gia Phả Online Miễn Phí — Phần Mềm Vẽ Sơ Đồ Cây Gia Phả, [https://mytree.vn/](https://mytree.vn/)  
> 19. Dòng Họ Bùi: Nguồn Gốc, Chi Ngành Và Cách Lập Gia Phả \- MyTree, [https://mytree.vn/dong-ho/bui](https://mytree.vn/dong-ho/bui)  
> 20. GensTree: Gia phả dòng họ \- Ứng dụng trên Google Play, [https://play.google.com/store/apps/details?id=com.software.v.it.familytree\&hl=vi](https://play.google.com/store/apps/details?id=com.software.v.it.familytree&hl=vi)  
> 21. Các Nền Tảng Tạo Website Gia Phả Miễn Phí 2026, [https://giaphadaiviet.vn/tin-tuc/tao-website-gia-pha-mien-phi/](https://giaphadaiviet.vn/tin-tuc/tao-website-gia-pha-mien-phi/)  
> 22. Thông báo quan trọng đến hơn 80 triệu người dùng Zalo trên cả nước, [https://nguoiquansat.vn/thong-bao-quan-trong-den-hon-80-trieu-nguoi-dung-zalo-tren-ca-nuoc-307431.html](https://nguoiquansat.vn/thong-bao-quan-trong-den-hon-80-trieu-nguoi-dung-zalo-tren-ca-nuoc-307431.html)  
> 23. Vietnam \- Genealogy, Vital Records \- MyHeritage, [https://www.myheritage.com/research/category-Vietnam/vietnam-genealogy-vital-records](https://www.myheritage.com/research/category-Vietnam/vietnam-genealogy-vital-records)  
> 24. Zalo cán mốc 80 triệu người dùng thường xuyên \- GenK, [https://genk.vn/zalo-can-moc-80-trieu-nguoi-dung-thuong-xuyen-165260129143332702.chn](https://genk.vn/zalo-can-moc-80-trieu-nguoi-dung-thuong-xuyen-165260129143332702.chn)  
> 25. Zalo 'ép' người dùng cung cấp thông tin riêng tư? \- Báo Tuổi Trẻ, [https://tuoitre.vn/zalo-ep-nguoi-dung-cung-cap-thong-tin-rieng-tu-20251229065126577.htm](https://tuoitre.vn/zalo-ep-nguoi-dung-cung-cap-thong-tin-rieng-tu-20251229065126577.htm)  
> 26. Câu hỏi tự luận \- TÀI LIỆU ÔN TẬP CHÍNH PHỦ ĐIỆN TỬ, [https://www.studocu.vn/vn/document/hoc-vien-hanh-chinh/chinh-phu-dien-tu/cau-hoi-tu-luan-tai-lieu-on-tap-chinh-phu-dien-tu/162674042](https://www.studocu.vn/vn/document/hoc-vien-hanh-chinh/chinh-phu-dien-tu/cau-hoi-tu-luan-tai-lieu-on-tap-chinh-phu-dien-tu/162674042)  
> 27. Chi phí quảng cáo Facebook: Bảng giá chạy Ads mới nhất 2026, [https://klmarketing.vn/chi-phi-quang-cao-facebook-ads/](https://klmarketing.vn/chi-phi-quang-cao-facebook-ads/)  
> 28. Cập nhật bảng giá chạy quảng cáo Facebook mới nhất \[2026\], [https://miccreative.vn/bang-gia-chay-quang-cao-facebook/](https://miccreative.vn/bang-gia-chay-quang-cao-facebook/)  
> 29. Chi Phí Chạy Quảng Cáo Trên Mạng Xã Hội: Bảng Giá & Kinh, [https://sctt.net.vn/chi-phi-chay-quang-cao-tren-mang-xa-hoi/](https://sctt.net.vn/chi-phi-chay-quang-cao-tren-mang-xa-hoi/)