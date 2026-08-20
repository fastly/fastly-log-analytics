const { chromium } = require('playwright');

async function runVerification() {
    const target = process.env.TARGET_URL || "https://fastly-se-demo.global.ssl.fastly.net";
    const launchOptions = { headless: true };
    if (process.platform === 'darwin') {
        launchOptions.channel = 'chrome';
    }

    console.log("===============================================================");
    console.log("   RUM CORE WEB VITALS CLS INJECTION VERIFICATION SCRIPT");
    console.log("===============================================================");

    const browser = await chromium.launch(launchOptions);

    // --- SCENARIO A: LIVE SITE AS IS (WITHOUT FIX) ---
    console.log("\n--- Scenario A: Live Site As-Is (No delay, no pushed element) ---");
    {
        const context = await browser.newContext({
            userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            extraHTTPHeaders: { 'X-RandomHack': 'true' }
        });
        const page = await context.newPage();

        let clsBeacon = null;
        page.on('request', req => {
            const url = req.url();
            if (url.includes('/rum-beacon') && url.includes('rum_metric_name=cls')) {
                clsBeacon = url;
            }
        });

        console.log(`  Opening live demo page: ${target}/rum-demo`);
        await page.goto(`${target}/rum-demo`);
        await page.waitForTimeout(1000);

        console.log("  Clicking 'Simulate Layout Shift (CLS)' button...");
        await page.click('#btn-trigger-cls');
        await page.waitForTimeout(2000);

        console.log("  Closing page/context to flush Faro Web Vitals...");
        await page.close();
        await context.close();

        if (clsBeacon) {
            console.log("  ✅ CLS Beacon Intercepted!");
            console.log(`     Raw URL: ${clsBeacon}`);
            const valueMatch = clsBeacon.match(/rum_metric_value=([^&]*)/);
            console.log(`     Parsed CLS Value: ${valueMatch ? valueMatch[1] : 'not found'}`);
        } else {
            console.log("  ❌ No CLS beacon was dispatched (or value was empty).");
        }
    }

    // --- SCENARIO B: WITH INJECTED FIX (1000MS DELAY & PUSHED ELEMENT) ---
    console.log("\n--- Scenario B: With Injected Fix (1000ms delay & pushed element) ---");
    {
        const context = await browser.newContext({
            userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            extraHTTPHeaders: { 'X-RandomHack': 'true' }
        });
        const page = await context.newPage();

        let clsBeacon = null;
        page.on('request', req => {
            const url = req.url();
            if (url.includes('/rum-beacon') && url.includes('rum_metric_name=cls')) {
                clsBeacon = url;
            }
        });

        console.log(`  Opening live demo page: ${target}/rum-demo`);
        await page.goto(`${target}/rum-demo`);
        await page.waitForTimeout(1000);

        console.log("  Injecting the fixed layout shift mechanics (delayed shift + pushed element)...");
        await page.evaluate(() => {
            const container = document.getElementById("layout-shift-container");
            container.innerHTML = ""; // Clear existing status

            // 1. Create a pushed element directly below the shifting element container
            const pushed = document.createElement("div");
            pushed.id = "pushed-element";
            pushed.style.marginTop = "12px";
            pushed.style.padding = "20px";
            pushed.style.backgroundColor = "#21262d";
            pushed.style.borderRadius = "6px";
            pushed.style.border = "1px solid #30363d";
            pushed.innerHTML = `<span id="shift-status" style="color: #8b949e; font-style: italic;">This element will be pushed downwards when CLS occurs.</span>`;

            container.parentNode.insertBefore(pushed, container.nextSibling);

            // 2. Override global triggerLayoutShift with our 1000ms delayed version
            window.triggerLayoutShift = function() {
                var status = document.getElementById("shift-status");
                status.innerText = "⏳ Preparing unexpected layout shift (delaying 1000ms)...";
                status.style.color = "#f0883e";

                setTimeout(function() {
                    var element = document.createElement("div");
                    element.className = "shifting-element";
                    element.innerText = "💥 CLS Shift Generated! Content was pushed downwards.";
                    element.style.height = "150px"; // Real heights ensure a significant viewport displacement
                    element.style.backgroundColor = "#f0883e";
                    element.style.color = "#0d1117";
                    element.style.padding = "24px";
                    element.style.borderRadius = "6px";
                    element.style.fontWeight = "bold";
                    element.style.textAlign = "center";
                    element.style.marginTop = "16px";

                    container.appendChild(element);
                    status.innerText = "💥 CLS Shift Triggered! The element was pushed downwards.";
                    status.style.color = "#da3637";

                    setTimeout(function() {
                        element.remove();
                        status.innerText = "This element will be pushed downwards when CLS occurs.";
                        status.style.color = "#8b949e";
                    }, 2000);
                }, 1000);
            };
        });

        console.log("  Clicking 'Simulate Layout Shift (CLS)' button...");
        await page.click('#btn-trigger-cls');
        // Wait 3 seconds to guarantee the 1000ms-delayed shift completes and registers fully
        await page.waitForTimeout(3000);

        console.log("  Closing page/context to flush Faro Web Vitals...");
        await page.close();
        await context.close();

        if (clsBeacon) {
            console.log("  🎉 CLS Beacon Intercepted with Fix!");
            console.log(`     Raw URL: ${clsBeacon}`);
            const valueMatch = clsBeacon.match(/rum_metric_value=([^&]*)/);
            const value = valueMatch ? valueMatch[1] : 'not found';
            console.log(`     Parsed CLS Value: ${value}`);
            if (value && parseFloat(value) > 0.0) {
                console.log(`     🚀 SUCCESS! CLS is now non-zero: ${value} (Visual stability score is working!)`);
            } else {
                console.log(`     ⚠️ CLS was captured but is still ${value}.`);
            }
        } else {
            console.log("  ❌ No CLS beacon was dispatched.");
        }
    }

    await browser.close();
    console.log("\n===============================================================");
}

runVerification();
