const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require('playwright');

async function main() {
    if (!process.argv[2]) throw new Error('Usage: node tools/check_map_filters.cjs REPORT_DIR [COUNTS_JSON]');
    const directory = path.resolve(process.argv[2]);
    const counts = JSON.parse(fs.readFileSync(process.argv[3] || path.join(directory, 'expected.json'), 'utf8'));
    const years = Object.keys(counts).map(Number).sort((first, second) => first - second);
    const total = Object.values(counts).reduce((sum, count) => sum + count, 0);
    const browser = await chromium.launch({ args: ['--no-sandbox'] });
    try {
        for (const filename of ['enhanced_earthquake_map.html', 'animated_earthquake_map.html']) {
            const animated = filename === 'animated_earthquake_map.html';
            const page = await browser.newPage({ viewport: { width: 1200, height: 800 } });
            const errors = [];
            page.on('pageerror', error => errors.push(error.message));
            await page.route('**/*', async route => {
                const url = new URL(route.request().url());
                if (url.hostname === 'map-test.invalid') {
                    const local = path.resolve(directory, '.' + decodeURIComponent(url.pathname));
                    if (local.startsWith(directory + path.sep) && fs.existsSync(local)) {
                        return route.fulfill({ path: local });
                    }
                }
                if (url.hostname === 'tile.openstreetmap.org') {
                    return route.fulfill({ contentType: 'image/png', body: Buffer.from(
                        'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a6xkAAAAASUVORK5CYII=', 'base64') });
                }
                return route.abort();
            });
            await page.goto('https://map-test.invalid/' + filename);
            await page.evaluate(() => {
                window.testMap = Object.values(window).find(value => value instanceof L.Map);
                window.testMarkers = Object.values(window).find(value =>
                    value instanceof L.LayerGroup && value.getLayers().some(marker => Number.isInteger(marker.options.eventYear)));
                window.testHeat = Object.values(window).find(value => L.HeatLayer && value instanceof L.HeatLayer);
                window.testReferences = Object.values(testMap._layers).filter(layer =>
                    layer instanceof L.GeoJSON).map(layer => layer.getLayers().length);
            });
            async function check(fromYear, toYear) {
                const expected = years.filter(year => year >= fromYear && year <= toYear)
                    .reduce((sum, year) => sum + counts[year], 0);
                const message = expected ? expected.toLocaleString('en') + ' earthquakes selected' : 'No earthquakes in these years';
                assert.equal(await page.locator('.mmeq-count').textContent(), message);
                const state = await page.evaluate(({ fromYear, toYear, animated }) => {
                    const layer = Object.values(testMap._layers).find(layer => layer._baseLayer);
                    const features = layer ? layer._baseLayer.toGeoJSON().features : [];
                    return {
                        markers: testMarkers ? testMarkers.getLayers().length : null,
                        heat: testHeat ? testHeat._latlngs.length : null,
                        events: features.length,
                        outside: features.filter(feature => {
                            const year = new Date(feature.properties.time).getUTCFullYear();
                            return year < fromYear || year > toYear;
                        }).length,
                        frames: animated ? testMap.timeDimension.getAvailableTimes().length : 0,
                        references: animated ? [] : Object.values(testMap._layers).filter(layer =>
                            layer instanceof L.GeoJSON).map(layer => layer.getLayers().length),
                        originalReferences: testReferences,
                    };
                }, { fromYear, toYear, animated });
                if (animated) {
                    assert.equal(state.events, expected);
                    assert.equal(state.outside, 0);
                    assert.equal(state.frames === 0, expected === 0);
                } else {
                    if (state.markers !== null) assert.equal(state.markers, expected);
                    if (state.heat !== null) assert.equal(state.heat, expected);
                    assert.deepEqual(state.references, state.originalReferences);
                }
                assert.deepEqual(errors, []);
            }
            await check(years[0], years.at(-1));
            await page.getByRole('button', { name: 'Latest year' }).click();
            await check(years.at(-1), years.at(-1));
            if (animated && await page.evaluate(() => testMap.timeDimension.getAvailableTimes().length > 1)) {
                await page.locator('.timecontrol-play').click();
                await page.waitForFunction(() => testMap.timeDimension.getCurrentTimeIndex() > 0);
                await page.locator('.timecontrol-play').click();
            }
            const earlier = years[Math.max(0, years.length - 2)];
            await page.getByLabel('From year (UTC)').selectOption(String(earlier));
            await check(earlier, years.at(-1));
            await page.getByLabel('To year (UTC)').selectOption(String(earlier));
            await check(earlier, earlier);
            await page.getByLabel('From year (UTC)').selectOption(String(years.at(-1)));
            await check(years.at(-1), years.at(-1));
            const gap = Array.from({ length: years.at(-1) - years[0] + 1 }, (unused, index) => years[0] + index)
                .find(year => !counts[year]);
            if (gap !== undefined) {
                await page.getByLabel('From year (UTC)').selectOption(String(gap));
                await page.getByLabel('To year (UTC)').selectOption(String(gap));
                await check(gap, gap);
            }
            await page.getByRole('button', { name: 'All years' }).click();
            await check(years[0], years.at(-1));
            if (animated) {
                await page.evaluate(() => testMap.timeDimension.setCurrentTimeIndex(testMap.timeDimension.getAvailableTimes().length - 1));
                const visible = await page.evaluate(() => Object.values(testMap._layers)
                    .find(layer => layer._baseLayer)._currentLayer.getLayers().length);
                assert.equal(visible, total);
            }
            await page.setViewportSize({ width: 390, height: 844 });
            await page.getByLabel('From year (UTC)').focus();
            await page.keyboard.press('Tab');
            assert.equal(await page.getByLabel('To year (UTC)').evaluate(element => element === document.activeElement), true);
            for (const selector of ['.mmeq-map-panel', '.leaflet-control-attribution', ...(animated ? ['.leaflet-bar-timecontrol'] : [])]) {
                const bounds = await page.locator(selector).boundingBox();
                assert.ok(bounds && bounds.x >= 0 && bounds.y >= 0 && bounds.x + bounds.width <= 390 && bounds.y + bounds.height <= 844, selector);
            }
            assert.deepEqual(errors, []);
            console.log('PASS', filename, 'year filters, reset, counts, controls, and playback');
            await page.close();
        }
    } finally {
        await browser.close();
    }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
