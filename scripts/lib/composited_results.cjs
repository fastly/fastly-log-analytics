const { randomUUID } = require('node:crypto');

async function sampleClock(page, cdp, key) {
  const read = () => page.evaluate(key => ({
    now: performance.now(), origin: performance.timeOrigin, document: Boolean(window[key]),
    width: innerWidth, height: innerHeight, scale: devicePixelRatio,
    scrollX, scrollY,
  }), key);
  const before = await read();
  const { metrics } = await cdp.send('Performance.getMetrics');
  const after = await read();
  const values = Object.fromEntries(metrics.map(row => [row.name, row.value]));
  const relative = (values.Timestamp - values.NavigationStart) * 1000;
  return {
    ...after, navigation: values.NavigationStart, span: after.now - before.now,
    relative_lower_offset_ms: Number.isFinite(relative) ? relative - before.now : null,
    relative_upper_offset_ms: Number.isFinite(relative) ? relative - after.now : null,
    verified: before.document && after.document && before.origin === after.origin
      && Number.isFinite(relative) && relative >= before.now - 1 && relative <= after.now + 1
      && after.now >= before.now && after.now - before.now <= 100,
  };
}

function validateReference(reference, viewport) {
  if (!reference || !['independent_render', 'canvas_backing_store', 'svg_snapshot'].includes(reference.source)
    || ![undefined, 'opaque_pixels'].includes(reference.mask)
    || !Array.isArray(reference.regions) || !reference.regions.length || reference.regions.length > 32) {
    throw new Error('An independent reference, never a screenshot baseline, is required');
  }
  let pixels = 0;
  for (const region of reference.regions) {
    const { x, y, width, height, pngBase64 } = region;
    pixels += width * height;
    if (![x, y, width, height].every(Number.isInteger) || x < 0 || y < 0 || width <= 0 || height <= 0
      || x + width > viewport.width || y + height > viewport.height || pixels > 8_000_000) {
      throw new Error('Invalid or oversized reference bounds');
    }
    if (typeof pngBase64 !== 'string' || pngBase64.length > 8_000_000
      || !/^[A-Za-z0-9+/]+={0,2}$/.test(pngBase64)) throw new Error('Invalid reference PNG');
    const png = Buffer.from(pngBase64, 'base64');
    if (png.length < 33 || !png.subarray(0, 8).equals(Buffer.from('89504e470d0a1a0a', 'hex'))
      || png.toString('ascii', 12, 16) !== 'IHDR'
      || png.readUInt32BE(16) !== width || png.readUInt32BE(20) !== height) {
      throw new Error('Reference PNG dimensions do not match its bounds');
    }
  }
}

async function matchFrames(page, frames, reference, viewport, event, navigation, cutoff) {
  return page.evaluate(async ({ frames, reference, viewport, event, navigation, cutoff }) => {
    const bitmap = encoded => {
      const bytes = Uint8Array.from(atob(encoded), character => character.charCodeAt(0));
      return createImageBitmap(new Blob([bytes], { type: 'image/png' }));
    };
    const pixels = (image, rect) => {
      const canvas = new OffscreenCanvas(rect.width, rect.height);
      const context = canvas.getContext('2d', { willReadFrequently: true });
      context.drawImage(image, -rect.x, -rect.y);
      return context.getImageData(0, 0, rect.width, rect.height).data;
    };
    const expected = [];
    for (const region of reference.regions) {
      const image = await bitmap(region.pngBase64);
      try {
        const rgba = pixels(image, { ...region, x: 0, y: 0 });
        let opaque = 0;
        for (let index = 3; index < rgba.length; index += 4) {
          if (rgba[index] === 255) opaque++;
          else if (rgba[index] !== 0 || reference.mask !== 'opaque_pixels') {
            throw new Error('Reference PNG must be opaque or use an explicit binary mask');
          }
        }
        if (!opaque) throw new Error('Reference PNG contains no required pixels');
        expected.push(rgba);
      } finally { image.close(); }
    }
    let first = null;
    let observed = 0;
    let preexisting = false;
    let geometryChanged = false;
    let clockMissing = false;
    let previous = -Infinity;
    const surface = new OffscreenCanvas(viewport.width, viewport.height);
    const context = surface.getContext('2d', { willReadFrequently: true });
    for (const frame of frames) {
      const metadata = frame.metadata;
      const time = metadata.monotonicTimestamp * 1000 - navigation * 1000;
      if (typeof metadata.monotonicTimestamp !== 'number' || !Number.isFinite(time)
        || time < 0 || time > cutoff + 1 || time < previous) {
        clockMissing = true;
        continue;
      }
      previous = time;
      if (metadata.deviceWidth !== viewport.width || metadata.deviceHeight !== viewport.height
        || metadata.pageScaleFactor !== 1 || metadata.offsetTop !== 0
        || metadata.scrollOffsetX !== viewport.scrollX || metadata.scrollOffsetY !== viewport.scrollY) {
        geometryChanged = true;
        continue;
      }
      const image = await bitmap(frame.data);
      try {
        if (image.width !== viewport.width || image.height !== viewport.height) {
          geometryChanged = true;
          continue;
        }
        context.clearRect(0, 0, viewport.width, viewport.height);
        context.drawImage(image, 0, 0);
        let matched = 0;
        for (const [index, region] of reference.regions.entries()) {
          const actual = context.getImageData(region.x, region.y, region.width, region.height).data;
          if (actual.every((value, offset) => expected[index][offset - offset % 4 + 3] === 0
            || value === expected[index][offset])) matched++;
        }
        if (event !== null && time >= event) {
          observed = Math.max(observed, matched);
          if (matched === reference.regions.length && first === null) first = time;
        } else if (event !== null && matched === reference.regions.length) {
          preexisting = true;
        }
      } finally { image.close(); }
    }
    return { first, observed, preexisting, geometryChanged, clockMissing };
  }, { frames, reference, viewport, event, navigation, cutoff });
}

async function measureCompositedResults(page, options, action) {
  const { target, reference, eventType = 'click', timeoutMs = 5000 } = options;
  if (typeof target !== 'string' || !target || !['click', 'keydown'].includes(eventType)
    || typeof reference !== 'function' || typeof action !== 'function'
    || !Number.isFinite(timeoutMs) || timeoutMs <= 0 || timeoutMs > 60000) {
    throw new Error('An explicit target, independent reference callback and bounded action are required');
  }
  const key = `__flaCompositor${randomUUID().replaceAll('-', '')}`;
  const frames = [];
  const pendingWork = new Set();
  let trustedEvent = null;
  let readingEvent = false;
  let eventSamplingFailed = false;
  let frameBytes = 0;
  let truncated = false;
  let framesDropped = 0;
  let earliestDropped = Infinity;
  let latestCaptured = -Infinity;
  let captureClockInvalid = false;
  let captureFailure;
  let firstFrame;
  const ready = new Promise(resolve => { firstFrame = resolve; });
  let timer;
  const deadline = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error('compositor_observation_timeout')), timeoutMs);
  });
  const bounded = promise => Promise.race([promise, deadline]);
  let cdp;
  let acquisition;
  let disposing = false;
  let listener;
  let result;
  let failure;
  try {
    acquisition = page.context().newCDPSession(page).then(async session => {
      if (disposing) { await session.detach(); return; }
      cdp = session;
    });
    await bounded(acquisition);
    await bounded(cdp.send('Performance.enable'));
    await bounded(page.evaluate(({ key, target, eventType }) => {
      document.querySelector(target);
      const state = { event: null };
      const listener = event => {
        if (!event.isTrusted || state.event !== null || !Number.isFinite(event.timeStamp)
          || event.timeStamp < 0 || event.timeStamp > performance.now()
          || !event.composedPath().some(node => node instanceof Element && node.matches(target))) return;
        state.event = event.timeStamp;
      };
      document.addEventListener(eventType, listener, true);
      window[key] = { state, cleanup() {
        document.removeEventListener(eventType, listener, true);
        delete window[key];
      } };
    }, { key, target, eventType }));
    const before = await bounded(sampleClock(page, cdp, key));
    if (before.width * before.height > 8_000_000) throw new Error('Compositor viewport is oversized');
    listener = frame => {
      const time = typeof frame.metadata.monotonicTimestamp === 'number'
        ? (frame.metadata.monotonicTimestamp - before.navigation) * 1000 : NaN;
      if (!Number.isFinite(time) || time < 0 || time < latestCaptured) captureClockInvalid = true;
      if (Number.isFinite(time)) latestCaptured = Math.max(latestCaptured, time);
      frameBytes += frame.data.length;
      frames.push(frame);
      while (frames.length > 120 || frameBytes > 64 * 1024 * 1024) {
        if (frames.length === 1) { truncated = true; break; }
        // Preserve the initial pre-input image and a bounded recent window.
        // Eviction can only make an observed completion upper bound later.
        const postInput = trustedEvent === null ? -1 : frames.findIndex((candidate, index) =>
          index > 0 && (candidate.metadata.monotonicTimestamp - before.navigation) * 1000 >= trustedEvent);
        const [removed] = frames.splice(postInput > 0 ? postInput : 1, 1);
        frameBytes -= removed.data.length;
        const removedTime = (removed.metadata.monotonicTimestamp - before.navigation) * 1000;
        if (Number.isFinite(removedTime)) earliestDropped = Math.min(earliestDropped, removedTime);
        framesDropped++;
      }
      const ack = cdp.send('Page.screencastFrameAck', { sessionId: frame.sessionId });
      pendingWork.add(ack);
      ack.then(() => pendingWork.delete(ack), error => {
        captureFailure ??= error;
        pendingWork.delete(ack);
      });
      if (trustedEvent === null && !readingEvent) {
        readingEvent = true;
        const read = page.evaluate(key => window[key]?.state.event ?? null, key).then(value => {
          if (typeof value === 'number' && Number.isFinite(value)) trustedEvent = value;
        }, () => { eventSamplingFailed = true; });
        pendingWork.add(read);
        read.then(() => { pendingWork.delete(read); readingEvent = false; });
      }
      firstFrame();
    };
    cdp.on('Page.screencastFrame', listener);
    await bounded(cdp.send('Page.startScreencast', { format: 'png', everyNthFrame: 1 }));
    await bounded(ready);
    await bounded(action());
    // Let the ready application frame reach capture before rendering its oracle.
    // These callbacks are not paint evidence; only matching captured pixels are.
    await bounded(page.evaluate(() => new Promise(resolve =>
      requestAnimationFrame(() => requestAnimationFrame(resolve)))));
    const expected = await bounded(reference(page));
    validateReference(expected, before);
    // Flush capture delivery; only matching compositor frames supply paint timestamps.
    await bounded(page.evaluate(() => new Promise(resolve =>
      requestAnimationFrame(() => requestAnimationFrame(resolve)))));
    await bounded(cdp.send('Page.stopScreencast'));
    cdp.off('Page.screencastFrame', listener);
    listener = null;
    await bounded(Promise.allSettled([...pendingWork]));
    if (captureFailure) throw captureFailure;
    const after = await bounded(sampleClock(page, cdp, key));
    const event = await bounded(page.evaluate(key => window[key]?.state.event ?? null, key));
    const clockVerified = before.verified && after.verified
      && before.origin === after.origin && before.navigation === after.navigation
      && !captureClockInvalid && latestCaptured <= after.now + 1;
    const matches = await bounded(matchFrames(page, frames, expected, before, event, before.navigation, after.now));
    const clockRejections = [];
    if (!before.verified) clockRejections.push('before_calibration_failed');
    if (!after.verified) clockRejections.push('after_calibration_failed');
    if (before.origin !== after.origin) clockRejections.push('document_origin_changed');
    if (before.navigation !== after.navigation) clockRejections.push('navigation_start_changed');
    if (captureClockInvalid) clockRejections.push('capture_timestamp_invalid_or_reordered');
    if (latestCaptured > after.now + 1) clockRejections.push('capture_timestamp_after_observation');
    if (matches.clockMissing) clockRejections.push('retained_frame_clock_invalid');
    const calibration = sample => ({
      verified: sample.verified, span_ms: sample.span,
      relative_lower_offset_ms: sample.relative_lower_offset_ms,
      relative_upper_offset_ms: sample.relative_upper_offset_ms,
    });
    const gaps = ['reference_semantics_unverified', 'scenario_correctness_unverified',
      'repeated_samples_unverified', 'cache_population_unverified', 'live_traffic_unverified'];
    if (!before.document || !after.document || before.origin !== after.origin) gaps.push('observation_document_changed');
    if (!clockVerified || matches.clockMissing) gaps.push('compositor_clock_unverified');
    if (before.scale !== 1 || after.scale !== 1 || matches.geometryChanged
      || before.width !== after.width || before.height !== after.height
      || before.scrollX !== after.scrollX || before.scrollY !== after.scrollY) gaps.push('compositor_geometry_changed');
    if (event === null) gaps.push('trusted_event_unobserved');
    if (truncated) gaps.push('compositor_frames_truncated');
    if (framesDropped) gaps.push('compositor_capture_window_reduced');
    if (eventSamplingFailed) gaps.push('capture_event_sampling_failed');
    if (event !== null && earliestDropped < event) gaps.push('pre_input_frame_coverage_incomplete');
    if (matches.preexisting) gaps.push('reference_visible_before_event');
    const observed = matches.first !== null && clockVerified && !matches.clockMissing
      && !gaps.some(gap => ['compositor_geometry_changed', 'trusted_event_unobserved',
        'compositor_frames_truncated', 'reference_visible_before_event'].includes(gap));
    const completePreInput = !gaps.includes('pre_input_frame_coverage_incomplete');
    if (!observed || !completePreInput) gaps.push('visible_results_paint_unobserved');
    result = {
      qualification: 'incomplete', event_arrival_ms: event,
      clock: { status: clockVerified && !matches.clockMissing ? 'verified' : 'unobserved',
        calibration_span_ms: Math.max(before.span, after.span),
        rejection_reasons: clockRejections,
        calibration: { before: calibration(before), after: calibration(after) } },
      results: { status: observed && completePreInput ? 'observed' : 'unobserved', source: 'compositor_frame',
        elapsed_ms: observed && completePreInput ? matches.first - event : null,
        panels_observed: matches.observed, panels_required: expected.regions.length,
        pixel_scope: expected.mask === 'opaque_pixels' ? 'opaque_reference_pixels' : 'complete_reference_regions',
        precision: framesDropped ? 'first_matching_retained_frame_upper_bound'
          : 'first_matching_captured_frame_upper_bound' },
      frames_observed: frames.length, frames_dropped: framesDropped, gaps,
    };
  } catch (error) { failure = error; }
  disposing = true;
  clearTimeout(timer);
  const errors = failure ? [failure] : [];
  let cleanupTimer;
  const cleanupDeadline = new Promise((_, reject) => {
    cleanupTimer = setTimeout(() => reject(new Error('compositor_cleanup_timeout')), 3000);
  });
  for (const cleanup of [
    async () => { if (!cdp && acquisition) await acquisition; },
    async () => { if (listener) cdp.off('Page.screencastFrame', listener); },
    async () => { if (cdp) await cdp.send('Page.stopScreencast'); },
    async () => {
      await Promise.allSettled([...pendingWork]);
      if (captureFailure) throw captureFailure;
    },
    async () => { if (!page.isClosed()) await page.evaluate(key => window[key]?.cleanup(), key); },
    async () => { if (cdp) await cdp.detach(); },
  ]) {
    try { await Promise.race([cleanup(), cleanupDeadline]); } catch (error) { errors.push(error); }
  }
  clearTimeout(cleanupTimer);
  if (errors.length) throw errors.length === 1 ? errors[0] : new AggregateError(errors, 'compositor_cleanup_failed');
  return result;
}

module.exports = { measureCompositedResults };
