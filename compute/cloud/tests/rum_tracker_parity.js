/* Faro Web SDK wrapper */
(function() {
  var SERVICE_ID = "svc1";
  var BEACON_ENDPOINT = "/rum-beacon";
  var CAPTURE_VITALS = true;
  var CAPTURE_PERFORMANCE = true;
  var CAPTURE_ERRORS = true;
  var CAPTURE_EVENTS = true;
  var SAMPLE_RATE = 1;
  var ERROR_SAMPLE_RATE = 1;
  var CAPTURE_RESOURCES = true;
  var RESOURCE_SAMPLE_RATE = 0.1;
  var MAX_RESOURCES = 50;
  var RESOURCE_REFILL_MS = 5000;
  var FLUSH_DELAY_MS = 1500;
  var MAX_URL_CHARS = 7500;

  var draw = NaN;
  try {
    draw = parseFloat(window.sessionStorage.getItem('rum_sample'));
  } catch (e) {}
  if (!(draw >= 0 && draw < 1)) {
    draw = Math.random();
    try {
      window.sessionStorage.setItem('rum_sample', String(draw));
    } catch (e) {}
  }
  var SAMPLED = draw < SAMPLE_RATE;
  var ERRORS_SAMPLED = CAPTURE_ERRORS && draw < ERROR_SAMPLE_RATE;
  var RESOURCES_SAMPLED = CAPTURE_RESOURCES && typeof PerformanceObserver === 'function' &&
    Math.random() < RESOURCE_SAMPLE_RATE;
  if (!SAMPLED && !ERRORS_SAMPLED && !RESOURCES_SAMPLED) return;

  var batch = { path: null, metrics: [], resources: [], errors: [] };
  var flushTimer = null;

  function enc(value) {
    return encodeURIComponent(String(value));
  }

  function queueItem(item) {
    if (!item || !item.payload) return;
    try {
      var metrics = [];
      var error = null;
      if (item.type === 'measurement') {
        if (!SAMPLED || !CAPTURE_VITALS) return;
        var m = item.payload;
        if (m.type === 'web-vitals') {
          var values = m.values || {};
          var rating = (m.context && m.context.rating) || (m.meta && m.meta.rating) || '';
          Object.keys(values).forEach(function(k) {
            if (k !== 'delta') {
              metrics.push(enc(k) + ':' + enc(values[k]) + ':' + enc(rating));
            }
          });
        }
      } else if (item.type === 'event') {
        if (!SAMPLED) return;
        var e = item.payload;
        if (e.name === 'faro.performance.navigation') {
          if (!CAPTURE_PERFORMANCE) return;
          var attrs = e.attributes || e.values || {};
          Object.keys(attrs).forEach(function(k) {
            metrics.push(enc(k) + ':' + enc(attrs[k]) + ':');
          });
        } else if (e.name && e.name.indexOf('faro.') !== 0) {
          if (!CAPTURE_EVENTS) return;
          metrics.push(enc('event_' + e.name) + ':1:');
        }
      } else if (item.type === 'exception') {
        if (!ERRORS_SAMPLED) return;
        var exc = item.payload;
        var message = exc.value || exc.message || '';
        if (!message) return;
        var frame = (exc.stacktrace && exc.stacktrace.frames && exc.stacktrace.frames[0]) || {};
        error = [message.slice(0, 500), frame.filename || '', frame.lineno || 0, frame.colno || 0]
          .map(enc).join('|');
      }
      if (metrics.length === 0 && !error) return;
      var path = window.location.pathname;
      if (batch.path !== null && batch.path !== path) flushBatch(false);
      batch.path = path;
      batch.metrics = batch.metrics.concat(metrics);
      if (error) batch.errors.push(error);
      scheduleFlush();
    } catch (e) {
      console.error('RUM: failed to queue beacon item:', e);
    }
  }

  function queueTiming(metrics, resources) {
    if (metrics.length === 0 && resources.length === 0) return;
    var path = window.location.pathname;
    if (batch.path !== null && batch.path !== path) flushBatch(false);
    batch.path = path;
    batch.metrics = batch.metrics.concat(metrics);
    batch.resources = batch.resources.concat(resources);
    scheduleFlush();
  }

  // One `rid:path:ms` per request that reached the edge: request start to last byte.
  function resourceTiming(entry) {
    if (!(entry.transferSize > 0) || !(entry.responseEnd > 0)) return null;
    var rid = '';
    (entry.serverTiming || []).forEach(function(t) {
      if (t.name === 'rid' && t.description) rid = t.description;
    });
    if (!rid) return null;
    var path;
    try {
      path = new URL(entry.name, window.location.href).pathname;
    } catch (e) {
      return null;
    }
    if (path === BEACON_ENDPOINT) return null;
    var ttlb = entry.responseEnd - (entry.requestStart > 0 ? entry.requestStart : entry.fetchStart || 0);
    if (!(ttlb >= 0)) return null;
    return enc(rid) + ':' + enc(path) + ':' + Math.round(ttlb);
  }

  // A page view may report MAX_RESOURCES requests at once, then one more per RESOURCE_REFILL_MS,
  // so a long session (video) keeps measuring later downloads at a bounded rate.
  var resourceBudget = MAX_RESOURCES;
  var resourceBudgetAt = Date.now();
  function takeResourceBudget() {
    var refill = Math.floor((Date.now() - resourceBudgetAt) / RESOURCE_REFILL_MS);
    if (refill > 0) {
      resourceBudget = Math.min(MAX_RESOURCES, resourceBudget + refill);
      resourceBudgetAt += refill * RESOURCE_REFILL_MS;
    }
    if (resourceBudget <= 0) return false;
    resourceBudget--;
    return true;
  }

  function observeTiming(type) {
    try {
      new PerformanceObserver(function(list) {
        try {
          var metrics = [];
          var resources = [];
          list.getEntries().forEach(function(entry) {
            if (type === 'navigation' && SAMPLED && CAPTURE_PERFORMANCE && entry.loadEventEnd > 0) {
              metrics.push('page_load:' + Math.round(entry.loadEventEnd - (entry.startTime || 0)) + ':');
            }
            if (!RESOURCES_SAMPLED) return;
            var timing = resourceTiming(entry);
            if (timing && takeResourceBudget()) resources.push(timing);
          });
          queueTiming(metrics, resources);
        } catch (e) {
          console.error('RUM: failed to queue timing entries:', e);
        }
      }).observe({ type: type, buffered: true });
    } catch (e) {}
  }
  if (typeof PerformanceObserver === 'function') {
    if (RESOURCES_SAMPLED || (SAMPLED && CAPTURE_PERFORMANCE)) observeTiming('navigation');
    if (RESOURCES_SAMPLED) observeTiming('resource');
  }

  function scheduleFlush() {
    if (flushTimer !== null) clearTimeout(flushTimer);
    var hidden = typeof document !== 'undefined' && document.visibilityState === 'hidden';
    flushTimer = setTimeout(function() {
      flushTimer = null;
      flushBatch(hidden);
    }, hidden ? 0 : FLUSH_DELAY_MS);
  }

  function beaconUrls() {
    var cid = '';
    var cookieMatch = document.cookie.match(/(?:^|; )rum_cid=([^;]+)/);
    if (cookieMatch) {
      try {
        cid = decodeURIComponent(cookieMatch[1]);
      } catch (e) {
        cid = cookieMatch[1];
      }
    }
    var head = BEACON_ENDPOINT + '?service_id=' + enc(SERVICE_ID) + (cid ? '&cid=' + enc(cid) : '') +
      '&p=' + enc(batch.path) + '&sr=' + SAMPLE_RATE + '&esr=' + ERROR_SAMPLE_RATE +
      (RESOURCES_SAMPLED ? '&rsr=' + RESOURCE_SAMPLE_RATE : '');
    var urls = [];
    var url = head;
    var openList = '';
    // `m` and `r` are comma-separated lists; every `e` is its own parameter.
    function append(part, key) {
      var isList = key !== 'e';
      var piece = isList && openList === key ? ',' + part : '&' + key + '=' + part;
      if (url !== head && url.length + piece.length > MAX_URL_CHARS) {
        urls.push(url);
        url = head;
        piece = '&' + key + '=' + part;
      }
      url += piece;
      openList = isList ? key : '';
    }
    batch.metrics.forEach(function(m) { append(m, 'm'); });
    batch.resources.forEach(function(r) { append(r, 'r'); });
    batch.errors.forEach(function(e) { append(e, 'e'); });
    if (url !== head) urls.push(url);
    return urls;
  }

  function flushBatch(useBeacon) {
    if (flushTimer !== null) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
    if (batch.metrics.length === 0 && batch.resources.length === 0 && batch.errors.length === 0) return;
    var urls = beaconUrls();
    batch = { path: null, metrics: [], resources: [], errors: [] };
    urls.forEach(function(url) {
      if (useBeacon && originalSendBeacon) {
        originalSendBeacon.call(navigator, url, '');
      } else if (originalFetch) {
        originalFetch.call(window, url, { method: 'POST', body: '', keepalive: true })
          .catch(function() {});
      }
    });
  }

  // Faro's own transport sends are dropped: the batch above carries the data.
  var originalSendBeacon = null;
  if (typeof navigator !== 'undefined' && typeof navigator.sendBeacon === 'function') {
    originalSendBeacon = navigator.sendBeacon;
    navigator.sendBeacon = function(url, data) {
      if (typeof url === 'string' && url.indexOf('/rum-beacon') !== -1) {
        return true;
      }
      return originalSendBeacon.call(this, url, data);
    };
  }

  var originalFetch = null;
  if (typeof window.fetch === 'function') {
    originalFetch = window.fetch;
    window.fetch = function(input, init) {
      if (typeof input === 'string' && input.indexOf('/rum-beacon') !== -1) {
        return Promise.resolve(new Response(null, { status: 204, statusText: 'No Content' }));
      }
      return originalFetch.call(this, input, init);
    };
  }

  if (typeof XMLHttpRequest === 'function') {
    var originalOpen = XMLHttpRequest.prototype.open;
    XMLHttpRequest.prototype.open = function(method, url) {
      if (typeof url === 'string' && url.indexOf('/rum-beacon') !== -1) {
        this._rumBeacon = true;
      }
      return originalOpen.apply(this, arguments);
    };

    var originalSend = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.send = function(body) {
      if (this._rumBeacon) {
        var self = this;
        setTimeout(function() {
          Object.defineProperty(self, 'readyState', { value: 4, writable: true });
          Object.defineProperty(self, 'status', { value: 204, writable: true });
          if (typeof self.onreadystatechange === 'function') {
            self.onreadystatechange();
          }
          if (typeof self.onload === 'function') {
            self.onload();
          }
        }, 0);
        return;
      }
      return originalSend.apply(this, arguments);
    };
  }

  document.addEventListener('visibilitychange', function() {
    if (document.visibilityState === 'hidden') flushBatch(true);
  });
  window.addEventListener('pagehide', function() {
    flushBatch(true);
  });

  // Check if Faro is present (either via window.Faro or window.GrafanaFaroWebSdk)
  var Faro = window.Faro || window.GrafanaFaroWebSdk;

  if (!SAMPLED && !ERRORS_SAMPLED) {
    // Resource timing alone reads the browser's own entries and needs no SDK.
  } else if (typeof Faro === 'undefined') {
    var script = document.createElement('script');
    script.src = '/js/faro-sdk.js';
    script.async = true;
    script.onload = function() {
      Faro = window.Faro || window.GrafanaFaroWebSdk;
      initializeFaro();
    };
    script.onerror = function() {
      console.error('Failed to load Faro SDK');
    };
    document.head.appendChild(script);
  } else {
    initializeFaro();
  }

  function initializeFaro() {
    if (typeof Faro === 'undefined' || typeof Faro.initializeFaro !== 'function') {
      console.error('Faro SDK not available or initializeFaro not a function');
      return;
    }

    var instrumentations = [];

    if (SAMPLED && Faro.WebVitalsInstrumentation) {
      instrumentations.push(new Faro.WebVitalsInstrumentation());
    }
    if (SAMPLED && Faro.ViewInstrumentation) {
      instrumentations.push(new Faro.ViewInstrumentation());
    }
    var ErrorInstrumentationCtor = Faro.ErrorsInstrumentation || Faro.ErrorInstrumentation;
    if (ErrorInstrumentationCtor) {
      instrumentations.push(new ErrorInstrumentationCtor());
    } else {
      console.warn('Faro SDK: no error instrumentation export found (checked ErrorsInstrumentation, ErrorInstrumentation) — JS error tracking disabled');
    }

    var config = {
      app: {
        name: 'rum-app',
        version: '1.0.0'
      },
      url: BEACON_ENDPOINT + '?service_id=' + enc(SERVICE_ID),
      instrumentations: instrumentations,
      samplingRate: 1,
      batching: {
        enabled: true,
        sendTimeout: 10,
        itemLimit: 1
      },
      beforeSend: function(event) {
        if (!event) return null;
        if (event.type) {
          queueItem(event);
        } else {
          (event.measurements || []).forEach(function(payload) {
            queueItem({ type: 'measurement', payload: payload });
          });
          (event.events || []).forEach(function(payload) {
            queueItem({ type: 'event', payload: payload });
          });
          (event.exceptions || []).forEach(function(payload) {
            queueItem({ type: 'exception', payload: payload });
          });
        }
        return null;
      }
    };

    try {
      Faro.initializeFaro(config);
    } catch (e) {
      console.error('Failed to initialize Faro:', e);
    }
  }
})();
