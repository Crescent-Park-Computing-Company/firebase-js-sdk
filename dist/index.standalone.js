'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

var Websocket = require('faye-websocket');
var util = require('@firebase/util');
var logger$1 = require('@firebase/logger');
var app = require('@firebase/app');
var component = require('@firebase/component');

function _interopDefaultLegacy (e) { return e && typeof e === 'object' && 'default' in e ? e : { 'default': e }; }

var Websocket__default = /*#__PURE__*/_interopDefaultLegacy(Websocket);

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const PROTOCOL_VERSION = '5';
const VERSION_PARAM = 'v';
const TRANSPORT_SESSION_PARAM = 's';
const REFERER_PARAM = 'r';
const FORGE_REF = 'f';
// Matches console.firebase.google.com, firebase-console-*.corp.google.com and
// firebase.corp.google.com
const FORGE_DOMAIN_RE = /(console\.firebase|firebase-console-\w+\.corp|firebase\.corp)\.google\.com/;
const LAST_SESSION_PARAM = 'ls';
const APPLICATION_ID_PARAM = 'p';
const APP_CHECK_TOKEN_PARAM = 'ac';
const WEBSOCKET = 'websocket';
const LONG_POLLING = 'long_polling';

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Wraps a DOM Storage object and:
 * - automatically encode objects as JSON strings before storing them to allow us to store arbitrary types.
 * - prefixes names with "firebase:" to avoid collisions with app data.
 *
 * We automatically (see storage.js) create two such wrappers, one for sessionStorage,
 * and one for localStorage.
 *
 */
class DOMStorageWrapper {
    /**
     * @param domStorage_ - The underlying storage object (e.g. localStorage or sessionStorage)
     */
    constructor(domStorage_) {
        this.domStorage_ = domStorage_;
        // Use a prefix to avoid collisions with other stuff saved by the app.
        this.prefix_ = 'firebase:';
    }
    /**
     * @param key - The key to save the value under
     * @param value - The value being stored, or null to remove the key.
     */
    set(key, value) {
        if (value == null) {
            this.domStorage_.removeItem(this.prefixedName_(key));
        }
        else {
            this.domStorage_.setItem(this.prefixedName_(key), util.stringify(value));
        }
    }
    /**
     * @returns The value that was stored under this key, or null
     */
    get(key) {
        const storedVal = this.domStorage_.getItem(this.prefixedName_(key));
        if (storedVal == null) {
            return null;
        }
        else {
            return util.jsonEval(storedVal);
        }
    }
    remove(key) {
        this.domStorage_.removeItem(this.prefixedName_(key));
    }
    prefixedName_(name) {
        return this.prefix_ + name;
    }
    toString() {
        return this.domStorage_.toString();
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * An in-memory storage implementation that matches the API of DOMStorageWrapper
 * (TODO: create interface for both to implement).
 */
class MemoryStorage {
    constructor() {
        this.cache_ = {};
        this.isInMemoryStorage = true;
    }
    set(key, value) {
        if (value == null) {
            delete this.cache_[key];
        }
        else {
            this.cache_[key] = value;
        }
    }
    get(key) {
        if (util.contains(this.cache_, key)) {
            return this.cache_[key];
        }
        return null;
    }
    remove(key) {
        delete this.cache_[key];
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Helper to create a DOMStorageWrapper or else fall back to MemoryStorage.
 * TODO: Once MemoryStorage and DOMStorageWrapper have a shared interface this method annotation should change
 * to reflect this type
 *
 * @param domStorageName - Name of the underlying storage object
 *   (e.g. 'localStorage' or 'sessionStorage').
 * @returns Turning off type information until a common interface is defined.
 */
const createStoragefor = function (domStorageName) {
    try {
        // NOTE: just accessing "localStorage" or "window['localStorage']" may throw a security exception,
        // so it must be inside the try/catch.
        if (typeof window !== 'undefined' &&
            typeof window[domStorageName] !== 'undefined') {
            // Need to test cache. Just because it's here doesn't mean it works
            const domStorage = window[domStorageName];
            domStorage.setItem('firebase:sentinel', 'cache');
            domStorage.removeItem('firebase:sentinel');
            return new DOMStorageWrapper(domStorage);
        }
    }
    catch (e) { }
    // Failed to create wrapper.  Just return in-memory storage.
    // TODO: log?
    return new MemoryStorage();
};
/** A storage object that lasts across sessions */
const PersistentStorage = createStoragefor('localStorage');
/** A storage object that only lasts one session */
const SessionStorage = createStoragefor('sessionStorage');

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const logClient = new logger$1.Logger('@firebase/database');
/**
 * Returns a locally-unique ID (generated by just incrementing up from 0 each time its called).
 */
const LUIDGenerator = (function () {
    let id = 1;
    return function () {
        return id++;
    };
})();
/**
 * Sha1 hash of the input string
 * @param str - The string to hash
 * @returns {!string} The resulting hash
 */
const sha1 = function (str) {
    const utf8Bytes = util.stringToByteArray(str);
    const sha1 = new util.Sha1();
    sha1.update(utf8Bytes);
    const sha1Bytes = sha1.digest();
    return util.base64.encodeByteArray(sha1Bytes);
};
const buildLogMessage_ = function (...varArgs) {
    let message = '';
    for (let i = 0; i < varArgs.length; i++) {
        const arg = varArgs[i];
        if (Array.isArray(arg) ||
            (arg &&
                typeof arg === 'object' &&
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                typeof arg.length === 'number')) {
            message += buildLogMessage_.apply(null, arg);
        }
        else if (typeof arg === 'object') {
            message += util.stringify(arg);
        }
        else {
            message += arg;
        }
        message += ' ';
    }
    return message;
};
/**
 * Use this for all debug messages in Firebase.
 */
let logger = null;
/**
 * Flag to check for log availability on first log message
 */
let firstLog_ = true;
/**
 * The implementation of Firebase.enableLogging (defined here to break dependencies)
 * @param logger_ - A flag to turn on logging, or a custom logger
 * @param persistent - Whether or not to persist logging settings across refreshes
 */
const enableLogging$1 = function (logger_, persistent) {
    util.assert(!persistent || logger_ === true || logger_ === false, "Can't turn on custom loggers persistently.");
    if (logger_ === true) {
        logClient.logLevel = logger$1.LogLevel.VERBOSE;
        logger = logClient.log.bind(logClient);
        if (persistent) {
            SessionStorage.set('logging_enabled', true);
        }
    }
    else if (typeof logger_ === 'function') {
        logger = logger_;
    }
    else {
        logger = null;
        SessionStorage.remove('logging_enabled');
    }
};
const log = function (...varArgs) {
    if (firstLog_ === true) {
        firstLog_ = false;
        if (logger === null && SessionStorage.get('logging_enabled') === true) {
            enableLogging$1(true);
        }
    }
    if (logger) {
        const message = buildLogMessage_.apply(null, varArgs);
        logger(message);
    }
};
const logWrapper = function (prefix) {
    return function (...varArgs) {
        log(prefix, ...varArgs);
    };
};
const error = function (...varArgs) {
    const message = 'FIREBASE INTERNAL ERROR: ' + buildLogMessage_(...varArgs);
    logClient.error(message);
};
const fatal = function (...varArgs) {
    const message = `FIREBASE FATAL ERROR: ${buildLogMessage_(...varArgs)}`;
    logClient.error(message);
    throw new Error(message);
};
const warn = function (...varArgs) {
    const message = 'FIREBASE WARNING: ' + buildLogMessage_(...varArgs);
    logClient.warn(message);
};
/**
 * Logs a warning if the containing page uses https. Called when a call to new Firebase
 * does not use https.
 */
const warnIfPageIsSecure = function () {
    // Be very careful accessing browser globals. Who knows what may or may not exist.
    if (typeof window !== 'undefined' &&
        window.location &&
        window.location.protocol &&
        window.location.protocol.indexOf('https:') !== -1) {
        warn('Insecure Firebase access from a secure page. ' +
            'Please use https in calls to new Firebase().');
    }
};
/**
 * Returns true if data is NaN, or +/- Infinity.
 */
const isInvalidJSONNumber = function (data) {
    return (typeof data === 'number' &&
        (data !== data || // NaN
            data === Number.POSITIVE_INFINITY ||
            data === Number.NEGATIVE_INFINITY));
};
const executeWhenDOMReady = function (fn) {
    if (util.isNodeSdk() || document.readyState === 'complete') {
        fn();
    }
    else {
        // Modeled after jQuery. Try DOMContentLoaded and onreadystatechange (which
        // fire before onload), but fall back to onload.
        let called = false;
        const wrappedFn = function () {
            if (!document.body) {
                setTimeout(wrappedFn, Math.floor(10));
                return;
            }
            if (!called) {
                called = true;
                fn();
            }
        };
        if (document.addEventListener) {
            document.addEventListener('DOMContentLoaded', wrappedFn, false);
            // fallback to onload.
            window.addEventListener('load', wrappedFn, false);
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
        }
        else if (document.attachEvent) {
            // IE.
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            document.attachEvent('onreadystatechange', () => {
                if (document.readyState === 'complete') {
                    wrappedFn();
                }
            });
            // fallback to onload.
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            window.attachEvent('onload', wrappedFn);
            // jQuery has an extra hack for IE that we could employ (based on
            // http://javascript.nwbox.com/IEContentLoaded/) But it looks really old.
            // I'm hoping we don't need it.
        }
    }
};
/**
 * Minimum key name. Invalid for actual data, used as a marker to sort before any valid names
 */
const MIN_NAME = '[MIN_NAME]';
/**
 * Maximum key name. Invalid for actual data, used as a marker to sort above any valid names
 */
const MAX_NAME = '[MAX_NAME]';
/**
 * Compares valid Firebase key names, plus min and max name
 */
const nameCompare = function (a, b) {
    if (a === b) {
        return 0;
    }
    else if (a === MIN_NAME || b === MAX_NAME) {
        return -1;
    }
    else if (b === MIN_NAME || a === MAX_NAME) {
        return 1;
    }
    else {
        const aAsInt = tryParseInt(a), bAsInt = tryParseInt(b);
        if (aAsInt !== null) {
            if (bAsInt !== null) {
                return aAsInt - bAsInt === 0 ? a.length - b.length : aAsInt - bAsInt;
            }
            else {
                return -1;
            }
        }
        else if (bAsInt !== null) {
            return 1;
        }
        else {
            return a < b ? -1 : 1;
        }
    }
};
/**
 * @returns {!number} comparison result.
 */
const stringCompare = function (a, b) {
    if (a === b) {
        return 0;
    }
    else if (a < b) {
        return -1;
    }
    else {
        return 1;
    }
};
const requireKey = function (key, obj) {
    if (obj && key in obj) {
        return obj[key];
    }
    else {
        throw new Error('Missing required key (' + key + ') in object: ' + util.stringify(obj));
    }
};
const ObjectToUniqueKey = function (obj) {
    if (typeof obj !== 'object' || obj === null) {
        return util.stringify(obj);
    }
    const keys = [];
    // eslint-disable-next-line guard-for-in
    for (const k in obj) {
        keys.push(k);
    }
    // Export as json, but with the keys sorted.
    keys.sort();
    let key = '{';
    for (let i = 0; i < keys.length; i++) {
        if (i !== 0) {
            key += ',';
        }
        key += util.stringify(keys[i]);
        key += ':';
        key += ObjectToUniqueKey(obj[keys[i]]);
    }
    key += '}';
    return key;
};
/**
 * Splits a string into a number of smaller segments of maximum size
 * @param str - The string
 * @param segsize - The maximum number of chars in the string.
 * @returns The string, split into appropriately-sized chunks
 */
const splitStringBySize = function (str, segsize) {
    const len = str.length;
    if (len <= segsize) {
        return [str];
    }
    const dataSegs = [];
    for (let c = 0; c < len; c += segsize) {
        if (c + segsize > len) {
            dataSegs.push(str.substring(c, len));
        }
        else {
            dataSegs.push(str.substring(c, c + segsize));
        }
    }
    return dataSegs;
};
/**
 * Apply a function to each (key, value) pair in an object or
 * apply a function to each (index, value) pair in an array
 * @param obj - The object or array to iterate over
 * @param fn - The function to apply
 */
function each(obj, fn) {
    for (const key in obj) {
        if (obj.hasOwnProperty(key)) {
            fn(key, obj[key]);
        }
    }
}
/**
 * Borrowed from http://hg.secondlife.com/llsd/src/tip/js/typedarray.js (MIT License)
 * I made one modification at the end and removed the NaN / Infinity
 * handling (since it seemed broken [caused an overflow] and we don't need it).  See MJL comments.
 * @param v - A double
 *
 */
const ieee754Buffer = new DataView(new ArrayBuffer(8));
const ieee754HexBytes = [];
for (let i = 0; i < 256; i++) {
    ieee754HexBytes.push((i < 16 ? '0' : '') + i.toString(16));
}
/**
 * Converts a double to the big-endian hex string of its IEEE 754 bits, as
 * used by the wire hash grammar. Implemented with DataView.setFloat64 — the
 * engine performs the exact IEEE 754 encoding, including -0 and denormals —
 * instead of the historical bit-by-bit Math.pow reconstruction, which was
 * ~34x slower and dominated compound hashing on number-heavy trees.
 */
const doubleToIEEE754String = function (v) {
    util.assert(!isInvalidJSONNumber(v), 'Invalid JSON number'); // MJL
    ieee754Buffer.setFloat64(0, v); // big-endian by default
    return (ieee754HexBytes[ieee754Buffer.getUint8(0)] +
        ieee754HexBytes[ieee754Buffer.getUint8(1)] +
        ieee754HexBytes[ieee754Buffer.getUint8(2)] +
        ieee754HexBytes[ieee754Buffer.getUint8(3)] +
        ieee754HexBytes[ieee754Buffer.getUint8(4)] +
        ieee754HexBytes[ieee754Buffer.getUint8(5)] +
        ieee754HexBytes[ieee754Buffer.getUint8(6)] +
        ieee754HexBytes[ieee754Buffer.getUint8(7)]);
};
/**
 * Used to detect if we're in a Chrome content script (which executes in an
 * isolated environment where long-polling doesn't work).
 */
const isChromeExtensionContentScript = function () {
    return !!(typeof window === 'object' &&
        window['chrome'] &&
        window['chrome']['extension'] &&
        !/^chrome/.test(window.location.href));
};
/**
 * Used to detect if we're in a Windows 8 Store app.
 */
const isWindowsStoreApp = function () {
    // Check for the presence of a couple WinRT globals
    return typeof Windows === 'object' && typeof Windows.UI === 'object';
};
/**
 * Converts a server error code to a JavaScript Error
 */
function errorForServerCode(code, query) {
    let reason = 'Unknown Error';
    if (code === 'too_big') {
        reason =
            'The data requested exceeds the maximum size ' +
                'that can be accessed with a single request.';
    }
    else if (code === 'permission_denied') {
        reason = "Client doesn't have permission to access the desired data.";
    }
    else if (code === 'unavailable') {
        reason = 'The service is unavailable';
    }
    const error = new Error(code + ' at ' + query._path.toString() + ': ' + reason);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    error.code = code.toUpperCase();
    return error;
}
/**
 * Used to test for integer-looking strings
 */
const INTEGER_REGEXP_ = new RegExp('^-?(0*)\\d{1,10}$');
/**
 * For use in keys, the minimum possible 32-bit integer.
 */
const INTEGER_32_MIN = -2147483648;
/**
 * For use in keys, the maximum possible 32-bit integer.
 */
const INTEGER_32_MAX = 2147483647;
/**
 * If the string contains a 32-bit integer, return it.  Else return null.
 */
const tryParseInt = function (str) {
    // Fast reject before the regex: nameCompare calls this for EVERY key pair
    // in every sorted-map operation, and real-world keys are overwhelmingly
    // named (non-numeric). A single charCode check skips the regex engine for
    // any key that cannot possibly be an integer.
    const first = str.charCodeAt(0);
    if ((first < 48 /* '0' */ || first > 57) /* '9' */ &&
        first !== 45 /* '-' */) {
        return null;
    }
    if (INTEGER_REGEXP_.test(str)) {
        const intVal = Number(str);
        if (intVal >= INTEGER_32_MIN && intVal <= INTEGER_32_MAX) {
            return intVal;
        }
    }
    return null;
};
/**
 * Helper to run some code but catch any exceptions and re-throw them later.
 * Useful for preventing user callbacks from breaking internal code.
 *
 * Re-throwing the exception from a setTimeout is a little evil, but it's very
 * convenient (we don't have to try to figure out when is a safe point to
 * re-throw it), and the behavior seems reasonable:
 *
 * * If you aren't pausing on exceptions, you get an error in the console with
 *   the correct stack trace.
 * * If you're pausing on all exceptions, the debugger will pause on your
 *   exception and then again when we rethrow it.
 * * If you're only pausing on uncaught exceptions, the debugger will only pause
 *   on us re-throwing it.
 *
 * @param fn - The code to guard.
 */
const exceptionGuard = function (fn) {
    try {
        fn();
    }
    catch (e) {
        // Re-throw exception when it's safe.
        setTimeout(() => {
            // It used to be that "throw e" would result in a good console error with
            // relevant context, but as of Chrome 39, you just get the firebase.js
            // file/line number where we re-throw it, which is useless. So we log
            // e.stack explicitly.
            const stack = e.stack || '';
            warn('Exception was thrown by user callback.', stack);
            throw e;
        }, Math.floor(0));
    }
};
/**
 * @returns {boolean} true if we think we're currently being crawled.
 */
const beingCrawled = function () {
    const userAgent = (typeof window === 'object' &&
        window['navigator'] &&
        window['navigator']['userAgent']) ||
        '';
    // For now we whitelist the most popular crawlers.  We should refine this to be the set of crawlers we
    // believe to support JavaScript/AJAX rendering.
    // NOTE: Google Webmaster Tools doesn't really belong, but their "This is how a visitor to your website
    // would have seen the page" is flaky if we don't treat it as a crawler.
    return (userAgent.search(/googlebot|google webmaster tools|bingbot|yahoo! slurp|baiduspider|yandexbot|duckduckbot/i) >= 0);
};
/**
 * Same as setTimeout() except on Node.JS it will /not/ prevent the process from exiting.
 *
 * It is removed with clearTimeout() as normal.
 *
 * @param fn - Function to run.
 * @param time - Milliseconds to wait before running.
 * @returns The setTimeout() return value.
 */
const setTimeoutNonBlocking = function (fn, time) {
    const timeout = setTimeout(fn, time);
    // Note: at the time of this comment, unrefTimer is under the unstable set of APIs. Run with --unstable to enable the API.
    if (typeof timeout === 'number' &&
        // @ts-ignore Is only defined in Deno environments.
        typeof Deno !== 'undefined' &&
        // @ts-ignore Deno and unrefTimer are only defined in Deno environments.
        Deno['unrefTimer']) {
        // @ts-ignore Deno and unrefTimer are only defined in Deno environments.
        Deno.unrefTimer(timeout);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
    }
    else if (typeof timeout === 'object' && timeout['unref']) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        timeout['unref']();
    }
    return timeout;
};

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * A class that holds metadata about a Repo object
 */
class RepoInfo {
    /**
     * @param host - Hostname portion of the url for the repo
     * @param secure - Whether or not this repo is accessed over ssl
     * @param namespace - The namespace represented by the repo
     * @param webSocketOnly - Whether to prefer websockets over all other transports (used by Nest).
     * @param nodeAdmin - Whether this instance uses Admin SDK credentials
     * @param persistenceKey - Override the default session persistence storage key
     */
    constructor(host, secure, namespace, webSocketOnly, nodeAdmin = false, persistenceKey = '', includeNamespaceInQueryParams = false, isUsingEmulator = false, emulatorOptions = null) {
        this.secure = secure;
        this.namespace = namespace;
        this.webSocketOnly = webSocketOnly;
        this.nodeAdmin = nodeAdmin;
        this.persistenceKey = persistenceKey;
        this.includeNamespaceInQueryParams = includeNamespaceInQueryParams;
        this.isUsingEmulator = isUsingEmulator;
        this.emulatorOptions = emulatorOptions;
        this._host = host.toLowerCase();
        this._domain = this._host.substr(this._host.indexOf('.') + 1);
        this.internalHost =
            PersistentStorage.get('host:' + host) || this._host;
    }
    isCacheableHost() {
        return this.internalHost.substr(0, 2) === 's-';
    }
    isCustomHost() {
        return (this._domain !== 'firebaseio.com' &&
            this._domain !== 'firebaseio-demo.com');
    }
    get host() {
        return this._host;
    }
    set host(newHost) {
        if (newHost !== this.internalHost) {
            this.internalHost = newHost;
            if (this.isCacheableHost()) {
                PersistentStorage.set('host:' + this._host, this.internalHost);
            }
        }
    }
    toString() {
        let str = this.toURLString();
        if (this.persistenceKey) {
            str += '<' + this.persistenceKey + '>';
        }
        return str;
    }
    toURLString() {
        const protocol = this.secure ? 'https://' : 'http://';
        const query = this.includeNamespaceInQueryParams
            ? `?ns=${this.namespace}`
            : '';
        return `${protocol}${this.host}/${query}`;
    }
}
function repoInfoNeedsQueryParam(repoInfo) {
    return (repoInfo.host !== repoInfo.internalHost ||
        repoInfo.isCustomHost() ||
        repoInfo.includeNamespaceInQueryParams);
}
/**
 * Returns the websocket URL for this repo
 * @param repoInfo - RepoInfo object
 * @param type - of connection
 * @param params - list
 * @returns The URL for this repo
 */
function repoInfoConnectionURL(repoInfo, type, params) {
    util.assert(typeof type === 'string', 'typeof type must == string');
    util.assert(typeof params === 'object', 'typeof params must == object');
    let connURL;
    if (type === WEBSOCKET) {
        connURL =
            (repoInfo.secure ? 'wss://' : 'ws://') + repoInfo.internalHost + '/.ws?';
    }
    else if (type === LONG_POLLING) {
        connURL =
            (repoInfo.secure ? 'https://' : 'http://') +
                repoInfo.internalHost +
                '/.lp?';
    }
    else {
        throw new Error('Unknown connection type: ' + type);
    }
    if (repoInfoNeedsQueryParam(repoInfo)) {
        params['ns'] = repoInfo.namespace;
    }
    const pairs = [];
    each(params, (key, value) => {
        pairs.push(key + '=' + value);
    });
    return connURL + pairs.join('&');
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Tracks a collection of stats.
 */
class StatsCollection {
    constructor() {
        this.counters_ = {};
    }
    incrementCounter(name, amount = 1) {
        if (!util.contains(this.counters_, name)) {
            this.counters_[name] = 0;
        }
        this.counters_[name] += amount;
    }
    get() {
        return util.deepCopy(this.counters_);
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const collections = {};
const reporters = {};
function statsManagerGetCollection(repoInfo) {
    const hashString = repoInfo.toString();
    if (!collections[hashString]) {
        collections[hashString] = new StatsCollection();
    }
    return collections[hashString];
}
function statsManagerGetOrCreateReporter(repoInfo, creatorFunction) {
    const hashString = repoInfo.toString();
    if (!reporters[hashString]) {
        reporters[hashString] = creatorFunction();
    }
    return reporters[hashString];
}

/**
 * @license
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/** The semver (www.semver.org) version of the SDK. */
let SDK_VERSION = '';
/**
 * SDK_VERSION should be set before any database instance is created
 * @internal
 */
function setSDKVersion(version) {
    SDK_VERSION = version;
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const WEBSOCKET_MAX_FRAME_SIZE = 16384;
const WEBSOCKET_KEEPALIVE_INTERVAL = 45000;
let WebSocketImpl = null;
if (typeof MozWebSocket !== 'undefined') {
    WebSocketImpl = MozWebSocket;
}
else if (typeof WebSocket !== 'undefined') {
    WebSocketImpl = WebSocket;
}
function setWebSocketImpl(impl) {
    WebSocketImpl = impl;
}
/**
 * Create a new websocket connection with the given callbacks.
 */
class WebSocketConnection {
    /**
     * @param connId identifier for this transport
     * @param repoInfo The info for the websocket endpoint.
     * @param applicationId The Firebase App ID for this project.
     * @param appCheckToken The App Check Token for this client.
     * @param authToken The Auth Token for this client.
     * @param transportSessionId Optional transportSessionId if this is connecting
     * to an existing transport session
     * @param lastSessionId Optional lastSessionId if there was a previous
     * connection
     */
    constructor(connId, repoInfo, applicationId, appCheckToken, authToken, transportSessionId, lastSessionId) {
        this.connId = connId;
        this.applicationId = applicationId;
        this.appCheckToken = appCheckToken;
        this.authToken = authToken;
        this.keepaliveTimer = null;
        this.frames = null;
        this.totalFrames = 0;
        this.bytesSent = 0;
        this.bytesReceived = 0;
        this.pendingMessageBytes_ = 0;
        this.log_ = logWrapper(this.connId);
        this.stats_ = statsManagerGetCollection(repoInfo);
        this.connURL = WebSocketConnection.connectionURL_(repoInfo, transportSessionId, lastSessionId, appCheckToken, applicationId);
        this.nodeAdmin = repoInfo.nodeAdmin;
    }
    /**
     * @param repoInfo - The info for the websocket endpoint.
     * @param transportSessionId - Optional transportSessionId if this is connecting to an existing transport
     *                                         session
     * @param lastSessionId - Optional lastSessionId if there was a previous connection
     * @returns connection url
     */
    static connectionURL_(repoInfo, transportSessionId, lastSessionId, appCheckToken, applicationId) {
        const urlParams = {};
        urlParams[VERSION_PARAM] = PROTOCOL_VERSION;
        if (!util.isNodeSdk() &&
            typeof location !== 'undefined' &&
            location.hostname &&
            FORGE_DOMAIN_RE.test(location.hostname)) {
            urlParams[REFERER_PARAM] = FORGE_REF;
        }
        if (transportSessionId) {
            urlParams[TRANSPORT_SESSION_PARAM] = transportSessionId;
        }
        if (lastSessionId) {
            urlParams[LAST_SESSION_PARAM] = lastSessionId;
        }
        if (appCheckToken) {
            urlParams[APP_CHECK_TOKEN_PARAM] = appCheckToken;
        }
        if (applicationId) {
            urlParams[APPLICATION_ID_PARAM] = applicationId;
        }
        return repoInfoConnectionURL(repoInfo, WEBSOCKET, urlParams);
    }
    /**
     * @param onMessage - Callback when messages arrive
     * @param onDisconnect - Callback with connection lost.
     */
    open(onMessage, onDisconnect) {
        this.onDisconnect = onDisconnect;
        this.onMessage = onMessage;
        this.log_('Websocket connecting to ' + this.connURL);
        this.everConnected_ = false;
        // Assume failure until proven otherwise.
        PersistentStorage.set('previous_websocket_failure', true);
        try {
            let options;
            if (util.isNodeSdk()) {
                const device = this.nodeAdmin ? 'AdminNode' : 'Node';
                // UA Format: Firebase/<wire_protocol>/<sdk_version>/<platform>/<device>
                options = {
                    headers: {
                        'User-Agent': `Firebase/${PROTOCOL_VERSION}/${SDK_VERSION}/${process.platform}/${device}`,
                        'X-Firebase-GMPID': this.applicationId || ''
                    }
                };
                // If using Node with admin creds, AppCheck-related checks are unnecessary.
                // Note that we send the credentials here even if they aren't admin credentials, which is
                // not a problem.
                // Note that this header is just used to bypass appcheck, and the token should still be sent
                // through the websocket connection once it is established.
                if (this.authToken) {
                    options.headers['Authorization'] = `Bearer ${this.authToken}`;
                }
                if (this.appCheckToken) {
                    options.headers['X-Firebase-AppCheck'] = this.appCheckToken;
                }
                // Plumb appropriate http_proxy environment variable into faye-websocket if it exists.
                const env = process['env'];
                const proxy = this.connURL.indexOf('wss://') === 0
                    ? env['HTTPS_PROXY'] || env['https_proxy']
                    : env['HTTP_PROXY'] || env['http_proxy'];
                if (proxy) {
                    options['proxy'] = { origin: proxy };
                }
            }
            this.mySock = new WebSocketImpl(this.connURL, [], options);
        }
        catch (e) {
            this.log_('Error instantiating WebSocket.');
            const error = e.message || e.data;
            if (error) {
                this.log_(error);
            }
            this.onClosed_();
            return;
        }
        this.mySock.onopen = () => {
            this.log_('Websocket connected.');
            this.everConnected_ = true;
        };
        this.mySock.onclose = () => {
            this.log_('Websocket connection was disconnected.');
            this.mySock = null;
            this.onClosed_();
        };
        this.mySock.onmessage = m => {
            this.handleIncomingFrame(m);
        };
        this.mySock.onerror = e => {
            this.log_('WebSocket error.  Closing connection.');
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const error = e.message || e.data;
            if (error) {
                this.log_(error);
            }
            this.onClosed_();
        };
    }
    /**
     * No-op for websockets, we don't need to do anything once the connection is confirmed as open
     */
    start() { }
    static forceDisallow() {
        WebSocketConnection.forceDisallow_ = true;
    }
    static isAvailable() {
        let isOldAndroid = false;
        if (typeof navigator !== 'undefined' && navigator.userAgent) {
            const oldAndroidRegex = /Android ([0-9]{0,}\.[0-9]{0,})/;
            const oldAndroidMatch = navigator.userAgent.match(oldAndroidRegex);
            if (oldAndroidMatch && oldAndroidMatch.length > 1) {
                if (parseFloat(oldAndroidMatch[1]) < 4.4) {
                    isOldAndroid = true;
                }
            }
        }
        return (!isOldAndroid &&
            WebSocketImpl !== null &&
            !WebSocketConnection.forceDisallow_);
    }
    /**
     * Returns true if we previously failed to connect with this transport.
     */
    static previouslyFailed() {
        // If our persistent storage is actually only in-memory storage,
        // we default to assuming that it previously failed to be safe.
        return (PersistentStorage.isInMemoryStorage ||
            PersistentStorage.get('previous_websocket_failure') === true);
    }
    markConnectionHealthy() {
        PersistentStorage.remove('previous_websocket_failure');
    }
    appendFrame_(data) {
        this.frames.push(data);
        if (this.frames.length === this.totalFrames) {
            const fullMess = this.frames.join('');
            this.frames = null;
            const jsonMess = util.jsonEval(fullMess);
            // Deliver the parsed message with its original frame bytes. Keeping the
            // byte count beside the message avoids re-stringifying large payloads
            // solely for diagnostics.
            const bytes = this.pendingMessageBytes_;
            this.pendingMessageBytes_ = 0;
            this.onMessage(jsonMess, bytes);
        }
    }
    /**
     * @param frameCount - The number of frames we are expecting from the server
     */
    handleNewFrameCount_(frameCount) {
        this.totalFrames = frameCount;
        this.frames = [];
    }
    /**
     * Attempts to parse a frame count out of some text. If it can't, assumes a value of 1
     * @returns Any remaining data to be process, or null if there is none
     */
    extractFrameCount_(data) {
        util.assert(this.frames === null, 'We already have a frame buffer');
        // TODO: The server is only supposed to send up to 9999 frames (i.e. length <= 4), but that isn't being enforced
        // currently.  So allowing larger frame counts (length <= 6).  See https://app.asana.com/0/search/8688598998380/8237608042508
        if (data.length <= 6) {
            const frameCount = Number(data);
            if (!isNaN(frameCount)) {
                this.handleNewFrameCount_(frameCount);
                return null;
            }
        }
        this.handleNewFrameCount_(1);
        return data;
    }
    /**
     * Process a websocket frame that has arrived from the server.
     * @param mess - The frame data
     */
    handleIncomingFrame(mess) {
        if (this.mySock === null) {
            return; // Chrome apparently delivers incoming packets even after we .close() the connection sometimes.
        }
        const data = mess['data'];
        const wireBytes = util.stringLength(data);
        this.pendingMessageBytes_ += wireBytes;
        this.bytesReceived += wireBytes;
        this.stats_.incrementCounter('bytes_received', wireBytes);
        this.resetKeepAlive();
        if (this.frames !== null) {
            // we're buffering
            this.appendFrame_(data);
        }
        else {
            // try to parse out a frame count, otherwise, assume 1 and process it
            const remainingData = this.extractFrameCount_(data);
            if (remainingData !== null) {
                this.appendFrame_(remainingData);
            }
        }
    }
    /**
     * Send a message to the server
     * @param data - The JSON object to transmit
     */
    send(data) {
        this.resetKeepAlive();
        const dataStr = util.stringify(data);
        const wireBytes = util.stringLength(dataStr);
        this.bytesSent += wireBytes;
        this.stats_.incrementCounter('bytes_sent', wireBytes);
        //We can only fit a certain amount in each websocket frame, so we need to split this request
        //up into multiple pieces if it doesn't fit in one request.
        const dataSegs = splitStringBySize(dataStr, WEBSOCKET_MAX_FRAME_SIZE);
        //Send the length header
        if (dataSegs.length > 1) {
            this.sendString_(String(dataSegs.length));
        }
        //Send the actual data in segments.
        for (let i = 0; i < dataSegs.length; i++) {
            this.sendString_(dataSegs[i]);
        }
    }
    shutdown_() {
        this.isClosed_ = true;
        if (this.keepaliveTimer) {
            clearInterval(this.keepaliveTimer);
            this.keepaliveTimer = null;
        }
        if (this.mySock) {
            this.mySock.close();
            this.mySock = null;
        }
    }
    onClosed_() {
        if (!this.isClosed_) {
            this.log_('WebSocket is closing itself');
            this.shutdown_();
            // since this is an internal close, trigger the close listener
            if (this.onDisconnect) {
                this.onDisconnect(this.everConnected_);
                this.onDisconnect = null;
            }
        }
    }
    /**
     * External-facing close handler.
     * Close the websocket and kill the connection.
     */
    close() {
        if (!this.isClosed_) {
            this.log_('WebSocket is being closed');
            this.shutdown_();
        }
    }
    /**
     * Kill the current keepalive timer and start a new one, to ensure that it always fires N seconds after
     * the last activity.
     */
    resetKeepAlive() {
        clearInterval(this.keepaliveTimer);
        this.keepaliveTimer = setInterval(() => {
            //If there has been no websocket activity for a while, send a no-op
            if (this.mySock) {
                this.sendString_('0');
            }
            this.resetKeepAlive();
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
        }, Math.floor(WEBSOCKET_KEEPALIVE_INTERVAL));
    }
    /**
     * Send a string over the websocket.
     *
     * @param str - String to send.
     */
    sendString_(str) {
        // Firefox seems to sometimes throw exceptions (NS_ERROR_UNEXPECTED) from websocket .send()
        // calls for some unknown reason.  We treat these as an error and disconnect.
        // See https://app.asana.com/0/58926111402292/68021340250410
        try {
            this.mySock.send(str);
        }
        catch (e) {
            this.log_('Exception thrown from WebSocket.send():', e.message || e.data, 'Closing connection.');
            setTimeout(this.onClosed_.bind(this), 0);
        }
    }
}
/**
 * Number of response before we consider the connection "healthy."
 */
WebSocketConnection.responsesRequiredToBeHealthy = 2;
/**
 * Time to wait for the connection te become healthy before giving up.
 */
WebSocketConnection.healthyTimeout = 30000;

/**
 * @license
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Abstraction around AppCheck's token fetching capabilities.
 */
class AppCheckTokenProvider {
    constructor(app$1, appCheckProvider) {
        this.appCheckProvider = appCheckProvider;
        this.appName = app$1.name;
        if (app._isFirebaseServerApp(app$1) && app$1.settings.appCheckToken) {
            this.serverAppAppCheckToken = app$1.settings.appCheckToken;
        }
        this.appCheck = appCheckProvider?.getImmediate({ optional: true });
        if (!this.appCheck) {
            appCheckProvider?.get().then(appCheck => (this.appCheck = appCheck));
        }
    }
    getToken(forceRefresh) {
        if (this.serverAppAppCheckToken) {
            if (forceRefresh) {
                throw new Error('Attempted reuse of `FirebaseServerApp.appCheckToken` after previous usage failed.');
            }
            return Promise.resolve({ token: this.serverAppAppCheckToken });
        }
        if (!this.appCheck) {
            return new Promise((resolve, reject) => {
                // Support delayed initialization of FirebaseAppCheck. This allows our
                // customers to initialize the RTDB SDK before initializing Firebase
                // AppCheck and ensures that all requests are authenticated if a token
                // becomes available before the timeout below expires.
                setTimeout(() => {
                    if (this.appCheck) {
                        this.getToken(forceRefresh).then(resolve, reject);
                    }
                    else {
                        resolve(null);
                    }
                }, 0);
            });
        }
        return this.appCheck.getToken(forceRefresh);
    }
    addTokenChangeListener(listener) {
        this.appCheckProvider
            ?.get()
            .then(appCheck => appCheck.addTokenListener(listener));
    }
    notifyForInvalidToken() {
        warn(`Provided AppCheck credentials for the app named "${this.appName}" ` +
            'are invalid. This usually indicates your app was not initialized correctly.');
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Abstraction around FirebaseApp's token fetching capabilities.
 */
class FirebaseAuthTokenProvider {
    constructor(appName_, firebaseOptions_, authProvider_) {
        this.appName_ = appName_;
        this.firebaseOptions_ = firebaseOptions_;
        this.authProvider_ = authProvider_;
        this.auth_ = null;
        this.auth_ = authProvider_.getImmediate({ optional: true });
        if (!this.auth_) {
            authProvider_.onInit(auth => (this.auth_ = auth));
        }
    }
    getToken(forceRefresh) {
        if (!this.auth_) {
            return new Promise((resolve, reject) => {
                // Support delayed initialization of FirebaseAuth. This allows our
                // customers to initialize the RTDB SDK before initializing Firebase
                // Auth and ensures that all requests are authenticated if a token
                // becomes available before the timeout below expires.
                setTimeout(() => {
                    if (this.auth_) {
                        this.getToken(forceRefresh).then(resolve, reject);
                    }
                    else {
                        resolve(null);
                    }
                }, 0);
            });
        }
        return this.auth_.getToken(forceRefresh).catch(error => {
            // TODO: Need to figure out all the cases this is raised and whether
            // this makes sense.
            if (error && error.code === 'auth/token-not-initialized') {
                log('Got auth/token-not-initialized error.  Treating as null token.');
                return null;
            }
            else {
                return Promise.reject(error);
            }
        });
    }
    addTokenChangeListener(listener) {
        // TODO: We might want to wrap the listener and call it with no args to
        // avoid a leaky abstraction, but that makes removing the listener harder.
        if (this.auth_) {
            this.auth_.addAuthTokenListener(listener);
        }
        else {
            this.authProvider_
                .get()
                .then(auth => auth.addAuthTokenListener(listener));
        }
    }
    removeTokenChangeListener(listener) {
        this.authProvider_
            .get()
            .then(auth => auth.removeAuthTokenListener(listener));
    }
    notifyForInvalidToken() {
        let errorMessage = 'Provided authentication credentials for the app named "' +
            this.appName_ +
            '" are invalid. This usually indicates your app was not ' +
            'initialized correctly. ';
        if ('credential' in this.firebaseOptions_) {
            errorMessage +=
                'Make sure the "credential" property provided to initializeApp() ' +
                    'is authorized to access the specified "databaseURL" and is from the correct ' +
                    'project.';
        }
        else if ('serviceAccount' in this.firebaseOptions_) {
            errorMessage +=
                'Make sure the "serviceAccount" property provided to initializeApp() ' +
                    'is authorized to access the specified "databaseURL" and is from the correct ' +
                    'project.';
        }
        else {
            errorMessage +=
                'Make sure the "apiKey" and "databaseURL" properties provided to ' +
                    'initializeApp() match the values provided for your app at ' +
                    'https://console.firebase.google.com/.';
        }
        warn(errorMessage);
    }
}
/* AuthTokenProvider that supplies a constant token. Used by Admin SDK or mockUserToken with emulators. */
class EmulatorTokenProvider {
    constructor(accessToken) {
        this.accessToken = accessToken;
    }
    getToken(forceRefresh) {
        return Promise.resolve({
            accessToken: this.accessToken
        });
    }
    addTokenChangeListener(listener) {
        // Invoke the listener immediately to match the behavior in Firebase Auth
        // (see packages/auth/src/auth.js#L1807)
        listener(this.accessToken);
    }
    removeTokenChangeListener(listener) { }
    notifyForInvalidToken() { }
}
/** A string that is treated as an admin access token by the RTDB emulator. Used by Admin SDK. */
EmulatorTokenProvider.OWNER = 'owner';

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class NamedNode {
    constructor(name, node) {
        this.name = name;
        this.node = node;
    }
    static Wrap(name, node) {
        return new NamedNode(name, node);
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class Index {
    /**
     * @returns A standalone comparison function for
     * this index
     */
    getCompare() {
        return this.compare.bind(this);
    }
    /**
     * Given a before and after value for a node, determine if the indexed value has changed. Even if they are different,
     * it's possible that the changes are isolated to parts of the snapshot that are not indexed.
     *
     *
     * @returns True if the portion of the snapshot being indexed changed between oldNode and newNode
     */
    indexedValueChanged(oldNode, newNode) {
        const oldWrapped = new NamedNode(MIN_NAME, oldNode);
        const newWrapped = new NamedNode(MIN_NAME, newNode);
        return this.compare(oldWrapped, newWrapped) !== 0;
    }
    /**
     * @returns a node wrapper that will sort equal to or less than
     * any other node wrapper, using this index
     */
    minPost() {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return NamedNode.MIN;
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
let __EMPTY_NODE;
class KeyIndex extends Index {
    static get __EMPTY_NODE() {
        return __EMPTY_NODE;
    }
    static set __EMPTY_NODE(val) {
        __EMPTY_NODE = val;
    }
    compare(a, b) {
        return nameCompare(a.name, b.name);
    }
    isDefinedOn(node) {
        // We could probably return true here (since every node has a key), but it's never called
        // so just leaving unimplemented for now.
        throw util.assertionError('KeyIndex.isDefinedOn not expected to be called.');
    }
    indexedValueChanged(oldNode, newNode) {
        return false; // The key for a node never changes.
    }
    minPost() {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return NamedNode.MIN;
    }
    maxPost() {
        // TODO: This should really be created once and cached in a static property, but
        // NamedNode isn't defined yet, so I can't use it in a static.  Bleh.
        return new NamedNode(MAX_NAME, __EMPTY_NODE);
    }
    makePost(indexValue, name) {
        util.assert(typeof indexValue === 'string', 'KeyIndex indexValue must always be a string.');
        // We just use empty node, but it'll never be compared, since our comparator only looks at name.
        return new NamedNode(indexValue, __EMPTY_NODE);
    }
    /**
     * @returns String representation for inclusion in a query spec
     */
    toString() {
        return '.key';
    }
}
const KEY_INDEX = new KeyIndex();

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/** Maximum key depth. */
const MAX_PATH_DEPTH = 32;
/** Maximum number of (UTF8) bytes in a Firebase path. */
const MAX_PATH_LENGTH_BYTES = 768;
/**
 * An immutable object representing a parsed path.  It's immutable so that you
 * can pass them around to other functions without worrying about them changing
 * it.
 */
class Path {
    /**
     * @param pathOrString - Path string to parse, or another path, or the raw
     * tokens array
     */
    constructor(pathOrString, pieceNum) {
        if (pieceNum === void 0) {
            this.pieces_ = pathOrString.split('/');
            // Remove empty pieces.
            let copyTo = 0;
            for (let i = 0; i < this.pieces_.length; i++) {
                if (this.pieces_[i].length > 0) {
                    this.pieces_[copyTo] = this.pieces_[i];
                    copyTo++;
                }
            }
            this.pieces_.length = copyTo;
            this.pieceNum_ = 0;
        }
        else {
            this.pieces_ = pathOrString;
            this.pieceNum_ = pieceNum;
        }
    }
    toString() {
        let pathString = '';
        for (let i = this.pieceNum_; i < this.pieces_.length; i++) {
            if (this.pieces_[i] !== '') {
                pathString += '/' + this.pieces_[i];
            }
        }
        return pathString || '/';
    }
}
function newEmptyPath() {
    return new Path('');
}
function pathGetFront(path) {
    if (path.pieceNum_ >= path.pieces_.length) {
        return null;
    }
    return path.pieces_[path.pieceNum_];
}
/**
 * @returns The number of segments in this path
 */
function pathGetLength(path) {
    return path.pieces_.length - path.pieceNum_;
}
function pathPopFront(path) {
    let pieceNum = path.pieceNum_;
    if (pieceNum < path.pieces_.length) {
        pieceNum++;
    }
    return new Path(path.pieces_, pieceNum);
}
function pathGetBack(path) {
    if (path.pieceNum_ < path.pieces_.length) {
        return path.pieces_[path.pieces_.length - 1];
    }
    return null;
}
function pathToUrlEncodedString(path) {
    let pathString = '';
    for (let i = path.pieceNum_; i < path.pieces_.length; i++) {
        if (path.pieces_[i] !== '') {
            pathString += '/' + encodeURIComponent(String(path.pieces_[i]));
        }
    }
    return pathString || '/';
}
/**
 * Shallow copy of the parts of the path.
 *
 */
function pathSlice(path, begin = 0) {
    return path.pieces_.slice(path.pieceNum_ + begin);
}
function pathParent(path) {
    if (path.pieceNum_ >= path.pieces_.length) {
        return null;
    }
    const pieces = [];
    for (let i = path.pieceNum_; i < path.pieces_.length - 1; i++) {
        pieces.push(path.pieces_[i]);
    }
    return new Path(pieces, 0);
}
function pathChild(path, childPathObj) {
    const pieces = [];
    for (let i = path.pieceNum_; i < path.pieces_.length; i++) {
        pieces.push(path.pieces_[i]);
    }
    if (childPathObj instanceof Path) {
        for (let i = childPathObj.pieceNum_; i < childPathObj.pieces_.length; i++) {
            pieces.push(childPathObj.pieces_[i]);
        }
    }
    else {
        const childPieces = childPathObj.split('/');
        for (let i = 0; i < childPieces.length; i++) {
            if (childPieces[i].length > 0) {
                pieces.push(childPieces[i]);
            }
        }
    }
    return new Path(pieces, 0);
}
/**
 * @returns True if there are no segments in this path
 */
function pathIsEmpty(path) {
    return path.pieceNum_ >= path.pieces_.length;
}
/**
 * @returns The path from outerPath to innerPath
 */
function newRelativePath(outerPath, innerPath) {
    const outer = pathGetFront(outerPath), inner = pathGetFront(innerPath);
    if (outer === null) {
        return innerPath;
    }
    else if (outer === inner) {
        return newRelativePath(pathPopFront(outerPath), pathPopFront(innerPath));
    }
    else {
        throw new Error('INTERNAL ERROR: innerPath (' +
            innerPath +
            ') is not within ' +
            'outerPath (' +
            outerPath +
            ')');
    }
}
/**
 * @returns -1, 0, 1 if left is less, equal, or greater than the right.
 */
function pathCompare(left, right) {
    const leftKeys = pathSlice(left, 0);
    const rightKeys = pathSlice(right, 0);
    for (let i = 0; i < leftKeys.length && i < rightKeys.length; i++) {
        const cmp = nameCompare(leftKeys[i], rightKeys[i]);
        if (cmp !== 0) {
            return cmp;
        }
    }
    if (leftKeys.length === rightKeys.length) {
        return 0;
    }
    return leftKeys.length < rightKeys.length ? -1 : 1;
}
/**
 * @returns true if paths are the same.
 */
function pathEquals(path, other) {
    if (pathGetLength(path) !== pathGetLength(other)) {
        return false;
    }
    for (let i = path.pieceNum_, j = other.pieceNum_; i <= path.pieces_.length; i++, j++) {
        if (path.pieces_[i] !== other.pieces_[j]) {
            return false;
        }
    }
    return true;
}
/**
 * @returns True if this path is a parent of (or the same as) other
 */
function pathContains(path, other) {
    let i = path.pieceNum_;
    let j = other.pieceNum_;
    if (pathGetLength(path) > pathGetLength(other)) {
        return false;
    }
    while (i < path.pieces_.length) {
        if (path.pieces_[i] !== other.pieces_[j]) {
            return false;
        }
        ++i;
        ++j;
    }
    return true;
}
/**
 * Dynamic (mutable) path used to count path lengths.
 *
 * This class is used to efficiently check paths for valid
 * length (in UTF8 bytes) and depth (used in path validation).
 *
 * Throws Error exception if path is ever invalid.
 *
 * The definition of a path always begins with '/'.
 */
class ValidationPath {
    /**
     * @param path - Initial Path.
     * @param errorPrefix_ - Prefix for any error messages.
     */
    constructor(path, errorPrefix_) {
        this.errorPrefix_ = errorPrefix_;
        this.parts_ = pathSlice(path, 0);
        /** Initialize to number of '/' chars needed in path. */
        this.byteLength_ = Math.max(1, this.parts_.length);
        for (let i = 0; i < this.parts_.length; i++) {
            this.byteLength_ += util.stringLength(this.parts_[i]);
        }
        validationPathCheckValid(this);
    }
}
function validationPathPush(validationPath, child) {
    // Count the needed '/'
    if (validationPath.parts_.length > 0) {
        validationPath.byteLength_ += 1;
    }
    validationPath.parts_.push(child);
    validationPath.byteLength_ += util.stringLength(child);
    validationPathCheckValid(validationPath);
}
function validationPathPop(validationPath) {
    const last = validationPath.parts_.pop();
    validationPath.byteLength_ -= util.stringLength(last);
    // Un-count the previous '/'
    if (validationPath.parts_.length > 0) {
        validationPath.byteLength_ -= 1;
    }
}
function validationPathCheckValid(validationPath) {
    if (validationPath.byteLength_ > MAX_PATH_LENGTH_BYTES) {
        throw new Error(validationPath.errorPrefix_ +
            'has a key path longer than ' +
            MAX_PATH_LENGTH_BYTES +
            ' bytes (' +
            validationPath.byteLength_ +
            ').');
    }
    if (validationPath.parts_.length > MAX_PATH_DEPTH) {
        throw new Error(validationPath.errorPrefix_ +
            'path specified exceeds the maximum depth that can be written (' +
            MAX_PATH_DEPTH +
            ') or object contains a cycle ' +
            validationPathToErrorString(validationPath));
    }
}
/**
 * String for use in error messages - uses '.' notation for path.
 */
function validationPathToErrorString(validationPath) {
    if (validationPath.parts_.length === 0) {
        return '';
    }
    return "in property '" + validationPath.parts_.join('.') + "'";
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
let MAX_NODE$2;
function setMaxNode$1(val) {
    MAX_NODE$2 = val;
}
/**
 * The hash text of a leaf value: `<typeof>:<serialized value>`. Numbers
 * serialize as IEEE-754 hex; everything else via String(). This is the one
 * definition of the leaf grammar shared by Node.hash() (v2 = false) and the
 * compound-hash range serialization (v2 = true, where strings are
 * JSON-quoted so ranges are unambiguous to reparse — Android calls this the
 * "V2" hash representation).
 */
function leafHashValueText(value, v2) {
    const type = typeof value;
    let text = type + ':';
    if (type === 'number') {
        text += doubleToIEEE754String(value);
    }
    else if (v2 && type === 'string') {
        text += hashQuotedString(value);
    }
    else {
        text += String(value);
    }
    return text;
}
/**
 * JSON-style quoting with only backslash and double quote escaped (the V2
 * hash grammar's string form).
 */
function hashQuotedString(value) {
    let escaped = value;
    if (escaped.indexOf('\\') !== -1) {
        escaped = escaped.replace(/\\/g, '\\\\');
    }
    if (escaped.indexOf('"') !== -1) {
        escaped = escaped.replace(/"/g, '\\"');
    }
    return '"' + escaped + '"';
}
const priorityHashText = function (priority) {
    return leafHashValueText(priority, /* v2= */ false);
};
/**
 * Validates that a priority snapshot Node is valid.
 */
const validatePriorityNode = function (priorityNode) {
    if (priorityNode.isLeafNode()) {
        const val = priorityNode.val();
        util.assert(typeof val === 'string' ||
            typeof val === 'number' ||
            (typeof val === 'object' && util.contains(val, '.sv')), 'Priority must be a string or number.');
    }
    else {
        util.assert(priorityNode === MAX_NODE$2 || priorityNode.isEmpty(), 'priority of unexpected type.');
    }
    // Don't call getPriority() on MAX_NODE to avoid hitting assertion.
    util.assert(priorityNode === MAX_NODE$2 || priorityNode.getPriority().isEmpty(), "Priority nodes can't have a priority of their own.");
};

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
let __childrenNodeConstructor;
/**
 * LeafNode is a class for storing leaf nodes in a DataSnapshot.  It
 * implements Node and stores the value of the node (a string,
 * number, or boolean) accessible via getValue().
 */
class LeafNode {
    static set __childrenNodeConstructor(val) {
        __childrenNodeConstructor = val;
    }
    static get __childrenNodeConstructor() {
        return __childrenNodeConstructor;
    }
    /**
     * @param value_ - The value to store in this leaf node. The object type is
     * possible in the event of a deferred value
     * @param priorityNode_ - The priority of this node.
     */
    constructor(value_, priorityNode_ = LeafNode.__childrenNodeConstructor.EMPTY_NODE) {
        this.value_ = value_;
        this.priorityNode_ = priorityNode_;
        this.lazyHash_ = null;
        util.assert(this.value_ !== undefined && this.value_ !== null, "LeafNode shouldn't be created with null/undefined value.");
        validatePriorityNode(this.priorityNode_);
    }
    /** @inheritDoc */
    isLeafNode() {
        return true;
    }
    /** @inheritDoc */
    getPriority() {
        return this.priorityNode_;
    }
    /** @inheritDoc */
    updatePriority(newPriorityNode) {
        return new LeafNode(this.value_, newPriorityNode);
    }
    /** @inheritDoc */
    getImmediateChild(childName) {
        // Hack to treat priority as a regular child
        if (childName === '.priority') {
            return this.priorityNode_;
        }
        else {
            return LeafNode.__childrenNodeConstructor.EMPTY_NODE;
        }
    }
    /** @inheritDoc */
    getChild(path) {
        if (pathIsEmpty(path)) {
            return this;
        }
        else if (pathGetFront(path) === '.priority') {
            return this.priorityNode_;
        }
        else {
            return LeafNode.__childrenNodeConstructor.EMPTY_NODE;
        }
    }
    hasChild() {
        return false;
    }
    /** @inheritDoc */
    getPredecessorChildName(childName, childNode) {
        return null;
    }
    /** @inheritDoc */
    updateImmediateChild(childName, newChildNode) {
        if (childName === '.priority') {
            return this.updatePriority(newChildNode);
        }
        else if (newChildNode.isEmpty() && childName !== '.priority') {
            return this;
        }
        else {
            return LeafNode.__childrenNodeConstructor.EMPTY_NODE.updateImmediateChild(childName, newChildNode).updatePriority(this.priorityNode_);
        }
    }
    /** @inheritDoc */
    updateChild(path, newChildNode) {
        const front = pathGetFront(path);
        if (front === null) {
            return newChildNode;
        }
        else if (newChildNode.isEmpty() && front !== '.priority') {
            return this;
        }
        else {
            util.assert(front !== '.priority' || pathGetLength(path) === 1, '.priority must be the last token in a path');
            return this.updateImmediateChild(front, LeafNode.__childrenNodeConstructor.EMPTY_NODE.updateChild(pathPopFront(path), newChildNode));
        }
    }
    /** @inheritDoc */
    isEmpty() {
        return false;
    }
    /** @inheritDoc */
    numChildren() {
        return 0;
    }
    /** @inheritDoc */
    forEachChild(index, action) {
        return false;
    }
    val(exportFormat) {
        if (exportFormat && !this.getPriority().isEmpty()) {
            return {
                '.value': this.getValue(),
                '.priority': this.getPriority().val()
            };
        }
        else {
            return this.getValue();
        }
    }
    /** @inheritDoc */
    hash() {
        if (this.lazyHash_ === null) {
            let toHash = '';
            if (!this.priorityNode_.isEmpty()) {
                toHash +=
                    'priority:' +
                        priorityHashText(this.priorityNode_.val()) +
                        ':';
            }
            toHash += leafHashValueText(this.value_, 
            /* v2= */ false);
            this.lazyHash_ = sha1(toHash);
        }
        return this.lazyHash_;
    }
    /** @inheritDoc */
    stampLazyHash(hash) {
        if (this.lazyHash_ === null) {
            this.lazyHash_ = hash;
        }
    }
    /**
     * Returns the value of the leaf node.
     * @returns The value of the node.
     */
    getValue() {
        return this.value_;
    }
    compareTo(other) {
        if (other === LeafNode.__childrenNodeConstructor.EMPTY_NODE) {
            return 1;
        }
        else if (other instanceof LeafNode.__childrenNodeConstructor) {
            return -1;
        }
        else {
            util.assert(other.isLeafNode(), 'Unknown node type');
            return this.compareToLeafNode_(other);
        }
    }
    /**
     * Comparison specifically for two leaf nodes
     */
    compareToLeafNode_(otherLeaf) {
        const otherLeafType = typeof otherLeaf.value_;
        const thisLeafType = typeof this.value_;
        const otherIndex = LeafNode.VALUE_TYPE_ORDER.indexOf(otherLeafType);
        const thisIndex = LeafNode.VALUE_TYPE_ORDER.indexOf(thisLeafType);
        util.assert(otherIndex >= 0, 'Unknown leaf type: ' + otherLeafType);
        util.assert(thisIndex >= 0, 'Unknown leaf type: ' + thisLeafType);
        if (otherIndex === thisIndex) {
            // Same type, compare values
            if (thisLeafType === 'object') {
                // Deferred value nodes are all equal, but we should also never get to this point...
                return 0;
            }
            else {
                // Note that this works because true > false, all others are number or string comparisons
                if (this.value_ < otherLeaf.value_) {
                    return -1;
                }
                else if (this.value_ === otherLeaf.value_) {
                    return 0;
                }
                else {
                    return 1;
                }
            }
        }
        else {
            return thisIndex - otherIndex;
        }
    }
    withIndex() {
        return this;
    }
    isIndexed() {
        return true;
    }
    equals(other) {
        if (other === this) {
            return true;
        }
        else if (other.isLeafNode()) {
            const otherLeaf = other;
            return (this.value_ === otherLeaf.value_ &&
                this.priorityNode_.equals(otherLeaf.priorityNode_));
        }
        else {
            return false;
        }
    }
}
/**
 * The sort order for comparing leaf nodes of different types. If two leaf nodes have
 * the same type, the comparison falls back to their value
 */
LeafNode.VALUE_TYPE_ORDER = ['object', 'boolean', 'number', 'string'];

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
let nodeFromJSON$1;
let MAX_NODE$1;
function setNodeFromJSON(val) {
    nodeFromJSON$1 = val;
}
function setMaxNode(val) {
    MAX_NODE$1 = val;
}
class PriorityIndex extends Index {
    compare(a, b) {
        const aPriority = a.node.getPriority();
        const bPriority = b.node.getPriority();
        const indexCmp = aPriority.compareTo(bPriority);
        if (indexCmp === 0) {
            return nameCompare(a.name, b.name);
        }
        else {
            return indexCmp;
        }
    }
    isDefinedOn(node) {
        return !node.getPriority().isEmpty();
    }
    indexedValueChanged(oldNode, newNode) {
        return !oldNode.getPriority().equals(newNode.getPriority());
    }
    minPost() {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return NamedNode.MIN;
    }
    maxPost() {
        return new NamedNode(MAX_NAME, new LeafNode('[PRIORITY-POST]', MAX_NODE$1));
    }
    makePost(indexValue, name) {
        const priorityNode = nodeFromJSON$1(indexValue);
        return new NamedNode(name, new LeafNode('[PRIORITY-POST]', priorityNode));
    }
    /**
     * @returns String representation for inclusion in a query spec
     */
    toString() {
        return '.priority';
    }
}
const PRIORITY_INDEX = new PriorityIndex();

/**
 * @license
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * A constant-size split strategy used by persistence. Unlike the protocol's
 * historical sqrt(tree-size) default, a fixed target gives IndexedDB records
 * a predictable upper bound across roots and generations. Boundaries remain
 * stable between writes; the target is consulted only when a dirty run is
 * re-emitted.
 */
function fixedSizeSplitStrategy(targetBytes) {
    const splitThreshold = Math.max(512, Math.floor(targetBytes));
    return state => state.hashLength() > splitThreshold &&
        state.currentPath()[state.currentPath().length - 1] !== '.priority';
}
/**
 * Iterates children in key order with the node's priority interleaved as a
 * `.priority` pseudo-child, matching the serialization the server hashes
 * (Android ChildrenNode.forEachChild(visitor, includePriority = true)): the
 * priority is emitted immediately before the first child key that sorts
 * after '.priority'. A priority that sorts after every child is dropped, as
 * it is on Android and iOS — both ends of the protocol must agree.
 */
function forEachChildWithPriority(node, action, includeTrailingPriority = false) {
    if (node.getPriority().isEmpty()) {
        node.forEachChild(KEY_INDEX, (key, child) => action(key, child, true));
        return;
    }
    let passedPriority = false;
    node.forEachChild(KEY_INDEX, (key, child) => {
        if (!passedPriority && nameCompare(key, '.priority') > 0) {
            passedPriority = true;
            action('.priority', node.getPriority(), true);
        }
        action(key, child, true);
    });
    if (!passedPriority && includeTrailingPriority) {
        // Android's compound grammar omits a priority that sorts after every
        // child, but export-format persistence must still serialize it.
        action('.priority', node.getPriority(), false);
    }
}
class CompoundHashBuilder {
    constructor(splitStrategy_, lengthOnly_ = false) {
        this.splitStrategy_ = splitStrategy_;
        this.lengthOnly_ = lengthOnly_;
        this.posts = [];
        this.hashes = [];
        /** Serialized text length of each completed range (same order as posts). */
        this.sizes = [];
        /**
         * When set, completed range texts are handed to the sink instead of being
         * hashed synchronously; `hashes` receives a placeholder the caller fills in
         * (the sink receives the index to fill). Lets the persistence flush hash
         * ranges with WebCrypto off the main thread's synchronous path.
         */
        this.hashSink = null;
        /**
         * Optional persistence sink for the export-format fragment represented by
         * each completed hash range. The fragment contains exactly the leaves in
         * that range's (exclusiveStart, inclusiveEnd] interval. Persistence unions
         * these disjoint fragments without range-deletion semantics.
         */
        this.payloadSink = null;
        /** null when not currently inside a range. */
        this.currentHash_ = null;
        this.currentHashLength_ = 0;
        /** Fresh, mutable accumulator for the current persisted range only. */
        this.currentPayload_ = undefined;
        /**
         * Key stack of the node being processed. Kept beyond currentDepth_ so the
         * path of the last processed leaf survives popping back out of its parent.
         */
        this.currentPath_ = [];
        this.currentDepth_ = 0;
        this.lastLeafDepth_ = -1;
        this.needsComma_ = true;
        this.splitState_ = {
            hashLength: () => this.currentHash_ === null ? 0 : this.currentHashLength_,
            currentPath: () => this.currentPath_.slice(0, this.currentDepth_)
        };
    }
    processLeaf(node) {
        this.ensureRange_();
        this.lastLeafDepth_ = this.currentDepth_;
        const leafText = leafHashRepresentation(node);
        if (!this.lengthOnly_) {
            this.currentHash_ += leafText;
        }
        this.currentHashLength_ += leafText.length;
        this.appendPayloadLeaf_(node);
        this.needsComma_ = true;
        if (this.splitStrategy_(this.splitState_)) {
            this.endRange_();
        }
    }
    startChild(key) {
        this.ensureRange_();
        if (this.needsComma_) {
            if (!this.lengthOnly_) {
                this.currentHash_ += ',';
            }
            this.currentHashLength_++;
        }
        const opening = hashQuotedString(key) + ':(';
        if (!this.lengthOnly_) {
            this.currentHash_ += opening;
        }
        this.currentHashLength_ += opening.length;
        if (this.currentDepth_ === this.currentPath_.length) {
            this.currentPath_.push(key);
        }
        else {
            this.currentPath_[this.currentDepth_] = key;
        }
        this.currentDepth_++;
        this.needsComma_ = false;
    }
    endChild() {
        this.currentDepth_--;
        if (this.currentHash_ !== null) {
            // Add closing parenthesis for the child that was just processed.
            if (!this.lengthOnly_) {
                this.currentHash_ += ')';
            }
            this.currentHashLength_++;
        }
        this.needsComma_ = true;
    }
    finishHashing() {
        if (this.currentHash_ !== null) {
            this.endRange_();
        }
        // Always close with the empty hash for the tail range to allow simple
        // appending at the server.
        this.hashes.push('');
    }
    /**
     * Seeds the builder into the exact state the natural full-tree walk has
     * immediately after ending a range at the leaf `path`: no open range, the
     * walker positioned at that leaf's depth. A subsequent walk of the leaves
     * AFTER `path` then serializes ranges byte-identically to the corresponding
     * portion of a full walk — the next range's opening parenthesis prefix is
     * reconstructed from the common path with this boundary, which is exactly
     * what ensureRange_ derives from currentPath_/currentDepth_.
     */
    seedBoundary(path) {
        this.currentPath_ = path.slice();
        this.currentDepth_ = path.length;
        this.lastLeafDepth_ = path.length;
        this.currentHash_ = null;
        this.currentHashLength_ = 0;
        this.needsComma_ = true;
    }
    /**
     * Ends the open range at the last processed leaf regardless of the split
     * strategy — used by the stable-range rewalk to close a dirty run exactly
     * at a preserved boundary post so the following clean range's interval is
     * untouched. No-op when no range is open.
     */
    forceEndRange() {
        if (this.currentHash_ !== null) {
            this.endRange_();
        }
    }
    ensureRange_() {
        if (this.currentHash_ === null) {
            let hash = '(';
            for (let i = 0; i < this.currentDepth_; i++) {
                hash += hashQuotedString(this.currentPath_[i]) + ':(';
            }
            this.currentHash_ = this.lengthOnly_ ? '' : hash;
            this.currentHashLength_ = hash.length;
            this.needsComma_ = false;
        }
    }
    /** Adds an interior-node priority to the persisted payload only. */
    processPriorityForPayload(path, priority) {
        if (!priority.isEmpty()) {
            this.appendPayloadValue_(path.concat('.priority'), priority.val());
        }
    }
    appendPayloadLeaf_(node) {
        this.appendPayloadValue_(this.currentPath_.slice(0, this.currentDepth_), node.val(true));
    }
    appendPayloadValue_(path, value) {
        if (this.payloadSink === null) {
            return;
        }
        if (path.length === 0) {
            this.currentPayload_ = value;
            return;
        }
        if (this.currentPayload_ === undefined ||
            this.currentPayload_ === null ||
            typeof this.currentPayload_ !== 'object') {
            this.currentPayload_ = {};
        }
        let cursor = this.currentPayload_;
        for (let i = 0; i < path.length - 1; i++) {
            const key = path[i];
            const existing = Object.prototype.hasOwnProperty.call(cursor, key)
                ? cursor[key]
                : undefined;
            if (existing === null || typeof existing !== 'object') {
                Object.defineProperty(cursor, key, {
                    value: {},
                    enumerable: true,
                    configurable: true,
                    writable: true
                });
            }
            cursor = cursor[key];
        }
        Object.defineProperty(cursor, path[path.length - 1], {
            value,
            enumerable: true,
            configurable: true,
            writable: true
        });
    }
    endRange_() {
        let hash = this.currentHash_;
        if (!this.lengthOnly_) {
            for (let i = 0; i < this.currentDepth_; i++) {
                hash += ')';
            }
            hash += ')';
        }
        const completedLength = this.currentHashLength_ + this.currentDepth_ + 1;
        const index = this.hashes.length;
        this.sizes.push(completedLength);
        if (this.lengthOnly_) {
            this.hashes.push('');
        }
        else if (this.hashSink !== null) {
            this.hashes.push('');
            this.hashSink(hash, index);
        }
        else {
            this.hashes.push(sha1(hash));
        }
        if (this.payloadSink !== null) {
            this.payloadSink(this.currentPayload_, index);
        }
        const post = this.currentPath_.slice(0, this.lastLeafDepth_).join('/');
        this.posts.push(post === '' ? '/' : post);
        this.currentHash_ = null;
        this.currentHashLength_ = 0;
        this.currentPayload_ = undefined;
        this.needsComma_ = true;
    }
}
/**
 * Compares two range markers (slash-joined leaf paths) in compound-hash leaf
 * order: segment-wise nameCompare, a strict prefix sorting first. Posts are
 * ordering markers only — they need not exist as leaves in the current tree,
 * so the comparison must be total over arbitrary paths.
 */
function compareRangeMarkers(a, b) {
    const n = Math.min(a.length, b.length);
    for (let i = 0; i < n; i++) {
        const cmp = nameCompare(a[i], b[i]);
        if (cmp !== 0) {
            return cmp;
        }
    }
    return a.length - b.length;
}
const ROOT_POST = '/';
function markerToPath(post) {
    return post === ROOT_POST || post === '' ? [] : post.split('/');
}
/**
 * Relation of the subtree rooted at `path` to the marker `post`:
 *   -1 → every leaf in the subtree sorts before-or-at the marker
 *    0 → the marker lies inside (or at the root of) the subtree
 *    1 → every leaf in the subtree sorts after the marker
 */
function subtreeVsMarker(path, post) {
    const n = Math.min(path.length, post.length);
    for (let i = 0; i < n; i++) {
        const cmp = nameCompare(path[i], post[i]);
        if (cmp < 0) {
            return -1;
        }
        if (cmp > 0) {
            return 1;
        }
    }
    if (path.length <= post.length) {
        // path is a (possibly equal) prefix of post: marker inside subtree. An
        // exactly-equal leaf path counts as inside; the walk emits it and the
        // interval's half-open bounds decide inclusion.
        return 0;
    }
    // post is a strict prefix of path: markers sort before their extensions,
    // so the whole subtree sorts after the marker.
    return 1;
}
/**
 * Walks the leaves of `node` whose paths lie in the half-open marker interval
 * (fromPost, toPost], feeding the builder exactly the startChild / endChild /
 * processLeaf sequence the natural full-tree walk produces for those leaves.
 * The builder must have been seeded at `fromPost` (seedBoundary) so the first
 * emitted range opens with the same common-ancestor prefix the full walk
 * would write. `toPost === null` walks to the end of the tree.
 *
 * Subtrees entirely outside the interval are pruned without reading them —
 * the cost is O(interval bytes + pruned fanout), not O(tree).
 */
function walkLeafInterval(node, fromPost, toPost, builder) {
    // Transition state: the path of the previously emitted leaf (or the seeded
    // boundary), from which endChild/startChild transitions are derived.
    let openPath = fromPost === null ? [] : fromPost;
    let openDepth = openPath.length;
    let started = fromPost !== null;
    let stopped = false;
    const emitLeaf = (path, leaf) => {
        if (!started) {
            // First leaf of a from-the-start walk: descend from the root.
            for (let i = 0; i < path.length; i++) {
                builder.startChild(path[i]);
            }
            started = true;
        }
        else {
            let common = 0;
            while (common < openDepth &&
                common < path.length &&
                openPath[common] === path[common]) {
                common++;
            }
            for (let i = openDepth; i > common; i--) {
                builder.endChild();
            }
            for (let i = common; i < path.length; i++) {
                builder.startChild(path[i]);
            }
        }
        builder.processLeaf(leaf);
        // Copy: `path` is the walker's live mutable array.
        openPath = path.slice();
        openDepth = openPath.length;
    };
    const walk = (current, path) => {
        if (stopped) {
            return;
        }
        if (fromPost !== null) {
            const rel = subtreeVsMarker(path, fromPost);
            if (rel === -1) {
                return; // entirely at-or-before the opening boundary
            }
            if (rel === 0 && current.isLeafNode()) {
                // The boundary leaf itself: excluded (interval is open at fromPost).
                if (compareRangeMarkers(path, fromPost) <= 0) {
                    return;
                }
            }
        }
        if (toPost !== null) {
            const rel = subtreeVsMarker(path, toPost);
            if (rel === 1) {
                stopped = true; // entirely after the closing boundary
                return;
            }
        }
        if (current.isLeafNode()) {
            emitLeaf(path, current);
            return;
        }
        // The mobile wire grammar deliberately drops a trailing interior-node
        // priority. Persistence cannot: store it in the sparse payload without
        // feeding it to the canonical hash builder.
        builder.processPriorityForPayload(path, current.getPriority());
        forEachChildWithPriority(current, (key, child) => {
            if (stopped) {
                return;
            }
            path.push(key);
            walk(child, path);
            path.pop();
        });
    };
    walk(node, []);
    // Pop back out of the last emitted leaf's ancestry so a caller chaining
    // further work sees a balanced builder; endChild is a no-op on text when
    // no range is open.
    builder.forceEndRange();
}
/**
 * The identity-diff: collects the paths of maximal subtrees that differ
 * between two versions of an immutable, structurally shared tree. Unchanged
 * subtrees are recognized by object identity and never descended. A child
 * present in only one version reports that child's path. Descends at most
 * `maxDepth` levels before treating a differing subtree as wholly changed —
 * dirty mapping only needs interval bounds, not precise leaves.
 */
function collectChangedSubtreePaths(before, after, maxDepth = 8, maxPaths = 512) {
    const changed = [];
    /**
     * Returns true when the caller must collapse this branch to stay within the
     * global path budget. A large atomic subtree update should dirty that
     * subtree's ranges, never fall back to dirtying the entire persisted root.
     */
    const visit = (a, b, path, depth) => {
        if (a === b) {
            return false;
        }
        const branchStart = changed.length;
        const collapseBranch = () => {
            changed.splice(branchStart);
            changed.push(path.slice());
            return changed.length > maxPaths;
        };
        if (depth >= maxDepth ||
            a.isLeafNode() ||
            b.isLeafNode() ||
            a.isEmpty() ||
            b.isEmpty()) {
            changed.push(path.slice());
            return changed.length > maxPaths;
        }
        // Sorted merge over (key, child) PAIRS captured by the iteration itself.
        // Re-resolving each common key through getImmediateChild would repeat an
        // O(log n) nameCompare tree descent per key — measured as the dominant
        // cost of the whole diff on wide roots — for nodes forEachChild already
        // visited.
        const aPairs = [];
        const bPairs = [];
        a.forEachChild(KEY_INDEX, (key, child) => {
            aPairs.push([key, child]);
        });
        b.forEachChild(KEY_INDEX, (key, child) => {
            bPairs.push([key, child]);
        });
        let i = 0;
        let j = 0;
        while (i < aPairs.length || j < bPairs.length) {
            let key;
            let cmp;
            if (i >= aPairs.length) {
                cmp = 1;
                key = bPairs[j][0];
            }
            else if (j >= bPairs.length) {
                cmp = -1;
                key = aPairs[i][0];
            }
            else {
                cmp = nameCompare(aPairs[i][0], bPairs[j][0]);
                key = cmp <= 0 ? aPairs[i][0] : bPairs[j][0];
            }
            path.push(key);
            let overBudget = false;
            if (cmp === 0) {
                overBudget = visit(aPairs[i][1], bPairs[j][1], path, depth + 1);
                i++;
                j++;
            }
            else {
                changed.push(path.slice());
                overBudget = changed.length > maxPaths;
                if (cmp < 0) {
                    i++;
                }
                else {
                    j++;
                }
            }
            path.pop();
            if (overBudget) {
                return collapseBranch();
            }
        }
        if (a.getPriority() !== b.getPriority()) {
            if (a.getPriority().isEmpty() !== b.getPriority().isEmpty() ||
                (!a.getPriority().isEmpty() &&
                    a.getPriority().val() !== b.getPriority().val())) {
                changed.push(path.slice());
                if (changed.length > maxPaths) {
                    return collapseBranch();
                }
            }
        }
        return false;
    };
    visit(before, after, [], 0);
    return changed;
}
/**
 * Marks the ranges whose leaf interval intersects any changed subtree. Range
 * i covers the half-open marker interval (posts[i-1], posts[i]]; the virtual
 * tail after the last post is reported via the returned `tailDirty` (leaves
 * appended after the previously last leaf fall there).
 */
function markDirtyRanges(ranges, changedPaths) {
    const dirty = new Array(ranges.length).fill(false);
    let tailDirty = false;
    const posts = ranges.map(r => markerToPath(r.post));
    for (const path of changedPaths) {
        if (path.length === 0) {
            dirty.fill(true);
            tailDirty = true;
            break;
        }
        // First range not entirely before the subtree: subtree's leaves start at
        // marker `path` (a prefix sorts before its extensions), so binary-search
        // the first post >= path.
        let lo = 0;
        let hi = ranges.length;
        while (lo < hi) {
            const mid = (lo + hi) >> 1;
            if (compareRangeMarkers(posts[mid], path) < 0) {
                lo = mid + 1;
            }
            else {
                hi = mid;
            }
        }
        if (lo === ranges.length) {
            tailDirty = true;
            continue;
        }
        // Mark ranges from lo while their interval intersects the subtree: the
        // interval (posts[i-1], posts[i]] intersects until the PREVIOUS post
        // already sorts past every leaf under `path` (after it, not inside it).
        for (let i = lo; i < ranges.length; i++) {
            if (i > lo) {
                // Stop once the subtree's leaves all sort at-or-before the PREVIOUS
                // post: the interval (posts[i-1], posts[i]] can no longer intersect.
                if (subtreeVsMarker(path, posts[i - 1]) === -1) {
                    break;
                }
            }
            dirty[i] = true;
            if (i === ranges.length - 1 &&
                subtreeVsMarker(path, posts[ranges.length - 1]) !== -1) {
                // The subtree extends past the last post into the virtual tail.
                tailDirty = true;
            }
        }
    }
    return { dirty, tailDirty };
}
/**
 * Produces the next generation's stable ranges: clean ranges carry over
 * verbatim; each maximal dirty run (pre-extended over undersized clean
 * neighbors) is re-serialized over the current tree between its preserved
 * outer boundaries, re-splitting naturally at the current ideal size. Ranges
 * are emitted through `builder`, whose hashSink/hashes the caller owns —
 * pass a sink to hash the dirty texts with WebCrypto afterwards.
 *
 * Returns the new range list with hashes for SINK-DEFERRED entries empty
 * (the caller fills them from the sink's completions, matching indexes in
 * builder.hashes/sizes/posts order for the dirty emissions).
 */
function rebuildStableRanges(node, previous, dirty, tailDirty, builder, fixedTargetBytes) {
    const ideal = fixedTargetBytes === undefined
        ? Math.max(512, Math.floor(Math.sqrt(estimateSerializedNodeSize(node) * 100)))
        : Math.max(512, Math.floor(fixedTargetBytes));
    const minSize = ideal >> 1;
    // Absorb undersized clean neighbors into adjacent dirty runs (merge side of
    // the hysteresis): they re-emit merged with the run's bytes.
    const effectiveDirty = dirty.slice();
    for (let i = 0; i < effectiveDirty.length; i++) {
        if (!effectiveDirty[i]) {
            continue;
        }
        for (let p = i - 1; p >= 0 && !effectiveDirty[p] && previous[p].size < minSize; p--) {
            effectiveDirty[p] = true;
        }
        for (let n = i + 1; n < effectiveDirty.length &&
            !effectiveDirty[n] &&
            previous[n].size < minSize; n++) {
            effectiveDirty[n] = true;
            i = n;
        }
    }
    const result = [];
    let i = 0;
    while (i < previous.length) {
        if (!effectiveDirty[i]) {
            result.push(previous[i]);
            i++;
            continue;
        }
        let j = i;
        while (j < previous.length && effectiveDirty[j]) {
            j++;
        }
        const runEndsAtTail = j === previous.length && tailDirty;
        const fromPost = i === 0 ? null : markerToPath(previous[i - 1].post);
        const toPost = runEndsAtTail ? null : markerToPath(previous[j - 1].post);
        const firstEmitIndex = builder.posts.length;
        if (fromPost !== null) {
            builder.seedBoundary(fromPost);
        }
        walkLeafInterval(node, fromPost, toPost, builder);
        for (let k = firstEmitIndex; k < builder.posts.length; k++) {
            result.push({
                post: builder.posts[k],
                hash: builder.hashes[k],
                size: builder.sizes[k]
            });
        }
        i = j;
    }
    if (tailDirty && previous.length > 0) {
        // Tail handled by extending the last run (runEndsAtTail) when the last
        // range was dirty; when it was clean, walk the pure tail interval.
        const lastWasClean = !effectiveDirty[previous.length - 1];
        if (lastWasClean) {
            const fromPost = markerToPath(previous[previous.length - 1].post);
            const firstEmitIndex = builder.posts.length;
            builder.seedBoundary(fromPost);
            walkLeafInterval(node, fromPost, null, builder);
            for (let k = firstEmitIndex; k < builder.posts.length; k++) {
                result.push({
                    post: builder.posts[k],
                    hash: builder.hashes[k],
                    size: builder.sizes[k]
                });
            }
        }
    }
    if (previous.length === 0) {
        // First-ever generation: one natural full walk.
        const firstEmitIndex = builder.posts.length;
        walkLeafInterval(node, null, null, builder);
        for (let k = firstEmitIndex; k < builder.posts.length; k++) {
            result.push({
                post: builder.posts[k],
                hash: builder.hashes[k],
                size: builder.sizes[k]
            });
        }
    }
    return result;
}
/**
 * The compound-hash representation of a leaf: the V2 grammar (strings and
 * keys JSON-quoted so range serializations are unambiguous to reparse; see
 * leafHashValueText), with the priority prefixed exactly as in Node.hash().
 */
function leafHashRepresentation(node) {
    let representation = '';
    const priority = node.getPriority();
    if (!priority.isEmpty()) {
        representation +=
            'priority:' +
                leafHashValueText(priority.val(), true) +
                ':';
    }
    representation += leafHashValueText(node.val(), true);
    return representation;
}
/**
 * Sizes computed for interior (children) nodes, keyed by node identity.
 * Nodes are immutable and structurally shared across server updates, so a
 * subtree's estimate stays valid for as long as the subtree object lives —
 * repeated estimations of a large mostly-unchanged tree (the persistence
 * write path re-plans its chunks on every flush) only walk the changed
 * spine. Leaves are cheap to size and are not cached.
 */
const serializedSizeCache = new WeakMap();
/**
 * Estimates the serialized size of a node in bytes — a cheap approximation
 * that only drives the default split threshold and the persistence chunk
 * planner, never a wire value (port of Android NodeSizeEstimator).
 */
function estimateSerializedNodeSize(node) {
    if (node.isEmpty()) {
        return 4; // null keyword
    }
    else if (node.isLeafNode()) {
        let valueSize;
        const value = node.val();
        if (typeof value === 'number') {
            valueSize = 8; // estimate each float with 8 bytes
        }
        else if (typeof value === 'boolean') {
            valueSize = 4; // true or false need roughly 4 bytes
        }
        else {
            // string: two quotes plus the payload
            valueSize = 2 + String(value).length;
        }
        if (node.getPriority().isEmpty()) {
            return valueSize;
        }
        // Account for the extra overhead of the ".value" and ".priority" keys.
        return 24 + valueSize + estimateSerializedNodeSize(node.getPriority());
    }
    else {
        const cached = serializedSizeCache.get(node);
        if (cached !== undefined) {
            return cached;
        }
        let sum = 1; // opening brace
        node.forEachChild(KEY_INDEX, (key, child) => {
            // key, quotes, colon, comma
            sum += key.length + 4 + estimateSerializedNodeSize(child);
        });
        if (!node.getPriority().isEmpty()) {
            sum += 12 + estimateSerializedNodeSize(node.getPriority());
        }
        serializedSizeCache.set(node, sum);
        return sum;
    }
}

/**
 * @license
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Stamps a precomputed canonical hash into the node's lazy-hash slot (so
 * hash() returns it without an O(tree) walk) and attaches the precomputed
 * compound hash for the listen to send. Both must describe exactly this
 * tree — the server certifies whatever the listen carries.
 *
 * An empty tree is returned unstamped: an empty node is the shared
 * ChildrenNode.EMPTY_NODE singleton, and stamping that would poison every
 * empty node in the app.
 */
function stampSeedHashes(node, hash, compoundHash) {
    if (node.isEmpty()) {
        return node;
    }
    if (typeof hash === 'string') {
        nodeCanonicalHashes.set(node, hash);
        if (hash.length > 0) {
            node.stampLazyHash(hash);
        }
    }
    if (compoundHash &&
        Array.isArray(compoundHash.hashes) &&
        Array.isArray(compoundHash.posts) &&
        compoundHash.hashes.length === compoundHash.posts.length + 1) {
        setNodeCompoundHash(node, compoundHash);
    }
    return node;
}
/**
 * The compound hash rides on the seeded node itself: once a server update
 * replaces the cached node the stamp is gone, so re-listens after real data
 * arrived send only the simple hash (which is then correct by construction).
 */
const nodeCompoundHashes = new WeakMap();
const nodeCanonicalHashes = new WeakMap();
function setNodeCompoundHash(node, compoundHash) {
    nodeCompoundHashes.set(node, compoundHash);
}
function getNodeCompoundHash(node) {
    return nodeCompoundHashes.get(node);
}
/**
 * The persisted canonical hash associated with a seeded node. This rides in
 * a WeakMap instead of being stamped into every subtree by node.hash(): a
 * compound-hash-only seed deliberately stores the empty simple hash, letting
 * the server validate its ranges without a full-tree hash pass that would
 * permanently retain one SHA string per node.
 */
function getNodeCanonicalHash(node) {
    return nodeCanonicalHashes.get(node);
}
/**
 * One-boot materialization handoff. The optimistic pre-auth peek
 * (getPersistedValue) materializes the restored tree to JS objects once;
 * the authenticated listener that adopts the SAME immutable Node then
 * replays it as a child_added burst whose per-child `snapshot.val()` calls
 * would materialize the identical tree a second time — two full JS copies
 * of a large workspace alive at the peak of boot.
 *
 * The peek stamps each materialized value here, keyed by its Node instance;
 * a consumer that OPTS IN via consumePersistedMaterialization() (api/
 * Reference_impl) takes a stamp (get + delete) instead of walking the node.
 * `DataSnapshot.val()` never consumes a stamp — its fresh-objects contract
 * is untouched. Consume-once means only the single designed peek→listener
 * handoff ever receives shared objects (which is the point — the optimistic
 * tree and the live tree then share child identity, so downstream
 * memoization sees unchanged branches as unchanged).
 *
 * Correctness is by construction: a Node is immutable, so a stamp can only
 * ever be returned for exactly the data it was computed from. Any server
 * delta between peek and replay produces a NEW child Node instance, which
 * misses the WeakMap and materializes fresh.
 *
 * Only non-null object values are stamped (a leaf's val() is O(1) already),
 * and never on an empty node — the empty ChildrenNode is a shared singleton
 * and stamping it would leak one boot's subtree to unrelated paths.
 */
const nodeMaterializedValues = new WeakMap();
function stampMaterializedValue(node, value) {
    if (value === null || typeof value !== 'object' || node.isEmpty()) {
        return;
    }
    nodeMaterializedValues.set(node, value);
}
function consumeMaterializedValue(node) {
    const value = nodeMaterializedValues.get(node);
    if (value !== undefined) {
        nodeMaterializedValues.delete(node);
    }
    return value;
}
/**
 * Repo-scoped manifest-first hash registry. Different Database instances can
 * listen to the same relative path while holding different caches; keeping
 * this store on Repo prevents one restore from overwriting or clearing
 * another Repo's pending hashes.
 */
class PendingListenHashStore {
    constructor() {
        this.pending_ = new Map();
    }
    set(pathString, hash, compoundHash) {
        this.pending_.set(pathString, { hash, compoundHash });
    }
    clear(pathString) {
        this.pending_.delete(pathString);
    }
    get(pathString) {
        return this.pending_.get(pathString);
    }
    clearAll() {
        this.pending_.clear();
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * An iterator over an LLRBNode.
 */
class SortedMapIterator {
    /**
     * @param node - Node to iterate.
     * @param isReverse_ - Whether or not to iterate in reverse
     */
    constructor(node, startKey, comparator, isReverse_, resultGenerator_ = null) {
        this.isReverse_ = isReverse_;
        this.resultGenerator_ = resultGenerator_;
        this.nodeStack_ = [];
        let cmp = 1;
        while (!node.isEmpty()) {
            node = node;
            cmp = startKey ? comparator(node.key, startKey) : 1;
            // flip the comparison if we're going in reverse
            if (isReverse_) {
                cmp *= -1;
            }
            if (cmp < 0) {
                // This node is less than our start key. ignore it
                if (this.isReverse_) {
                    node = node.left;
                }
                else {
                    node = node.right;
                }
            }
            else if (cmp === 0) {
                // This node is exactly equal to our start key. Push it on the stack, but stop iterating;
                this.nodeStack_.push(node);
                break;
            }
            else {
                // This node is greater than our start key, add it to the stack and move to the next one
                this.nodeStack_.push(node);
                if (this.isReverse_) {
                    node = node.right;
                }
                else {
                    node = node.left;
                }
            }
        }
    }
    getNext() {
        if (this.nodeStack_.length === 0) {
            return null;
        }
        let node = this.nodeStack_.pop();
        let result;
        if (this.resultGenerator_) {
            result = this.resultGenerator_(node.key, node.value);
        }
        else {
            result = { key: node.key, value: node.value };
        }
        if (this.isReverse_) {
            node = node.left;
            while (!node.isEmpty()) {
                this.nodeStack_.push(node);
                node = node.right;
            }
        }
        else {
            node = node.right;
            while (!node.isEmpty()) {
                this.nodeStack_.push(node);
                node = node.left;
            }
        }
        return result;
    }
    hasNext() {
        return this.nodeStack_.length > 0;
    }
    peek() {
        if (this.nodeStack_.length === 0) {
            return null;
        }
        const node = this.nodeStack_[this.nodeStack_.length - 1];
        if (this.resultGenerator_) {
            return this.resultGenerator_(node.key, node.value);
        }
        else {
            return { key: node.key, value: node.value };
        }
    }
}
/**
 * Represents a node in a Left-leaning Red-Black tree.
 */
class LLRBNode {
    /**
     * @param key - Key associated with this node.
     * @param value - Value associated with this node.
     * @param color - Whether this node is red.
     * @param left - Left child.
     * @param right - Right child.
     */
    constructor(key, value, color, left, right) {
        this.key = key;
        this.value = value;
        this.color = color != null ? color : LLRBNode.RED;
        this.left =
            left != null ? left : SortedMap.EMPTY_NODE;
        this.right =
            right != null ? right : SortedMap.EMPTY_NODE;
    }
    /**
     * Returns a copy of the current node, optionally replacing pieces of it.
     *
     * @param key - New key for the node, or null.
     * @param value - New value for the node, or null.
     * @param color - New color for the node, or null.
     * @param left - New left child for the node, or null.
     * @param right - New right child for the node, or null.
     * @returns The node copy.
     */
    copy(key, value, color, left, right) {
        return new LLRBNode(key != null ? key : this.key, value != null ? value : this.value, color != null ? color : this.color, left != null ? left : this.left, right != null ? right : this.right);
    }
    /**
     * @returns The total number of nodes in the tree.
     */
    count() {
        return this.left.count() + 1 + this.right.count();
    }
    /**
     * @returns True if the tree is empty.
     */
    isEmpty() {
        return false;
    }
    /**
     * Traverses the tree in key order and calls the specified action function
     * for each node.
     *
     * @param action - Callback function to be called for each
     *   node.  If it returns true, traversal is aborted.
     * @returns The first truthy value returned by action, or the last falsey
     *   value returned by action
     */
    inorderTraversal(action) {
        return (this.left.inorderTraversal(action) ||
            !!action(this.key, this.value) ||
            this.right.inorderTraversal(action));
    }
    /**
     * Traverses the tree in reverse key order and calls the specified action function
     * for each node.
     *
     * @param action - Callback function to be called for each
     * node.  If it returns true, traversal is aborted.
     * @returns True if traversal was aborted.
     */
    reverseTraversal(action) {
        return (this.right.reverseTraversal(action) ||
            action(this.key, this.value) ||
            this.left.reverseTraversal(action));
    }
    /**
     * @returns The minimum node in the tree.
     */
    min_() {
        if (this.left.isEmpty()) {
            return this;
        }
        else {
            return this.left.min_();
        }
    }
    /**
     * @returns The maximum key in the tree.
     */
    minKey() {
        return this.min_().key;
    }
    /**
     * @returns The maximum key in the tree.
     */
    maxKey() {
        if (this.right.isEmpty()) {
            return this.key;
        }
        else {
            return this.right.maxKey();
        }
    }
    /**
     * @param key - Key to insert.
     * @param value - Value to insert.
     * @param comparator - Comparator.
     * @returns New tree, with the key/value added.
     */
    insert(key, value, comparator) {
        let n = this;
        const cmp = comparator(key, n.key);
        if (cmp < 0) {
            n = n.copy(null, null, null, n.left.insert(key, value, comparator), null);
        }
        else if (cmp === 0) {
            n = n.copy(null, value, null, null, null);
        }
        else {
            n = n.copy(null, null, null, null, n.right.insert(key, value, comparator));
        }
        return n.fixUp_();
    }
    /**
     * @returns New tree, with the minimum key removed.
     */
    removeMin_() {
        if (this.left.isEmpty()) {
            return SortedMap.EMPTY_NODE;
        }
        let n = this;
        if (!n.left.isRed_() && !n.left.left.isRed_()) {
            n = n.moveRedLeft_();
        }
        n = n.copy(null, null, null, n.left.removeMin_(), null);
        return n.fixUp_();
    }
    /**
     * @param key - The key of the item to remove.
     * @param comparator - Comparator.
     * @returns New tree, with the specified item removed.
     */
    remove(key, comparator) {
        let n, smallest;
        n = this;
        if (comparator(key, n.key) < 0) {
            if (!n.left.isEmpty() && !n.left.isRed_() && !n.left.left.isRed_()) {
                n = n.moveRedLeft_();
            }
            n = n.copy(null, null, null, n.left.remove(key, comparator), null);
        }
        else {
            if (n.left.isRed_()) {
                n = n.rotateRight_();
            }
            if (!n.right.isEmpty() && !n.right.isRed_() && !n.right.left.isRed_()) {
                n = n.moveRedRight_();
            }
            if (comparator(key, n.key) === 0) {
                if (n.right.isEmpty()) {
                    return SortedMap.EMPTY_NODE;
                }
                else {
                    smallest = n.right.min_();
                    n = n.copy(smallest.key, smallest.value, null, null, n.right.removeMin_());
                }
            }
            n = n.copy(null, null, null, null, n.right.remove(key, comparator));
        }
        return n.fixUp_();
    }
    /**
     * @returns Whether this is a RED node.
     */
    isRed_() {
        return this.color;
    }
    /**
     * @returns New tree after performing any needed rotations.
     */
    fixUp_() {
        let n = this;
        if (n.right.isRed_() && !n.left.isRed_()) {
            n = n.rotateLeft_();
        }
        if (n.left.isRed_() && n.left.left.isRed_()) {
            n = n.rotateRight_();
        }
        if (n.left.isRed_() && n.right.isRed_()) {
            n = n.colorFlip_();
        }
        return n;
    }
    /**
     * @returns New tree, after moveRedLeft.
     */
    moveRedLeft_() {
        let n = this.colorFlip_();
        if (n.right.left.isRed_()) {
            n = n.copy(null, null, null, null, n.right.rotateRight_());
            n = n.rotateLeft_();
            n = n.colorFlip_();
        }
        return n;
    }
    /**
     * @returns New tree, after moveRedRight.
     */
    moveRedRight_() {
        let n = this.colorFlip_();
        if (n.left.left.isRed_()) {
            n = n.rotateRight_();
            n = n.colorFlip_();
        }
        return n;
    }
    /**
     * @returns New tree, after rotateLeft.
     */
    rotateLeft_() {
        const nl = this.copy(null, null, LLRBNode.RED, null, this.right.left);
        return this.right.copy(null, null, this.color, nl, null);
    }
    /**
     * @returns New tree, after rotateRight.
     */
    rotateRight_() {
        const nr = this.copy(null, null, LLRBNode.RED, this.left.right, null);
        return this.left.copy(null, null, this.color, null, nr);
    }
    /**
     * @returns Newt ree, after colorFlip.
     */
    colorFlip_() {
        const left = this.left.copy(null, null, !this.left.color, null, null);
        const right = this.right.copy(null, null, !this.right.color, null, null);
        return this.copy(null, null, !this.color, left, right);
    }
    /**
     * For testing.
     *
     * @returns True if all is well.
     */
    checkMaxDepth_() {
        const blackDepth = this.check_();
        return Math.pow(2.0, blackDepth) <= this.count() + 1;
    }
    check_() {
        if (this.isRed_() && this.left.isRed_()) {
            throw new Error('Red node has red child(' + this.key + ',' + this.value + ')');
        }
        if (this.right.isRed_()) {
            throw new Error('Right child of (' + this.key + ',' + this.value + ') is red');
        }
        const blackDepth = this.left.check_();
        if (blackDepth !== this.right.check_()) {
            throw new Error('Black depths differ');
        }
        else {
            return blackDepth + (this.isRed_() ? 0 : 1);
        }
    }
}
LLRBNode.RED = true;
LLRBNode.BLACK = false;
/**
 * Represents an empty node (a leaf node in the Red-Black Tree).
 */
class LLRBEmptyNode {
    /**
     * Returns a copy of the current node.
     *
     * @returns The node copy.
     */
    copy(key, value, color, left, right) {
        return this;
    }
    /**
     * Returns a copy of the tree, with the specified key/value added.
     *
     * @param key - Key to be added.
     * @param value - Value to be added.
     * @param comparator - Comparator.
     * @returns New tree, with item added.
     */
    insert(key, value, comparator) {
        return new LLRBNode(key, value, null);
    }
    /**
     * Returns a copy of the tree, with the specified key removed.
     *
     * @param key - The key to remove.
     * @param comparator - Comparator.
     * @returns New tree, with item removed.
     */
    remove(key, comparator) {
        return this;
    }
    /**
     * @returns The total number of nodes in the tree.
     */
    count() {
        return 0;
    }
    /**
     * @returns True if the tree is empty.
     */
    isEmpty() {
        return true;
    }
    /**
     * Traverses the tree in key order and calls the specified action function
     * for each node.
     *
     * @param action - Callback function to be called for each
     * node.  If it returns true, traversal is aborted.
     * @returns True if traversal was aborted.
     */
    inorderTraversal(action) {
        return false;
    }
    /**
     * Traverses the tree in reverse key order and calls the specified action function
     * for each node.
     *
     * @param action - Callback function to be called for each
     * node.  If it returns true, traversal is aborted.
     * @returns True if traversal was aborted.
     */
    reverseTraversal(action) {
        return false;
    }
    minKey() {
        return null;
    }
    maxKey() {
        return null;
    }
    check_() {
        return 0;
    }
    /**
     * @returns Whether this node is red.
     */
    isRed_() {
        return false;
    }
}
/**
 * An immutable sorted map implementation, based on a Left-leaning Red-Black
 * tree.
 */
class SortedMap {
    /**
     * @param comparator_ - Key comparator.
     * @param root_ - Optional root node for the map.
     */
    constructor(comparator_, root_ = SortedMap.EMPTY_NODE) {
        this.comparator_ = comparator_;
        this.root_ = root_;
    }
    /**
     * Returns a copy of the map, with the specified key/value added or replaced.
     * (TODO: We should perhaps rename this method to 'put')
     *
     * @param key - Key to be added.
     * @param value - Value to be added.
     * @returns New map, with item added.
     */
    insert(key, value) {
        return new SortedMap(this.comparator_, this.root_
            .insert(key, value, this.comparator_)
            .copy(null, null, LLRBNode.BLACK, null, null));
    }
    /**
     * Returns a copy of the map, with the specified key removed.
     *
     * @param key - The key to remove.
     * @returns New map, with item removed.
     */
    remove(key) {
        return new SortedMap(this.comparator_, this.root_
            .remove(key, this.comparator_)
            .copy(null, null, LLRBNode.BLACK, null, null));
    }
    /**
     * Returns the value of the node with the given key, or null.
     *
     * @param key - The key to look up.
     * @returns The value of the node with the given key, or null if the
     * key doesn't exist.
     */
    get(key) {
        let cmp;
        let node = this.root_;
        while (!node.isEmpty()) {
            cmp = this.comparator_(key, node.key);
            if (cmp === 0) {
                return node.value;
            }
            else if (cmp < 0) {
                node = node.left;
            }
            else if (cmp > 0) {
                node = node.right;
            }
        }
        return null;
    }
    /**
     * Returns the key of the item *before* the specified key, or null if key is the first item.
     * @param key - The key to find the predecessor of
     * @returns The predecessor key.
     */
    getPredecessorKey(key) {
        let cmp, node = this.root_, rightParent = null;
        while (!node.isEmpty()) {
            cmp = this.comparator_(key, node.key);
            if (cmp === 0) {
                if (!node.left.isEmpty()) {
                    node = node.left;
                    while (!node.right.isEmpty()) {
                        node = node.right;
                    }
                    return node.key;
                }
                else if (rightParent) {
                    return rightParent.key;
                }
                else {
                    return null; // first item.
                }
            }
            else if (cmp < 0) {
                node = node.left;
            }
            else if (cmp > 0) {
                rightParent = node;
                node = node.right;
            }
        }
        throw new Error('Attempted to find predecessor key for a nonexistent key.  What gives?');
    }
    /**
     * @returns True if the map is empty.
     */
    isEmpty() {
        return this.root_.isEmpty();
    }
    /**
     * @returns The total number of nodes in the map.
     */
    count() {
        return this.root_.count();
    }
    /**
     * @returns The minimum key in the map.
     */
    minKey() {
        return this.root_.minKey();
    }
    /**
     * @returns The maximum key in the map.
     */
    maxKey() {
        return this.root_.maxKey();
    }
    /**
     * Traverses the map in key order and calls the specified action function
     * for each key/value pair.
     *
     * @param action - Callback function to be called
     * for each key/value pair.  If action returns true, traversal is aborted.
     * @returns The first truthy value returned by action, or the last falsey
     *   value returned by action
     */
    inorderTraversal(action) {
        return this.root_.inorderTraversal(action);
    }
    /**
     * Traverses the map in reverse key order and calls the specified action function
     * for each key/value pair.
     *
     * @param action - Callback function to be called
     * for each key/value pair.  If action returns true, traversal is aborted.
     * @returns True if the traversal was aborted.
     */
    reverseTraversal(action) {
        return this.root_.reverseTraversal(action);
    }
    /**
     * Returns an iterator over the SortedMap.
     * @returns The iterator.
     */
    getIterator(resultGenerator) {
        return new SortedMapIterator(this.root_, null, this.comparator_, false, resultGenerator);
    }
    getIteratorFrom(key, resultGenerator) {
        return new SortedMapIterator(this.root_, key, this.comparator_, false, resultGenerator);
    }
    getReverseIteratorFrom(key, resultGenerator) {
        return new SortedMapIterator(this.root_, key, this.comparator_, true, resultGenerator);
    }
    getReverseIterator(resultGenerator) {
        return new SortedMapIterator(this.root_, null, this.comparator_, true, resultGenerator);
    }
}
/**
 * Always use the same empty node, to reduce memory.
 */
SortedMap.EMPTY_NODE = new LLRBEmptyNode();

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
function NAME_ONLY_COMPARATOR(left, right) {
    return nameCompare(left.name, right.name);
}
function NAME_COMPARATOR(left, right) {
    return nameCompare(left, right);
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const LOG_2 = Math.log(2);
class Base12Num {
    constructor(length) {
        const logBase2 = (num) => 
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        parseInt((Math.log(num) / LOG_2), 10);
        const bitMask = (bits) => parseInt(Array(bits + 1).join('1'), 2);
        this.count = logBase2(length + 1);
        this.current_ = this.count - 1;
        const mask = bitMask(this.count);
        this.bits_ = (length + 1) & mask;
    }
    nextBitIsOne() {
        //noinspection JSBitwiseOperatorUsage
        const result = !(this.bits_ & (0x1 << this.current_));
        this.current_--;
        return result;
    }
}
/**
 * Takes a list of child nodes and constructs a SortedSet using the given comparison
 * function
 *
 * Uses the algorithm described in the paper linked here:
 * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.46.1458
 *
 * @param childList - Unsorted list of children
 * @param cmp - The comparison method to be used
 * @param keyFn - An optional function to extract K from a node wrapper, if K's
 * type is not NamedNode
 * @param mapSortFn - An optional override for comparator used by the generated sorted map
 */
const buildChildSet = function (childList, cmp, keyFn, mapSortFn) {
    childList.sort(cmp);
    const buildBalancedTree = function (low, high) {
        const length = high - low;
        let namedNode;
        let key;
        if (length === 0) {
            return null;
        }
        else if (length === 1) {
            namedNode = childList[low];
            key = keyFn ? keyFn(namedNode) : namedNode;
            return new LLRBNode(key, namedNode.node, LLRBNode.BLACK, null, null);
        }
        else {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const middle = parseInt((length / 2), 10) + low;
            const left = buildBalancedTree(low, middle);
            const right = buildBalancedTree(middle + 1, high);
            namedNode = childList[middle];
            key = keyFn ? keyFn(namedNode) : namedNode;
            return new LLRBNode(key, namedNode.node, LLRBNode.BLACK, left, right);
        }
    };
    const buildFrom12Array = function (base12) {
        let node = null;
        let root = null;
        let index = childList.length;
        const buildPennant = function (chunkSize, color) {
            const low = index - chunkSize;
            const high = index;
            index -= chunkSize;
            const childTree = buildBalancedTree(low + 1, high);
            const namedNode = childList[low];
            const key = keyFn ? keyFn(namedNode) : namedNode;
            attachPennant(new LLRBNode(key, namedNode.node, color, null, childTree));
        };
        const attachPennant = function (pennant) {
            if (node) {
                node.left = pennant;
                node = pennant;
            }
            else {
                root = pennant;
                node = pennant;
            }
        };
        for (let i = 0; i < base12.count; ++i) {
            const isOne = base12.nextBitIsOne();
            // The number of nodes taken in each slice is 2^(arr.length - (i + 1))
            const chunkSize = Math.pow(2, base12.count - (i + 1));
            if (isOne) {
                buildPennant(chunkSize, LLRBNode.BLACK);
            }
            else {
                // current == 2
                buildPennant(chunkSize, LLRBNode.BLACK);
                buildPennant(chunkSize, LLRBNode.RED);
            }
        }
        return root;
    };
    const base12 = new Base12Num(childList.length);
    const root = buildFrom12Array(base12);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return new SortedMap(mapSortFn || cmp, root);
};

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
let _defaultIndexMap;
const fallbackObject = {};
class IndexMap {
    /**
     * The default IndexMap for nodes without a priority
     */
    static get Default() {
        util.assert(fallbackObject && PRIORITY_INDEX, 'ChildrenNode.ts has not been loaded');
        _defaultIndexMap =
            _defaultIndexMap ||
                new IndexMap({ '.priority': fallbackObject }, { '.priority': PRIORITY_INDEX });
        return _defaultIndexMap;
    }
    constructor(indexes_, indexSet_) {
        this.indexes_ = indexes_;
        this.indexSet_ = indexSet_;
    }
    get(indexKey) {
        const sortedMap = util.safeGet(this.indexes_, indexKey);
        if (!sortedMap) {
            throw new Error('No index defined for ' + indexKey);
        }
        if (sortedMap instanceof SortedMap) {
            return sortedMap;
        }
        else {
            // The index exists, but it falls back to just name comparison. Return null so that the calling code uses the
            // regular child map
            return null;
        }
    }
    hasIndex(indexDefinition) {
        return util.contains(this.indexSet_, indexDefinition.toString());
    }
    addIndex(indexDefinition, existingChildren) {
        util.assert(indexDefinition !== KEY_INDEX, "KeyIndex always exists and isn't meant to be added to the IndexMap.");
        const childList = [];
        let sawIndexedValue = false;
        const iter = existingChildren.getIterator(NamedNode.Wrap);
        let next = iter.getNext();
        while (next) {
            sawIndexedValue =
                sawIndexedValue || indexDefinition.isDefinedOn(next.node);
            childList.push(next);
            next = iter.getNext();
        }
        let newIndex;
        if (sawIndexedValue) {
            newIndex = buildChildSet(childList, indexDefinition.getCompare());
        }
        else {
            newIndex = fallbackObject;
        }
        const indexName = indexDefinition.toString();
        const newIndexSet = { ...this.indexSet_ };
        newIndexSet[indexName] = indexDefinition;
        const newIndexes = { ...this.indexes_ };
        newIndexes[indexName] = newIndex;
        return new IndexMap(newIndexes, newIndexSet);
    }
    /**
     * Ensure that this node is properly tracked in any indexes that we're maintaining
     */
    addToIndexes(namedNode, existingChildren) {
        const newIndexes = util.map(this.indexes_, (indexedChildren, indexName) => {
            const index = util.safeGet(this.indexSet_, indexName);
            util.assert(index, 'Missing index implementation for ' + indexName);
            if (indexedChildren === fallbackObject) {
                // Check to see if we need to index everything
                if (index.isDefinedOn(namedNode.node)) {
                    // We need to build this index
                    const childList = [];
                    const iter = existingChildren.getIterator(NamedNode.Wrap);
                    let next = iter.getNext();
                    while (next) {
                        if (next.name !== namedNode.name) {
                            childList.push(next);
                        }
                        next = iter.getNext();
                    }
                    childList.push(namedNode);
                    return buildChildSet(childList, index.getCompare());
                }
                else {
                    // No change, this remains a fallback
                    return fallbackObject;
                }
            }
            else {
                const existingSnap = existingChildren.get(namedNode.name);
                let newChildren = indexedChildren;
                if (existingSnap) {
                    newChildren = newChildren.remove(new NamedNode(namedNode.name, existingSnap));
                }
                return newChildren.insert(namedNode, namedNode.node);
            }
        });
        return new IndexMap(newIndexes, this.indexSet_);
    }
    /**
     * Create a new IndexMap instance with the given value removed
     */
    removeFromIndexes(namedNode, existingChildren) {
        const newIndexes = util.map(this.indexes_, (indexedChildren) => {
            if (indexedChildren === fallbackObject) {
                // This is the fallback. Just return it, nothing to do in this case
                return indexedChildren;
            }
            else {
                const existingSnap = existingChildren.get(namedNode.name);
                if (existingSnap) {
                    return indexedChildren.remove(new NamedNode(namedNode.name, existingSnap));
                }
                else {
                    // No record of this child
                    return indexedChildren;
                }
            }
        });
        return new IndexMap(newIndexes, this.indexSet_);
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// TODO: For memory savings, don't store priorityNode_ if it's empty.
let EMPTY_NODE;
/**
 * ChildrenNode is a class for storing internal nodes in a DataSnapshot
 * (i.e. nodes with children).  It implements Node and stores the
 * list of children in the children property, sorted by child name.
 */
class ChildrenNode {
    static get EMPTY_NODE() {
        return (EMPTY_NODE ||
            (EMPTY_NODE = new ChildrenNode(new SortedMap(NAME_COMPARATOR), null, IndexMap.Default)));
    }
    /**
     * @param children_ - List of children of this node..
     * @param priorityNode_ - The priority of this node (as a snapshot node).
     */
    constructor(children_, priorityNode_, indexMap_) {
        this.children_ = children_;
        this.priorityNode_ = priorityNode_;
        this.indexMap_ = indexMap_;
        this.lazyHash_ = null;
        /**
         * Note: The only reason we allow null priority is for EMPTY_NODE, since we can't use
         * EMPTY_NODE as the priority of EMPTY_NODE.  We might want to consider making EMPTY_NODE its own
         * class instead of an empty ChildrenNode.
         */
        if (this.priorityNode_) {
            validatePriorityNode(this.priorityNode_);
        }
        if (this.children_.isEmpty()) {
            util.assert(!this.priorityNode_ || this.priorityNode_.isEmpty(), 'An empty node cannot have a priority');
        }
    }
    /** @inheritDoc */
    isLeafNode() {
        return false;
    }
    /** @inheritDoc */
    getPriority() {
        return this.priorityNode_ || EMPTY_NODE;
    }
    /** @inheritDoc */
    updatePriority(newPriorityNode) {
        if (this.children_.isEmpty()) {
            // Don't allow priorities on empty nodes
            return this;
        }
        else {
            return new ChildrenNode(this.children_, newPriorityNode, this.indexMap_);
        }
    }
    /** @inheritDoc */
    getImmediateChild(childName) {
        // Hack to treat priority as a regular child
        if (childName === '.priority') {
            return this.getPriority();
        }
        else {
            const child = this.children_.get(childName);
            return child === null ? EMPTY_NODE : child;
        }
    }
    /** @inheritDoc */
    getChild(path) {
        const front = pathGetFront(path);
        if (front === null) {
            return this;
        }
        return this.getImmediateChild(front).getChild(pathPopFront(path));
    }
    /** @inheritDoc */
    hasChild(childName) {
        return this.children_.get(childName) !== null;
    }
    /** @inheritDoc */
    updateImmediateChild(childName, newChildNode) {
        util.assert(newChildNode, 'We should always be passing snapshot nodes');
        if (childName === '.priority') {
            return this.updatePriority(newChildNode);
        }
        else {
            const namedNode = new NamedNode(childName, newChildNode);
            let newChildren, newIndexMap;
            if (newChildNode.isEmpty()) {
                newChildren = this.children_.remove(childName);
                newIndexMap = this.indexMap_.removeFromIndexes(namedNode, this.children_);
            }
            else {
                newChildren = this.children_.insert(childName, newChildNode);
                newIndexMap = this.indexMap_.addToIndexes(namedNode, this.children_);
            }
            const newPriority = newChildren.isEmpty()
                ? EMPTY_NODE
                : this.priorityNode_;
            return new ChildrenNode(newChildren, newPriority, newIndexMap);
        }
    }
    /** @inheritDoc */
    updateChild(path, newChildNode) {
        const front = pathGetFront(path);
        if (front === null) {
            return newChildNode;
        }
        else {
            util.assert(pathGetFront(path) !== '.priority' || pathGetLength(path) === 1, '.priority must be the last token in a path');
            const newImmediateChild = this.getImmediateChild(front).updateChild(pathPopFront(path), newChildNode);
            return this.updateImmediateChild(front, newImmediateChild);
        }
    }
    /** @inheritDoc */
    isEmpty() {
        return this.children_.isEmpty();
    }
    /** @inheritDoc */
    numChildren() {
        return this.children_.count();
    }
    /** @inheritDoc */
    val(exportFormat) {
        if (this.isEmpty()) {
            return null;
        }
        const obj = {};
        let numKeys = 0, maxKey = 0, allIntegerKeys = true;
        this.forEachChild(PRIORITY_INDEX, (key, childNode) => {
            obj[key] = childNode.val(exportFormat);
            numKeys++;
            // charCode fast-reject: named keys can never be integers; skip the
            // regex for them (val() over a large workspace calls this per key).
            if (allIntegerKeys &&
                key.charCodeAt(0) >= 48 /* '0' */ &&
                key.charCodeAt(0) <= 57 /* '9' */ &&
                ChildrenNode.INTEGER_REGEXP_.test(key)) {
                maxKey = Math.max(maxKey, Number(key));
            }
            else {
                allIntegerKeys = false;
            }
        });
        if (!exportFormat && allIntegerKeys && maxKey < 2 * numKeys) {
            // convert to array.
            const array = [];
            // eslint-disable-next-line guard-for-in
            for (const key in obj) {
                array[key] = obj[key];
            }
            return array;
        }
        else {
            if (exportFormat && !this.getPriority().isEmpty()) {
                obj['.priority'] = this.getPriority().val();
            }
            return obj;
        }
    }
    /** @inheritDoc */
    hash() {
        if (this.lazyHash_ === null) {
            let toHash = '';
            if (!this.getPriority().isEmpty()) {
                toHash +=
                    'priority:' +
                        priorityHashText(this.getPriority().val()) +
                        ':';
            }
            this.forEachChild(PRIORITY_INDEX, (key, childNode) => {
                const childHash = childNode.hash();
                if (childHash !== '') {
                    toHash += ':' + key + ':' + childHash;
                }
            });
            this.lazyHash_ = toHash === '' ? '' : sha1(toHash);
        }
        return this.lazyHash_;
    }
    /** @inheritDoc */
    stampLazyHash(hash) {
        if (this.lazyHash_ === null) {
            this.lazyHash_ = hash;
        }
    }
    /** @inheritDoc */
    getPredecessorChildName(childName, childNode, index) {
        const idx = this.resolveIndex_(index);
        if (idx) {
            const predecessor = idx.getPredecessorKey(new NamedNode(childName, childNode));
            return predecessor ? predecessor.name : null;
        }
        else {
            return this.children_.getPredecessorKey(childName);
        }
    }
    getFirstChildName(indexDefinition) {
        const idx = this.resolveIndex_(indexDefinition);
        if (idx) {
            const minKey = idx.minKey();
            return minKey && minKey.name;
        }
        else {
            return this.children_.minKey();
        }
    }
    getFirstChild(indexDefinition) {
        const minKey = this.getFirstChildName(indexDefinition);
        if (minKey) {
            return new NamedNode(minKey, this.children_.get(minKey));
        }
        else {
            return null;
        }
    }
    /**
     * Given an index, return the key name of the largest value we have, according to that index
     */
    getLastChildName(indexDefinition) {
        const idx = this.resolveIndex_(indexDefinition);
        if (idx) {
            const maxKey = idx.maxKey();
            return maxKey && maxKey.name;
        }
        else {
            return this.children_.maxKey();
        }
    }
    getLastChild(indexDefinition) {
        const maxKey = this.getLastChildName(indexDefinition);
        if (maxKey) {
            return new NamedNode(maxKey, this.children_.get(maxKey));
        }
        else {
            return null;
        }
    }
    forEachChild(index, action) {
        const idx = this.resolveIndex_(index);
        if (idx) {
            return idx.inorderTraversal(wrappedNode => {
                return action(wrappedNode.name, wrappedNode.node);
            });
        }
        else {
            return this.children_.inorderTraversal(action);
        }
    }
    getIterator(indexDefinition) {
        return this.getIteratorFrom(indexDefinition.minPost(), indexDefinition);
    }
    getIteratorFrom(startPost, indexDefinition) {
        const idx = this.resolveIndex_(indexDefinition);
        if (idx) {
            return idx.getIteratorFrom(startPost, key => key);
        }
        else {
            const iterator = this.children_.getIteratorFrom(startPost.name, NamedNode.Wrap);
            let next = iterator.peek();
            while (next != null && indexDefinition.compare(next, startPost) < 0) {
                iterator.getNext();
                next = iterator.peek();
            }
            return iterator;
        }
    }
    getReverseIterator(indexDefinition) {
        return this.getReverseIteratorFrom(indexDefinition.maxPost(), indexDefinition);
    }
    getReverseIteratorFrom(endPost, indexDefinition) {
        const idx = this.resolveIndex_(indexDefinition);
        if (idx) {
            return idx.getReverseIteratorFrom(endPost, key => {
                return key;
            });
        }
        else {
            const iterator = this.children_.getReverseIteratorFrom(endPost.name, NamedNode.Wrap);
            let next = iterator.peek();
            while (next != null && indexDefinition.compare(next, endPost) > 0) {
                iterator.getNext();
                next = iterator.peek();
            }
            return iterator;
        }
    }
    compareTo(other) {
        if (this.isEmpty()) {
            if (other.isEmpty()) {
                return 0;
            }
            else {
                return -1;
            }
        }
        else if (other.isLeafNode() || other.isEmpty()) {
            return 1;
        }
        else if (other === MAX_NODE) {
            return -1;
        }
        else {
            // Must be another node with children.
            return 0;
        }
    }
    withIndex(indexDefinition) {
        if (indexDefinition === KEY_INDEX ||
            this.indexMap_.hasIndex(indexDefinition)) {
            return this;
        }
        else {
            const newIndexMap = this.indexMap_.addIndex(indexDefinition, this.children_);
            return new ChildrenNode(this.children_, this.priorityNode_, newIndexMap);
        }
    }
    isIndexed(index) {
        return index === KEY_INDEX || this.indexMap_.hasIndex(index);
    }
    equals(other) {
        if (other === this) {
            return true;
        }
        else if (other.isLeafNode()) {
            return false;
        }
        else {
            const otherChildrenNode = other;
            if (!this.getPriority().equals(otherChildrenNode.getPriority())) {
                return false;
            }
            else if (this.children_.count() === otherChildrenNode.children_.count()) {
                const thisIter = this.getIterator(PRIORITY_INDEX);
                const otherIter = otherChildrenNode.getIterator(PRIORITY_INDEX);
                let thisCurrent = thisIter.getNext();
                let otherCurrent = otherIter.getNext();
                while (thisCurrent && otherCurrent) {
                    if (thisCurrent.name !== otherCurrent.name ||
                        !thisCurrent.node.equals(otherCurrent.node)) {
                        return false;
                    }
                    thisCurrent = thisIter.getNext();
                    otherCurrent = otherIter.getNext();
                }
                return thisCurrent === null && otherCurrent === null;
            }
            else {
                return false;
            }
        }
    }
    /**
     * Returns a SortedMap ordered by index, or null if the default (by-key) ordering can be used
     * instead.
     *
     */
    resolveIndex_(indexDefinition) {
        if (indexDefinition === KEY_INDEX) {
            return null;
        }
        else {
            return this.indexMap_.get(indexDefinition.toString());
        }
    }
}
ChildrenNode.INTEGER_REGEXP_ = /^(0|[1-9]\d*)$/;
class MaxNode extends ChildrenNode {
    constructor() {
        super(new SortedMap(NAME_COMPARATOR), ChildrenNode.EMPTY_NODE, IndexMap.Default);
    }
    compareTo(other) {
        if (other === this) {
            return 0;
        }
        else {
            return 1;
        }
    }
    equals(other) {
        // Not that we every compare it, but MAX_NODE is only ever equal to itself
        return other === this;
    }
    getPriority() {
        return this;
    }
    getImmediateChild(childName) {
        return ChildrenNode.EMPTY_NODE;
    }
    isEmpty() {
        return false;
    }
}
/**
 * Marker that will sort higher than any other snapshot.
 */
const MAX_NODE = new MaxNode();
Object.defineProperties(NamedNode, {
    MIN: {
        value: new NamedNode(MIN_NAME, ChildrenNode.EMPTY_NODE)
    },
    MAX: {
        value: new NamedNode(MAX_NAME, MAX_NODE)
    }
});
/**
 * Reference Extensions
 */
KeyIndex.__EMPTY_NODE = ChildrenNode.EMPTY_NODE;
LeafNode.__childrenNodeConstructor = ChildrenNode;
setMaxNode$1(MAX_NODE);
setMaxNode(MAX_NODE);

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const USE_HINZE = true;
/**
 * Constructs a snapshot node representing the passed JSON and returns it.
 * @param json - JSON to create a node for.
 * @param priority - Optional priority to use.  This will be ignored if the
 * passed JSON contains a .priority property.
 */
function nodeFromJSON(json, priority = null) {
    if (json === null) {
        return ChildrenNode.EMPTY_NODE;
    }
    if (typeof json === 'object' && '.priority' in json) {
        priority = json['.priority'];
    }
    util.assert(priority === null ||
        typeof priority === 'string' ||
        typeof priority === 'number' ||
        (typeof priority === 'object' && '.sv' in priority), 'Invalid priority type found: ' + typeof priority);
    if (typeof json === 'object' && '.value' in json && json['.value'] !== null) {
        json = json['.value'];
    }
    // Valid leaf nodes include non-objects or server-value wrapper objects
    if (typeof json !== 'object' || '.sv' in json) {
        const jsonLeaf = json;
        return new LeafNode(jsonLeaf, nodeFromJSON(priority));
    }
    if (!(json instanceof Array) && USE_HINZE) {
        const children = [];
        let childrenHavePriority = false;
        const hinzeJsonObj = json;
        each(hinzeJsonObj, (key, child) => {
            if (key.substring(0, 1) !== '.') {
                // Ignore metadata nodes
                const childNode = nodeFromJSON(child);
                if (!childNode.isEmpty()) {
                    childrenHavePriority =
                        childrenHavePriority || !childNode.getPriority().isEmpty();
                    children.push(new NamedNode(key, childNode));
                }
            }
        });
        if (children.length === 0) {
            return ChildrenNode.EMPTY_NODE;
        }
        const childSet = buildChildSet(children, NAME_ONLY_COMPARATOR, namedNode => namedNode.name, NAME_COMPARATOR);
        if (childrenHavePriority) {
            const sortedChildSet = buildChildSet(children, PRIORITY_INDEX.getCompare());
            return new ChildrenNode(childSet, nodeFromJSON(priority), new IndexMap({ '.priority': sortedChildSet }, { '.priority': PRIORITY_INDEX }));
        }
        else {
            return new ChildrenNode(childSet, nodeFromJSON(priority), IndexMap.Default);
        }
    }
    else {
        let node = ChildrenNode.EMPTY_NODE;
        each(json, (key, childData) => {
            if (util.contains(json, key)) {
                if (key.substring(0, 1) !== '.') {
                    // ignore metadata nodes.
                    const childNode = nodeFromJSON(childData);
                    if (childNode.isLeafNode() || !childNode.isEmpty()) {
                        node = node.updateImmediateChild(key, childNode);
                    }
                }
            }
        });
        return node.updatePriority(nodeFromJSON(priority));
    }
}
setNodeFromJSON(nodeFromJSON);

/**
 * @license
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Client-side persistence of the server cache, in the spirit of the mobile
 * SDKs' setPersistenceEnabled(true): the SDK itself stores what the server
 * sent for each listened root and restores it on the next startup, so a
 * reload serves cached data immediately and revalidates with the server via
 * the hash protocol (see ServerCacheSeed) instead of re-downloading.
 *
 * STORAGE MODEL — one manifest plus immutable fixed-target range records.
 * Each persisted root stores:
 *
 *   - a MANIFEST (`<prefix>|<path>`): revision, timestamps, auth scope, and
 *     ordered stable ranges `{recordId, post, hash, size}`. It is the complete
 *     compound-listen descriptor and reads in milliseconds.
 *   - one structured-clone RANGE record per stable interval
 *     (`<prefix>|<path>#range:<recordId>`): start/end markers plus a sparse
 *     export-format fragment containing exactly that interval's leaves.
 *
 * Dirty/split/merged ranges receive new immutable ids; clean ranges keep the
 * exact prior record. New records and the manifest commit in ONE transaction.
 * An optimistic manifest-revision check prevents a stale tab from reusing a
 * different tab's records; on conflict it retries with a self-contained full
 * range generation. Retired ids are deleted in the commit, and a guarded
 * key-only GC plus the expiry sweep reclaim older crash/legacy/orphan ids.
 * No full-root val(true) or full-root structured clone occurs on a steady
 * state flush.
 *
 * MANIFEST-FIRST BOOT. Because the manifest alone carries the protocol
 * hashes, a warm boot reads it first and hands the hashes to the caller
 * (see restoreForListen's onManifest) so the range listen can be sent
 * IMMEDIATELY — the server round-trip overlaps the tree record's read and
 * Node construction. A warm boot computes NO hashes.
 *
 * SELF-CONSISTENT GENERATIONS. Range hashes are maintained at write time,
 * inside the flush: an identity-diff of the immutable trees marks the
 * ranges a change dirtied, only those ranges are re-serialized and
 * re-hashed (between preserved boundary posts — see CompoundHash's stable
 * ranges), and the manifest commits with hashes that exactly describe the
 * tree record beside it. Clean ranges carry over without their bytes ever
 * being read. Stale hashes are never persisted: a stale hash could falsely
 * match a reverted server range, the one corruption the range handshake
 * cannot self-heal.
 *
 * WRITE POLICY — single-flight coalescing flush. The first change after a
 * committed generation arms a NON-restarting timer (writeDelayMs, default
 * PERSISTENCE_WRITE_DEBOUNCE_MS); later changes coalesce into the pending
 * window without resetting it. At most one flush is ever in flight; changes
 * landing mid-flush only re-arm the next window. Effective cadence is
 * max(delay, flush duration) — natural backpressure, bounded staleness.
 * Cache writes are never awaited by the UI, certification, or navigation.
 *
 * All storage failures degrade to cold loads; nothing here may ever break
 * the live connection.
 */
const STORE = 'firebase-server-cache';
// Version 3 invalidated pre-chunking caches; version 8 is current. The
// upgrade clears the store inside IndexedDB without materializing old
// (potentially huge) values into JavaScript memory.
const PERSISTENCE_DB_VERSION = 9;
// Format 11: immutable fixed-target range records + one stable-range
// manifest. Earlier monolithic/chunked formats restore as misses and are
// reclaimed without materializing their payloads.
const PERSISTENCE_FORMAT_VERSION = 11;
const PERSISTENCE_SCHEMA_MARKER_KEY = 'firebase-database-persistence-schema';
function readSchemaMarker() {
    if (typeof localStorage === 'undefined') {
        // Node/tests and non-browser embeddings: let IndexedDB itself decide.
        return true;
    }
    try {
        return (localStorage.getItem(PERSISTENCE_SCHEMA_MARKER_KEY) ===
            String(PERSISTENCE_DB_VERSION));
    }
    catch (e) {
        // Storage-disabled browsers still get best-effort persistence.
        return true;
    }
}
function writeSchemaMarker() {
    if (typeof localStorage === 'undefined') {
        return;
    }
    try {
        localStorage.setItem(PERSISTENCE_SCHEMA_MARKER_KEY, String(PERSISTENCE_DB_VERSION));
    }
    catch (e) {
        // Best-effort optimization only.
    }
}
/**
 * Records older than this are dropped (staleness makes a full download
 * likely anyway; bounded retention caps disk use).
 */
const PERSISTENCE_MAX_AGE_MS = 14 * 24 * 60 * 60 * 1000;
const PERSISTENCE_MAX_CACHE_BYTES = 100 * 1024 * 1024;
const PERSISTENCE_MAX_PRUNABLE_ROOTS = 1000;
const PERSISTENCE_PRUNE_TARGET_RATIO = 0.8;
const PERSISTENCE_MAX_CONCURRENT_RESTORES = 4;
/**
 * Default width of the flush coalescing window (see the write policy in the
 * file header). Configurable per manager (writeDelayMs). The window is
 * non-restarting: a root that churns continuously still flushes every
 * window, and never more often than one in-flight flush allows.
 * @internal
 */
const PERSISTENCE_WRITE_DEBOUNCE_MS = 15000;
/**
 * Constant canonical-text target for one persisted/hash range. Boundaries are
 * stable across generations and only dirty runs reconsult this target. The
 * constructor accepts an override so 128/256/512 KiB can be benchmarked
 * without changing protocol code.
 * @internal
 */
const PERSISTENCE_RANGE_TARGET_BYTES = 256 * 1024;
/**
 * Maximum gap with NO restore progress before the listen attaches unseeded.
 * Progress (a completed manifest or tree read) resets this budget. The same
 * bound applies to each IndexedDB open/transaction, so a request that fires
 * neither success nor error can never hold the live listen forever.
 */
const PERSISTENCE_RESTORE_TIMEOUT_MS = 8000;
/**
 * How long a completed optimistic peek's decoded tree stays retained for its
 * real (authenticated) listener AFTER the app has confirmed the auth scope.
 * From that moment the listener is normally milliseconds away, so a short
 * grace suffices.
 */
const PERSISTENCE_PEEK_HANDOFF_MS = 30000;
/**
 * The same retention while the auth scope is only PRIMED by the peek itself
 * (getPersistedValue's trusted expected identity) and real app auth has not
 * confirmed it yet. Auth hydration is local but can be arbitrarily slow on a
 * loaded profile (service-worker congestion, IndexedDB contention); racing it
 * with a short wall-clock timer silently defeats the one-decode-per-boot
 * handoff exactly on the machines that need it most — the listener then
 * re-reads and re-decodes the full tree while the peek's copy is still alive,
 * doubling peak boot memory. A mismatching or signed-out identity still
 * clears the retained read IMMEDIATELY via setAuthScope; this long backstop
 * only bounds the true leak case (auth never resolving at all), where the
 * page is stuck on its auth spinner anyway.
 */
const PERSISTENCE_PEEK_PREAUTH_HANDOFF_MS = 5 * 60 * 1000;
/**
 * A stored tree whose content hasn't changed is left untouched by flushes
 * until its manifest is this old, then the manifest alone is rewritten with
 * a fresh timestamp (the tree record stays put) — so a tree that never
 * changes but is used daily never ages into the expiry cutoff.
 * @internal
 */
const PERSISTENCE_REFRESH_AGE_MS = 24 * 60 * 60 * 1000;
/**
 * How long after startup the expiry sweep runs. Deferred so the sweep's
 * store-wide transaction can never delay the boot restores, which IndexedDB
 * would otherwise queue behind it.
 * @internal
 */
const PERSISTENCE_SWEEP_DELAY_MS = 15000;
/**
 * Cap on precisely-accumulated changed paths per root between flushes.
 * Matches collectChangedSubtreePaths' default budget: past it the identity
 * diff is the cheaper, equally-correct answer.
 */
const MAX_ACCUMULATED_CHANGED_PATHS = 512;
/**
 * Counters for observing persistence effectiveness.
 * @internal
 */
const persistenceStats = {
    restoredRoots: [],
    restoreMisses: [],
    writeThroughs: 0,
    rangesHashed: 0,
    rangesReused: 0,
    evictions: 0,
    storageFailures: 0,
    events: []
};
function recordPersistenceEvent(path, event, detail) {
    persistenceStats.events.push({ at: Date.now(), path, event, detail });
    if (persistenceStats.events.length > 100) {
        persistenceStats.events.splice(0, persistenceStats.events.length - 100);
    }
}
const RANGE_KEY_INFIX = '#range:';
function wireCompoundHashFromRanges(ranges) {
    const posts = [];
    const hashes = [];
    for (const range of ranges) {
        posts.push(range.post);
        hashes.push(range.hash);
    }
    // The empty tail hash lets the server append past the last post.
    hashes.push('');
    return { posts, hashes };
}
/**
 * Hashes the dirty ranges' serialized texts — WebCrypto where available
 * (native, off the JavaScript thread, ~40x the JS implementation), the
 * synchronous JS sha1 otherwise (Node without webcrypto, insecure contexts).
 * Falls back wholesale on any WebCrypto failure: a flush must never fail on
 * the choice of hash backend.
 */
function digestRangeTexts(texts) {
    if (texts.length === 0) {
        return Promise.resolve([]);
    }
    const subtle = typeof crypto !== 'undefined' &&
        typeof TextEncoder !== 'undefined' &&
        crypto.subtle &&
        typeof crypto.subtle.digest === 'function'
        ? crypto.subtle
        : null;
    if (subtle === null) {
        return Promise.resolve(texts.map(sha1));
    }
    const encoder = new TextEncoder();
    return Promise.all(texts.map(text => subtle
        .digest('SHA-1', encoder.encode(text))
        .then(digest => util.base64.encodeByteArray(new Uint8Array(digest))))).catch(() => texts.map(sha1));
}
/**
 * Unions two disjoint sparse export fragments without range-deletion
 * semantics. RangeMerge is correct for authoritative server deltas, where an
 * omitted value inside the interval means delete; persisted fragments instead
 * partition one complete snapshot, so omission means "owned by another
 * record". In particular this preserves a prioritized leaf at the exclusive
 * boundary of the following range.
 */
function mergePersistedFragment(base, fragment) {
    if (base.isEmpty()) {
        return fragment;
    }
    if (fragment.isEmpty()) {
        return base;
    }
    if (fragment.isLeafNode()) {
        return fragment;
    }
    let result = base;
    fragment.forEachChild(KEY_INDEX, (key, child) => {
        result = result.updateImmediateChild(key, mergePersistedFragment(result.getImmediateChild(key), child));
    });
    if (!fragment.getPriority().isEmpty()) {
        result = result.updatePriority(fragment.getPriority());
    }
    return result;
}
/**
 * Structural validation of a stored manifest: current format, string
 * revision, and a non-empty, well-formed range list. Shared by the full
 * restore read and the manifest-only baseline adoption.
 */
function structurallyValidManifest(manifest) {
    return (manifest !== null &&
        manifest !== undefined &&
        manifest.formatVersion === PERSISTENCE_FORMAT_VERSION &&
        typeof manifest.revision === 'string' &&
        typeof manifest.updatedAt === 'number' &&
        typeof manifest.hash === 'string' &&
        Array.isArray(manifest.ranges) &&
        manifest.ranges.length > 0 &&
        manifest.ranges.every(range => range !== null &&
            typeof range === 'object' &&
            typeof range.recordId === 'string' &&
            range.recordId.length > 0 &&
            typeof range.post === 'string' &&
            typeof range.hash === 'string' &&
            typeof range.size === 'number'));
}
/** Heartbeat cadence while holding the writer lease. @internal */
const LEASE_HEARTBEAT_MS = 20000;
/**
 * A holder whose heartbeat is older than this is considered suspended and
 * may be stolen from. Must comfortably exceed the worst legitimate
 * heartbeat gap (Chrome background timer clamping is 60s). @internal
 */
const LEASE_STALE_MS = 120000;
function defaultHeartbeatStore() {
    try {
        if (typeof localStorage !== 'undefined' && localStorage !== null) {
            return localStorage;
        }
    }
    catch (e) {
        // Access itself can throw (storage-disabled documents).
    }
    return null;
}
function webLocks() {
    if (typeof navigator === 'undefined') {
        return null;
    }
    const locks = navigator.locks;
    return locks && typeof locks.request === 'function' ? locks : null;
}
class PersistenceManager {
    isAuthScopeConfigured() {
        return this.authScopeConfigured_;
    }
    /**
     * The current identity-scope generation — bumped by every setAuthScope
     * that changes the scope. Callers whose continuation spans an await after
     * peek() resolves capture this before the wait and compare after, so a
     * scope switch mid-continuation invalidates the result exactly like
     * peek()'s own resolution-time check. @internal
     */
    authGeneration() {
        return this.authGeneration_;
    }
    /**
     * True while THE read that decoded `node` is still RETAINED at this root
     * for a future listener join (see readRecord_'s retainAfterResolve) — the
     * only window in which materialization stamps have a consumer. Identity-
     * bound on purpose: a path-only check would also pass for a REPLACEMENT
     * read (the original consumed by a listener mid-walk, a second peek
     * retained since), and stamps would then ride the consumed read's live
     * nodes with no replay ever taking them — a session-long pinned copy of
     * each subtree. False once the read was consumed, expired, superseded,
     * or the manager disposed. @internal
     */
    hasRetainedPeek(pathString, node) {
        const entry = this.activeReads_.get(pathString);
        return entry?.retainAfterResolve === true && entry.resolvedNode === node;
    }
    setAuthScope(scope, confirmedByApp = true) {
        const changed = !this.authScopeConfigured_ || scope !== this.authScope_;
        this.authScopeConfigured_ = true;
        if (confirmedByApp) {
            if (!this.authScopeConfirmed_) {
                this.authScopeConfirmed_ = true;
                if (!changed) {
                    // Real auth confirmed the exact scope a pre-auth peek primed: the
                    // handoff window is open NOW. Retained reads waiting under the
                    // long pre-auth backstop drop to the short post-auth grace —
                    // counted from this moment, not from when the read finished.
                    this.rearmRetainedReads_();
                }
            }
        }
        else if (changed) {
            // A prime that CHANGES the scope describes an identity the app has not
            // confirmed yet; its retentions must run under the pre-auth backstop.
            this.authScopeConfirmed_ = false;
        }
        if (!changed) {
            return false;
        }
        this.authGeneration_++;
        for (const timer of this.writeTimers_.values()) {
            clearTimeout(timer);
        }
        this.writeTimers_.clear();
        this.flushPending_.clear();
        this.writesDeferredUntilRestores_.clear();
        this.latest_.clear();
        this.lastFlush_.clear();
        this.changedSinceFlush_.clear();
        // Clear retention timers BEFORE dropping the map: a pending cleanupTimer's
        // closure otherwise keeps the entry (and its decoded tree) alive until it
        // fires — minutes, under the pre-auth backstop.
        for (const read of this.activeReads_.values()) {
            if (read.cleanupTimer !== null) {
                clearTimeout(read.cleanupTimer);
            }
        }
        this.activeReads_.clear();
        this.restoreReasons_.clear();
        this.authScope_ = scope;
        recordPersistenceEvent('*', 'auth-scope-change', scope ? 'signed-in' : 'signed-out');
        return true;
    }
    constructor(prefix_, idbFactory_ = util.isIndexedDBAvailable()
        ? indexedDB
        : null, schemaKnownCurrent_ = readSchemaMarker(), operationTimeoutMs_ = PERSISTENCE_RESTORE_TIMEOUT_MS, cacheMaxBytes_ = PERSISTENCE_MAX_CACHE_BYTES, writeDelayMs_ = PERSISTENCE_WRITE_DEBOUNCE_MS, rangeTargetBytes_ = PERSISTENCE_RANGE_TARGET_BYTES, peekHandoffMs_ = PERSISTENCE_PEEK_HANDOFF_MS, peekPreAuthHandoffMs_ = PERSISTENCE_PEEK_PREAUTH_HANDOFF_MS, leaseHeartbeatMs_ = LEASE_HEARTBEAT_MS, leaseStaleMs_ = LEASE_STALE_MS, heartbeatStore = defaultHeartbeatStore()) {
        this.prefix_ = prefix_;
        this.idbFactory_ = idbFactory_;
        this.schemaKnownCurrent_ = schemaKnownCurrent_;
        this.operationTimeoutMs_ = operationTimeoutMs_;
        this.cacheMaxBytes_ = cacheMaxBytes_;
        this.writeDelayMs_ = writeDelayMs_;
        this.rangeTargetBytes_ = rangeTargetBytes_;
        this.peekHandoffMs_ = peekHandoffMs_;
        this.peekPreAuthHandoffMs_ = peekPreAuthHandoffMs_;
        this.leaseHeartbeatMs_ = leaseHeartbeatMs_;
        this.leaseStaleMs_ = leaseStaleMs_;
        this.db_ = null;
        /** Roots explicitly selected by the application (keepSynced semantics). */
        this.persistentRoots_ = new Map();
        /** Active selected roots currently flowing through persistence. */
        this.trackedRoots_ = new Set();
        /**
         * Latest server tree per root. Revisions come from a single manager-wide
         * counter, so no revision is ever reissued — an in-flight flush can never
         * collide with a tree that arrived after its root was evicted and
         * re-tracked.
         */
        /**
         * Changed subtree paths accumulated since the flush baseline
         * (lastFlush_.rootNode), keyed by root. The server names the exact path of
         * every ordinary data push, so steady-state flushes can mark dirty ranges
         * from this list directly instead of re-discovering the same information
         * with a full-width identity diff of two ~60MB trees (the diff's sorted
         * child merges were the single largest CPU slice of a flush).
         *
         * `null` = imprecise: an update arrived whose changed path is unknown or
         * at/above the root (range merges, listen completions, foreign rebases) —
         * the flush falls back to the identity diff, which is exactly today's
         * behavior. Entries reset to [] whenever lastFlush_ gains a fresh baseline.
         */
        this.changedSinceFlush_ = new Map();
        this.latest_ = new Map();
        /**
         * What IndexedDB currently holds per root (see FlushedState) — the basis
         * for identity-diff dirty marking and no-op flushes.
         */
        this.lastFlush_ = new Map();
        /**
         * Distinguishes this manager's write tokens from every other tab's and
         * session's — numeric counters restart at zero on reload, which would let
         * a new data write pair up with a write token from another manager
         * instance.
         */
        this.instanceId_ = Math.random().toString(36).slice(2, 10);
        this.writeCounter_ = 0;
        /**
         * The single-flight coalescing window per root: `timer` is the pending
         * (non-restarting) window; `rearm` marks a change that landed while the
         * root's queue was busy flushing — exactly one follow-up window is armed
         * when the queue drains, however many changes landed meanwhile.
         */
        this.writeTimers_ = new Map();
        this.flushPending_ = new Set();
        /** In-flight storage operations per root (see enqueue_). */
        this.queues_ = new Map();
        /**
         * One physical IndexedDB decode per root. The pre-auth peek and the
         * authenticated listener often overlap; without coalescing they each read
         * the tree record and rebuilt the same large Node tree concurrently.
         */
        this.activeReads_ = new Map();
        this.restoreReasons_ = new Map();
        /** One writer lease per TRACKED root (see the WriterLease notes). */
        this.writerLeases_ = new Map();
        /**
         * True while the repo's network is deliberately interrupted (goOffline /
         * repoInterrupt). LIVENESS is not ELIGIBILITY: an offline tab's JS keeps
         * running and heartbeating, but its server cache is frozen — if it kept
         * its leases (or the fail-open gate), an online tab receiving newer
         * server state could never persist it, and storage would hold the
         * disconnected tab's stale tree. While suspended this manager holds no
         * leases, queues none, steals none, and the write gate is CLOSED even
         * where Web Locks don't exist — a stale flush from an offline tab must
         * not overwrite an online writer's fresh generation in the CAS-only
         * environment either. Roots stay tracked; trees stay in memory; resume
         * re-acquires and the armed write windows flush whatever was pending.
         * (Deliberate-offline only: an involuntary network drop hits every tab
         * on the machine alike — no online follower exists to starve — and the
         * connection self-reconnects, so leases follow repoInterrupt/repoResume,
         * not transient socket state.)
         */
        this.networkSuspended_ = false;
        /** One timer for all leases: held → heartbeat, requested → steal check. */
        this.leaseTimer_ = null;
        /**
         * Identifies THIS manager's heartbeat stamps (`<ms>|<token>`), so a
         * clean release can remove its own stamp without ever deleting a
         * successor's. Without cleanup, a departed holder's stamp lingers: a
         * later holder whose storage cannot WRITE never overwrites it, and a
         * follower that can READ sees a PRESENT-but-stale heartbeat — and
         * steals from a perfectly healthy writer, contradicting the documented
         * page-death fallback for storage-denied holders.
         */
        this.heartbeatToken_ = Date.now().toString(36) + Math.random().toString(36).slice(2);
        this.activeRestoreCount_ = 0;
        this.restoreQueue_ = [];
        this.writesDeferredUntilRestores_ = new Set();
        this.sweepTimer_ = null;
        this.sweepInFlight_ = null;
        this.disposed_ = false;
        this.authScope_ = null;
        this.authScopeConfigured_ = false;
        /**
         * True once the APP's auth integration (setPersistenceAuthScope) has
         * confirmed the scope — as opposed to a pre-auth peek merely priming it
         * with a trusted expected identity. Selects the peek-retention budget:
         * a primed-only scope holds the long pre-auth backstop, a confirmed one
         * the short handoff grace (see PERSISTENCE_PEEK_PREAUTH_HANDOFF_MS).
         */
        this.authScopeConfirmed_ = false;
        this.authGeneration_ = 0;
        this.heartbeatStore_ = heartbeatStore;
        if (!this.schemaKnownCurrent_) {
            // Do not put the cold server listen behind a potentially slow Safari
            // version-change transaction. Migration runs in the background; restore
            // APIs return a cache miss synchronously for this boot.
            void this.open_();
        }
    }
    rebindTo(prefix) {
        const scope = this.authScope_;
        const selectedRoots = [...this.persistentRoots_];
        this.dispose();
        const rebound = new PersistenceManager(prefix, this.idbFactory_, this.schemaKnownCurrent_, this.operationTimeoutMs_, this.cacheMaxBytes_, this.writeDelayMs_, this.rangeTargetBytes_, this.peekHandoffMs_, this.peekPreAuthHandoffMs_, this.leaseHeartbeatMs_, this.leaseStaleMs_, this.heartbeatStore_);
        rebound.networkSuspended_ = this.networkSuspended_;
        if (this.authScopeConfigured_) {
            rebound.setAuthScope(scope, this.authScopeConfirmed_);
        }
        for (const [pathString, count] of selectedRoots) {
            rebound.persistentRoots_.set(pathString, count);
        }
        return rebound;
    }
    setPersistentPath(pathString, enabled) {
        const current = this.persistentRoots_.get(pathString) ?? 0;
        if (enabled) {
            this.persistentRoots_.set(pathString, current + 1);
        }
        else if (current <= 1) {
            this.persistentRoots_.delete(pathString);
            this.untrack(pathString);
        }
        else {
            this.persistentRoots_.set(pathString, current - 1);
        }
    }
    isPersistentPath(pathString) {
        return this.persistentRoots_.has(pathString);
    }
    /**
     * Marks a root as persistence-managed; write-throughs only run for
     * tracked roots (and their descendants' updates).
     */
    track(pathString) {
        this.trackedRoots_.add(pathString);
        this.ensureWriterLease_(pathString);
    }
    /**
     * Follows the repo's DELIBERATE network state (repoInterrupt/repoResume,
     * i.e. goOffline/goOnline — see networkSuspended_). Suspending returns
     * every lease so an online tab becomes each root's writer; roots stay
     * tracked and trees stay in memory. Resuming re-queues politely (never
     * steals) and re-arms the write windows, so data seen before or during
     * the offline stretch persists once this tab is eligible again — in the
     * lock-less environment the re-armed window is the whole story, since
     * eligibility there is only the gate.
     */
    setNetworkSuspended(suspended) {
        if (this.networkSuspended_ === suspended || this.disposed_) {
            return;
        }
        this.networkSuspended_ = suspended;
        if (suspended) {
            this.releaseAllWriterLeases_();
            return;
        }
        for (const pathString of this.trackedRoots_) {
            this.ensureWriterLease_(pathString);
            this.armWriteWindowIfPending_(pathString);
        }
    }
    /**
     * True when this manager may write the root: it holds the root's writer
     * lease, or leases are unenforceable here (no Web Locks, or the root has
     * no lease entry — the manifest CAS remains the correctness backstop).
     */
    holdsWriterLease_(pathString) {
        if (this.networkSuspended_) {
            // Ineligible, not merely lease-less: with no lease entry the gate
            // would fail OPEN, and an offline tab's adopt-then-restage would
            // overwrite an online writer's fresh generation with stale data.
            return false;
        }
        const lease = this.writerLeases_.get(pathString);
        return lease === undefined ? true : lease.state === 'held';
    }
    /** The root's shared heartbeat key. */
    heartbeatKey_(pathString) {
        return 'firebase-database-persistence-writer|' + this.key_(pathString);
    }
    writeHeartbeat_(pathString) {
        try {
            this.heartbeatStore_?.setItem(this.heartbeatKey_(pathString), Date.now() + '|' + this.heartbeatToken_);
        }
        catch (e) {
            // Storage that exists but THROWS (storage-disabled documents, quota)
            // means this manager cannot participate in the heartbeat protocol at
            // all: keep trying and every holder stamp fails silently while
            // followers keep reading whatever is there. Disable the channel for
            // this manager's lifetime — takeover degrades to page death, which is
            // the documented no-shared-storage mode.
            this.heartbeatStore_ = null;
        }
    }
    readHeartbeat_(pathString) {
        try {
            const raw = this.heartbeatStore_?.getItem(this.heartbeatKey_(pathString));
            // `<ms>|<token>` (and bare `<ms>` from older stamps) both parse.
            const value = raw === null || raw === undefined
                ? NaN
                : Number(String(raw).split('|')[0]);
            return isNaN(value) ? 0 : value;
        }
        catch (e) {
            // See writeHeartbeat_: a throwing store is a dead channel, and a
            // reader that cannot see heartbeats must never steal (the tick's
            // null-store guard makes this permanent, not just this tick).
            this.heartbeatStore_ = null;
            return 0;
        }
    }
    /**
     * One tick, role by lease state: a holder proves liveness (heartbeat); a
     * queued follower checks the holder's liveness and STEALS the lock when
     * the heartbeat is PRESENT but stale — the holder stamped once (every
     * holder stamps at grant) and then went silent: frozen, cached,
     * suspended, or wedged, and would otherwise starve every live tab's
     * writes for as long as it existed. An ABSENT heartbeat never justifies a
     * steal: it means the liveness protocol is not operating for this lock —
     * the holder's storage throws, the stamp was cleared, or nothing was
     * ever granted — and stealing on silence alone would take the lock from
     * a perfectly healthy writer over and over (each stolen holder re-queues
     * and, reading the same absence, steals right back). Without a readable
     * heartbeat, takeover degrades to page death — the documented
     * no-shared-storage mode. The request-time anchor additionally prevents
     * stealing within the staleness budget of first joining the queue.
     */
    onLeaseTick_() {
        if (this.disposed_) {
            return;
        }
        for (const [pathString, lease] of this.writerLeases_) {
            if (lease.state === 'held') {
                this.writeHeartbeat_(pathString);
                continue;
            }
            if (this.heartbeatStore_ === null) {
                return;
            }
            const heartbeat = this.readHeartbeat_(pathString);
            if (heartbeat <= 0) {
                continue;
            }
            const freshest = Math.max(heartbeat, lease.requestedAt);
            if (Date.now() - freshest > this.leaseStaleMs_) {
                this.requestWriterLease_(pathString, true);
            }
        }
    }
    /** Requests the root's writer lease once (idempotent per root). */
    ensureWriterLease_(pathString) {
        if (this.writerLeases_.has(pathString) ||
            this.disposed_ ||
            this.networkSuspended_) {
            return;
        }
        const locks = webLocks();
        if (locks === null) {
            return;
        }
        this.requestWriterLease_(pathString, false);
        if (this.leaseTimer_ === null && this.writerLeases_.size > 0) {
            this.leaseTimer_ = setInterval(() => {
                this.onLeaseTick_();
            }, this.leaseHeartbeatMs_);
            this.leaseTimer_.unref?.();
        }
    }
    /**
     * Puts a lease request for the root in the browser's queue, superseding
     * any current one (`steal` preempts a stale holder; see onLeaseTick_).
     */
    requestWriterLease_(pathString, steal) {
        const locks = webLocks();
        if (locks === null) {
            return;
        }
        const previous = this.writerLeases_.get(pathString);
        const lease = {
            state: 'requested',
            release: null,
            // The Web Locks spec FORBIDS combining `signal` with `steal`
            // (NotSupportedError — verified in Chrome: the request rejects
            // immediately and no steal happens). A steal needs no abort path
            // anyway: it is granted almost at once, and a steal that lands after
            // this lease was superseded/disposed is handed straight back by the
            // grant callback's identity check.
            controller: !steal && typeof AbortController !== 'undefined'
                ? new AbortController()
                : null,
            requestedAt: Date.now()
        };
        this.writerLeases_.set(pathString, lease);
        // Replace-then-abort: the superseded request's rejection sees a
        // different current lease and is a no-op.
        if (previous !== undefined && previous.state === 'requested') {
            previous.controller?.abort();
        }
        const options = { mode: 'exclusive' };
        if (lease.controller !== null) {
            options.signal = lease.controller.signal;
        }
        if (steal) {
            options.steal = true;
        }
        const onSettled = (failed) => {
            if (this.writerLeases_.get(pathString) !== lease) {
                return; // superseded or released: nothing to do
            }
            if (lease.state === 'held') {
                // A held lock's request promise only settles early when another
                // tab STOLE it (a stale-heartbeat takeover while this page was
                // suspended, or a lock-manager failure treated the same way). Stop
                // writing at once and re-queue politely — never steal back
                // unprompted; any stale baseline reconciles through the flush CAS.
                //
                // Settle the STOLEN callback first: the UA keeps the holder
                // callback pending until the promise it returned settles, and
                // re-queueing replaces the map entry, so no later release or
                // dispose could ever reach this resolver again. Left unsettled,
                // every steal leaks one pending callback — whose closure retains
                // this manager (and, once disposed, its baselines) indefinitely.
                lease.release?.();
                lease.release = null;
                this.requestWriterLease_(pathString, false);
                return;
            }
            if (failed) {
                // Queued-request failure (not a supersede — those hit the identity
                // guard above): fail open rather than never persisting, and re-arm
                // the write window a skipped flush may have consumed.
                this.writerLeases_.delete(pathString);
                this.stopLeaseTimerIfIdle_();
                this.armWriteWindowIfPending_(pathString);
            }
        };
        try {
            void locks
                .request(this.writerLeaseName_(pathString), options, () => {
                if (this.writerLeases_.get(pathString) !== lease || this.disposed_) {
                    // Superseded/released/disposed while queued: hand the lock
                    // straight back so the next tab's request is granted.
                    return Promise.resolve();
                }
                lease.state = 'held';
                this.writeHeartbeat_(pathString);
                // Writes for this root were skipped while another tab held its
                // lease; whatever is pending in memory enters the ordinary write
                // window now. A stale baseline (the old holder committed)
                // resolves through the flush CAS + adoptCommittedBaseline_,
                // exactly once.
                this.armWriteWindowIfPending_(pathString);
                return new Promise(resolve => {
                    lease.release = resolve;
                });
            })
                .then(() => onSettled(false), () => onSettled(true));
        }
        catch (e) {
            // A synchronously-throwing request() must not break the listen path
            // that called track().
            onSettled(true);
        }
    }
    writerLeaseName_(pathString) {
        return 'firebase-database-persistence-write|' + this.key_(pathString);
    }
    /**
     * Removes THIS manager's own heartbeat stamp (token-checked, so a
     * successor's stamp is never deleted). The get→remove pair is not
     * atomic; the benign worst case is deleting a successor stamp written
     * in between — absence never justifies a steal, and the successor
     * re-stamps on its next tick. A crashed holder never runs this, so its
     * stamp can linger: a follower may then steal once from a write-denied
     * successor — accepted residual; the stealer stamps and it stabilizes.
     */
    clearOwnHeartbeat_(pathString) {
        const store = this.heartbeatStore_;
        if (store === null || typeof store.removeItem !== 'function') {
            return;
        }
        try {
            const key = this.heartbeatKey_(pathString);
            const raw = store.getItem(key);
            if (typeof raw === 'string' && raw.endsWith('|' + this.heartbeatToken_)) {
                store.removeItem(key);
            }
        }
        catch (e) {
            this.heartbeatStore_ = null;
        }
    }
    /** Returns the root's writer lease to the browser (idempotent). */
    releaseWriterLease_(pathString) {
        const lease = this.writerLeases_.get(pathString);
        if (lease === undefined) {
            return;
        }
        this.writerLeases_.delete(pathString);
        this.stopLeaseTimerIfIdle_();
        if (lease.state === 'held') {
            // Clean handoff: take the stamp with us, so a successor that cannot
            // write storage is judged by ABSENCE (page-death handoff), not by
            // our lingering, eventually-stale stamp (see heartbeatToken_).
            this.clearOwnHeartbeat_(pathString);
            lease.release?.();
        }
        else {
            lease.controller?.abort();
        }
    }
    /**
     * Cleanup-completion rule shared by untrack paths: return the root's
     * lease unless the root was re-tracked meanwhile — the new listen owns
     * it now.
     */
    releaseWriterLeaseIfUntracked_(pathString) {
        if (!this.trackedRoots_.has(pathString)) {
            this.releaseWriterLease_(pathString);
        }
    }
    /** Returns every lease (dispose). */
    releaseAllWriterLeases_() {
        for (const pathString of [...this.writerLeases_.keys()]) {
            this.releaseWriterLease_(pathString);
        }
    }
    /**
     * A tab tracking nothing must neither heartbeat nor evaluate steals: the
     * tick stops with the last lease and restarts with the next track().
     */
    stopLeaseTimerIfIdle_() {
        if (this.writerLeases_.size === 0 && this.leaseTimer_ !== null) {
            clearInterval(this.leaseTimer_);
            this.leaseTimer_ = null;
        }
    }
    /**
     * The root's last listen stopped. When a live tracked ancestor covers the
     * root, its record — which contains this subtree and keeps flushing — is
     * the one future sessions should restore, so the child's own record is
     * deleted rather than left to shadow it. Otherwise any pending
     * write-through is flushed so IndexedDB holds the final tree for the next
     * session. Either way the in-memory copies are released — only live
     * listens need them.
     */
    untrack(pathString) {
        if (!this.trackedRoots_.has(pathString)) {
            return;
        }
        this.trackedRoots_.delete(pathString);
        const timer = this.writeTimers_.get(pathString);
        if (timer) {
            clearTimeout(timer);
            this.writeTimers_.delete(pathString);
        }
        this.flushPending_.delete(pathString);
        if (this.trackedRootFor(pathString) !== null) {
            // Housekeeping delete (the covering ancestor's record is the one
            // future sessions should restore): guarded to the one generation
            // this manager itself verified or wrote — a revision-NAMED delete is
            // safe without lock ownership by construction, because it can never
            // remove a successor generation some other writer committed. An
            // ADOPTED baseline (rootNode null) carries another writer's revision
            // for content this manager never saw — it authorizes nothing, and
            // skipping is safe (a leftover record is at worst a slightly stale
            // shadow the hash protocol revalidates).
            const prev = this.lastFlush_.get(pathString);
            const ownedRevision = prev !== undefined && prev.rootNode !== null ? prev.revision : null;
            this.latest_.delete(pathString);
            this.lastFlush_.delete(pathString);
            this.changedSinceFlush_.delete(pathString);
            if (ownedRevision !== null) {
                void this.enqueue_(pathString, () => this.deleteRecordIfRevision_(pathString, ownedRevision));
            }
            // The delete authorizes itself (revision-named), so the lease can
            // return right away; the re-track guard keeps a fresh listen's lease.
            this.releaseWriterLeaseIfUntracked_(pathString);
            return;
        }
        // Release the tree only after the final flush settles (flush_ reads
        // latest_ when it runs). Skip the delete if the root was re-tracked
        // meanwhile — the new listen owns the entry now. flushNow never rejects
        // today, but cleanup on both callbacks keeps this leak-proof either way.
        const release = () => {
            if (!this.trackedRoots_.has(pathString)) {
                this.latest_.delete(pathString);
                this.lastFlush_.delete(pathString);
                this.changedSinceFlush_.delete(pathString);
            }
            // AFTER the final flush so the last tree still writes under this
            // tab's lease; a re-tracked root keeps it (the new listen owns it).
            this.releaseWriterLeaseIfUntracked_(pathString);
        };
        void this.flushNow(pathString).then(release, release);
    }
    /**
     * The nearest (deepest) tracked root at-or-above `pathString`, or null.
     */
    trackedRootFor(pathString) {
        let best = null;
        for (const root of this.trackedRoots_) {
            if (pathString === root ||
                (pathString.length > root.length &&
                    pathString.startsWith(root === '/' ? root : root + '/'))) {
                if (best === null || root.length > best.length) {
                    best = root;
                }
            }
        }
        return best;
    }
    open_() {
        if (this.db_) {
            return this.db_;
        }
        this.db_ = this.openAtVersion_(undefined)
            .then(db => {
            if (db === null) {
                return null;
            }
            // One-time migration away from monolithic / shared-transaction cache
            // formats. Close and upgrade BEFORE any get(): clearing in the version
            // change transaction drops the old values inside IndexedDB, without
            // structured-cloning them into the WebKit heap (which is exactly what
            // crashed large legacy accounts during restore).
            if (db.version < PERSISTENCE_DB_VERSION) {
                db.close();
                return this.openAtVersion_(PERSISTENCE_DB_VERSION);
            }
            if (db.objectStoreNames.contains(STORE)) {
                return db;
            }
            const nextVersion = db.version + 1;
            db.close();
            return this.openAtVersion_(nextVersion);
        })
            .then(db => {
            if (db !== null && !db.objectStoreNames.contains(STORE)) {
                persistenceStats.storageFailures++;
                db.close();
                return null;
            }
            if (db !== null) {
                this.schemaKnownCurrent_ = true;
                writeSchemaMarker();
            }
            return db;
        });
        void this.db_.then(db => {
            if (db !== null && !this.disposed_ && this.sweepTimer_ === null) {
                this.sweepTimer_ = setTimeout(() => {
                    void this.sweepExpired_();
                }, PERSISTENCE_SWEEP_DELAY_MS);
                this.sweepTimer_.unref?.();
            }
        });
        return this.db_;
    }
    /**
     * Test seam: runs the deferred expiry sweep immediately.
     * @internal
     */
    sweepNow() {
        if (this.sweepTimer_ !== null) {
            clearTimeout(this.sweepTimer_);
        }
        return this.sweepExpired_();
    }
    /**
     * Deletes this manager's expired records (see PERSISTENCE_MAX_AGE_MS).
     * Expiry is decided by each root's manifest: the '#'-suffixed tree record
     * carries no authority of its own and is dropped exactly when its
     * manifest is dropped, is missing (an orphan), or belongs to a different
     * revision. Scoped to this manager's key range and reading keys before
     * values, where the platform allows, so foreign records are never
     * materialized. Best-effort: any failure leaves the records for the next
     * session's sweep.
     */
    sweepExpired_() {
        if (this.activeRestoreCount_ > 0 ||
            this.restoreQueue_.length > 0 ||
            this.queues_.size > 0) {
            if (!this.disposed_) {
                this.sweepTimer_ = setTimeout(() => {
                    void this.sweepExpired_();
                }, 5000);
                this.sweepTimer_.unref?.();
            }
            return Promise.resolve();
        }
        this.sweepTimer_ = null;
        if (this.sweepInFlight_ !== null) {
            return this.sweepInFlight_;
        }
        const cutoff = Date.now() - PERSISTENCE_MAX_AGE_MS;
        const prefix = this.key_('');
        let range;
        try {
            // Not in every embedding (Node test environments) — without it the
            // cursor walks the whole store and filters by prefix in JS.
            range =
                typeof IDBKeyRange !== 'undefined'
                    ? IDBKeyRange.bound(prefix, prefix + String.fromCharCode(0xffff))
                    : undefined;
        }
        catch (e) {
            range = undefined;
        }
        const work = this.withStore_('readonly', [], (store, done) => {
            const keys = [];
            // openKeyCursor never materializes values; the value-cursor fallback
            // (test fakes) walks values but only retains keys.
            const keyCursorStore = store;
            const req = typeof keyCursorStore.openKeyCursor === 'function'
                ? keyCursorStore.openKeyCursor(range)
                : store.openCursor(range);
            req.onsuccess = () => {
                const cursor = req.result;
                if (!cursor) {
                    done(keys);
                    return;
                }
                if (typeof cursor.key === 'string' && cursor.key.startsWith(prefix)) {
                    keys.push(cursor.key);
                }
                cursor.continue();
            };
        }).then(keys => {
            if (keys.length === 0) {
                return;
            }
            const baseKeys = [];
            const suffixedKeys = [];
            for (const key of keys) {
                if (key.indexOf('#', prefix.length) === -1) {
                    baseKeys.push(key);
                }
                else {
                    suffixedKeys.push(key);
                }
            }
            return this.withStore_('readwrite', undefined, store => {
                // Decide each base record (manifests are small), then settle the
                // suffixed records against those decisions — all in one transaction,
                // so a flush cannot interleave between the read and the delete.
                const decisions = new Map();
                let index = 0;
                const pruneLru = () => {
                    const activeKeys = new Set([...this.trackedRoots_].map(path => this.key_(path)));
                    const live = [...decisions.entries()].filter(([, d]) => !d.expired);
                    let totalBytes = live.reduce((sum, [, d]) => sum + d.estimatedBytes, 0);
                    if (totalBytes <= this.cacheMaxBytes_ &&
                        live.length <= PERSISTENCE_MAX_PRUNABLE_ROOTS) {
                        return;
                    }
                    const targetBytes = this.cacheMaxBytes_ * PERSISTENCE_PRUNE_TARGET_RATIO;
                    const targetRoots = Math.floor(PERSISTENCE_MAX_PRUNABLE_ROOTS * PERSISTENCE_PRUNE_TARGET_RATIO);
                    const candidates = live
                        .filter(([key]) => !activeKeys.has(key))
                        .sort((a, b) => a[1].updatedAt - b[1].updatedAt);
                    let liveRoots = live.length;
                    for (const [, decision] of candidates) {
                        if (totalBytes <= targetBytes && liveRoots <= targetRoots) {
                            break;
                        }
                        decision.expired = true;
                        totalBytes -= decision.estimatedBytes;
                        liveRoots--;
                    }
                };
                const settleSuffixed = () => {
                    for (const key of suffixedKeys) {
                        const base = key.slice(0, key.indexOf('#', prefix.length));
                        const decision = decisions.get(base);
                        // Keep only immutable payloads referenced by the current live
                        // manifest. Retired range versions and every legacy sidecar are
                        // garbage-collected without materializing their values.
                        const drop = decision === undefined ||
                            decision.expired ||
                            !decision.liveRangeKeys.has(key);
                        if (drop) {
                            store.delete(key);
                        }
                    }
                };
                const step = () => {
                    if (index >= baseKeys.length) {
                        pruneLru();
                        for (const [key, decision] of decisions) {
                            if (decision.expired) {
                                persistenceStats.evictions++;
                                store.delete(key);
                            }
                        }
                        settleSuffixed();
                        return;
                    }
                    const key = baseKeys[index++];
                    const req = store.get(key);
                    req.onsuccess = () => {
                        const record = req.result;
                        const expired = !record ||
                            record.formatVersion !== PERSISTENCE_FORMAT_VERSION ||
                            typeof record.updatedAt !== 'number' ||
                            record.updatedAt < cutoff;
                        decisions.set(key, {
                            expired,
                            updatedAt: record && typeof record.updatedAt === 'number'
                                ? record.updatedAt
                                : 0,
                            estimatedBytes: record && typeof record.estimatedBytes === 'number'
                                ? record.estimatedBytes
                                : 0,
                            revision: record && typeof record.revision === 'string'
                                ? record.revision
                                : null,
                            liveRangeKeys: record &&
                                record.formatVersion === PERSISTENCE_FORMAT_VERSION &&
                                Array.isArray(record.ranges)
                                ? new Set(record.ranges
                                    .filter((range) => range !== null &&
                                    typeof range === 'object' &&
                                    typeof range.recordId ===
                                        'string')
                                    .map(range => key + RANGE_KEY_INFIX + range.recordId))
                                : new Set()
                        });
                        step();
                    };
                };
                step();
            });
        });
        this.sweepInFlight_ = work.finally(() => {
            this.sweepInFlight_ = null;
        });
        return this.sweepInFlight_;
    }
    openAtVersion_(version) {
        return new Promise(resolve => {
            if (!this.idbFactory_) {
                resolve(null);
                return;
            }
            let settled = false;
            const finish = (db, failed = false) => {
                if (settled) {
                    db?.close();
                    return;
                }
                settled = true;
                clearTimeout(timer);
                if (failed) {
                    persistenceStats.storageFailures++;
                }
                resolve(db);
            };
            const timer = setTimeout(() => {
                // Some WebKit IndexedDB requests fire neither success nor error. A
                // cache miss is always safer than blocking the network listen.
                finish(null, true);
            }, this.operationTimeoutMs_);
            try {
                const req = version === undefined
                    ? this.idbFactory_.open('firebase-database-persistence')
                    : this.idbFactory_.open('firebase-database-persistence', version);
                req.onupgradeneeded = () => {
                    const db = req.result;
                    if (!db.objectStoreNames.contains(STORE)) {
                        db.createObjectStore(STORE);
                    }
                    else if (version === PERSISTENCE_DB_VERSION) {
                        // Clear in-IDB: no old record is cloned into JS memory.
                        req.transaction.objectStore(STORE).clear();
                    }
                };
                req.onsuccess = () => {
                    const db = req.result;
                    if (settled) {
                        // An open that completed after the timeout must not leak a
                        // connection or block a future schema upgrade.
                        db.close();
                        return;
                    }
                    db.onversionchange = () => db.close();
                    finish(db);
                };
                req.onerror = () => finish(null, true);
                req.onblocked = () => finish(null);
            }
            catch (e) {
                finish(null, true);
            }
        });
    }
    key_(pathString) {
        return this.prefix_ + '|' + pathString;
    }
    /**
     * Runs `body` against the object store in a transaction of the given mode
     * and resolves with what `body` chose to deliver (via its `done` callback)
     * once the transaction completes. Every failure path — no database, a
     * throwing store call, an aborted transaction — resolves `fallback` and
     * counts one storageFailure (except when IndexedDB is absent altogether,
     * which is a supported cold-load configuration, not a failure).
     */
    withStore_(mode, fallback, body, onProgress = () => { }) {
        return this.open_().then(db => new Promise(resolve => {
            if (!db) {
                resolve(fallback);
                return;
            }
            let settled = false;
            let timer = null;
            const finish = (value, failed = false) => {
                if (settled) {
                    return;
                }
                settled = true;
                if (timer !== null) {
                    clearTimeout(timer);
                }
                if (failed) {
                    persistenceStats.storageFailures++;
                }
                resolve(value);
            };
            try {
                const tx = db.transaction(STORE, mode);
                let value = fallback;
                const arm = () => {
                    if (settled) {
                        return;
                    }
                    if (timer !== null) {
                        clearTimeout(timer);
                    }
                    timer = setTimeout(() => {
                        // Abort a stalled transaction so an abandoned cache read
                        // cannot keep buffering network data indefinitely.
                        try {
                            tx.abort();
                        }
                        catch (e) {
                            // It may have completed between the timer firing and abort().
                        }
                        finish(fallback, true);
                    }, this.operationTimeoutMs_);
                    onProgress();
                };
                body(tx.objectStore(STORE), v => {
                    value = v;
                }, arm);
                arm();
                tx.oncomplete = () => finish(value);
                tx.onabort = tx.onerror = () => finish(fallback, true);
            }
            catch (e) {
                finish(fallback, true);
            }
        }));
    }
    /**
     * Reads a root's committed manifest and every immutable range it references
     * in one readonly transaction. `onManifest` fires as soon as the requests
     * are queued, overlapping network reconciliation with structured-clone
     * range reads and private Node assembly. Missing/mismatched ranges fail the
     * whole restore; Repo then performs the structural-failure cold relisten.
     */
    readRecord_(pathString, onProgress = () => { }, retainAfterResolve = false, expectedAuthScope = this.authScope_, onManifest = () => { }) {
        const active = this.activeReads_.get(pathString);
        if (active) {
            active.progress.add(onProgress);
            // Joining an already-progressing read is itself progress. A pre-auth
            // peek may have started the physical read, so replay any already-read
            // manifest to the real listener instead of making it wait for assembly.
            onProgress();
            if (active.manifestHashes !== null) {
                onManifest(active.manifestHashes);
            }
            else {
                active.manifestCallbacks.add(onManifest);
            }
            if (retainAfterResolve) {
                active.retainAfterResolve = true;
            }
            else if (active.retainAfterResolve) {
                // A real listener consumes the optimistic peek's completed read. The
                // returned promise still owns the result; the manager no longer needs
                // a second retained handle to it.
                active.retainAfterResolve = false;
                if (active.cleanupTimer !== null) {
                    clearTimeout(active.cleanupTimer);
                    this.activeReads_.delete(pathString);
                }
            }
            return active.promise;
        }
        const progress = new Set([onProgress]);
        const emitProgress = () => {
            for (const callback of progress) {
                callback();
            }
        };
        const entry = {
            promise: Promise.resolve(null),
            progress,
            retainAfterResolve,
            cleanupTimer: null,
            manifestHashes: null,
            manifestCallbacks: new Set([onManifest]),
            resolvedNode: null
        };
        const promise = this.readRecordOnce_(pathString, emitProgress, expectedAuthScope, hashes => {
            entry.manifestHashes = hashes;
            for (const callback of entry.manifestCallbacks) {
                callback(hashes);
            }
            entry.manifestCallbacks.clear();
        });
        entry.promise = promise;
        this.activeReads_.set(pathString, entry);
        const release = (result) => {
            entry.progress.clear();
            entry.manifestCallbacks.clear();
            entry.resolvedNode = result === null ? null : result.record.node;
            if (this.activeReads_.get(pathString) !== entry) {
                return;
            }
            if (!entry.retainAfterResolve) {
                this.activeReads_.delete(pathString);
                return;
            }
            // Only a completed DECODE earns the long pre-auth budget: it is the
            // one-tree-per-boot handoff auth must not race. A miss/failed read
            // retains nothing worth waiting for — keep the short expiry so a
            // record written meanwhile (another tab) is re-read fresh.
            entry.cleanupTimer = setTimeout(() => {
                if (this.activeReads_.get(pathString) === entry) {
                    this.activeReads_.delete(pathString);
                }
            }, result !== null && !this.authScopeConfirmed_
                ? this.peekPreAuthHandoffMs_
                : this.peekHandoffMs_);
        };
        void promise.then(release, () => release(null));
        return promise;
    }
    /**
     * Auth just confirmed the scope a pre-auth peek primed: every retained
     * completed read waiting under the long pre-auth backstop switches to the
     * short post-auth grace, counted from now. Entries still resolving (no
     * cleanupTimer yet) pick the right budget in their own release().
     */
    rearmRetainedReads_() {
        for (const [pathString, entry] of this.activeReads_) {
            if (entry.cleanupTimer === null || !entry.retainAfterResolve) {
                continue;
            }
            clearTimeout(entry.cleanupTimer);
            entry.cleanupTimer = setTimeout(() => {
                if (this.activeReads_.get(pathString) === entry) {
                    this.activeReads_.delete(pathString);
                }
            }, this.peekHandoffMs_);
        }
    }
    readRecordOnce_(pathString, onProgress, expectedAuthScope = this.authScope_, onManifest = () => { }) {
        const key = this.key_(pathString);
        // The revision of the generation a corrupt/expired verdict was reached
        // ON — the cleanup below deletes only THAT generation (a revision-named
        // delete cannot remove a successor another writer commits between this
        // read and the cleanup transaction). Null when the stored manifest is
        // too malformed to even carry a string revision (nothing current can be
        // named; see the cleanup site).
        let cleanupRevision = null;
        // One readonly transaction is the consistency boundary for manifest +
        // immutable range records. The manifest callback fires after every range
        // request has been synchronously queued, but before those payloads finish
        // cloning, so the network comparison overlaps the complete local restore.
        // The transaction itself only VALIDATES and collects raw structured
        // clones; decode runs after it resolves, in yielded slices (below).
        return this.withStore_('readonly', null, (store, done, progress) => {
            const manifestReq = store.get(key);
            manifestReq.onsuccess = () => {
                progress();
                // ABSENT (undefined) is a plain miss; a stored literal `null` is
                // NOT — it is garbage that must flow to the corrupt branch so the
                // in-transaction cleanup reclaims it and its sidecars (folding it
                // into the miss would leave it cached forever).
                if (manifestReq.result === undefined) {
                    done(null);
                    return;
                }
                const manifest = manifestReq.result;
                if (!structurallyValidManifest(manifest)) {
                    const rawRevision = manifest === null
                        ? undefined
                        : manifest.revision;
                    if (typeof rawRevision === 'string') {
                        cleanupRevision = rawRevision;
                    }
                    this.restoreReasons_.set(pathString, 'corrupt');
                    done(null);
                    return;
                }
                if (manifest.authScope !== expectedAuthScope) {
                    this.restoreReasons_.set(pathString, 'auth');
                    done(null);
                    return;
                }
                if (manifest.updatedAt < Date.now() - PERSISTENCE_MAX_AGE_MS) {
                    cleanupRevision = manifest.revision;
                    this.restoreReasons_.set(pathString, 'expired');
                    done(null);
                    return;
                }
                // The onsuccess callbacks only VALIDATE and collect the raw
                // structured clones — decoding (nodeFromJSON + merge) is deferred
                // to a yielded post-transaction loop below. Chrome coalesces
                // same-transaction request callbacks into one task, so decoding
                // inline produced multi-hundred-ms long tasks (and held every raw
                // clone alive until the last record decoded). The deferred loop
                // bounds task length and releases each clone as it is consumed —
                // both matter on mobile WebKit, where a long-task + peak-memory
                // spike at boot is what gets the page killed.
                let failed = false;
                let remaining = manifest.ranges.length;
                let previousPost = null;
                const rawTrees = new Array(manifest.ranges.length).fill(null);
                manifest.ranges.forEach((range, index) => {
                    const expectedStart = previousPost;
                    previousPost = range.post;
                    const req = store.get(key + RANGE_KEY_INFIX + range.recordId);
                    req.onsuccess = () => {
                        progress();
                        if (!failed) {
                            const record = req.result;
                            if (!record ||
                                record.recordId !== range.recordId ||
                                record.start !== expectedStart ||
                                record.end !== range.post ||
                                record.tree === null ||
                                record.tree === undefined) {
                                failed = true;
                                cleanupRevision = manifest.revision;
                                this.restoreReasons_.set(pathString, 'corrupt');
                            }
                            else {
                                rawTrees[index] = record.tree;
                            }
                        }
                        remaining--;
                        if (remaining === 0) {
                            done(failed ? null : { manifest, rawTrees });
                        }
                    };
                });
                // Every referenced get is now queued in this same snapshot. It is
                // safe to put the range listen on the wire immediately.
                onManifest({
                    hash: manifest.hash,
                    compoundHash: wireCompoundHashFromRanges(manifest.ranges)
                });
            };
        }, onProgress).then(async (collected) => {
            let result = null;
            if (collected !== null) {
                const assembled = await this.decodeFragmentsSliced_(collected.rawTrees, onProgress);
                if (this.disposed_) {
                    // Disposed mid-decode: the record on disk is fine — do not mark it
                    // corrupt (which would delete it below).
                    return null;
                }
                if (assembled === null || assembled.isEmpty()) {
                    cleanupRevision = collected.manifest.revision;
                    this.restoreReasons_.set(pathString, 'corrupt');
                }
                else {
                    result = {
                        record: {
                            node: assembled,
                            hash: collected.manifest.hash,
                            compoundHash: wireCompoundHashFromRanges(collected.manifest.ranges),
                            updatedAt: collected.manifest.updatedAt,
                            revision: collected.manifest.revision
                        },
                        ranges: collected.manifest.ranges
                    };
                }
            }
            if (result === null) {
                const reason = this.restoreReasons_.get(pathString);
                if (reason === 'corrupt' || reason === 'expired') {
                    // Best-effort cleanup, NAMED to the generation the verdict was
                    // reached on: a successor committed meanwhile (this manager may
                    // be a follower queued behind another tab's lease) must survive.
                    // Auth/missing misses must not delete another identity's
                    // otherwise valid cache record. A manifest too malformed to carry
                    // a revision cannot be named — its cleanup re-reaches the verdict
                    // INSIDE the delete transaction instead (deleteRecordIfInvalid_):
                    // "no CAS writer produced this" was established by a readonly
                    // read and does not hold across the transaction boundary — a
                    // concurrent writer may have replaced the garbage with a valid
                    // generation by the time the delete runs.
                    void (cleanupRevision !== null
                        ? this.deleteRecordIfRevision_(pathString, cleanupRevision)
                        : this.deleteRecordIfInvalid_(pathString));
                }
            }
            return result;
        });
    }
    /**
     * Decodes and merges raw persisted range clones into one Node in yielded
     * slices. Each slice decodes a few records, then yields a macrotask so the
     * main thread can paint/GC between slices; consumed entries are nulled so
     * the structured clones are collectable while later slices run. Returns
     * null when any fragment fails to decode.
     */
    async decodeFragmentsSliced_(rawTrees, progress) {
        const SLICE_SIZE = 8;
        let assembled = ChildrenNode.EMPTY_NODE;
        for (let i = 0; i < rawTrees.length; i++) {
            if (i > 0 && i % SLICE_SIZE === 0) {
                await new Promise(resolve => setTimeout(resolve, 0));
                if (this.disposed_) {
                    return null;
                }
                progress();
            }
            const raw = rawTrees[i];
            rawTrees[i] = null;
            try {
                assembled = mergePersistedFragment(assembled, nodeFromJSON(raw));
            }
            catch (e) {
                return null;
            }
        }
        return assembled;
    }
    /**
     * Cleanup for a manifest judged structurally invalid: the verdict is
     * re-reached INSIDE the readwrite transaction, so a valid generation a
     * concurrent writer committed after the (readonly) judgement is never
     * touched. Still-invalid garbage — whatever garbage it is by now — goes.
     */
    deleteRecordIfInvalid_(pathString) {
        const key = this.key_(pathString);
        return this.withStore_('readwrite', undefined, (store, done) => {
            const req = store.get(key);
            req.onsuccess = () => {
                const manifest = req.result;
                if (manifest === undefined || structurallyValidManifest(manifest)) {
                    done(undefined);
                    return;
                }
                this.deleteRecordInStore_(store, key);
                done(undefined);
            };
        });
    }
    /**
     * Housekeeping variant of deleteRecord_: deletes the root's record only
     * while the committed manifest still carries `expectedRevision` — the one
     * generation this manager itself verified or wrote. An unconditional
     * housekeeping delete could erase a FRESH generation another tab
     * committed for this root after this manager last looked (that tab keeps
     * flushing under its own lease and would skip identical rewrites against
     * a lastFlush_ that no longer describes storage). Check and delete run in
     * ONE readwrite transaction, so a concurrent commit cannot interleave
     * between them. Skipping is always safe: a record left behind is at
     * worst a slightly stale shadow, and every restored record is
     * revalidated against the server by the hash protocol anyway.
     */
    deleteRecordIfRevision_(pathString, expectedRevision) {
        const key = this.key_(pathString);
        return this.withStore_('readwrite', undefined, store => {
            const req = store.get(key);
            req.onsuccess = () => {
                const manifest = req.result;
                if (!manifest || manifest.revision !== expectedRevision) {
                    return;
                }
                this.deleteRecordInStore_(store, key);
            };
        });
    }
    /** Deletes a root's manifest and every '#'-suffixed sidecar in `store`. */
    deleteRecordInStore_(store, key) {
        store.delete(key);
        // Immutable ranges and legacy chunk/hash/tree sidecars share '#'.
        // suffix namespace. Range-delete where the platform has IDBKeyRange;
        // cursor-walk otherwise (Node, test fakes) — key-only, no values.
        if (typeof IDBKeyRange !== 'undefined') {
            try {
                store.delete(IDBKeyRange.bound(key + '#', key + '#' + String.fromCharCode(0xffff)));
                return;
            }
            catch (e) {
                // Fall through to the cursor walk.
            }
        }
        try {
            const req = store.openCursor();
            req.onsuccess = () => {
                const cursor = req.result;
                if (!cursor) {
                    return;
                }
                if (typeof cursor.key === 'string' &&
                    cursor.key.startsWith(key + '#')) {
                    cursor.delete();
                }
                cursor.continue();
            };
        }
        catch (e) {
            // Sidecar cleanup is best-effort; the sweep reclaims leftovers.
        }
    }
    withRestoreSlot_(work) {
        const run = () => {
            this.activeRestoreCount_++;
            return work().finally(() => {
                this.activeRestoreCount_--;
                const next = this.restoreQueue_.shift();
                if (next) {
                    next();
                }
                else if (this.activeRestoreCount_ === 0) {
                    this.flushWritesDeferredUntilRestores_();
                }
            });
        };
        if (this.activeRestoreCount_ < PERSISTENCE_MAX_CONCURRENT_RESTORES) {
            return run();
        }
        return new Promise((resolve, reject) => {
            this.restoreQueue_.push(() => {
                if (this.disposed_) {
                    resolve(null);
                    return;
                }
                void run().then(resolve, reject);
            });
        });
    }
    /**
     * Projects an exact-path peek from a covering root that is already restored
     * or actively restoring in this manager. This never starts a large ancestor
     * read just to answer a tiny token lookup; it only reuses work the app is
     * already paying for, preserving the exact-root fast path on direct boots.
     */
    peekFromCoveringRead_(pathString, expectedAuthScope) {
        if (!this.authScopeConfigured_ || expectedAuthScope !== this.authScope_) {
            return null;
        }
        let bestRoot = null;
        let source = null;
        for (const [root, read] of this.activeReads_) {
            if (pathString !== root &&
                (root === '/' || pathString.startsWith(root + '/')) &&
                (bestRoot === null || root.length > bestRoot.length)) {
                bestRoot = root;
                source = read.promise;
            }
        }
        for (const [root, state] of this.lastFlush_) {
            if (state.rootNode !== null &&
                pathString !== root &&
                (root === '/' || pathString.startsWith(root + '/')) &&
                (bestRoot === null || root.length > bestRoot.length)) {
                const rootNode = state.rootNode;
                bestRoot = root;
                source = Promise.resolve({
                    record: {
                        node: rootNode,
                        updatedAt: state.storedUpdatedAt,
                        revision: state.revision
                    },
                    ranges: state.ranges
                });
            }
        }
        if (bestRoot === null || source === null) {
            return null;
        }
        const relative = bestRoot === '/'
            ? pathString.replace(/^\/+/, '')
            : pathString.slice(bestRoot.length).replace(/^\/+/, '');
        return source.then(result => {
            if (result === null) {
                return null;
            }
            const node = result.record.node.getChild(new Path(relative));
            return node.isEmpty()
                ? null
                : {
                    node,
                    updatedAt: result.record.updatedAt,
                    revision: result.record.revision
                };
        });
    }
    /**
     * Exact-root optimistic peek. The completed range assembly is retained briefly
     * so the authenticated listener consumes the same immutable Node instead of
     * reconstructing the root twice during boot.
     */
    peek(pathString, expectedAuthScope = this.authScope_) {
        if (this.disposed_ ||
            !this.schemaKnownCurrent_ ||
            !this.authScopeConfigured_) {
            recordPersistenceEvent(pathString, 'peek-miss', this.disposed_
                ? 'disposed'
                : !this.authScopeConfigured_
                    ? 'auth-scope-unconfigured'
                    : 'schema-migration');
            return Promise.resolve(null);
        }
        const authGeneration = this.authGeneration_;
        const covering = this.peekFromCoveringRead_(pathString, expectedAuthScope);
        if (covering !== null) {
            return covering.then(record => authGeneration === this.authGeneration_ &&
                expectedAuthScope === this.authScope_
                ? record
                : null);
        }
        return this.withRestoreSlot_(() => this.raceRestoreTimeout_(onProgress => this.readRecord_(pathString, onProgress, true, expectedAuthScope).then(result => {
            recordPersistenceEvent(pathString, result ? 'peek-hit' : 'peek-miss');
            return result === null ? null : result.record;
        }), this.operationTimeoutMs_, () => recordPersistenceEvent(pathString, 'peek-idle-timeout'))).then(record => authGeneration === this.authGeneration_ &&
            expectedAuthScope === this.authScope_
            ? record
            : null);
    }
    /**
     * Listener restore with an idle (no-progress) bound. `onManifest` fires as
     * soon as the stored generation's hashes are known — typically
     * milliseconds — letting the caller send the range listen while immutable
     * range records are still being read and assembled. The callback is suppressed after
     * a timeout/miss resolution, and never fires once the returned promise has
     * settled null.
     */
    restoreForListen(pathString, onManifest = () => { }) {
        this.restoreReasons_.delete(pathString);
        const authGeneration = this.authGeneration_;
        const expectedAuthScope = this.authScope_;
        if (this.disposed_ ||
            !this.schemaKnownCurrent_ ||
            !this.authScopeConfigured_) {
            const reason = this.authScopeConfigured_
                ? 'missing'
                : 'auth';
            this.restoreReasons_.set(pathString, reason);
            recordPersistenceEvent(pathString, 'restore-miss', this.disposed_
                ? 'disposed'
                : !this.authScopeConfigured_
                    ? 'auth-scope-unconfigured'
                    : 'schema-migration');
            return Promise.resolve({ record: null, reason });
        }
        let settledNull = false;
        const guardedOnManifest = (hashes) => {
            if (!settledNull &&
                authGeneration === this.authGeneration_ &&
                !this.disposed_ &&
                this.trackedRoots_.has(pathString)) {
                onManifest(hashes);
            }
        };
        return this.withRestoreSlot_(() => this.raceRestoreTimeout_(onProgress => this.readRecord_(pathString, onProgress, false, expectedAuthScope, guardedOnManifest).then(result => {
            if (result === null ||
                authGeneration !== this.authGeneration_ ||
                expectedAuthScope !== this.authScope_ ||
                this.disposed_ ||
                !this.trackedRoots_.has(pathString)) {
                return null;
            }
            // Updates accumulated before this point were named against a
            // pre-restore chain; the restored record starts a new baseline.
            this.changedSinceFlush_.set(pathString, null);
            this.lastFlush_.set(pathString, {
                rootNode: result.record.node,
                revision: result.record.revision,
                ranges: result.ranges,
                storedUpdatedAt: result.record.updatedAt
            });
            persistenceStats.restoredRoots.push(pathString);
            return result.record;
        }), this.operationTimeoutMs_, () => {
            this.restoreReasons_.set(pathString, 'timeout');
            recordPersistenceEvent(pathString, 'restore-idle-timeout');
        })).then(record => {
            if (authGeneration !== this.authGeneration_ ||
                expectedAuthScope !== this.authScope_) {
                this.restoreReasons_.set(pathString, 'auth');
                record = null;
            }
            if (record === null) {
                settledNull = true;
            }
            const reason = record
                ? undefined
                : this.restoreReasons_.get(pathString) ?? 'missing';
            recordPersistenceEvent(pathString, record ? 'restore-hit' : 'restore-miss', reason);
            return record ? { record } : { record: null, reason };
        });
    }
    /**
     * Bounds a read by an IDLE (no-progress) timeout. The factory form lets
     * chunked restores reset the timer after every completed chunk; callers
     * that pass an already-started Promise retain the old total-time bound.
     */
    raceRestoreTimeout_(readOrStart, timeoutMs = this.operationTimeoutMs_, onTimeout = () => { }) {
        let timer;
        let settled = false;
        let timeoutResolve = () => { };
        const timeout = new Promise(resolve => {
            timeoutResolve = resolve;
        });
        const arm = () => {
            if (settled) {
                return;
            }
            clearTimeout(timer);
            timer = setTimeout(() => {
                onTimeout();
                timeoutResolve(null);
            }, timeoutMs);
        };
        const read = typeof readOrStart === 'function' ? readOrStart(arm) : readOrStart;
        arm();
        return Promise.race([read, timeout]).then(result => {
            settled = true;
            clearTimeout(timer);
            return result;
        });
    }
    /**
     * Write-through: the server confirmed `node` as the state of the tracked
     * root `path`. Coalesced per root under the single-flight window (see the
     * file header): the first change arms a non-restarting timer; later
     * changes coalesce; a change landing while a flush is in flight re-arms
     * exactly one follow-up window when the queue drains. A tree the store is
     * known to already hold — the warm boot's listen-'ok' certifying the
     * restored tree unchanged — is skipped outright unless its stored
     * timestamp needs a refresh (see PERSISTENCE_REFRESH_AGE_MS).
     */
    flushWritesDeferredUntilRestores_() {
        const paths = [...this.writesDeferredUntilRestores_];
        this.writesDeferredUntilRestores_.clear();
        for (const pathString of paths) {
            // The deferral must not bypass the write window: draining the restore
            // wave IS the cold-boot moment (LCP, initial render). Re-arm the same
            // non-restarting window a direct write-through would have entered.
            this.armWriteWindow_(pathString);
        }
    }
    /**
     * Re-enters the ordinary write window when the root still has work: it is
     * tracked and holds a pending tree in latest_ (flush_ reads latest_ when
     * it runs, so whatever landed meanwhile is covered). The one definition
     * used by every deferred-retry path — a lease grant after skipped writes,
     * a failed-open lock acquisition, and the stale-baseline adoption.
     */
    armWriteWindowIfPending_(pathString) {
        if (!this.disposed_ &&
            this.trackedRoots_.has(pathString) &&
            this.latest_.has(pathString)) {
            this.armWriteWindow_(pathString);
        }
    }
    /** Arms the non-restarting single-flight write window for a root. */
    armWriteWindow_(pathString) {
        if (!this.writeTimers_.has(pathString)) {
            this.writeTimers_.set(pathString, setTimeout(() => {
                this.writeTimers_.delete(pathString);
                this.scheduleFlush_(pathString);
            }, this.writeDelayMs_));
        }
    }
    accumulateChangedPaths_(pathString, changedPaths) {
        if (changedPaths === undefined) {
            this.changedSinceFlush_.set(pathString, null);
            return;
        }
        const existing = this.changedSinceFlush_.get(pathString);
        if (existing === null) {
            return; // already imprecise until the next flush baseline
        }
        const list = existing ?? [];
        for (const changedPath of changedPaths) {
            if (list.length >= MAX_ACCUMULATED_CHANGED_PATHS) {
                this.changedSinceFlush_.set(pathString, null);
                return;
            }
            list.push(changedPath);
        }
        this.changedSinceFlush_.set(pathString, list);
    }
    /**
     * `changedPaths` — the root-relative paths of the subtrees this update
     * changed, when the caller knows them precisely: an ordinary server data
     * push names its own path (`[relative]`), a listen certification confirms
     * already-accounted state (`[]`, nothing new). Omitted/undefined marks the
     * accumulated change-set imprecise — a range merge, or any update whose
     * shape the caller cannot name — falling the next flush back to the
     * identity diff.
     */
    serverCacheUpdated(path, node, changedPaths) {
        if (this.disposed_ || !this.authScopeConfigured_) {
            return;
        }
        const pathString = path.toString();
        if (!this.trackedRoots_.has(pathString)) {
            return;
        }
        const prev = this.lastFlush_.get(pathString);
        if (prev &&
            prev.rootNode === node &&
            Date.now() - prev.storedUpdatedAt < PERSISTENCE_REFRESH_AGE_MS) {
            return;
        }
        this.accumulateChangedPaths_(pathString, changedPaths);
        this.latest_.set(pathString, {
            node,
            revision: this.instanceId_ + '-' + (++this.writeCounter_).toString(36),
            authScope: this.authScope_
        });
        // IndexedDB serializes readwrite transactions for this object store. A
        // cold root must not begin a large write while another selected root is
        // still restoring, or the restore can hit its idle timeout behind its own
        // write-through. Android gets this ordering from one persistence runloop;
        // the web manager reproduces it explicitly.
        if (this.activeRestoreCount_ > 0 || this.restoreQueue_.length > 0) {
            this.writesDeferredUntilRestores_.add(pathString);
            return;
        }
        // Every generation, including the first, enters the non-restarting
        // window. Cache creation is an optional accelerator and must not compete
        // with the cold page's initial render/LCP. The flush reads
        // latest_ when it runs, so it always writes the newest tree.
        this.armWriteWindow_(pathString);
    }
    /**
     * Enqueues a flush unless the root's queue is still working — then one
     * flush is marked pending and enqueued when the queue drains. Without the
     * mark, a root whose flush takes longer than the window would queue
     * flushes faster than they complete, unboundedly. This is the
     * single-flight guarantee: at most one flush in flight per root, effective
     * cadence max(writeDelayMs, flush duration).
     */
    scheduleFlush_(pathString) {
        if (this.queues_.has(pathString)) {
            this.flushPending_.add(pathString);
            return;
        }
        void this.enqueue_(pathString, () => this.flush_(pathString));
    }
    /** Drop an unusable persisted record but keep the live root tracked. */
    invalidate(path) {
        const pathString = path.toString();
        // The record being invalidated is the one this manager just RESTORED —
        // its revision sits in lastFlush_ (set by restoreForListen). Name the
        // delete to it so a successor generation another writer committed
        // meanwhile survives. Unnamable (no verified baseline): skip — the next
        // restore of a genuinely bad record fails again and readRecordOnce_'s
        // own named cleanup removes it.
        const prev = this.lastFlush_.get(pathString);
        const restoredRevision = prev !== undefined && prev.rootNode !== null ? prev.revision : null;
        this.latest_.delete(pathString);
        this.lastFlush_.delete(pathString);
        this.changedSinceFlush_.delete(pathString);
        recordPersistenceEvent(pathString, 'invalidate', 'corrupt-or-incompatible');
        if (restoredRevision !== null) {
            void this.enqueue_(pathString, () => this.deleteRecordIfRevision_(pathString, restoredRevision));
        }
    }
    /**
     * The viewer lost access to a root: a cached copy must not outlive the
     * access that produced it, and the root leaves write-through tracking
     * entirely — SyncTree never calls stopListening for server-revoked
     * listens, so nothing else would ever untrack it.
     */
    evict(path) {
        const pathString = path.toString();
        this.trackedRoots_.delete(pathString);
        this.latest_.delete(pathString);
        this.lastFlush_.delete(pathString);
        this.changedSinceFlush_.delete(pathString);
        this.flushPending_.delete(pathString);
        const timer = this.writeTimers_.get(pathString);
        if (timer) {
            clearTimeout(timer);
            this.writeTimers_.delete(pathString);
        }
        persistenceStats.evictions++;
        recordPersistenceEvent(pathString, 'evict', 'permission-or-revocation');
        // The root left tracking: return its lease so a tab that still tracks
        // it can write. Order relative to the queued purge is free — the purge
        // authorizes itself inside its own transaction, never via the lease.
        this.releaseWriterLease_(pathString);
        // The purge is IMMEDIATE and atomic — it must never wait for the
        // writer lease. The lease is held for the holder tab's lifetime, and a
        // holder that does not listen to this root never receives the
        // revocation itself: a purge deferred to lease grant would leave the
        // revoked bytes cached for as long as that tab lives, violating the
        // invariant above. Deleting without the lease is made safe by SCOPE,
        // checked in the same readwrite transaction: only the writer can have
        // committed a newer generation here, and a same-scope writer tracking
        // this root receives the same revocation and evicts too (clearing its
        // own lastFlush_, so no stale identical-rewrite short-circuit
        // survives); a writer NOT tracking this root never writes it at all. A
        // manifest under ANOTHER identity's scope is left alone — this user's
        // revoked bytes are not in it, and the other identity's access is its
        // own. (Residual, accepted: a same-scope writer that legitimately
        // RETAINS access through different query-level rules can have a fresh
        // generation purged and skip identical rewrites against its stale
        // lastFlush_ until the manifest-refresh path self-heals it — bounded
        // cache staleness, never corruption.) Through the root's queue, so a
        // flush of this manager already in flight finishes first.
        // The scope whose access was revoked is CAPTURED NOW, not read later:
        // the purge runs behind any in-flight per-root work, and an account
        // switch (setAuthScope) can land in that gap. Compared against the
        // manager's LIVE scope, the old identity's revoked record would read
        // as "another identity's" and be preserved, while a fresh record the
        // NEW identity just committed would match and be deleted — exactly
        // backwards. The reference value for in-transaction validation must be
        // immutable, like deleteRecordIfRevision_'s expectedRevision.
        const revokedScope = this.authScope_;
        void this.enqueue_(pathString, () => this.purgeEvictedRecord_(pathString, revokedScope));
    }
    /**
     * Eviction's delete: manifest + sidecars in one transaction, gated on the
     * stored manifest belonging to the REVOKED scope (captured at evict();
     * see the comment there). `null` is a REAL scope — the anonymous
     * identity — not malformation: an anonymous user's valid record must
     * survive a signed-in tab's eviction exactly like any other identity's.
     * The unconditional purge is reserved for values no live writer produced
     * — a structurally invalid manifest, a malformed scope field (neither
     * string nor null), or a stored value that is not an object at all
     * (null, primitives): eviction is exactly the moment to drop those WITH
     * their sidecars, which may still carry revoked bytes. Only a truly
     * ABSENT record (undefined) is a no-op. Field reads happen only after
     * structural validation — a stored literal `null` passes an
     * undefined-check and then throws on property access, aborting the
     * transaction and silently RETAINING the revoked record.
     */
    purgeEvictedRecord_(pathString, revokedScope) {
        const key = this.key_(pathString);
        return this.withStore_('readwrite', undefined, (store, done) => {
            const req = store.get(key);
            req.onsuccess = () => {
                const stored = req.result;
                if (stored === undefined) {
                    done(undefined);
                    return;
                }
                if (structurallyValidManifest(stored)) {
                    const scope = stored.authScope;
                    if ((typeof scope === 'string' || scope === null) &&
                        scope !== revokedScope) {
                        // Another identity's valid record: the revoked bytes are not
                        // in it, and the other identity's access is its own.
                        done(undefined);
                        return;
                    }
                }
                this.deleteRecordInStore_(store, key);
                done(undefined);
            };
        });
    }
    dispose() {
        this.disposed_ = true;
        this.releaseAllWriterLeases_();
        for (const timer of this.writeTimers_.values()) {
            clearTimeout(timer);
        }
        this.writeTimers_.clear();
        if (this.sweepTimer_ !== null) {
            clearTimeout(this.sweepTimer_);
        }
        this.flushPending_.clear();
        const queuedRestores = this.restoreQueue_;
        this.restoreQueue_ = [];
        for (const resume of queuedRestores) {
            resume();
        }
        for (const read of this.activeReads_.values()) {
            if (read.cleanupTimer !== null) {
                clearTimeout(read.cleanupTimer);
            }
        }
        this.activeReads_.clear();
        this.persistentRoots_.clear();
        this.latest_.clear();
        this.lastFlush_.clear();
        this.changedSinceFlush_.clear();
        void this.db_?.then(db => db?.close());
    }
    /**
     * Test seam: forces a pending flush window to fire now.
     */
    flushNow(pathString) {
        const timer = this.writeTimers_.get(pathString);
        if (timer) {
            clearTimeout(timer);
            this.writeTimers_.delete(pathString);
        }
        this.flushPending_.delete(pathString);
        return this.enqueue_(pathString, () => this.flush_(pathString));
    }
    /**
     * Chains an operation onto the root's queue. One writer per root at a
     * time: a flush's manifest, chunks, and integrated hashes stay revision-coupled
     * before the next flush or delete for that root starts, which is the
     * whole storage consistency argument — no cross-operation races to
     * reason about.
     */
    enqueue_(pathString, op) {
        const next = (this.queues_.get(pathString) ?? Promise.resolve()).then(op);
        // Settle-or-not, the chain must continue; storage failures are already
        // absorbed (and counted) inside withStore_.
        const settled = next.catch(() => { });
        this.queues_.set(pathString, settled);
        void settled.then(() => {
            if (this.queues_.get(pathString) === settled) {
                this.queues_.delete(pathString);
                if (this.flushPending_.delete(pathString) && !this.disposed_) {
                    void this.enqueue_(pathString, () => this.flush_(pathString));
                }
            }
        });
        return next;
    }
    /**
     * Adopts the currently COMMITTED generation as the next flush baseline
     * WITHOUT reading or decoding its range payloads — a manifest-only read.
     *
     * Used when this manager discovers its baseline is stale (the flush CAS
     * lost to another writer, or the stored generation vanished): the
     * winner's revision + ranges are all the next CAS needs, while its tree
     * stays undecoded (rootNode: null). The follow-up flush cannot diff
     * against an absent tree, so it stages a fresh self-contained generation
     * — the same write the old adopt-and-diff produced anyway (a freshly
     * decoded tree shares no identity with the live one, so its identity
     * diff marked every range dirty) minus the full IndexedDB read and Node
     * decode of the entire root that made every cross-tab conflict as
     * expensive as a cold restore.
     *
     * The retry enters the ordinary NON-RESTARTING write window instead of
     * re-flushing immediately: under sustained cross-tab churn an immediate
     * retry conflicts again back-to-back — full-tree work with no pause
     * between attempts (the multi-tab thrash the write leases exist to
     * prevent, kept bounded here for lease-less environments too).
     */
    adoptCommittedBaseline_(pathString) {
        const key = this.key_(pathString);
        return this.withStore_('readonly', null, (store, done) => {
            const req = store.get(key);
            req.onsuccess = () => {
                done(req.result ?? null);
            };
        }).then(manifest => {
            if (this.disposed_) {
                return;
            }
            // The adopted baseline is another generation's tree; paths named
            // against our own chain do not describe diffs from it.
            this.changedSinceFlush_.set(pathString, null);
            if (structurallyValidManifest(manifest) &&
                manifest.authScope === this.authScope_) {
                this.lastFlush_.set(pathString, {
                    rootNode: null,
                    revision: manifest.revision,
                    ranges: manifest.ranges,
                    storedUpdatedAt: manifest.updatedAt
                });
            }
            else {
                // Missing, foreign-scope, or unreadable: the next flush stages
                // under the absent / replaceable-foreign CAS arm instead.
                this.lastFlush_.delete(pathString);
            }
            // The write window is the ONLY retry path. A window that elapsed
            // while the losing flush was in flight marked flushPending_, and the
            // queue drain would re-flush IMMEDIATELY on settle — full-tree
            // staging back-to-back under sustained lease-less churn, bypassing
            // the debounce this adoption exists to provide. The armed window
            // supersedes it: flush_ reads latest_ when it runs, so the update
            // that marked the queue pending is still fully covered, just
            // deferred. For an untracked root (the final flush from untrack lost
            // the CAS) there is deliberately no retry at all: the winner's
            // generation is a coherent snapshot seconds-fresh at most, and the
            // hash protocol revalidates it on the next boot — not worth keeping
            // the tree and lease alive past untrack (this matches the pre-lease
            // behavior, whose drain retry always found latest_ already released).
            this.flushPending_.delete(pathString);
            this.armWriteWindowIfPending_(pathString);
        });
    }
    /**
     * One generation: identity-diff against the last known stored tree marks
     * the dirty ranges; only those are re-serialized (between preserved
     * boundary posts), re-hashed, and written under new immutable ids. Clean
     * range records carry over verbatim and are never cloned. New records plus
     * the manifest commit in ONE transaction, so every
     * committed generation's hashes exactly describe its stored tree — which
     * is what lets the next boot listen straight off the manifest with zero
     * hashing.
     */
    flush_(pathString) {
        if (this.sweepInFlight_ !== null) {
            return this.sweepInFlight_.then(() => this.flush_(pathString));
        }
        const entry = this.latest_.get(pathString);
        if (!entry || this.disposed_ || !this.authScopeConfigured_) {
            return Promise.resolve();
        }
        if (!this.holdsWriterLease_(pathString)) {
            // Another tab is this root's writer. latest_ keeps the newest tree in
            // memory; if the lease ever transfers here, the grant callback
            // re-enters the ordinary write window for this root.
            return Promise.resolve();
        }
        const { node, revision, authScope } = entry;
        const prev = this.lastFlush_.get(pathString);
        const now = Date.now();
        if (prev &&
            prev.rootNode === node &&
            now - prev.storedUpdatedAt < PERSISTENCE_REFRESH_AGE_MS) {
            return Promise.resolve();
        }
        const key = this.key_(pathString);
        if (node.isEmpty()) {
            // An empty tree is a GENERATION, and deleting the record is its
            // commit — so it obeys the exact CAS arms a manifest commit does,
            // inside one readwrite transaction. The lease check above ran before
            // async work: Web Locks `steal` can revoke it while this flush is
            // suspended, and an unconditional delete on resume would erase the
            // manifest and ranges the NEW holder committed meanwhile. With a
            // baseline, delete only the baseline's revision (adopted counts —
            // this is commit CAS, not an ownership guard); with none, only an
            // absent record or a replaceable-foreign manifest (same live scope
            // staging over another identity/format — see the commit arms) may be
            // removed. Anything else is a CAS conflict: adopt the winner
            // manifest-only and let the write window retry — where the lease
            // gate runs again, so a stolen holder never retries as a writer.
            return this.withStore_('readwrite', false, (store, done, progress) => {
                // Eligibility is re-checked INSIDE the commit transaction: the
                // gate at flush entry ran before async staging, and the lease can
                // be released (goOffline) or lost (steal) while this flush was
                // suspended in between. Losing it reads as a CAS conflict — the
                // adopt + write-window retry re-runs the entry gate. Re-running
                // the LIVE gate (not a captured token) deliberately still allows
                // a suspend→resume→re-granted holder to commit: it is the
                // eligible writer again, and nothing newer can exist locally.
                if (!this.holdsWriterLease_(pathString)) {
                    done(false);
                    return;
                }
                const req = store.get(key);
                req.onsuccess = () => {
                    progress();
                    // A stored literal `null` is garbage no CAS writer produced;
                    // normalized to ABSENT so the empty commit succeeds instead of
                    // throwing on the field reads below (an exception here aborts
                    // the transaction, and every window retry would abort the same
                    // way — the root could never flush again). Restore-side
                    // cleanup (deleteRecordIfInvalid_) reclaims the value and its
                    // sidecars.
                    const current = (req.result ?? undefined);
                    if (current === undefined) {
                        done(true);
                        return;
                    }
                    const replaceableForeign = !prev &&
                        authScope === this.authScope_ &&
                        (current.formatVersion !== PERSISTENCE_FORMAT_VERSION ||
                            current.authScope !== authScope);
                    if ((prev && current.revision === prev.revision) ||
                        replaceableForeign) {
                        this.deleteRecordInStore_(store, key);
                        done(true);
                        return;
                    }
                    done(false);
                };
            }).then(ok => {
                if (this.disposed_) {
                    return;
                }
                if (ok) {
                    this.lastFlush_.delete(pathString);
                    return;
                }
                return this.adoptCommittedBaseline_(pathString);
            });
        }
        if (prev && prev.rootNode === node) {
            // Content-identical: refresh only the manifest timestamp. The revision
            // guard prevents a stale tab from refreshing a superseded generation.
            // 'ineligible' (the lease was released or lost between the entry gate
            // and this transaction — see the empty-path comment) is a plain
            // no-op, NOT a conflict: the stored generation may be perfectly
            // current, and an ineligible tab must neither extend its perceived
            // freshness nor discard its own decoded baseline over it.
            return this.withStore_('readwrite', 'conflict', (store, done, progress) => {
                if (!this.holdsWriterLease_(pathString)) {
                    done('ineligible');
                    return;
                }
                const req = store.get(key);
                req.onsuccess = () => {
                    progress();
                    const current = req.result;
                    if (current && current.revision === prev.revision) {
                        const put = store.put({ ...current, updatedAt: now }, key);
                        put.onsuccess = progress;
                        done('refreshed');
                    }
                    else {
                        done('conflict');
                    }
                };
            }).then(outcome => {
                if (this.disposed_ || outcome === 'ineligible') {
                    return;
                }
                if (outcome === 'refreshed') {
                    this.lastFlush_.set(pathString, { ...prev, storedUpdatedAt: now });
                    return;
                }
                // The stored generation is gone (another identity's manifest, a
                // sweep, or manual storage clearing). lastFlush_ no longer describes
                // storage; left in place, every future identical-node write-through
                // would skip against it and the root would stay unpersisted for the
                // whole session. Resync from the committed manifest — never a full
                // range read/decode — and rebuild once, through the write window.
                return this.adoptCommittedBaseline_(pathString);
            });
        }
        // Dirty ranges come from the changed paths the server already named
        // (accumulateChangedPath_), consumed against the flush baseline; when
        // the accumulated set is imprecise (null: a range merge, a listen
        // completion, an unknown-path update, overflow) fall back to the
        // identity diff of the two trees — the exact pre-accumulator behavior.
        // Consume-on-read: whatever happens to this flush, the paths below are
        // relative to the CURRENT baseline only once.
        const accumulated = this.changedSinceFlush_.get(pathString);
        this.changedSinceFlush_.set(pathString, []);
        let previousRanges = [];
        let dirty = [];
        let tailDirty = false;
        let changed = null;
        // An adopted baseline (rootNode null — another writer's committed
        // manifest) has UNKNOWN content: neither the identity diff nor paths
        // accumulated against our own chain describe differences from it, and
        // carrying any of its ranges over unverified would splice two server
        // snapshots into one stored tree. Stage a fresh full generation; its
        // revision still CASes against the adopted manifest.
        if (prev && prev.ranges.length > 0 && prev.rootNode !== null) {
            changed =
                accumulated !== null && accumulated !== undefined
                    ? accumulated
                    : collectChangedSubtreePaths(prev.rootNode, node);
            if (changed !== null) {
                previousRanges = prev.ranges;
                if (changed.length === 0) {
                    dirty = new Array(prev.ranges.length).fill(false);
                }
                else {
                    const marked = markDirtyRanges(prev.ranges, changed);
                    dirty = marked.dirty;
                    tailDirty = marked.tailDirty;
                }
            }
        }
        // First pass: boundaries/sizes only. It never creates canonical strings
        // or export payloads, so a first generation cannot retain another full
        // copy of the root merely to decide its ranges.
        const planner = new CompoundHashBuilder(fixedSizeSplitStrategy(this.rangeTargetBytes_), true);
        let rebuilt;
        try {
            rebuilt = rebuildStableRanges(node, previousRanges, dirty, tailDirty, planner, this.rangeTargetBytes_);
        }
        catch (e) {
            persistenceStats.storageFailures++;
            recordPersistenceEvent(pathString, 'flush-plan-error');
            // Protective cleanup of THIS manager's own possibly-implicated
            // generation — housekeeping, so it follows the ownership rule (see
            // deleteRecordIfRevision_ / the covered-untrack delete): only a
            // revision this manager itself verified or wrote. An ADOPTED baseline
            // (rootNode null) is another writer's generation — a local planning
            // failure says nothing about it — and with no baseline at all there
            // is nothing of ours to protect against. A lease lost to a steal
            // while this flush was suspended is covered the same way: the
            // revision-named delete cannot touch the new holder's generation.
            const owned = prev !== undefined && prev.rootNode !== null ? prev.revision : null;
            this.lastFlush_.delete(pathString);
            return owned !== null
                ? this.deleteRecordIfRevision_(pathString, owned)
                : Promise.resolve();
        }
        const dirtyPlans = [];
        let previousPost = null;
        let dirtyIndex = 0;
        const ranges = rebuilt.map(range => {
            const carried = range;
            if (range.hash !== '' && typeof carried.recordId === 'string') {
                previousPost = range.post;
                return carried;
            }
            const persisted = {
                ...range,
                recordId: revision + '-' + dirtyIndex.toString(36)
            };
            dirtyPlans.push({
                range: persisted,
                start: previousPost,
                end: range.post
            });
            previousPost = range.post;
            dirtyIndex++;
            return persisted;
        });
        const stagedIds = [];
        const stageBatch = async (plans) => {
            const texts = [];
            const records = [];
            for (const plan of plans) {
                const builder = new CompoundHashBuilder(() => false);
                let text;
                let payload = undefined;
                builder.hashSink = completed => {
                    text = completed;
                };
                builder.payloadSink = completed => {
                    payload = completed;
                };
                const from = plan.start === null
                    ? null
                    : plan.start === '/'
                        ? []
                        : plan.start.split('/');
                const to = plan.end === '/' ? [] : plan.end.split('/');
                if (from !== null) {
                    builder.seedBoundary(from);
                }
                walkLeafInterval(node, from, to, builder);
                if (text === undefined ||
                    payload === undefined ||
                    builder.posts.length !== 1 ||
                    builder.posts[0] !== plan.end) {
                    throw new Error('Dirty range did not serialize to its planned boundary');
                }
                texts.push(text);
                records.push({
                    recordId: plan.range.recordId,
                    start: plan.start,
                    end: plan.end,
                    tree: payload
                });
            }
            const digests = await digestRangeTexts(texts);
            for (let i = 0; i < plans.length; i++) {
                plans[i].range.hash = digests[i];
            }
            const stored = await this.withStore_('readwrite', false, (store, done, progress) => {
                for (const record of records) {
                    const put = store.put(record, key + RANGE_KEY_INFIX + record.recordId);
                    put.onsuccess = progress;
                }
                done(true);
            });
            if (!stored) {
                throw new Error('Failed to stage persisted ranges');
            }
            stagedIds.push(...records.map(record => record.recordId));
            // Async activation records can otherwise retain completed IDB request
            // inputs until the whole generation settles. Drop every large reference
            // explicitly and yield a macrotask so WebKit can collect between batches.
            for (const record of records) {
                record.tree = undefined;
            }
            records.length = 0;
            texts.length = 0;
            digests.length = 0;
            await new Promise(resolve => setTimeout(resolve, 0));
        };
        const stageAll = async () => {
            const batchSize = 4;
            for (let i = 0; i < dirtyPlans.length; i += batchSize) {
                await stageBatch(dirtyPlans.slice(i, i + batchSize));
            }
        };
        return stageAll()
            .then(() => {
            if (this.disposed_) {
                return;
            }
            const manifest = {
                formatVersion: PERSISTENCE_FORMAT_VERSION,
                revision,
                updatedAt: now,
                authScope,
                estimatedBytes: estimateSerializedNodeSize(node),
                hash: '',
                ranges
            };
            const liveIds = new Set(ranges.map(range => range.recordId));
            const retiredIds = prev
                ? prev.ranges
                    .map(range => range.recordId)
                    .filter(recordId => !liveIds.has(recordId))
                : [];
            // The range payloads are immutable staging records. This tiny CAS
            // transaction is the atomic authority switch: until the manifest put
            // commits, a crash leaves the previous generation fully live.
            return this.withStore_('readwrite', false, (store, done, progress) => {
                // Same in-transaction eligibility re-check as the empty path:
                // the entry gate ran before staging, and the lease can be
                // released (goOffline) or lost (steal) while the staging work
                // was in flight. Failing reads as a CAS conflict — staged ids
                // are reclaimed and the write window (which re-runs the entry
                // gate) owns any retry.
                if (!this.holdsWriterLease_(pathString)) {
                    done(false);
                    return;
                }
                const currentReq = store.get(key);
                currentReq.onsuccess = () => {
                    progress();
                    // Stored `null` normalizes to ABSENT (see the empty-path
                    // comment): the first-generation commit then OVERWRITES the
                    // garbage instead of throwing on the replaceableForeign
                    // field reads and aborting every commit of this root forever.
                    const current = (currentReq.result ?? undefined);
                    // A first generation may REPLACE a manifest this manager can
                    // never restore (another identity's scope, or an unknown
                    // format): treating those as CAS winners would strand the
                    // adopt-and-retry loser forever — its readRecord_ always
                    // resolves null against a foreign manifest, so every retry
                    // re-stages the full tree and conflicts again. Same-scope
                    // manifests keep strict CAS semantics. Replacement is LIVE
                    // scope only: a generation staged under a superseded identity
                    // may still publish into an absent key under its own label
                    // (reads are scope-checked; see the in-flight relabel test)
                    // but must never replace the new identity's fresh manifest.
                    const replaceableForeign = !prev &&
                        current !== undefined &&
                        authScope === this.authScope_ &&
                        (current.formatVersion !== PERSISTENCE_FORMAT_VERSION ||
                            current.authScope !== authScope);
                    if ((prev && (!current || current.revision !== prev.revision)) ||
                        (!prev && current !== undefined && !replaceableForeign)) {
                        done(false);
                        return;
                    }
                    const commit = () => {
                        for (const recordId of retiredIds) {
                            const remove = store.delete(key + RANGE_KEY_INFIX + recordId);
                            remove.onsuccess = progress;
                        }
                        const manifestPut = store.put(manifest, key);
                        manifestPut.onsuccess = progress;
                        done(true);
                    };
                    if (stagedIds.length === 0) {
                        commit();
                        return;
                    }
                    // Another tab's sweep classifies suffixed records against the
                    // manifest that is COMMITTED, so records staged for this still
                    // unpublished generation look like orphans there and can be
                    // reclaimed between staging and this transaction without
                    // moving the manifest revision (in-memory guards only cover
                    // this tab). Publishing would durably reference missing
                    // payloads. Re-verify every staged id inside the same atomic
                    // switch — key-only reads — and treat a loss exactly like a
                    // CAS conflict. Ordering is airtight because readwrite
                    // transactions on one store serialize: a sweep that ran before
                    // this transaction is observed here; one that runs after reads
                    // this manifest and keeps its records.
                    let missing = false;
                    let verified = 0;
                    for (const recordId of stagedIds) {
                        const stagedKey = key + RANGE_KEY_INFIX + recordId;
                        // Key-only where the platform (or fake) provides it; the
                        // fallback get only runs in environments without getKey.
                        const check = typeof store.getKey === 'function'
                            ? store.getKey(stagedKey)
                            : store.get(stagedKey);
                        check.onsuccess = () => {
                            progress();
                            if (missing) {
                                return;
                            }
                            if (check.result === undefined) {
                                missing = true;
                                done(false);
                                return;
                            }
                            if (++verified === stagedIds.length) {
                                commit();
                            }
                        };
                    }
                };
            }).then(ok => {
                if (!ok || this.disposed_) {
                    if (this.disposed_) {
                        return;
                    }
                    // A different tab committed while we staged, or a concurrent
                    // sweep reclaimed our still-unreferenced staged records. Remove
                    // our immutable ids and adopt the winning manifest as the CAS
                    // baseline (manifest-only — no range read, no decode); the next
                    // window stages one fresh self-contained generation against it.
                    return this.withStore_('readwrite', undefined, store => {
                        for (const recordId of stagedIds) {
                            store.delete(key + RANGE_KEY_INFIX + recordId);
                        }
                    }).then(() => this.adoptCommittedBaseline_(pathString));
                }
                persistenceStats.rangesHashed += dirtyPlans.length;
                persistenceStats.rangesReused += ranges.length - dirtyPlans.length;
                persistenceStats.writeThroughs++;
                this.lastFlush_.set(pathString, {
                    rootNode: node,
                    revision,
                    ranges,
                    storedUpdatedAt: now
                });
                stampSeedHashes(node, manifest.hash, wireCompoundHashFromRanges(ranges));
                recordPersistenceEvent(pathString, 'stored', `${ranges.length} ranges, ${dirtyPlans.length} written`);
                if (!prev) {
                    void this.gcRangeRecords_(pathString, revision, liveIds);
                }
            });
        })
            .catch(() => {
            persistenceStats.storageFailures++;
            recordPersistenceEvent(pathString, 'flush-range-stage-error');
            // The flush consumed the accumulated changed-paths at its start, but
            // nothing was committed: lastFlush_ still describes the stored
            // baseline, so the paths this flush was covering must flow into the
            // next diff or its ranges would be carried forward stale. Merge them
            // back with whatever accrued since (either side already imprecise
            // stays imprecise).
            const since = this.changedSinceFlush_.get(pathString);
            if (accumulated === null || since === null) {
                this.changedSinceFlush_.set(pathString, null);
            }
            else if (accumulated !== undefined && accumulated.length > 0) {
                const merged = accumulated.concat(since ?? []);
                this.changedSinceFlush_.set(pathString, merged.length > MAX_ACCUMULATED_CHANGED_PATHS ? null : merged);
            }
            // Staged immutable records are non-authoritative and are reclaimed by
            // the next successful full-generation GC or the deferred sweep.
            if (stagedIds.length > 0) {
                void this.withStore_('readwrite', undefined, store => {
                    for (const recordId of stagedIds) {
                        store.delete(key + RANGE_KEY_INFIX + recordId);
                    }
                });
            }
        });
    }
    gcRangeRecords_(pathString, revision, liveIds) {
        const key = this.key_(pathString);
        const prefix = key + RANGE_KEY_INFIX;
        return this.withStore_('readwrite', undefined, (store, done) => {
            const manifestReq = store.get(key);
            manifestReq.onsuccess = () => {
                const manifest = manifestReq.result;
                if (!manifest || manifest.revision !== revision) {
                    done(undefined);
                    return;
                }
                let range;
                try {
                    range =
                        typeof IDBKeyRange !== 'undefined'
                            ? IDBKeyRange.bound(prefix, prefix + String.fromCharCode(0xffff))
                            : undefined;
                }
                catch (e) {
                    range = undefined;
                }
                const keyCursorStore = store;
                // A value cursor structured-clones every range payload; on a 65 MB
                // root that would recreate the full-read cost solely to discover keys.
                const req = typeof keyCursorStore.openKeyCursor === 'function'
                    ? keyCursorStore.openKeyCursor(range)
                    : store.openCursor(range);
                req.onsuccess = () => {
                    const cursor = req.result;
                    if (!cursor) {
                        done(undefined);
                        return;
                    }
                    if (typeof cursor.key === 'string' && cursor.key.startsWith(prefix)) {
                        const recordId = cursor.key.slice(prefix.length);
                        if (!liveIds.has(recordId)) {
                            cursor.delete();
                        }
                    }
                    cursor.continue();
                };
            };
        });
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * This class ensures the packets from the server arrive in order
 * This class takes data from the server and ensures it gets passed into the callbacks in order.
 */
class PacketReceiver {
    /**
     * @param onMessage_
     */
    constructor(onMessage_) {
        this.onMessage_ = onMessage_;
        this.pendingResponses = [];
        this.currentResponseNum = 0;
        this.closeAfterResponse = -1;
        this.onClose = null;
    }
    closeAfter(responseNum, callback) {
        this.closeAfterResponse = responseNum;
        this.onClose = callback;
        if (this.closeAfterResponse < this.currentResponseNum) {
            this.onClose();
            this.onClose = null;
        }
    }
    /**
     * Each message from the server comes with a response number, and an array of data. The responseNumber
     * allows us to ensure that we process them in the right order, since we can't be guaranteed that all
     * browsers will respond in the same order as the requests we sent
     */
    handleResponse(requestNum, data, bytes = 0) {
        this.pendingResponses[requestNum] = { data, bytes };
        while (this.pendingResponses[this.currentResponseNum]) {
            const pending = this.pendingResponses[this.currentResponseNum];
            const toProcess = pending.data;
            delete this.pendingResponses[this.currentResponseNum];
            for (let i = 0; i < toProcess.length; ++i) {
                if (toProcess[i]) {
                    exceptionGuard(() => {
                        // The long-poll callback receives a batch. Attribute its measured
                        // bytes once, to the first message, rather than serializing every
                        // parsed item again.
                        this.onMessage_(toProcess[i], i === 0 ? pending.bytes : 0);
                    });
                }
            }
            if (this.currentResponseNum === this.closeAfterResponse) {
                if (this.onClose) {
                    this.onClose();
                    this.onClose = null;
                }
                break;
            }
            this.currentResponseNum++;
        }
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// URL query parameters associated with longpolling
const FIREBASE_LONGPOLL_START_PARAM = 'start';
const FIREBASE_LONGPOLL_CLOSE_COMMAND = 'close';
const FIREBASE_LONGPOLL_COMMAND_CB_NAME = 'pLPCommand';
const FIREBASE_LONGPOLL_DATA_CB_NAME = 'pRTLPCB';
const FIREBASE_LONGPOLL_ID_PARAM = 'id';
const FIREBASE_LONGPOLL_PW_PARAM = 'pw';
const FIREBASE_LONGPOLL_SERIAL_PARAM = 'ser';
const FIREBASE_LONGPOLL_CALLBACK_ID_PARAM = 'cb';
const FIREBASE_LONGPOLL_SEGMENT_NUM_PARAM = 'seg';
const FIREBASE_LONGPOLL_SEGMENTS_IN_PACKET = 'ts';
const FIREBASE_LONGPOLL_DATA_PARAM = 'd';
const FIREBASE_LONGPOLL_DISCONN_FRAME_REQUEST_PARAM = 'dframe';
//Data size constants.
//TODO: Perf: the maximum length actually differs from browser to browser.
// We should check what browser we're on and set accordingly.
const MAX_URL_DATA_SIZE = 1870;
const SEG_HEADER_SIZE = 30; //ie: &seg=8299234&ts=982389123&d=
const MAX_PAYLOAD_SIZE = MAX_URL_DATA_SIZE - SEG_HEADER_SIZE;
/**
 * Keepalive period
 * send a fresh request at minimum every 25 seconds. Opera has a maximum request
 * length of 30 seconds that we can't exceed.
 */
const KEEPALIVE_REQUEST_INTERVAL = 25000;
/**
 * How long to wait before aborting a long-polling connection attempt.
 */
const LP_CONNECT_TIMEOUT = 30000;
/**
 * This class manages a single long-polling connection.
 */
class BrowserPollConnection {
    /**
     * @param connId An identifier for this connection, used for logging
     * @param repoInfo The info for the endpoint to send data to.
     * @param applicationId The Firebase App ID for this project.
     * @param appCheckToken The AppCheck token for this client.
     * @param authToken The AuthToken to use for this connection.
     * @param transportSessionId Optional transportSessionid if we are
     * reconnecting for an existing transport session
     * @param lastSessionId Optional lastSessionId if the PersistentConnection has
     * already created a connection previously
     */
    constructor(connId, repoInfo, applicationId, appCheckToken, authToken, transportSessionId, lastSessionId) {
        this.connId = connId;
        this.repoInfo = repoInfo;
        this.applicationId = applicationId;
        this.appCheckToken = appCheckToken;
        this.authToken = authToken;
        this.transportSessionId = transportSessionId;
        this.lastSessionId = lastSessionId;
        this.bytesSent = 0;
        this.bytesReceived = 0;
        this.everConnected_ = false;
        this.log_ = logWrapper(connId);
        this.stats_ = statsManagerGetCollection(repoInfo);
        this.urlFn = (params) => {
            // Always add the token if we have one.
            if (this.appCheckToken) {
                params[APP_CHECK_TOKEN_PARAM] = this.appCheckToken;
            }
            return repoInfoConnectionURL(repoInfo, LONG_POLLING, params);
        };
    }
    /**
     * @param onMessage - Callback when messages arrive
     * @param onDisconnect - Callback with connection lost.
     */
    open(onMessage, onDisconnect) {
        this.curSegmentNum = 0;
        this.onDisconnect_ = onDisconnect;
        this.myPacketOrderer = new PacketReceiver(onMessage);
        this.isClosed_ = false;
        this.connectTimeoutTimer_ = setTimeout(() => {
            this.log_('Timed out trying to connect.');
            // Make sure we clear the host cache
            this.onClosed_();
            this.connectTimeoutTimer_ = null;
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
        }, Math.floor(LP_CONNECT_TIMEOUT));
        // Ensure we delay the creation of the iframe until the DOM is loaded.
        executeWhenDOMReady(() => {
            if (this.isClosed_) {
                return;
            }
            //Set up a callback that gets triggered once a connection is set up.
            this.scriptTagHolder = new FirebaseIFrameScriptHolder((...args) => {
                const [command, arg1, arg2, arg3, arg4] = args;
                this.incrementIncomingBytes_(args);
                if (!this.scriptTagHolder) {
                    return; // we closed the connection.
                }
                if (this.connectTimeoutTimer_) {
                    clearTimeout(this.connectTimeoutTimer_);
                    this.connectTimeoutTimer_ = null;
                }
                this.everConnected_ = true;
                if (command === FIREBASE_LONGPOLL_START_PARAM) {
                    this.id = arg1;
                    this.password = arg2;
                }
                else if (command === FIREBASE_LONGPOLL_CLOSE_COMMAND) {
                    // Don't clear the host cache. We got a response from the server, so we know it's reachable
                    if (arg1) {
                        // We aren't expecting any more data (other than what the server's already in the process of sending us
                        // through our already open polls), so don't send any more.
                        this.scriptTagHolder.sendNewPolls = false;
                        // arg1 in this case is the last response number sent by the server. We should try to receive
                        // all of the responses up to this one before closing
                        this.myPacketOrderer.closeAfter(arg1, () => {
                            this.onClosed_();
                        });
                    }
                    else {
                        this.onClosed_();
                    }
                }
                else {
                    throw new Error('Unrecognized command received: ' + command);
                }
            }, (...args) => {
                const [pN, data] = args;
                const bytes = this.incrementIncomingBytes_(args);
                this.myPacketOrderer.handleResponse(pN, data, bytes);
            }, () => {
                this.onClosed_();
            }, this.urlFn);
            //Send the initial request to connect. The serial number is simply to keep the browser from pulling previous results
            //from cache.
            const urlParams = {};
            urlParams[FIREBASE_LONGPOLL_START_PARAM] = 't';
            urlParams[FIREBASE_LONGPOLL_SERIAL_PARAM] = Math.floor(Math.random() * 100000000);
            if (this.scriptTagHolder.uniqueCallbackIdentifier) {
                urlParams[FIREBASE_LONGPOLL_CALLBACK_ID_PARAM] =
                    this.scriptTagHolder.uniqueCallbackIdentifier;
            }
            urlParams[VERSION_PARAM] = PROTOCOL_VERSION;
            if (this.transportSessionId) {
                urlParams[TRANSPORT_SESSION_PARAM] = this.transportSessionId;
            }
            if (this.lastSessionId) {
                urlParams[LAST_SESSION_PARAM] = this.lastSessionId;
            }
            if (this.applicationId) {
                urlParams[APPLICATION_ID_PARAM] = this.applicationId;
            }
            if (this.appCheckToken) {
                urlParams[APP_CHECK_TOKEN_PARAM] = this.appCheckToken;
            }
            if (typeof location !== 'undefined' &&
                location.hostname &&
                FORGE_DOMAIN_RE.test(location.hostname)) {
                urlParams[REFERER_PARAM] = FORGE_REF;
            }
            const connectURL = this.urlFn(urlParams);
            this.log_('Connecting via long-poll to ' + connectURL);
            this.scriptTagHolder.addTag(connectURL, () => {
                /* do nothing */
            });
        });
    }
    /**
     * Call this when a handshake has completed successfully and we want to consider the connection established
     */
    start() {
        this.scriptTagHolder.startLongPoll(this.id, this.password);
        this.addDisconnectPingFrame(this.id, this.password);
    }
    /**
     * Forces long polling to be considered as a potential transport
     */
    static forceAllow() {
        BrowserPollConnection.forceAllow_ = true;
    }
    /**
     * Forces longpolling to not be considered as a potential transport
     */
    static forceDisallow() {
        BrowserPollConnection.forceDisallow_ = true;
    }
    // Static method, use string literal so it can be accessed in a generic way
    static isAvailable() {
        if (util.isNodeSdk()) {
            return false;
        }
        else if (BrowserPollConnection.forceAllow_) {
            return true;
        }
        else {
            // NOTE: In React-Native there's normally no 'document', but if you debug a React-Native app in
            // the Chrome debugger, 'document' is defined, but document.createElement is null (2015/06/08).
            return (!BrowserPollConnection.forceDisallow_ &&
                typeof document !== 'undefined' &&
                document.createElement != null &&
                !isChromeExtensionContentScript() &&
                !isWindowsStoreApp());
        }
    }
    /**
     * No-op for polling
     */
    markConnectionHealthy() { }
    /**
     * Stops polling and cleans up the iframe
     */
    shutdown_() {
        this.isClosed_ = true;
        if (this.scriptTagHolder) {
            this.scriptTagHolder.close();
            this.scriptTagHolder = null;
        }
        //remove the disconnect frame, which will trigger an XHR call to the server to tell it we're leaving.
        if (this.myDisconnFrame) {
            document.body.removeChild(this.myDisconnFrame);
            this.myDisconnFrame = null;
        }
        if (this.connectTimeoutTimer_) {
            clearTimeout(this.connectTimeoutTimer_);
            this.connectTimeoutTimer_ = null;
        }
    }
    /**
     * Triggered when this transport is closed
     */
    onClosed_() {
        if (!this.isClosed_) {
            this.log_('Longpoll is closing itself');
            this.shutdown_();
            if (this.onDisconnect_) {
                this.onDisconnect_(this.everConnected_);
                this.onDisconnect_ = null;
            }
        }
    }
    /**
     * External-facing close handler. RealTime has requested we shut down. Kill our connection and tell the server
     * that we've left.
     */
    close() {
        if (!this.isClosed_) {
            this.log_('Longpoll is being closed.');
            this.shutdown_();
        }
    }
    /**
     * Send the JSON object down to the server. It will need to be stringified, base64 encoded, and then
     * broken into chunks (since URLs have a small maximum length).
     * @param data - The JSON data to transmit.
     */
    send(data) {
        const dataStr = util.stringify(data);
        this.bytesSent += dataStr.length;
        this.stats_.incrementCounter('bytes_sent', dataStr.length);
        //first, lets get the base64-encoded data
        const base64data = util.base64Encode(dataStr);
        //We can only fit a certain amount in each URL, so we need to split this request
        //up into multiple pieces if it doesn't fit in one request.
        const dataSegs = splitStringBySize(base64data, MAX_PAYLOAD_SIZE);
        //Enqueue each segment for transmission. We assign each chunk a sequential ID and a total number
        //of segments so that we can reassemble the packet on the server.
        for (let i = 0; i < dataSegs.length; i++) {
            this.scriptTagHolder.enqueueSegment(this.curSegmentNum, dataSegs.length, dataSegs[i]);
            this.curSegmentNum++;
        }
    }
    /**
     * This is how we notify the server that we're leaving.
     * We aren't able to send requests with DHTML on a window close event, but we can
     * trigger XHR requests in some browsers (everything but Opera basically).
     */
    addDisconnectPingFrame(id, pw) {
        if (util.isNodeSdk()) {
            return;
        }
        this.myDisconnFrame = document.createElement('iframe');
        const urlParams = {};
        urlParams[FIREBASE_LONGPOLL_DISCONN_FRAME_REQUEST_PARAM] = 't';
        urlParams[FIREBASE_LONGPOLL_ID_PARAM] = id;
        urlParams[FIREBASE_LONGPOLL_PW_PARAM] = pw;
        this.myDisconnFrame.src = this.urlFn(urlParams);
        this.myDisconnFrame.style.display = 'none';
        document.body.appendChild(this.myDisconnFrame);
    }
    /**
     * Used to track the bytes received by this client
     */
    incrementIncomingBytes_(args) {
        // TODO: This is an annoying perf hit just to track the number of incoming bytes.  Maybe it should be opt-in.
        const bytesReceived = util.stringify(args).length;
        this.bytesReceived += bytesReceived;
        this.stats_.incrementCounter('bytes_received', bytesReceived);
        return bytesReceived;
    }
}
/*********************************************************************************************
 * A wrapper around an iframe that is used as a long-polling script holder.
 *********************************************************************************************/
class FirebaseIFrameScriptHolder {
    /**
     * @param commandCB - The callback to be called when control commands are received from the server.
     * @param onMessageCB - The callback to be triggered when responses arrive from the server.
     * @param onDisconnect - The callback to be triggered when this tag holder is closed
     * @param urlFn - A function that provides the URL of the endpoint to send data to.
     */
    constructor(commandCB, onMessageCB, onDisconnect, urlFn) {
        this.onDisconnect = onDisconnect;
        this.urlFn = urlFn;
        //We maintain a count of all of the outstanding requests, because if we have too many active at once it can cause
        //problems in some browsers.
        this.outstandingRequests = new Set();
        //A queue of the pending segments waiting for transmission to the server.
        this.pendingSegs = [];
        //A serial number. We use this for two things:
        // 1) A way to ensure the browser doesn't cache responses to polls
        // 2) A way to make the server aware when long-polls arrive in a different order than we started them. The
        //    server needs to release both polls in this case or it will cause problems in Opera since Opera can only execute
        //    JSONP code in the order it was added to the iframe.
        this.currentSerial = Math.floor(Math.random() * 100000000);
        // This gets set to false when we're "closing down" the connection (e.g. we're switching transports but there's still
        // incoming data from the server that we're waiting for).
        this.sendNewPolls = true;
        if (!util.isNodeSdk()) {
            //Each script holder registers a couple of uniquely named callbacks with the window. These are called from the
            //iframes where we put the long-polling script tags. We have two callbacks:
            //   1) Command Callback - Triggered for control issues, like starting a connection.
            //   2) Message Callback - Triggered when new data arrives.
            this.uniqueCallbackIdentifier = LUIDGenerator();
            window[FIREBASE_LONGPOLL_COMMAND_CB_NAME + this.uniqueCallbackIdentifier] = commandCB;
            window[FIREBASE_LONGPOLL_DATA_CB_NAME + this.uniqueCallbackIdentifier] =
                onMessageCB;
            //Create an iframe for us to add script tags to.
            this.myIFrame = FirebaseIFrameScriptHolder.createIFrame_();
            // Set the iframe's contents.
            let script = '';
            // if we set a javascript url, it's IE and we need to set the document domain. The javascript url is sufficient
            // for ie9, but ie8 needs to do it again in the document itself.
            if (this.myIFrame.src &&
                this.myIFrame.src.substr(0, 'javascript:'.length) === 'javascript:') {
                const currentDomain = document.domain;
                script = '<script>document.domain="' + currentDomain + '";</script>';
            }
            const iframeContents = '<html><body>' + script + '</body></html>';
            try {
                this.myIFrame.doc.open();
                this.myIFrame.doc.write(iframeContents);
                this.myIFrame.doc.close();
            }
            catch (e) {
                log('frame writing exception');
                if (e.stack) {
                    log(e.stack);
                }
                log(e);
            }
        }
        else {
            this.commandCB = commandCB;
            this.onMessageCB = onMessageCB;
        }
    }
    /**
     * Each browser has its own funny way to handle iframes. Here we mush them all together into one object that I can
     * actually use.
     */
    static createIFrame_() {
        const iframe = document.createElement('iframe');
        iframe.style.display = 'none';
        // This is necessary in order to initialize the document inside the iframe
        if (document.body) {
            document.body.appendChild(iframe);
            try {
                // If document.domain has been modified in IE, this will throw an error, and we need to set the
                // domain of the iframe's document manually. We can do this via a javascript: url as the src attribute
                // Also note that we must do this *after* the iframe has been appended to the page. Otherwise it doesn't work.
                const a = iframe.contentWindow.document;
                if (!a) {
                    // Apologies for the log-spam, I need to do something to keep closure from optimizing out the assignment above.
                    log('No IE domain setting required');
                }
            }
            catch (e) {
                const domain = document.domain;
                iframe.src =
                    "javascript:void((function(){document.open();document.domain='" +
                        domain +
                        "';document.close();})())";
            }
        }
        else {
            // LongPollConnection attempts to delay initialization until the document is ready, so hopefully this
            // never gets hit.
            throw 'Document body has not initialized. Wait to initialize Firebase until after the document is ready.';
        }
        // Get the document of the iframe in a browser-specific way.
        if (iframe.contentDocument) {
            iframe.doc = iframe.contentDocument; // Firefox, Opera, Safari
        }
        else if (iframe.contentWindow) {
            iframe.doc = iframe.contentWindow.document; // Internet Explorer
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
        }
        else if (iframe.document) {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            iframe.doc = iframe.document; //others?
        }
        return iframe;
    }
    /**
     * Cancel all outstanding queries and remove the frame.
     */
    close() {
        //Mark this iframe as dead, so no new requests are sent.
        this.alive = false;
        if (this.myIFrame) {
            //We have to actually remove all of the html inside this iframe before removing it from the
            //window, or IE will continue loading and executing the script tags we've already added, which
            //can lead to some errors being thrown. Setting textContent seems to be the safest way to do this.
            this.myIFrame.doc.body.textContent = '';
            setTimeout(() => {
                if (this.myIFrame !== null) {
                    document.body.removeChild(this.myIFrame);
                    this.myIFrame = null;
                }
            }, Math.floor(0));
        }
        // Protect from being called recursively.
        const onDisconnect = this.onDisconnect;
        if (onDisconnect) {
            this.onDisconnect = null;
            onDisconnect();
        }
    }
    /**
     * Actually start the long-polling session by adding the first script tag(s) to the iframe.
     * @param id - The ID of this connection
     * @param pw - The password for this connection
     */
    startLongPoll(id, pw) {
        this.myID = id;
        this.myPW = pw;
        this.alive = true;
        //send the initial request. If there are requests queued, make sure that we transmit as many as we are currently able to.
        while (this.newRequest_()) { }
    }
    /**
     * This is called any time someone might want a script tag to be added. It adds a script tag when there aren't
     * too many outstanding requests and we are still alive.
     *
     * If there are outstanding packet segments to send, it sends one. If there aren't, it sends a long-poll anyways if
     * needed.
     */
    newRequest_() {
        // We keep one outstanding request open all the time to receive data, but if we need to send data
        // (pendingSegs.length > 0) then we create a new request to send the data.  The server will automatically
        // close the old request.
        if (this.alive &&
            this.sendNewPolls &&
            this.outstandingRequests.size < (this.pendingSegs.length > 0 ? 2 : 1)) {
            //construct our url
            this.currentSerial++;
            const urlParams = {};
            urlParams[FIREBASE_LONGPOLL_ID_PARAM] = this.myID;
            urlParams[FIREBASE_LONGPOLL_PW_PARAM] = this.myPW;
            urlParams[FIREBASE_LONGPOLL_SERIAL_PARAM] = this.currentSerial;
            let theURL = this.urlFn(urlParams);
            //Now add as much data as we can.
            let curDataString = '';
            let i = 0;
            while (this.pendingSegs.length > 0) {
                //first, lets see if the next segment will fit.
                const nextSeg = this.pendingSegs[0];
                if (nextSeg.d.length +
                    SEG_HEADER_SIZE +
                    curDataString.length <=
                    MAX_URL_DATA_SIZE) {
                    //great, the segment will fit. Lets append it.
                    const theSeg = this.pendingSegs.shift();
                    curDataString =
                        curDataString +
                            '&' +
                            FIREBASE_LONGPOLL_SEGMENT_NUM_PARAM +
                            i +
                            '=' +
                            theSeg.seg +
                            '&' +
                            FIREBASE_LONGPOLL_SEGMENTS_IN_PACKET +
                            i +
                            '=' +
                            theSeg.ts +
                            '&' +
                            FIREBASE_LONGPOLL_DATA_PARAM +
                            i +
                            '=' +
                            theSeg.d;
                    i++;
                }
                else {
                    break;
                }
            }
            theURL = theURL + curDataString;
            this.addLongPollTag_(theURL, this.currentSerial);
            return true;
        }
        else {
            return false;
        }
    }
    /**
     * Queue a packet for transmission to the server.
     * @param segnum - A sequential id for this packet segment used for reassembly
     * @param totalsegs - The total number of segments in this packet
     * @param data - The data for this segment.
     */
    enqueueSegment(segnum, totalsegs, data) {
        //add this to the queue of segments to send.
        this.pendingSegs.push({ seg: segnum, ts: totalsegs, d: data });
        //send the data immediately if there isn't already data being transmitted, unless
        //startLongPoll hasn't been called yet.
        if (this.alive) {
            this.newRequest_();
        }
    }
    /**
     * Add a script tag for a regular long-poll request.
     * @param url - The URL of the script tag.
     * @param serial - The serial number of the request.
     */
    addLongPollTag_(url, serial) {
        //remember that we sent this request.
        this.outstandingRequests.add(serial);
        const doNewRequest = () => {
            this.outstandingRequests.delete(serial);
            this.newRequest_();
        };
        // If this request doesn't return on its own accord (by the server sending us some data), we'll
        // create a new one after the KEEPALIVE interval to make sure we always keep a fresh request open.
        const keepaliveTimeout = setTimeout(doNewRequest, Math.floor(KEEPALIVE_REQUEST_INTERVAL));
        const readyStateCB = () => {
            // Request completed.  Cancel the keepalive.
            clearTimeout(keepaliveTimeout);
            // Trigger a new request so we can continue receiving data.
            doNewRequest();
        };
        this.addTag(url, readyStateCB);
    }
    /**
     * Add an arbitrary script tag to the iframe.
     * @param url - The URL for the script tag source.
     * @param loadCB - A callback to be triggered once the script has loaded.
     */
    addTag(url, loadCB) {
        if (util.isNodeSdk()) {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            this.doNodeLongPoll(url, loadCB);
        }
        else {
            setTimeout(() => {
                try {
                    // if we're already closed, don't add this poll
                    if (!this.sendNewPolls) {
                        return;
                    }
                    const newScript = this.myIFrame.doc.createElement('script');
                    newScript.type = 'text/javascript';
                    newScript.async = true;
                    newScript.src = url;
                    // eslint-disable-next-line @typescript-eslint/no-explicit-any
                    newScript.onload = newScript.onreadystatechange =
                        function () {
                            // eslint-disable-next-line @typescript-eslint/no-explicit-any
                            const rstate = newScript.readyState;
                            if (!rstate || rstate === 'loaded' || rstate === 'complete') {
                                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                                newScript.onload = newScript.onreadystatechange = null;
                                if (newScript.parentNode) {
                                    newScript.parentNode.removeChild(newScript);
                                }
                                loadCB();
                            }
                        };
                    newScript.onerror = () => {
                        log('Long-poll script failed to load: ' + url);
                        this.sendNewPolls = false;
                        this.close();
                    };
                    this.myIFrame.doc.body.appendChild(newScript);
                }
                catch (e) {
                    // TODO: we should make this error visible somehow
                }
            }, Math.floor(1));
        }
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Currently simplistic, this class manages what transport a Connection should use at various stages of its
 * lifecycle.
 *
 * It starts with longpolling in a browser, and httppolling on node. It then upgrades to websockets if
 * they are available.
 */
class TransportManager {
    static get ALL_TRANSPORTS() {
        return [BrowserPollConnection, WebSocketConnection];
    }
    /**
     * Returns whether transport has been selected to ensure WebSocketConnection or BrowserPollConnection are not called after
     * TransportManager has already set up transports_
     */
    static get IS_TRANSPORT_INITIALIZED() {
        return this.globalTransportInitialized_;
    }
    /**
     * @param repoInfo - Metadata around the namespace we're connecting to
     */
    constructor(repoInfo) {
        this.initTransports_(repoInfo);
    }
    initTransports_(repoInfo) {
        const isWebSocketsAvailable = WebSocketConnection && WebSocketConnection['isAvailable']();
        let isSkipPollConnection = isWebSocketsAvailable && !WebSocketConnection.previouslyFailed();
        if (repoInfo.webSocketOnly) {
            if (!isWebSocketsAvailable) {
                warn("wss:// URL used, but browser isn't known to support websockets.  Trying anyway.");
            }
            isSkipPollConnection = true;
        }
        if (isSkipPollConnection) {
            this.transports_ = [WebSocketConnection];
        }
        else {
            const transports = (this.transports_ = []);
            for (const transport of TransportManager.ALL_TRANSPORTS) {
                if (transport && transport['isAvailable']()) {
                    transports.push(transport);
                }
            }
            TransportManager.globalTransportInitialized_ = true;
        }
    }
    /**
     * @returns The constructor for the initial transport to use
     */
    initialTransport() {
        if (this.transports_.length > 0) {
            return this.transports_[0];
        }
        else {
            throw new Error('No transports available');
        }
    }
    /**
     * @returns The constructor for the next transport, or null
     */
    upgradeTransport() {
        if (this.transports_.length > 1) {
            return this.transports_[1];
        }
        else {
            return null;
        }
    }
}
// Keeps track of whether the TransportManager has already chosen a transport to use
TransportManager.globalTransportInitialized_ = false;

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Abort upgrade attempt if it takes longer than 60s.
const UPGRADE_TIMEOUT = 60000;
// For some transports (WebSockets), we need to "validate" the transport by exchanging a few requests and responses.
// If we haven't sent enough requests within 5s, we'll start sending noop ping requests.
const DELAY_BEFORE_SENDING_EXTRA_REQUESTS = 5000;
// If the initial data sent triggers a lot of bandwidth (i.e. it's a large put or a listen for a large amount of data)
// then we may not be able to exchange our ping/pong requests within the healthy timeout.  So if we reach the timeout
// but we've sent/received enough bytes, we don't cancel the connection.
const BYTES_SENT_HEALTHY_OVERRIDE = 10 * 1024;
const BYTES_RECEIVED_HEALTHY_OVERRIDE = 100 * 1024;
const MESSAGE_TYPE = 't';
const MESSAGE_DATA = 'd';
const CONTROL_SHUTDOWN = 's';
const CONTROL_RESET = 'r';
const CONTROL_ERROR = 'e';
const CONTROL_PONG = 'o';
const SWITCH_ACK = 'a';
const END_TRANSMISSION = 'n';
const PING = 'p';
const SERVER_HELLO = 'h';
/**
 * Creates a new real-time connection to the server using whichever method works
 * best in the current browser.
 */
class Connection {
    /**
     * @param id - an id for this connection
     * @param repoInfo_ - the info for the endpoint to connect to
     * @param applicationId_ - the Firebase App ID for this project
     * @param appCheckToken_ - The App Check Token for this device.
     * @param authToken_ - The auth token for this session.
     * @param onMessage_ - the callback to be triggered when a server-push message arrives
     * @param onReady_ - the callback to be triggered when this connection is ready to send messages.
     * @param onDisconnect_ - the callback to be triggered when a connection was lost
     * @param onKill_ - the callback to be triggered when this connection has permanently shut down.
     * @param lastSessionId - last session id in persistent connection. is used to clean up old session in real-time server
     */
    constructor(id, repoInfo_, applicationId_, appCheckToken_, authToken_, onMessage_, onReady_, onDisconnect_, onKill_, lastSessionId) {
        this.id = id;
        this.repoInfo_ = repoInfo_;
        this.applicationId_ = applicationId_;
        this.appCheckToken_ = appCheckToken_;
        this.authToken_ = authToken_;
        this.onMessage_ = onMessage_;
        this.onReady_ = onReady_;
        this.onDisconnect_ = onDisconnect_;
        this.onKill_ = onKill_;
        this.lastSessionId = lastSessionId;
        this.connectionCount = 0;
        this.pendingDataMessages = [];
        this.state_ = 0 /* RealtimeState.CONNECTING */;
        this.log_ = logWrapper('c:' + this.id + ':');
        this.transportManager_ = new TransportManager(repoInfo_);
        this.log_('Connection created');
        this.start_();
    }
    /**
     * Starts a connection attempt
     */
    start_() {
        const conn = this.transportManager_.initialTransport();
        this.conn_ = new conn(this.nextTransportId_(), this.repoInfo_, this.applicationId_, this.appCheckToken_, this.authToken_, null, this.lastSessionId);
        // For certain transports (WebSockets), we need to send and receive several messages back and forth before we
        // can consider the transport healthy.
        this.primaryResponsesRequired_ = conn['responsesRequiredToBeHealthy'] || 0;
        const onMessageReceived = this.connReceiver_(this.conn_);
        const onConnectionLost = this.disconnReceiver_(this.conn_);
        this.tx_ = this.conn_;
        this.rx_ = this.conn_;
        this.secondaryConn_ = null;
        this.isHealthy_ = false;
        /*
         * Firefox doesn't like when code from one iframe tries to create another iframe by way of the parent frame.
         * This can occur in the case of a redirect, i.e. we guessed wrong on what server to connect to and received a reset.
         * Somehow, setTimeout seems to make this ok. That doesn't make sense from a security perspective, since you should
         * still have the context of your originating frame.
         */
        setTimeout(() => {
            // this.conn_ gets set to null in some of the tests. Check to make sure it still exists before using it
            this.conn_ && this.conn_.open(onMessageReceived, onConnectionLost);
        }, Math.floor(0));
        const healthyTimeoutMS = conn['healthyTimeout'] || 0;
        if (healthyTimeoutMS > 0) {
            this.healthyTimeout_ = setTimeoutNonBlocking(() => {
                this.healthyTimeout_ = null;
                if (!this.isHealthy_) {
                    if (this.conn_ &&
                        this.conn_.bytesReceived > BYTES_RECEIVED_HEALTHY_OVERRIDE) {
                        this.log_('Connection exceeded healthy timeout but has received ' +
                            this.conn_.bytesReceived +
                            ' bytes.  Marking connection healthy.');
                        this.isHealthy_ = true;
                        this.conn_.markConnectionHealthy();
                    }
                    else if (this.conn_ &&
                        this.conn_.bytesSent > BYTES_SENT_HEALTHY_OVERRIDE) {
                        this.log_('Connection exceeded healthy timeout but has sent ' +
                            this.conn_.bytesSent +
                            ' bytes.  Leaving connection alive.');
                        // NOTE: We don't want to mark it healthy, since we have no guarantee that the bytes have made it to
                        // the server.
                    }
                    else {
                        this.log_('Closing unhealthy connection after timeout.');
                        this.close();
                    }
                }
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
            }, Math.floor(healthyTimeoutMS));
        }
    }
    nextTransportId_() {
        return 'c:' + this.id + ':' + this.connectionCount++;
    }
    disconnReceiver_(conn) {
        return everConnected => {
            if (conn === this.conn_) {
                this.onConnectionLost_(everConnected);
            }
            else if (conn === this.secondaryConn_) {
                this.log_('Secondary connection lost.');
                this.onSecondaryConnectionLost_();
            }
            else {
                this.log_('closing an old connection');
            }
        };
    }
    connReceiver_(conn) {
        return (message, bytes = 0) => {
            if (this.state_ !== 2 /* RealtimeState.DISCONNECTED */) {
                if (conn === this.rx_) {
                    this.onPrimaryMessageReceived_(message, bytes);
                }
                else if (conn === this.secondaryConn_) {
                    this.onSecondaryMessageReceived_(message, bytes);
                }
                else {
                    this.log_('message on old connection');
                }
            }
        };
    }
    /**
     * @param dataMsg - An arbitrary data message to be sent to the server
     */
    sendRequest(dataMsg) {
        // wrap in a data message envelope and send it on
        const msg = { t: 'd', d: dataMsg };
        this.sendData_(msg);
    }
    tryCleanupConnection() {
        if (this.tx_ === this.secondaryConn_ && this.rx_ === this.secondaryConn_) {
            this.log_('cleaning up and promoting a connection: ' + this.secondaryConn_.connId);
            this.conn_ = this.secondaryConn_;
            this.secondaryConn_ = null;
            // the server will shutdown the old connection
        }
    }
    onSecondaryControl_(controlData) {
        if (MESSAGE_TYPE in controlData) {
            const cmd = controlData[MESSAGE_TYPE];
            if (cmd === SWITCH_ACK) {
                this.upgradeIfSecondaryHealthy_();
            }
            else if (cmd === CONTROL_RESET) {
                // Most likely the session wasn't valid. Abandon the switch attempt
                this.log_('Got a reset on secondary, closing it');
                this.secondaryConn_.close();
                // If we were already using this connection for something, than we need to fully close
                if (this.tx_ === this.secondaryConn_ ||
                    this.rx_ === this.secondaryConn_) {
                    this.close();
                }
            }
            else if (cmd === CONTROL_PONG) {
                this.log_('got pong on secondary.');
                this.secondaryResponsesRequired_--;
                this.upgradeIfSecondaryHealthy_();
            }
        }
    }
    onSecondaryMessageReceived_(parsedData, bytes) {
        const layer = requireKey('t', parsedData);
        const data = requireKey('d', parsedData);
        if (layer === 'c') {
            this.onSecondaryControl_(data);
        }
        else if (layer === 'd') {
            // got a data message, but we're still second connection. Need to buffer it up
            this.pendingDataMessages.push({ data, bytes });
        }
        else {
            throw new Error('Unknown protocol layer: ' + layer);
        }
    }
    upgradeIfSecondaryHealthy_() {
        if (this.secondaryResponsesRequired_ <= 0) {
            this.log_('Secondary connection is healthy.');
            this.isHealthy_ = true;
            this.secondaryConn_.markConnectionHealthy();
            this.proceedWithUpgrade_();
        }
        else {
            // Send a ping to make sure the connection is healthy.
            this.log_('sending ping on secondary.');
            this.secondaryConn_.send({ t: 'c', d: { t: PING, d: {} } });
        }
    }
    proceedWithUpgrade_() {
        // tell this connection to consider itself open
        this.secondaryConn_.start();
        // send ack
        this.log_('sending client ack on secondary');
        this.secondaryConn_.send({ t: 'c', d: { t: SWITCH_ACK, d: {} } });
        // send end packet on primary transport, switch to sending on this one
        // can receive on this one, buffer responses until end received on primary transport
        this.log_('Ending transmission on primary');
        this.conn_.send({ t: 'c', d: { t: END_TRANSMISSION, d: {} } });
        this.tx_ = this.secondaryConn_;
        this.tryCleanupConnection();
    }
    onPrimaryMessageReceived_(parsedData, bytes) {
        // Must refer to parsedData properties in quotes, so closure doesn't touch them.
        const layer = requireKey('t', parsedData);
        const data = requireKey('d', parsedData);
        if (layer === 'c') {
            this.onControl_(data);
        }
        else if (layer === 'd') {
            this.onDataMessage_(data, bytes);
        }
    }
    onDataMessage_(message, bytes = 0) {
        this.onPrimaryResponse_();
        // We don't do anything with data messages, just kick them up a level
        this.onMessage_(message, bytes);
    }
    onPrimaryResponse_() {
        if (!this.isHealthy_) {
            this.primaryResponsesRequired_--;
            if (this.primaryResponsesRequired_ <= 0) {
                this.log_('Primary connection is healthy.');
                this.isHealthy_ = true;
                this.conn_.markConnectionHealthy();
            }
        }
    }
    onControl_(controlData) {
        const cmd = requireKey(MESSAGE_TYPE, controlData);
        if (MESSAGE_DATA in controlData) {
            const payload = controlData[MESSAGE_DATA];
            if (cmd === SERVER_HELLO) {
                const handshakePayload = {
                    ...payload
                };
                if (this.repoInfo_.isUsingEmulator) {
                    // Upon connecting, the emulator will pass the hostname that it's aware of, but we prefer the user's set hostname via `connectDatabaseEmulator` over what the emulator passes.
                    handshakePayload.h = this.repoInfo_.host;
                }
                this.onHandshake_(handshakePayload);
            }
            else if (cmd === END_TRANSMISSION) {
                this.log_('recvd end transmission on primary');
                this.rx_ = this.secondaryConn_;
                for (const pending of this.pendingDataMessages) {
                    this.onDataMessage_(pending.data, pending.bytes);
                }
                this.pendingDataMessages = [];
                this.tryCleanupConnection();
            }
            else if (cmd === CONTROL_SHUTDOWN) {
                // This was previously the 'onKill' callback passed to the lower-level connection
                // payload in this case is the reason for the shutdown. Generally a human-readable error
                this.onConnectionShutdown_(payload);
            }
            else if (cmd === CONTROL_RESET) {
                // payload in this case is the host we should contact
                this.onReset_(payload);
            }
            else if (cmd === CONTROL_ERROR) {
                error('Server Error: ' + payload);
            }
            else if (cmd === CONTROL_PONG) {
                this.log_('got pong on primary.');
                this.onPrimaryResponse_();
                this.sendPingOnPrimaryIfNecessary_();
            }
            else {
                error('Unknown control packet command: ' + cmd);
            }
        }
    }
    /**
     * @param handshake - The handshake data returned from the server
     */
    onHandshake_(handshake) {
        const timestamp = handshake.ts;
        const version = handshake.v;
        const host = handshake.h;
        this.sessionId = handshake.s;
        this.repoInfo_.host = host;
        // if we've already closed the connection, then don't bother trying to progress further
        if (this.state_ === 0 /* RealtimeState.CONNECTING */) {
            this.conn_.start();
            this.onConnectionEstablished_(this.conn_, timestamp);
            if (PROTOCOL_VERSION !== version) {
                warn('Protocol version mismatch detected');
            }
            // TODO: do we want to upgrade? when? maybe a delay?
            this.tryStartUpgrade_();
        }
    }
    tryStartUpgrade_() {
        const conn = this.transportManager_.upgradeTransport();
        if (conn) {
            this.startUpgrade_(conn);
        }
    }
    startUpgrade_(conn) {
        this.secondaryConn_ = new conn(this.nextTransportId_(), this.repoInfo_, this.applicationId_, this.appCheckToken_, this.authToken_, this.sessionId);
        // For certain transports (WebSockets), we need to send and receive several messages back and forth before we
        // can consider the transport healthy.
        this.secondaryResponsesRequired_ =
            conn['responsesRequiredToBeHealthy'] || 0;
        const onMessage = this.connReceiver_(this.secondaryConn_);
        const onDisconnect = this.disconnReceiver_(this.secondaryConn_);
        this.secondaryConn_.open(onMessage, onDisconnect);
        // If we haven't successfully upgraded after UPGRADE_TIMEOUT, give up and kill the secondary.
        setTimeoutNonBlocking(() => {
            if (this.secondaryConn_) {
                this.log_('Timed out trying to upgrade.');
                this.secondaryConn_.close();
            }
        }, Math.floor(UPGRADE_TIMEOUT));
    }
    onReset_(host) {
        this.log_('Reset packet received.  New host: ' + host);
        this.repoInfo_.host = host;
        // TODO: if we're already "connected", we need to trigger a disconnect at the next layer up.
        // We don't currently support resets after the connection has already been established
        if (this.state_ === 1 /* RealtimeState.CONNECTED */) {
            this.close();
        }
        else {
            // Close whatever connections we have open and start again.
            this.closeConnections_();
            this.start_();
        }
    }
    onConnectionEstablished_(conn, timestamp) {
        this.log_('Realtime connection established.');
        this.conn_ = conn;
        this.state_ = 1 /* RealtimeState.CONNECTED */;
        if (this.onReady_) {
            this.onReady_(timestamp, this.sessionId);
            this.onReady_ = null;
        }
        // If after 5 seconds we haven't sent enough requests to the server to get the connection healthy,
        // send some pings.
        if (this.primaryResponsesRequired_ === 0) {
            this.log_('Primary connection is healthy.');
            this.isHealthy_ = true;
        }
        else {
            setTimeoutNonBlocking(() => {
                this.sendPingOnPrimaryIfNecessary_();
            }, Math.floor(DELAY_BEFORE_SENDING_EXTRA_REQUESTS));
        }
    }
    sendPingOnPrimaryIfNecessary_() {
        // If the connection isn't considered healthy yet, we'll send a noop ping packet request.
        if (!this.isHealthy_ && this.state_ === 1 /* RealtimeState.CONNECTED */) {
            this.log_('sending ping on primary.');
            this.sendData_({ t: 'c', d: { t: PING, d: {} } });
        }
    }
    onSecondaryConnectionLost_() {
        const conn = this.secondaryConn_;
        this.secondaryConn_ = null;
        if (this.tx_ === conn || this.rx_ === conn) {
            // we are relying on this connection already in some capacity. Therefore, a failure is real
            this.close();
        }
    }
    /**
     * @param everConnected - Whether or not the connection ever reached a server. Used to determine if
     * we should flush the host cache
     */
    onConnectionLost_(everConnected) {
        this.conn_ = null;
        // NOTE: IF you're seeing a Firefox error for this line, I think it might be because it's getting
        // called on window close and RealtimeState.CONNECTING is no longer defined.  Just a guess.
        if (!everConnected && this.state_ === 0 /* RealtimeState.CONNECTING */) {
            this.log_('Realtime connection failed.');
            // Since we failed to connect at all, clear any cached entry for this namespace in case the machine went away
            if (this.repoInfo_.isCacheableHost()) {
                PersistentStorage.remove('host:' + this.repoInfo_.host);
                // reset the internal host to what we would show the user, i.e. <ns>.firebaseio.com
                this.repoInfo_.internalHost = this.repoInfo_.host;
            }
        }
        else if (this.state_ === 1 /* RealtimeState.CONNECTED */) {
            this.log_('Realtime connection lost.');
        }
        this.close();
    }
    onConnectionShutdown_(reason) {
        this.log_('Connection shutdown command received. Shutting down...');
        if (this.onKill_) {
            this.onKill_(reason);
            this.onKill_ = null;
        }
        // We intentionally don't want to fire onDisconnect (kill is a different case),
        // so clear the callback.
        this.onDisconnect_ = null;
        this.close();
    }
    sendData_(data) {
        if (this.state_ !== 1 /* RealtimeState.CONNECTED */) {
            throw 'Connection is not connected';
        }
        else {
            this.tx_.send(data);
        }
    }
    /**
     * Cleans up this connection, calling the appropriate callbacks
     */
    close() {
        if (this.state_ !== 2 /* RealtimeState.DISCONNECTED */) {
            this.log_('Closing realtime connection.');
            this.state_ = 2 /* RealtimeState.DISCONNECTED */;
            this.closeConnections_();
            if (this.onDisconnect_) {
                this.onDisconnect_();
                this.onDisconnect_ = null;
            }
        }
    }
    closeConnections_() {
        this.log_('Shutting down all connections');
        if (this.conn_) {
            this.conn_.close();
            this.conn_ = null;
        }
        if (this.secondaryConn_) {
            this.secondaryConn_.close();
            this.secondaryConn_ = null;
        }
        if (this.healthyTimeout_) {
            clearTimeout(this.healthyTimeout_);
            this.healthyTimeout_ = null;
        }
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Interface defining the set of actions that can be performed against the Firebase server
 * (basically corresponds to our wire protocol).
 *
 * @interface
 */
class ServerActions {
    put(pathString, data, onComplete, hash) { }
    merge(pathString, data, onComplete, hash) { }
    /**
     * Refreshes the auth token for the current connection.
     * @param token - The authentication token
     */
    refreshAuthToken(token) { }
    /**
     * Refreshes the app check token for the current connection.
     * @param token The app check token
     */
    refreshAppCheckToken(token) { }
    onDisconnectPut(pathString, data, onComplete) { }
    onDisconnectMerge(pathString, data, onComplete) { }
    onDisconnectCancel(pathString, onComplete) { }
    reportStats(stats) { }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Base class to be used if you want to emit events. Call the constructor with
 * the set of allowed event names.
 */
class EventEmitter {
    constructor(allowedEvents_) {
        this.allowedEvents_ = allowedEvents_;
        this.listeners_ = {};
        util.assert(Array.isArray(allowedEvents_) && allowedEvents_.length > 0, 'Requires a non-empty array');
    }
    /**
     * To be called by derived classes to trigger events.
     */
    trigger(eventType, ...varArgs) {
        if (Array.isArray(this.listeners_[eventType])) {
            // Clone the list, since callbacks could add/remove listeners.
            const listeners = [...this.listeners_[eventType]];
            for (let i = 0; i < listeners.length; i++) {
                listeners[i].callback.apply(listeners[i].context, varArgs);
            }
        }
    }
    on(eventType, callback, context) {
        this.validateEventType_(eventType);
        this.listeners_[eventType] = this.listeners_[eventType] || [];
        this.listeners_[eventType].push({ callback, context });
        const eventData = this.getInitialEvent(eventType);
        if (eventData) {
            callback.apply(context, eventData);
        }
    }
    off(eventType, callback, context) {
        this.validateEventType_(eventType);
        const listeners = this.listeners_[eventType] || [];
        for (let i = 0; i < listeners.length; i++) {
            if (listeners[i].callback === callback &&
                (!context || context === listeners[i].context)) {
                listeners.splice(i, 1);
                return;
            }
        }
    }
    validateEventType_(eventType) {
        util.assert(this.allowedEvents_.find(et => {
            return et === eventType;
        }), 'Unknown event: ' + eventType);
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Monitors online state (as reported by window.online/offline events).
 *
 * The expectation is that this could have many false positives (thinks we are online
 * when we're not), but no false negatives.  So we can safely use it to determine when
 * we definitely cannot reach the internet.
 */
class OnlineMonitor extends EventEmitter {
    static getInstance() {
        return new OnlineMonitor();
    }
    constructor() {
        super(['online']);
        this.online_ = true;
        // We've had repeated complaints that Cordova apps can get stuck "offline", e.g.
        // https://forum.ionicframework.com/t/firebase-connection-is-lost-and-never-come-back/43810
        // It would seem that the 'online' event does not always fire consistently. So we disable it
        // for Cordova.
        if (typeof window !== 'undefined' &&
            typeof window.addEventListener !== 'undefined' &&
            !util.isMobileCordova()) {
            window.addEventListener('online', () => {
                if (!this.online_) {
                    this.online_ = true;
                    this.trigger('online', true);
                }
            }, false);
            window.addEventListener('offline', () => {
                if (this.online_) {
                    this.online_ = false;
                    this.trigger('online', false);
                }
            }, false);
        }
    }
    getInitialEvent(eventType) {
        util.assert(eventType === 'online', 'Unknown event type: ' + eventType);
        return [this.online_];
    }
    currentlyOnline() {
        return this.online_;
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class VisibilityMonitor extends EventEmitter {
    static getInstance() {
        return new VisibilityMonitor();
    }
    constructor() {
        super(['visible']);
        let hidden;
        let visibilityChange;
        if (typeof document !== 'undefined' &&
            typeof document.addEventListener !== 'undefined') {
            if (typeof document['hidden'] !== 'undefined') {
                // Opera 12.10 and Firefox 18 and later support
                visibilityChange = 'visibilitychange';
                hidden = 'hidden';
            }
            else if (typeof document['mozHidden'] !== 'undefined') {
                visibilityChange = 'mozvisibilitychange';
                hidden = 'mozHidden';
            }
            else if (typeof document['msHidden'] !== 'undefined') {
                visibilityChange = 'msvisibilitychange';
                hidden = 'msHidden';
            }
            else if (typeof document['webkitHidden'] !== 'undefined') {
                visibilityChange = 'webkitvisibilitychange';
                hidden = 'webkitHidden';
            }
        }
        // Initially, we always assume we are visible. This ensures that in browsers
        // without page visibility support or in cases where we are never visible
        // (e.g. chrome extension), we act as if we are visible, i.e. don't delay
        // reconnects
        this.visible_ = true;
        if (visibilityChange) {
            document.addEventListener(visibilityChange, () => {
                const visible = !document[hidden];
                if (visible !== this.visible_) {
                    this.visible_ = visible;
                    this.trigger('visible', visible);
                }
            }, false);
        }
    }
    getInitialEvent(eventType) {
        util.assert(eventType === 'visible', 'Unknown event type: ' + eventType);
        return [this.visible_];
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const RECONNECT_MIN_DELAY = 1000;
const RECONNECT_MAX_DELAY_DEFAULT = 60 * 5 * 1000; // 5 minutes in milliseconds (Case: 1858)
const RECONNECT_MAX_DELAY_FOR_ADMINS = 30 * 1000; // 30 seconds for admin clients (likely to be a backend server)
const RECONNECT_DELAY_MULTIPLIER = 1.3;
const RECONNECT_DELAY_RESET_TIMEOUT = 30000; // Reset delay back to MIN_DELAY after being connected for 30sec.
const SERVER_KILL_INTERRUPT_REASON = 'server_kill';
// If auth fails repeatedly, we'll assume something is wrong and log a warning / back off.
const INVALID_TOKEN_THRESHOLD = 3;
/**
 * Firebase connection.  Abstracts wire protocol and handles reconnecting.
 *
 * NOTE: All JSON objects sent to the realtime connection must have property names enclosed
 * in quotes to make sure the closure compiler does not minify them.
 */
class PersistentConnection extends ServerActions {
    /**
     * @param repoInfo_ - Data about the namespace we are connecting to
     * @param applicationId_ - The Firebase App ID for this project
     * @param onDataUpdate_ - A callback for new data from the server
     */
    constructor(repoInfo_, applicationId_, onDataUpdate_, onConnectStatus_, onServerInfoUpdate_, authTokenProvider_, appCheckTokenProvider_, authOverride_, onRangeMergeUpdate_) {
        super();
        this.repoInfo_ = repoInfo_;
        this.applicationId_ = applicationId_;
        this.onDataUpdate_ = onDataUpdate_;
        this.onConnectStatus_ = onConnectStatus_;
        this.onServerInfoUpdate_ = onServerInfoUpdate_;
        this.authTokenProvider_ = authTokenProvider_;
        this.appCheckTokenProvider_ = appCheckTokenProvider_;
        this.authOverride_ = authOverride_;
        this.onRangeMergeUpdate_ = onRangeMergeUpdate_;
        // Used for diagnostic logging.
        this.id = PersistentConnection.nextPersistentConnectionId_++;
        this.log_ = logWrapper('p:' + this.id + ':');
        this.interruptReasons_ = {};
        this.listens = new Map();
        this.outstandingPuts_ = [];
        this.outstandingGets_ = [];
        this.outstandingPutCount_ = 0;
        this.outstandingGetCount_ = 0;
        this.onDisconnectRequestQueue_ = [];
        this.connected_ = false;
        this.reconnectDelay_ = RECONNECT_MIN_DELAY;
        this.maxReconnectDelay_ = RECONNECT_MAX_DELAY_DEFAULT;
        this.securityDebugCallback_ = null;
        this.lastSessionId = null;
        this.establishConnectionTimer_ = null;
        this.visible_ = false;
        // Before we get connected, we keep a queue of pending messages to send.
        this.requestCBHash_ = {};
        this.requestNumber_ = 0;
        this.realtime_ = null;
        this.authToken_ = null;
        this.appCheckToken_ = null;
        this.forceTokenRefresh_ = false;
        this.invalidAuthTokenCount_ = 0;
        this.invalidAppCheckTokenCount_ = 0;
        this.firstConnection_ = true;
        this.lastConnectionAttemptTime_ = null;
        this.lastConnectionEstablishedTime_ = null;
        if (authOverride_ && !util.isNodeSdk()) {
            throw new Error('Auth override specified in options, but not supported on non Node.js platforms');
        }
        VisibilityMonitor.getInstance().on('visible', this.onVisible_, this);
        if (repoInfo_.host.indexOf('fblocal') === -1) {
            OnlineMonitor.getInstance().on('online', this.onOnline_, this);
        }
    }
    sendRequest(action, body, onResponse) {
        const curReqNum = ++this.requestNumber_;
        const msg = { r: curReqNum, a: action, b: body };
        this.log_(util.stringify(msg));
        util.assert(this.connected_, "sendRequest call when we're not connected not allowed.");
        this.realtime_.sendRequest(msg);
        if (onResponse) {
            this.requestCBHash_[curReqNum] = onResponse;
        }
    }
    get(query) {
        this.initConnection_();
        const deferred = new util.Deferred();
        const request = {
            p: query._path.toString(),
            q: query._queryObject
        };
        const outstandingGet = {
            action: 'g',
            request,
            onComplete: (message) => {
                const payload = message['d'];
                if (message['s'] === 'ok') {
                    deferred.resolve(payload);
                }
                else {
                    deferred.reject(payload);
                }
            }
        };
        this.outstandingGets_.push(outstandingGet);
        this.outstandingGetCount_++;
        const index = this.outstandingGets_.length - 1;
        if (this.connected_) {
            this.sendGet_(index);
        }
        return deferred.promise;
    }
    listen(query, currentHashFn, tag, onComplete, onProgress) {
        this.initConnection_();
        const queryId = query._queryIdentifier;
        const pathString = query._path.toString();
        this.log_('Listen called for ' + pathString + ' ' + queryId);
        if (!this.listens.has(pathString)) {
            this.listens.set(pathString, new Map());
        }
        util.assert(query._queryParams.isDefault() || !query._queryParams.loadsAllData(), 'listen() called for non-default but complete query');
        util.assert(!this.listens.get(pathString).has(queryId), `listen() called twice for same path/queryId.`);
        const listenSpec = {
            onComplete,
            onProgress,
            hashFn: currentHashFn,
            query,
            tag,
            bytes: 0,
            hadHash: false,
            hadCompoundHash: false,
            dataReceived: false,
            rangeMerged: false
        };
        this.listens.get(pathString).set(queryId, listenSpec);
        if (this.connected_) {
            this.sendListen_(listenSpec);
        }
    }
    sendGet_(index) {
        const get = this.outstandingGets_[index];
        this.sendRequest('g', get.request, (message) => {
            delete this.outstandingGets_[index];
            this.outstandingGetCount_--;
            if (this.outstandingGetCount_ === 0) {
                this.outstandingGets_ = [];
            }
            if (get.onComplete) {
                get.onComplete(message);
            }
        });
    }
    sendListen_(listenSpec) {
        const query = listenSpec.query;
        const pathString = query._path.toString();
        const queryId = query._queryIdentifier;
        this.log_('Listen on ' + pathString + ' for ' + queryId);
        const req = { /*path*/ p: pathString };
        const action = 'q';
        // Only bother to send query if it's non-default.
        if (listenSpec.tag) {
            req['q'] = query._queryObject;
            req['t'] = listenSpec.tag;
        }
        req[ /*hash*/'h'] = listenSpec.hashFn();
        // When the server cache was seeded from app-persisted data, send its
        // compound hash too: a miss on the simple hash then downgrades to
        // range merges covering only the changed ranges instead of a full
        // download (see ServerCacheSeed).
        const compoundHash = listenSpec.hashFn.compoundHash?.();
        if (compoundHash) {
            req['ch'] = { hs: compoundHash.hashes, ps: compoundHash.posts };
        }
        listenSpec.hadHash = req['h'] !== '';
        listenSpec.hadCompoundHash = compoundHash !== undefined;
        listenSpec.bytes = 0;
        listenSpec.dataReceived = false;
        listenSpec.rangeMerged = false;
        this.sendRequest(action, req, (message, responseBytes = 0) => {
            const payload = message[ /*data*/'d'];
            const status = message[ /*status*/'s'];
            // print warnings in any case...
            PersistentConnection.warnOnListenWarnings_(payload, query);
            const currentListenSpec = this.listens.get(pathString) &&
                this.listens.get(pathString).get(queryId);
            // only trigger actions if the listen hasn't been removed and readded
            if (currentListenSpec === listenSpec) {
                this.log_('listen response', message);
                if (status !== 'ok') {
                    this.removeListen_(pathString, queryId);
                }
                if (listenSpec.onComplete) {
                    listenSpec.onComplete(status, payload, {
                        ...this.listenWireResult_(listenSpec),
                        bytes: listenSpec.bytes + responseBytes
                    });
                }
            }
        });
    }
    static warnOnListenWarnings_(payload, query) {
        if (payload && typeof payload === 'object' && util.contains(payload, 'w')) {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            const warnings = util.safeGet(payload, 'w');
            if (Array.isArray(warnings) && ~warnings.indexOf('no_index')) {
                const indexSpec = '".indexOn": "' + query._queryParams.getIndex().toString() + '"';
                const indexPath = query._path.toString();
                warn(`Using an unspecified index. Your data will be downloaded and ` +
                    `filtered on the client. Consider adding ${indexSpec} at ` +
                    `${indexPath} to your security rules for better performance.`);
            }
        }
    }
    refreshAuthToken(token) {
        this.authToken_ = token;
        this.log_('Auth token refreshed');
        if (this.authToken_) {
            this.tryAuth();
        }
        else {
            //If we're connected we want to let the server know to unauthenticate us. If we're not connected, simply delete
            //the credential so we dont become authenticated next time we connect.
            if (this.connected_) {
                this.sendRequest('unauth', {}, () => { });
            }
        }
        this.reduceReconnectDelayIfAdminCredential_(token);
    }
    reduceReconnectDelayIfAdminCredential_(credential) {
        // NOTE: This isn't intended to be bulletproof (a malicious developer can always just modify the client).
        // Additionally, we don't bother resetting the max delay back to the default if auth fails / expires.
        const isFirebaseSecret = credential && credential.length === 40;
        if (isFirebaseSecret || util.isAdmin(credential)) {
            this.log_('Admin auth credential detected.  Reducing max reconnect time.');
            this.maxReconnectDelay_ = RECONNECT_MAX_DELAY_FOR_ADMINS;
        }
    }
    refreshAppCheckToken(token) {
        this.appCheckToken_ = token;
        this.log_('App check token refreshed');
        if (this.appCheckToken_) {
            this.tryAppCheck();
        }
        else {
            //If we're connected we want to let the server know to unauthenticate us.
            //If we're not connected, simply delete the credential so we dont become
            // authenticated next time we connect.
            if (this.connected_) {
                this.sendRequest('unappeck', {}, () => { });
            }
        }
    }
    /**
     * Attempts to authenticate with the given credentials. If the authentication attempt fails, it's triggered like
     * a auth revoked (the connection is closed).
     */
    tryAuth() {
        if (this.connected_ && this.authToken_) {
            const token = this.authToken_;
            const authMethod = util.isValidFormat(token) ? 'auth' : 'gauth';
            const requestData = { cred: token };
            if (this.authOverride_ === null) {
                requestData['noauth'] = true;
            }
            else if (typeof this.authOverride_ === 'object') {
                requestData['authvar'] = this.authOverride_;
            }
            this.sendRequest(authMethod, requestData, (res) => {
                const status = res[ /*status*/'s'];
                const data = res[ /*data*/'d'] || 'error';
                if (this.authToken_ === token) {
                    if (status === 'ok') {
                        this.invalidAuthTokenCount_ = 0;
                    }
                    else {
                        // Triggers reconnect and force refresh for auth token
                        this.onAuthRevoked_(status, data);
                    }
                }
            });
        }
    }
    /**
     * Attempts to authenticate with the given token. If the authentication
     * attempt fails, it's triggered like the token was revoked (the connection is
     * closed).
     */
    tryAppCheck() {
        if (this.connected_ && this.appCheckToken_) {
            this.sendRequest('appcheck', { 'token': this.appCheckToken_ }, (res) => {
                const status = res[ /*status*/'s'];
                const data = res[ /*data*/'d'] || 'error';
                if (status === 'ok') {
                    this.invalidAppCheckTokenCount_ = 0;
                }
                else {
                    this.onAppCheckRevoked_(status, data);
                }
            });
        }
    }
    /**
     * @inheritDoc
     */
    unlisten(query, tag) {
        const pathString = query._path.toString();
        const queryId = query._queryIdentifier;
        this.log_('Unlisten called for ' + pathString + ' ' + queryId);
        util.assert(query._queryParams.isDefault() || !query._queryParams.loadsAllData(), 'unlisten() called for non-default but complete query');
        const listen = this.removeListen_(pathString, queryId);
        if (listen && this.connected_) {
            this.sendUnlisten_(pathString, queryId, query._queryObject, tag);
        }
    }
    sendUnlisten_(pathString, queryId, queryObj, tag) {
        this.log_('Unlisten on ' + pathString + ' for ' + queryId);
        const req = { /*path*/ p: pathString };
        const action = 'n';
        // Only bother sending queryId if it's non-default.
        if (tag) {
            req['q'] = queryObj;
            req['t'] = tag;
        }
        this.sendRequest(action, req);
    }
    onDisconnectPut(pathString, data, onComplete) {
        this.initConnection_();
        if (this.connected_) {
            this.sendOnDisconnect_('o', pathString, data, onComplete);
        }
        else {
            this.onDisconnectRequestQueue_.push({
                pathString,
                action: 'o',
                data,
                onComplete
            });
        }
    }
    onDisconnectMerge(pathString, data, onComplete) {
        this.initConnection_();
        if (this.connected_) {
            this.sendOnDisconnect_('om', pathString, data, onComplete);
        }
        else {
            this.onDisconnectRequestQueue_.push({
                pathString,
                action: 'om',
                data,
                onComplete
            });
        }
    }
    onDisconnectCancel(pathString, onComplete) {
        this.initConnection_();
        if (this.connected_) {
            this.sendOnDisconnect_('oc', pathString, null, onComplete);
        }
        else {
            this.onDisconnectRequestQueue_.push({
                pathString,
                action: 'oc',
                data: null,
                onComplete
            });
        }
    }
    sendOnDisconnect_(action, pathString, data, onComplete) {
        const request = { /*path*/ p: pathString, /*data*/ d: data };
        this.log_('onDisconnect ' + action, request);
        this.sendRequest(action, request, (response) => {
            if (onComplete) {
                setTimeout(() => {
                    onComplete(response[ /*status*/'s'], response[ /* data */'d']);
                }, Math.floor(0));
            }
        });
    }
    put(pathString, data, onComplete, hash) {
        this.putInternal('p', pathString, data, onComplete, hash);
    }
    merge(pathString, data, onComplete, hash) {
        this.putInternal('m', pathString, data, onComplete, hash);
    }
    putInternal(action, pathString, data, onComplete, hash) {
        this.initConnection_();
        const request = {
            /*path*/ p: pathString,
            /*data*/ d: data
        };
        if (hash !== undefined) {
            request[ /*hash*/'h'] = hash;
        }
        // TODO: Only keep track of the most recent put for a given path?
        this.outstandingPuts_.push({
            action,
            request,
            onComplete
        });
        this.outstandingPutCount_++;
        const index = this.outstandingPuts_.length - 1;
        if (this.connected_) {
            this.sendPut_(index);
        }
        else {
            this.log_('Buffering put: ' + pathString);
        }
    }
    sendPut_(index) {
        const action = this.outstandingPuts_[index].action;
        const request = this.outstandingPuts_[index].request;
        const onComplete = this.outstandingPuts_[index].onComplete;
        this.outstandingPuts_[index].queued = this.connected_;
        this.sendRequest(action, request, (message) => {
            this.log_(action + ' response', message);
            delete this.outstandingPuts_[index];
            this.outstandingPutCount_--;
            // Clean up array occasionally.
            if (this.outstandingPutCount_ === 0) {
                this.outstandingPuts_ = [];
            }
            if (onComplete) {
                onComplete(message[ /*status*/'s'], message[ /* data */'d']);
            }
        });
    }
    reportStats(stats) {
        // If we're not connected, we just drop the stats.
        if (this.connected_) {
            const request = { /*counters*/ c: stats };
            this.log_('reportStats', request);
            this.sendRequest(/*stats*/ 's', request, result => {
                const status = result[ /*status*/'s'];
                if (status !== 'ok') {
                    const errorReason = result[ /* data */'d'];
                    this.log_('reportStats', 'Error sending stats: ' + errorReason);
                }
            });
        }
    }
    onDataMessage_(message, bytes = 0) {
        if ('r' in message) {
            // this is a response
            this.log_('from server: ' + util.stringify(message));
            const reqNum = message['r'];
            const onResponse = this.requestCBHash_[reqNum];
            if (onResponse) {
                delete this.requestCBHash_[reqNum];
                onResponse(message[ /*body*/'b'], bytes);
            }
        }
        else if ('error' in message) {
            throw 'A server-side error has occurred: ' + message['error'];
        }
        else if ('a' in message) {
            // a and b are action and body, respectively
            this.onDataPush_(message['a'], message['b'], bytes);
        }
    }
    listenWireResult_(listen) {
        return {
            bytes: listen.bytes,
            hadHash: listen.hadHash,
            hadCompoundHash: listen.hadCompoundHash,
            dataReceived: listen.dataReceived,
            rangeMerged: listen.rangeMerged
        };
    }
    onDataPush_(action, body, bytes) {
        this.log_('handleServerMessage', action, body);
        if ((action === 'd' || action === 'm' || action === 'rm') &&
            body &&
            body['p'] !== undefined) {
            const pushPath = new Path(body['p']).toString();
            const listensAtPath = this.listens.get(pushPath);
            if (listensAtPath) {
                for (const listen of listensAtPath.values()) {
                    listen.bytes += bytes;
                    listen.dataReceived = true;
                    if (action === 'rm') {
                        listen.rangeMerged = true;
                    }
                    listen.onProgress?.(this.listenWireResult_(listen));
                }
            }
        }
        if (action === 'd') {
            this.onDataUpdate_(body[ /*path*/'p'], body[ /*data*/'d'], 
            /*isMerge*/ false, body['t']);
        }
        else if (action === 'm') {
            this.onDataUpdate_(body[ /*path*/'p'], body[ /*data*/'d'], 
            /*isMerge=*/ true, body['t']);
        }
        else if (action === 'rm') {
            // Range merge: the listen carried a compound hash and only some of its
            // ranges differed — the server resends just those ranges.
            this.onRangeMergeUpdate_?.(body[ /*path*/'p'], body[ /*ranges*/'d'], body['t']);
        }
        else if (action === 'c') {
            this.onListenRevoked_(body[ /*path*/'p'], body[ /*query*/'q']);
        }
        else if (action === 'ac') {
            this.onAuthRevoked_(body[ /*status code*/'s'], body[ /* explanation */'d']);
        }
        else if (action === 'apc') {
            this.onAppCheckRevoked_(body[ /*status code*/'s'], body[ /* explanation */'d']);
        }
        else if (action === 'sd') {
            this.onSecurityDebugPacket_(body);
        }
        else {
            error('Unrecognized action received from server: ' +
                util.stringify(action) +
                '\nAre you using the latest client?');
        }
    }
    onReady_(timestamp, sessionId) {
        this.log_('connection ready');
        this.connected_ = true;
        this.lastConnectionEstablishedTime_ = new Date().getTime();
        this.handleTimestamp_(timestamp);
        this.lastSessionId = sessionId;
        if (this.firstConnection_) {
            this.sendConnectStats_();
        }
        this.restoreState_();
        this.firstConnection_ = false;
        this.onConnectStatus_(true);
    }
    scheduleConnect_(timeout) {
        util.assert(!this.realtime_, "Scheduling a connect when we're already connected/ing?");
        if (this.establishConnectionTimer_) {
            clearTimeout(this.establishConnectionTimer_);
        }
        // NOTE: Even when timeout is 0, it's important to do a setTimeout to work around an infuriating "Security Error" in
        // Firefox when trying to write to our long-polling iframe in some scenarios (e.g. Forge or our unit tests).
        this.establishConnectionTimer_ = setTimeout(() => {
            this.establishConnectionTimer_ = null;
            this.establishConnection_();
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
        }, Math.floor(timeout));
    }
    initConnection_() {
        if (!this.realtime_ && this.firstConnection_) {
            this.scheduleConnect_(0);
        }
    }
    onVisible_(visible) {
        // NOTE: Tabbing away and back to a window will defeat our reconnect backoff, but I think that's fine.
        if (visible &&
            !this.visible_ &&
            this.reconnectDelay_ === this.maxReconnectDelay_) {
            this.log_('Window became visible.  Reducing delay.');
            this.reconnectDelay_ = RECONNECT_MIN_DELAY;
            if (!this.realtime_) {
                this.scheduleConnect_(0);
            }
        }
        this.visible_ = visible;
    }
    onOnline_(online) {
        if (online) {
            this.log_('Browser went online.');
            this.reconnectDelay_ = RECONNECT_MIN_DELAY;
            if (!this.realtime_) {
                this.scheduleConnect_(0);
            }
        }
        else {
            this.log_('Browser went offline.  Killing connection.');
            if (this.realtime_) {
                this.realtime_.close();
            }
        }
    }
    onRealtimeDisconnect_() {
        this.log_('data client disconnected');
        this.connected_ = false;
        this.realtime_ = null;
        // Since we don't know if our sent transactions succeeded or not, we need to cancel them.
        this.cancelSentTransactions_();
        // Clear out the pending requests.
        this.requestCBHash_ = {};
        if (this.shouldReconnect_()) {
            if (!this.visible_) {
                this.log_("Window isn't visible.  Delaying reconnect.");
                this.reconnectDelay_ = this.maxReconnectDelay_;
                this.lastConnectionAttemptTime_ = new Date().getTime();
            }
            else if (this.lastConnectionEstablishedTime_) {
                // If we've been connected long enough, reset reconnect delay to minimum.
                const timeSinceLastConnectSucceeded = new Date().getTime() - this.lastConnectionEstablishedTime_;
                if (timeSinceLastConnectSucceeded > RECONNECT_DELAY_RESET_TIMEOUT) {
                    this.reconnectDelay_ = RECONNECT_MIN_DELAY;
                }
                this.lastConnectionEstablishedTime_ = null;
            }
            const timeSinceLastConnectAttempt = Math.max(0, new Date().getTime() - this.lastConnectionAttemptTime_);
            let reconnectDelay = Math.max(0, this.reconnectDelay_ - timeSinceLastConnectAttempt);
            reconnectDelay = Math.random() * reconnectDelay;
            this.log_('Trying to reconnect in ' + reconnectDelay + 'ms');
            this.scheduleConnect_(reconnectDelay);
            // Adjust reconnect delay for next time.
            this.reconnectDelay_ = Math.min(this.maxReconnectDelay_, this.reconnectDelay_ * RECONNECT_DELAY_MULTIPLIER);
        }
        this.onConnectStatus_(false);
    }
    async establishConnection_() {
        if (this.shouldReconnect_()) {
            this.log_('Making a connection attempt');
            this.lastConnectionAttemptTime_ = new Date().getTime();
            this.lastConnectionEstablishedTime_ = null;
            const onDataMessage = this.onDataMessage_.bind(this);
            const onReady = this.onReady_.bind(this);
            const onDisconnect = this.onRealtimeDisconnect_.bind(this);
            const connId = this.id + ':' + PersistentConnection.nextConnectionId_++;
            const lastSessionId = this.lastSessionId;
            let canceled = false;
            let connection = null;
            const closeFn = function () {
                if (connection) {
                    connection.close();
                }
                else {
                    canceled = true;
                    onDisconnect();
                }
            };
            const sendRequestFn = function (msg) {
                util.assert(connection, "sendRequest call when we're not connected not allowed.");
                connection.sendRequest(msg);
            };
            this.realtime_ = {
                close: closeFn,
                sendRequest: sendRequestFn
            };
            const forceRefresh = this.forceTokenRefresh_;
            this.forceTokenRefresh_ = false;
            try {
                // First fetch auth and app check token, and establish connection after
                // fetching the token was successful
                const [authToken, appCheckToken] = await Promise.all([
                    this.authTokenProvider_.getToken(forceRefresh),
                    this.appCheckTokenProvider_.getToken(forceRefresh)
                ]);
                if (!canceled) {
                    log('getToken() completed. Creating connection.');
                    this.authToken_ = authToken && authToken.accessToken;
                    this.appCheckToken_ = appCheckToken && appCheckToken.token;
                    connection = new Connection(connId, this.repoInfo_, this.applicationId_, this.appCheckToken_, this.authToken_, onDataMessage, onReady, onDisconnect, 
                    /* onKill= */ reason => {
                        warn(reason + ' (' + this.repoInfo_.toString() + ')');
                        this.interrupt(SERVER_KILL_INTERRUPT_REASON);
                    }, lastSessionId);
                }
                else {
                    log('getToken() completed but was canceled');
                }
            }
            catch (error) {
                this.log_('Failed to get token: ' + error);
                if (!canceled) {
                    if (this.repoInfo_.nodeAdmin) {
                        // This may be a critical error for the Admin Node.js SDK, so log a warning.
                        // But getToken() may also just have temporarily failed, so we still want to
                        // continue retrying.
                        warn(error);
                    }
                    closeFn();
                }
            }
        }
    }
    interrupt(reason) {
        log('Interrupting connection for reason: ' + reason);
        this.interruptReasons_[reason] = true;
        if (this.realtime_) {
            this.realtime_.close();
        }
        else {
            if (this.establishConnectionTimer_) {
                clearTimeout(this.establishConnectionTimer_);
                this.establishConnectionTimer_ = null;
            }
            if (this.connected_) {
                this.onRealtimeDisconnect_();
            }
        }
    }
    resume(reason) {
        log('Resuming connection for reason: ' + reason);
        delete this.interruptReasons_[reason];
        if (util.isEmpty(this.interruptReasons_)) {
            this.reconnectDelay_ = RECONNECT_MIN_DELAY;
            if (!this.realtime_) {
                this.scheduleConnect_(0);
            }
        }
    }
    handleTimestamp_(timestamp) {
        const delta = timestamp - new Date().getTime();
        this.onServerInfoUpdate_({ serverTimeOffset: delta });
    }
    cancelSentTransactions_() {
        for (let i = 0; i < this.outstandingPuts_.length; i++) {
            const put = this.outstandingPuts_[i];
            if (put && /*hash*/ 'h' in put.request && put.queued) {
                if (put.onComplete) {
                    put.onComplete('disconnect');
                }
                delete this.outstandingPuts_[i];
                this.outstandingPutCount_--;
            }
        }
        // Clean up array occasionally.
        if (this.outstandingPutCount_ === 0) {
            this.outstandingPuts_ = [];
        }
    }
    onListenRevoked_(pathString, query) {
        // Remove the listen and manufacture a "permission_denied" error for the failed listen.
        let queryId;
        if (!query) {
            queryId = 'default';
        }
        else {
            queryId = query.map(q => ObjectToUniqueKey(q)).join('$');
        }
        const listen = this.removeListen_(pathString, queryId);
        if (listen && listen.onComplete) {
            listen.onComplete('permission_denied', null, {
                bytes: listen.bytes,
                hadHash: listen.hadHash,
                hadCompoundHash: listen.hadCompoundHash,
                dataReceived: listen.dataReceived,
                rangeMerged: listen.rangeMerged
            });
        }
    }
    removeListen_(pathString, queryId) {
        const normalizedPathString = new Path(pathString).toString(); // normalize path.
        let listen;
        if (this.listens.has(normalizedPathString)) {
            const map = this.listens.get(normalizedPathString);
            listen = map.get(queryId);
            map.delete(queryId);
            if (map.size === 0) {
                this.listens.delete(normalizedPathString);
            }
        }
        else {
            // all listens for this path has already been removed
            listen = undefined;
        }
        return listen;
    }
    onAuthRevoked_(statusCode, explanation) {
        log('Auth token revoked: ' + statusCode + '/' + explanation);
        this.authToken_ = null;
        this.forceTokenRefresh_ = true;
        this.realtime_.close();
        if (statusCode === 'invalid_token' || statusCode === 'permission_denied') {
            // We'll wait a couple times before logging the warning / increasing the
            // retry period since oauth tokens will report as "invalid" if they're
            // just expired. Plus there may be transient issues that resolve themselves.
            this.invalidAuthTokenCount_++;
            if (this.invalidAuthTokenCount_ >= INVALID_TOKEN_THRESHOLD) {
                // Set a long reconnect delay because recovery is unlikely
                this.reconnectDelay_ = RECONNECT_MAX_DELAY_FOR_ADMINS;
                // Notify the auth token provider that the token is invalid, which will log
                // a warning
                this.authTokenProvider_.notifyForInvalidToken();
            }
        }
    }
    onAppCheckRevoked_(statusCode, explanation) {
        log('App check token revoked: ' + statusCode + '/' + explanation);
        this.appCheckToken_ = null;
        this.forceTokenRefresh_ = true;
        // Note: We don't close the connection as the developer may not have
        // enforcement enabled. The backend closes connections with enforcements.
        if (statusCode === 'invalid_token' || statusCode === 'permission_denied') {
            // We'll wait a couple times before logging the warning / increasing the
            // retry period since oauth tokens will report as "invalid" if they're
            // just expired. Plus there may be transient issues that resolve themselves.
            this.invalidAppCheckTokenCount_++;
            if (this.invalidAppCheckTokenCount_ >= INVALID_TOKEN_THRESHOLD) {
                this.appCheckTokenProvider_.notifyForInvalidToken();
            }
        }
    }
    onSecurityDebugPacket_(body) {
        if (this.securityDebugCallback_) {
            this.securityDebugCallback_(body);
        }
        else {
            if ('msg' in body) {
                console.log('FIREBASE: ' + body['msg'].replace('\n', '\nFIREBASE: '));
            }
        }
    }
    restoreState_() {
        //Re-authenticate ourselves if we have a credential stored.
        this.tryAuth();
        this.tryAppCheck();
        // Puts depend on having received the corresponding data update from the server before they complete, so we must
        // make sure to send listens before puts.
        for (const queries of this.listens.values()) {
            for (const listenSpec of queries.values()) {
                this.sendListen_(listenSpec);
            }
        }
        for (let i = 0; i < this.outstandingPuts_.length; i++) {
            if (this.outstandingPuts_[i]) {
                this.sendPut_(i);
            }
        }
        while (this.onDisconnectRequestQueue_.length) {
            const request = this.onDisconnectRequestQueue_.shift();
            this.sendOnDisconnect_(request.action, request.pathString, request.data, request.onComplete);
        }
        for (let i = 0; i < this.outstandingGets_.length; i++) {
            if (this.outstandingGets_[i]) {
                this.sendGet_(i);
            }
        }
    }
    /**
     * Sends client stats for first connection
     */
    sendConnectStats_() {
        const stats = {};
        let clientName = 'js';
        if (util.isNodeSdk()) {
            if (this.repoInfo_.nodeAdmin) {
                clientName = 'admin_node';
            }
            else {
                clientName = 'node';
            }
        }
        stats['sdk.' + clientName + '.' + SDK_VERSION.replace(/\./g, '-')] = 1;
        if (util.isMobileCordova()) {
            stats['framework.cordova'] = 1;
        }
        else if (util.isReactNative()) {
            stats['framework.reactnative'] = 1;
        }
        this.reportStats(stats);
    }
    shouldReconnect_() {
        const online = OnlineMonitor.getInstance().currentlyOnline();
        return util.isEmpty(this.interruptReasons_) && online;
    }
}
PersistentConnection.nextPersistentConnectionId_ = 0;
/**
 * Counter for number of connections created. Mainly used for tagging in the logs
 */
PersistentConnection.nextConnectionId_ = 0;

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class PathIndex extends Index {
    constructor(indexPath_) {
        super();
        this.indexPath_ = indexPath_;
        util.assert(!pathIsEmpty(indexPath_) && pathGetFront(indexPath_) !== '.priority', "Can't create PathIndex with empty path or .priority key");
    }
    extractChild(snap) {
        return snap.getChild(this.indexPath_);
    }
    isDefinedOn(node) {
        return !node.getChild(this.indexPath_).isEmpty();
    }
    compare(a, b) {
        const aChild = this.extractChild(a.node);
        const bChild = this.extractChild(b.node);
        const indexCmp = aChild.compareTo(bChild);
        if (indexCmp === 0) {
            return nameCompare(a.name, b.name);
        }
        else {
            return indexCmp;
        }
    }
    makePost(indexValue, name) {
        const valueNode = nodeFromJSON(indexValue);
        const node = ChildrenNode.EMPTY_NODE.updateChild(this.indexPath_, valueNode);
        return new NamedNode(name, node);
    }
    maxPost() {
        const node = ChildrenNode.EMPTY_NODE.updateChild(this.indexPath_, MAX_NODE);
        return new NamedNode(MAX_NAME, node);
    }
    toString() {
        return pathSlice(this.indexPath_, 0).join('/');
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class ValueIndex extends Index {
    compare(a, b) {
        const indexCmp = a.node.compareTo(b.node);
        if (indexCmp === 0) {
            return nameCompare(a.name, b.name);
        }
        else {
            return indexCmp;
        }
    }
    isDefinedOn(node) {
        return true;
    }
    indexedValueChanged(oldNode, newNode) {
        return !oldNode.equals(newNode);
    }
    minPost() {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return NamedNode.MIN;
    }
    maxPost() {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return NamedNode.MAX;
    }
    makePost(indexValue, name) {
        const valueNode = nodeFromJSON(indexValue);
        return new NamedNode(name, valueNode);
    }
    /**
     * @returns String representation for inclusion in a query spec
     */
    toString() {
        return '.value';
    }
}
const VALUE_INDEX = new ValueIndex();

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
function changeValue(snapshotNode) {
    return { type: "value" /* ChangeType.VALUE */, snapshotNode };
}
function changeChildAdded(childName, snapshotNode) {
    return { type: "child_added" /* ChangeType.CHILD_ADDED */, snapshotNode, childName };
}
function changeChildRemoved(childName, snapshotNode) {
    return { type: "child_removed" /* ChangeType.CHILD_REMOVED */, snapshotNode, childName };
}
function changeChildChanged(childName, snapshotNode, oldSnap) {
    return {
        type: "child_changed" /* ChangeType.CHILD_CHANGED */,
        snapshotNode,
        childName,
        oldSnap
    };
}
function changeChildMoved(childName, snapshotNode) {
    return { type: "child_moved" /* ChangeType.CHILD_MOVED */, snapshotNode, childName };
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Doesn't really filter nodes but applies an index to the node and keeps track of any changes
 */
class IndexedFilter {
    constructor(index_) {
        this.index_ = index_;
    }
    updateChild(snap, key, newChild, affectedPath, source, optChangeAccumulator) {
        util.assert(snap.isIndexed(this.index_), 'A node must be indexed if only a child is updated');
        const oldChild = snap.getImmediateChild(key);
        // Check if anything actually changed.
        if (oldChild.getChild(affectedPath).equals(newChild.getChild(affectedPath))) {
            // There's an edge case where a child can enter or leave the view because affectedPath was set to null.
            // In this case, affectedPath will appear null in both the old and new snapshots.  So we need
            // to avoid treating these cases as "nothing changed."
            if (oldChild.isEmpty() === newChild.isEmpty()) {
                // Nothing changed.
                // This assert should be valid, but it's expensive (can dominate perf testing) so don't actually do it.
                //assert(oldChild.equals(newChild), 'Old and new snapshots should be equal.');
                return snap;
            }
        }
        if (optChangeAccumulator != null) {
            if (newChild.isEmpty()) {
                if (snap.hasChild(key)) {
                    optChangeAccumulator.trackChildChange(changeChildRemoved(key, oldChild));
                }
                else {
                    util.assert(snap.isLeafNode(), 'A child remove without an old child only makes sense on a leaf node');
                }
            }
            else if (oldChild.isEmpty()) {
                optChangeAccumulator.trackChildChange(changeChildAdded(key, newChild));
            }
            else {
                optChangeAccumulator.trackChildChange(changeChildChanged(key, newChild, oldChild));
            }
        }
        if (snap.isLeafNode() && newChild.isEmpty()) {
            return snap;
        }
        else {
            // Make sure the node is indexed
            return snap.updateImmediateChild(key, newChild).withIndex(this.index_);
        }
    }
    updateFullNode(oldSnap, newSnap, optChangeAccumulator) {
        if (optChangeAccumulator != null) {
            if (!oldSnap.isLeafNode()) {
                oldSnap.forEachChild(PRIORITY_INDEX, (key, childNode) => {
                    if (!newSnap.hasChild(key)) {
                        optChangeAccumulator.trackChildChange(changeChildRemoved(key, childNode));
                    }
                });
            }
            if (!newSnap.isLeafNode()) {
                newSnap.forEachChild(PRIORITY_INDEX, (key, childNode) => {
                    if (oldSnap.hasChild(key)) {
                        const oldChild = oldSnap.getImmediateChild(key);
                        if (!oldChild.equals(childNode)) {
                            optChangeAccumulator.trackChildChange(changeChildChanged(key, childNode, oldChild));
                        }
                    }
                    else {
                        optChangeAccumulator.trackChildChange(changeChildAdded(key, childNode));
                    }
                });
            }
        }
        return newSnap.withIndex(this.index_);
    }
    updatePriority(oldSnap, newPriority) {
        if (oldSnap.isEmpty()) {
            return ChildrenNode.EMPTY_NODE;
        }
        else {
            return oldSnap.updatePriority(newPriority);
        }
    }
    filtersNodes() {
        return false;
    }
    getIndexedFilter() {
        return this;
    }
    getIndex() {
        return this.index_;
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Filters nodes by range and uses an IndexFilter to track any changes after filtering the node
 */
class RangedFilter {
    constructor(params) {
        this.indexedFilter_ = new IndexedFilter(params.getIndex());
        this.index_ = params.getIndex();
        this.startPost_ = RangedFilter.getStartPost_(params);
        this.endPost_ = RangedFilter.getEndPost_(params);
        this.startIsInclusive_ = !params.startAfterSet_;
        this.endIsInclusive_ = !params.endBeforeSet_;
    }
    getStartPost() {
        return this.startPost_;
    }
    getEndPost() {
        return this.endPost_;
    }
    matches(node) {
        const isWithinStart = this.startIsInclusive_
            ? this.index_.compare(this.getStartPost(), node) <= 0
            : this.index_.compare(this.getStartPost(), node) < 0;
        const isWithinEnd = this.endIsInclusive_
            ? this.index_.compare(node, this.getEndPost()) <= 0
            : this.index_.compare(node, this.getEndPost()) < 0;
        return isWithinStart && isWithinEnd;
    }
    updateChild(snap, key, newChild, affectedPath, source, optChangeAccumulator) {
        if (!this.matches(new NamedNode(key, newChild))) {
            newChild = ChildrenNode.EMPTY_NODE;
        }
        return this.indexedFilter_.updateChild(snap, key, newChild, affectedPath, source, optChangeAccumulator);
    }
    updateFullNode(oldSnap, newSnap, optChangeAccumulator) {
        if (newSnap.isLeafNode()) {
            // Make sure we have a children node with the correct index, not a leaf node;
            newSnap = ChildrenNode.EMPTY_NODE;
        }
        let filtered = newSnap.withIndex(this.index_);
        // Don't support priorities on queries
        filtered = filtered.updatePriority(ChildrenNode.EMPTY_NODE);
        const self = this;
        newSnap.forEachChild(PRIORITY_INDEX, (key, childNode) => {
            if (!self.matches(new NamedNode(key, childNode))) {
                filtered = filtered.updateImmediateChild(key, ChildrenNode.EMPTY_NODE);
            }
        });
        return this.indexedFilter_.updateFullNode(oldSnap, filtered, optChangeAccumulator);
    }
    updatePriority(oldSnap, newPriority) {
        // Don't support priorities on queries
        return oldSnap;
    }
    filtersNodes() {
        return true;
    }
    getIndexedFilter() {
        return this.indexedFilter_;
    }
    getIndex() {
        return this.index_;
    }
    static getStartPost_(params) {
        if (params.hasStart()) {
            const startName = params.getIndexStartName();
            return params.getIndex().makePost(params.getIndexStartValue(), startName);
        }
        else {
            return params.getIndex().minPost();
        }
    }
    static getEndPost_(params) {
        if (params.hasEnd()) {
            const endName = params.getIndexEndName();
            return params.getIndex().makePost(params.getIndexEndValue(), endName);
        }
        else {
            return params.getIndex().maxPost();
        }
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Applies a limit and a range to a node and uses RangedFilter to do the heavy lifting where possible
 */
class LimitedFilter {
    constructor(params) {
        this.withinDirectionalStart = (node) => this.reverse_ ? this.withinEndPost(node) : this.withinStartPost(node);
        this.withinDirectionalEnd = (node) => this.reverse_ ? this.withinStartPost(node) : this.withinEndPost(node);
        this.withinStartPost = (node) => {
            const compareRes = this.index_.compare(this.rangedFilter_.getStartPost(), node);
            return this.startIsInclusive_ ? compareRes <= 0 : compareRes < 0;
        };
        this.withinEndPost = (node) => {
            const compareRes = this.index_.compare(node, this.rangedFilter_.getEndPost());
            return this.endIsInclusive_ ? compareRes <= 0 : compareRes < 0;
        };
        this.rangedFilter_ = new RangedFilter(params);
        this.index_ = params.getIndex();
        this.limit_ = params.getLimit();
        this.reverse_ = !params.isViewFromLeft();
        this.startIsInclusive_ = !params.startAfterSet_;
        this.endIsInclusive_ = !params.endBeforeSet_;
    }
    updateChild(snap, key, newChild, affectedPath, source, optChangeAccumulator) {
        if (!this.rangedFilter_.matches(new NamedNode(key, newChild))) {
            newChild = ChildrenNode.EMPTY_NODE;
        }
        if (snap.getImmediateChild(key).equals(newChild)) {
            // No change
            return snap;
        }
        else if (snap.numChildren() < this.limit_) {
            return this.rangedFilter_
                .getIndexedFilter()
                .updateChild(snap, key, newChild, affectedPath, source, optChangeAccumulator);
        }
        else {
            return this.fullLimitUpdateChild_(snap, key, newChild, source, optChangeAccumulator);
        }
    }
    updateFullNode(oldSnap, newSnap, optChangeAccumulator) {
        let filtered;
        if (newSnap.isLeafNode() || newSnap.isEmpty()) {
            // Make sure we have a children node with the correct index, not a leaf node;
            filtered = ChildrenNode.EMPTY_NODE.withIndex(this.index_);
        }
        else {
            if (this.limit_ * 2 < newSnap.numChildren() &&
                newSnap.isIndexed(this.index_)) {
                // Easier to build up a snapshot, since what we're given has more than twice the elements we want
                filtered = ChildrenNode.EMPTY_NODE.withIndex(this.index_);
                // anchor to the startPost, endPost, or last element as appropriate
                let iterator;
                if (this.reverse_) {
                    iterator = newSnap.getReverseIteratorFrom(this.rangedFilter_.getEndPost(), this.index_);
                }
                else {
                    iterator = newSnap.getIteratorFrom(this.rangedFilter_.getStartPost(), this.index_);
                }
                let count = 0;
                while (iterator.hasNext() && count < this.limit_) {
                    const next = iterator.getNext();
                    if (!this.withinDirectionalStart(next)) {
                        // if we have not reached the start, skip to the next element
                        continue;
                    }
                    else if (!this.withinDirectionalEnd(next)) {
                        // if we have reached the end, stop adding elements
                        break;
                    }
                    else {
                        filtered = filtered.updateImmediateChild(next.name, next.node);
                        count++;
                    }
                }
            }
            else {
                // The snap contains less than twice the limit. Faster to delete from the snap than build up a new one
                filtered = newSnap.withIndex(this.index_);
                // Don't support priorities on queries
                filtered = filtered.updatePriority(ChildrenNode.EMPTY_NODE);
                let iterator;
                if (this.reverse_) {
                    iterator = filtered.getReverseIterator(this.index_);
                }
                else {
                    iterator = filtered.getIterator(this.index_);
                }
                let count = 0;
                while (iterator.hasNext()) {
                    const next = iterator.getNext();
                    const inRange = count < this.limit_ &&
                        this.withinDirectionalStart(next) &&
                        this.withinDirectionalEnd(next);
                    if (inRange) {
                        count++;
                    }
                    else {
                        filtered = filtered.updateImmediateChild(next.name, ChildrenNode.EMPTY_NODE);
                    }
                }
            }
        }
        return this.rangedFilter_
            .getIndexedFilter()
            .updateFullNode(oldSnap, filtered, optChangeAccumulator);
    }
    updatePriority(oldSnap, newPriority) {
        // Don't support priorities on queries
        return oldSnap;
    }
    filtersNodes() {
        return true;
    }
    getIndexedFilter() {
        return this.rangedFilter_.getIndexedFilter();
    }
    getIndex() {
        return this.index_;
    }
    fullLimitUpdateChild_(snap, childKey, childSnap, source, changeAccumulator) {
        // TODO: rename all cache stuff etc to general snap terminology
        let cmp;
        if (this.reverse_) {
            const indexCmp = this.index_.getCompare();
            cmp = (a, b) => indexCmp(b, a);
        }
        else {
            cmp = this.index_.getCompare();
        }
        const oldEventCache = snap;
        util.assert(oldEventCache.numChildren() === this.limit_, '');
        const newChildNamedNode = new NamedNode(childKey, childSnap);
        const windowBoundary = this.reverse_
            ? oldEventCache.getFirstChild(this.index_)
            : oldEventCache.getLastChild(this.index_);
        const inRange = this.rangedFilter_.matches(newChildNamedNode);
        if (oldEventCache.hasChild(childKey)) {
            const oldChildSnap = oldEventCache.getImmediateChild(childKey);
            let nextChild = source.getChildAfterChild(this.index_, windowBoundary, this.reverse_);
            while (nextChild != null &&
                (nextChild.name === childKey || oldEventCache.hasChild(nextChild.name))) {
                // There is a weird edge case where a node is updated as part of a merge in the write tree, but hasn't
                // been applied to the limited filter yet. Ignore this next child which will be updated later in
                // the limited filter...
                nextChild = source.getChildAfterChild(this.index_, nextChild, this.reverse_);
            }
            const compareNext = nextChild == null ? 1 : cmp(nextChild, newChildNamedNode);
            const remainsInWindow = inRange && !childSnap.isEmpty() && compareNext >= 0;
            if (remainsInWindow) {
                if (changeAccumulator != null) {
                    changeAccumulator.trackChildChange(changeChildChanged(childKey, childSnap, oldChildSnap));
                }
                return oldEventCache.updateImmediateChild(childKey, childSnap);
            }
            else {
                if (changeAccumulator != null) {
                    changeAccumulator.trackChildChange(changeChildRemoved(childKey, oldChildSnap));
                }
                const newEventCache = oldEventCache.updateImmediateChild(childKey, ChildrenNode.EMPTY_NODE);
                const nextChildInRange = nextChild != null && this.rangedFilter_.matches(nextChild);
                if (nextChildInRange) {
                    if (changeAccumulator != null) {
                        changeAccumulator.trackChildChange(changeChildAdded(nextChild.name, nextChild.node));
                    }
                    return newEventCache.updateImmediateChild(nextChild.name, nextChild.node);
                }
                else {
                    return newEventCache;
                }
            }
        }
        else if (childSnap.isEmpty()) {
            // we're deleting a node, but it was not in the window, so ignore it
            return snap;
        }
        else if (inRange) {
            if (cmp(windowBoundary, newChildNamedNode) >= 0) {
                if (changeAccumulator != null) {
                    changeAccumulator.trackChildChange(changeChildRemoved(windowBoundary.name, windowBoundary.node));
                    changeAccumulator.trackChildChange(changeChildAdded(childKey, childSnap));
                }
                return oldEventCache
                    .updateImmediateChild(childKey, childSnap)
                    .updateImmediateChild(windowBoundary.name, ChildrenNode.EMPTY_NODE);
            }
            else {
                return snap;
            }
        }
        else {
            return snap;
        }
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * This class is an immutable-from-the-public-api struct containing a set of query parameters defining a
 * range to be returned for a particular location. It is assumed that validation of parameters is done at the
 * user-facing API level, so it is not done here.
 *
 * @internal
 */
class QueryParams {
    constructor() {
        this.limitSet_ = false;
        this.startSet_ = false;
        this.startNameSet_ = false;
        this.startAfterSet_ = false; // can only be true if startSet_ is true
        this.endSet_ = false;
        this.endNameSet_ = false;
        this.endBeforeSet_ = false; // can only be true if endSet_ is true
        this.limit_ = 0;
        this.viewFrom_ = '';
        this.indexStartValue_ = null;
        this.indexStartName_ = '';
        this.indexEndValue_ = null;
        this.indexEndName_ = '';
        this.index_ = PRIORITY_INDEX;
    }
    hasStart() {
        return this.startSet_;
    }
    /**
     * @returns True if it would return from left.
     */
    isViewFromLeft() {
        if (this.viewFrom_ === '') {
            // limit(), rather than limitToFirst or limitToLast was called.
            // This means that only one of startSet_ and endSet_ is true. Use them
            // to calculate which side of the view to anchor to. If neither is set,
            // anchor to the end.
            return this.startSet_;
        }
        else {
            return this.viewFrom_ === "l" /* WIRE_PROTOCOL_CONSTANTS.VIEW_FROM_LEFT */;
        }
    }
    /**
     * Only valid to call if hasStart() returns true
     */
    getIndexStartValue() {
        util.assert(this.startSet_, 'Only valid if start has been set');
        return this.indexStartValue_;
    }
    /**
     * Only valid to call if hasStart() returns true.
     * Returns the starting key name for the range defined by these query parameters
     */
    getIndexStartName() {
        util.assert(this.startSet_, 'Only valid if start has been set');
        if (this.startNameSet_) {
            return this.indexStartName_;
        }
        else {
            return MIN_NAME;
        }
    }
    hasEnd() {
        return this.endSet_;
    }
    /**
     * Only valid to call if hasEnd() returns true.
     */
    getIndexEndValue() {
        util.assert(this.endSet_, 'Only valid if end has been set');
        return this.indexEndValue_;
    }
    /**
     * Only valid to call if hasEnd() returns true.
     * Returns the end key name for the range defined by these query parameters
     */
    getIndexEndName() {
        util.assert(this.endSet_, 'Only valid if end has been set');
        if (this.endNameSet_) {
            return this.indexEndName_;
        }
        else {
            return MAX_NAME;
        }
    }
    hasLimit() {
        return this.limitSet_;
    }
    /**
     * @returns True if a limit has been set and it has been explicitly anchored
     */
    hasAnchoredLimit() {
        return this.limitSet_ && this.viewFrom_ !== '';
    }
    /**
     * Only valid to call if hasLimit() returns true
     */
    getLimit() {
        util.assert(this.limitSet_, 'Only valid if limit has been set');
        return this.limit_;
    }
    getIndex() {
        return this.index_;
    }
    loadsAllData() {
        return !(this.startSet_ || this.endSet_ || this.limitSet_);
    }
    isDefault() {
        return this.loadsAllData() && this.index_ === PRIORITY_INDEX;
    }
    copy() {
        const copy = new QueryParams();
        copy.limitSet_ = this.limitSet_;
        copy.limit_ = this.limit_;
        copy.startSet_ = this.startSet_;
        copy.startAfterSet_ = this.startAfterSet_;
        copy.indexStartValue_ = this.indexStartValue_;
        copy.startNameSet_ = this.startNameSet_;
        copy.indexStartName_ = this.indexStartName_;
        copy.endSet_ = this.endSet_;
        copy.endBeforeSet_ = this.endBeforeSet_;
        copy.indexEndValue_ = this.indexEndValue_;
        copy.endNameSet_ = this.endNameSet_;
        copy.indexEndName_ = this.indexEndName_;
        copy.index_ = this.index_;
        copy.viewFrom_ = this.viewFrom_;
        return copy;
    }
}
function queryParamsGetNodeFilter(queryParams) {
    if (queryParams.loadsAllData()) {
        return new IndexedFilter(queryParams.getIndex());
    }
    else if (queryParams.hasLimit()) {
        return new LimitedFilter(queryParams);
    }
    else {
        return new RangedFilter(queryParams);
    }
}
function queryParamsLimitToFirst(queryParams, newLimit) {
    const newParams = queryParams.copy();
    newParams.limitSet_ = true;
    newParams.limit_ = newLimit;
    newParams.viewFrom_ = "l" /* WIRE_PROTOCOL_CONSTANTS.VIEW_FROM_LEFT */;
    return newParams;
}
function queryParamsLimitToLast(queryParams, newLimit) {
    const newParams = queryParams.copy();
    newParams.limitSet_ = true;
    newParams.limit_ = newLimit;
    newParams.viewFrom_ = "r" /* WIRE_PROTOCOL_CONSTANTS.VIEW_FROM_RIGHT */;
    return newParams;
}
function queryParamsStartAt(queryParams, indexValue, key) {
    const newParams = queryParams.copy();
    newParams.startSet_ = true;
    if (indexValue === undefined) {
        indexValue = null;
    }
    newParams.indexStartValue_ = indexValue;
    if (key != null) {
        newParams.startNameSet_ = true;
        newParams.indexStartName_ = key;
    }
    else {
        newParams.startNameSet_ = false;
        newParams.indexStartName_ = '';
    }
    return newParams;
}
function queryParamsStartAfter(queryParams, indexValue, key) {
    let params;
    if (queryParams.index_ === KEY_INDEX || !!key) {
        params = queryParamsStartAt(queryParams, indexValue, key);
    }
    else {
        params = queryParamsStartAt(queryParams, indexValue, MAX_NAME);
    }
    params.startAfterSet_ = true;
    return params;
}
function queryParamsEndAt(queryParams, indexValue, key) {
    const newParams = queryParams.copy();
    newParams.endSet_ = true;
    if (indexValue === undefined) {
        indexValue = null;
    }
    newParams.indexEndValue_ = indexValue;
    if (key !== undefined) {
        newParams.endNameSet_ = true;
        newParams.indexEndName_ = key;
    }
    else {
        newParams.endNameSet_ = false;
        newParams.indexEndName_ = '';
    }
    return newParams;
}
function queryParamsEndBefore(queryParams, indexValue, key) {
    let params;
    if (queryParams.index_ === KEY_INDEX || !!key) {
        params = queryParamsEndAt(queryParams, indexValue, key);
    }
    else {
        params = queryParamsEndAt(queryParams, indexValue, MIN_NAME);
    }
    params.endBeforeSet_ = true;
    return params;
}
function queryParamsOrderBy(queryParams, index) {
    const newParams = queryParams.copy();
    newParams.index_ = index;
    return newParams;
}
/**
 * Returns a set of REST query string parameters representing this query.
 *
 * @returns query string parameters
 */
function queryParamsToRestQueryStringParameters(queryParams) {
    const qs = {};
    if (queryParams.isDefault()) {
        return qs;
    }
    let orderBy;
    if (queryParams.index_ === PRIORITY_INDEX) {
        orderBy = "$priority" /* REST_QUERY_CONSTANTS.PRIORITY_INDEX */;
    }
    else if (queryParams.index_ === VALUE_INDEX) {
        orderBy = "$value" /* REST_QUERY_CONSTANTS.VALUE_INDEX */;
    }
    else if (queryParams.index_ === KEY_INDEX) {
        orderBy = "$key" /* REST_QUERY_CONSTANTS.KEY_INDEX */;
    }
    else {
        util.assert(queryParams.index_ instanceof PathIndex, 'Unrecognized index type!');
        orderBy = queryParams.index_.toString();
    }
    qs["orderBy" /* REST_QUERY_CONSTANTS.ORDER_BY */] = util.stringify(orderBy);
    if (queryParams.startSet_) {
        const startParam = queryParams.startAfterSet_
            ? "startAfter" /* REST_QUERY_CONSTANTS.START_AFTER */
            : "startAt" /* REST_QUERY_CONSTANTS.START_AT */;
        qs[startParam] = util.stringify(queryParams.indexStartValue_);
        if (queryParams.startNameSet_) {
            qs[startParam] += ',' + util.stringify(queryParams.indexStartName_);
        }
    }
    if (queryParams.endSet_) {
        const endParam = queryParams.endBeforeSet_
            ? "endBefore" /* REST_QUERY_CONSTANTS.END_BEFORE */
            : "endAt" /* REST_QUERY_CONSTANTS.END_AT */;
        qs[endParam] = util.stringify(queryParams.indexEndValue_);
        if (queryParams.endNameSet_) {
            qs[endParam] += ',' + util.stringify(queryParams.indexEndName_);
        }
    }
    if (queryParams.limitSet_) {
        if (queryParams.isViewFromLeft()) {
            qs["limitToFirst" /* REST_QUERY_CONSTANTS.LIMIT_TO_FIRST */] = queryParams.limit_;
        }
        else {
            qs["limitToLast" /* REST_QUERY_CONSTANTS.LIMIT_TO_LAST */] = queryParams.limit_;
        }
    }
    return qs;
}
function queryParamsGetQueryObject(queryParams) {
    const obj = {};
    if (queryParams.startSet_) {
        obj["sp" /* WIRE_PROTOCOL_CONSTANTS.INDEX_START_VALUE */] =
            queryParams.indexStartValue_;
        if (queryParams.startNameSet_) {
            obj["sn" /* WIRE_PROTOCOL_CONSTANTS.INDEX_START_NAME */] =
                queryParams.indexStartName_;
        }
        obj["sin" /* WIRE_PROTOCOL_CONSTANTS.INDEX_START_IS_INCLUSIVE */] =
            !queryParams.startAfterSet_;
    }
    if (queryParams.endSet_) {
        obj["ep" /* WIRE_PROTOCOL_CONSTANTS.INDEX_END_VALUE */] = queryParams.indexEndValue_;
        if (queryParams.endNameSet_) {
            obj["en" /* WIRE_PROTOCOL_CONSTANTS.INDEX_END_NAME */] = queryParams.indexEndName_;
        }
        obj["ein" /* WIRE_PROTOCOL_CONSTANTS.INDEX_END_IS_INCLUSIVE */] =
            !queryParams.endBeforeSet_;
    }
    if (queryParams.limitSet_) {
        obj["l" /* WIRE_PROTOCOL_CONSTANTS.LIMIT */] = queryParams.limit_;
        let viewFrom = queryParams.viewFrom_;
        if (viewFrom === '') {
            if (queryParams.isViewFromLeft()) {
                viewFrom = "l" /* WIRE_PROTOCOL_CONSTANTS.VIEW_FROM_LEFT */;
            }
            else {
                viewFrom = "r" /* WIRE_PROTOCOL_CONSTANTS.VIEW_FROM_RIGHT */;
            }
        }
        obj["vf" /* WIRE_PROTOCOL_CONSTANTS.VIEW_FROM */] = viewFrom;
    }
    // For now, priority index is the default, so we only specify if it's some other index
    if (queryParams.index_ !== PRIORITY_INDEX) {
        obj["i" /* WIRE_PROTOCOL_CONSTANTS.INDEX */] = queryParams.index_.toString();
    }
    return obj;
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * An implementation of ServerActions that communicates with the server via REST requests.
 * This is mostly useful for compatibility with crawlers, where we don't want to spin up a full
 * persistent connection (using WebSockets or long-polling)
 */
class ReadonlyRestClient extends ServerActions {
    reportStats(stats) {
        throw new Error('Method not implemented.');
    }
    static getListenId_(query, tag) {
        if (tag !== undefined) {
            return 'tag$' + tag;
        }
        else {
            util.assert(query._queryParams.isDefault(), "should have a tag if it's not a default query.");
            return query._path.toString();
        }
    }
    /**
     * @param repoInfo_ - Data about the namespace we are connecting to
     * @param onDataUpdate_ - A callback for new data from the server
     */
    constructor(repoInfo_, onDataUpdate_, authTokenProvider_, appCheckTokenProvider_) {
        super();
        this.repoInfo_ = repoInfo_;
        this.onDataUpdate_ = onDataUpdate_;
        this.authTokenProvider_ = authTokenProvider_;
        this.appCheckTokenProvider_ = appCheckTokenProvider_;
        /** @private {function(...[*])} */
        this.log_ = logWrapper('p:rest:');
        /**
         * We don't actually need to track listens, except to prevent us calling an onComplete for a listen
         * that's been removed. :-/
         */
        this.listens_ = {};
    }
    /** @inheritDoc */
    listen(query, currentHashFn, tag, onComplete, onProgress) {
        const pathString = query._path.toString();
        this.log_('Listen called for ' + pathString + ' ' + query._queryIdentifier);
        // Mark this listener so we can tell if it's removed.
        const listenId = ReadonlyRestClient.getListenId_(query, tag);
        const thisListen = {};
        this.listens_[listenId] = thisListen;
        const queryStringParameters = queryParamsToRestQueryStringParameters(query._queryParams);
        this.restRequest_(pathString + '.json', queryStringParameters, (error, result) => {
            let data = result;
            if (error === 404) {
                data = null;
                error = null;
            }
            if (error === null) {
                this.onDataUpdate_(pathString, data, /*isMerge=*/ false, tag);
            }
            if (util.safeGet(this.listens_, listenId) === thisListen) {
                let status;
                if (!error) {
                    status = 'ok';
                }
                else if (error === 401) {
                    status = 'permission_denied';
                }
                else {
                    status = 'rest_error:' + error;
                }
                const wire = {
                    bytes: 0,
                    hadHash: false,
                    hadCompoundHash: false,
                    dataReceived: error === null,
                    rangeMerged: false
                };
                if (wire.dataReceived) {
                    onProgress?.(wire);
                }
                onComplete(status, null, wire);
            }
        });
    }
    /** @inheritDoc */
    unlisten(query, tag) {
        const listenId = ReadonlyRestClient.getListenId_(query, tag);
        delete this.listens_[listenId];
    }
    get(query) {
        const queryStringParameters = queryParamsToRestQueryStringParameters(query._queryParams);
        const pathString = query._path.toString();
        const deferred = new util.Deferred();
        this.restRequest_(pathString + '.json', queryStringParameters, (error, result) => {
            let data = result;
            if (error === 404) {
                data = null;
                error = null;
            }
            if (error === null) {
                this.onDataUpdate_(pathString, data, 
                /*isMerge=*/ false, 
                /*tag=*/ null);
                deferred.resolve(data);
            }
            else {
                deferred.reject(new Error(data));
            }
        });
        return deferred.promise;
    }
    /** @inheritDoc */
    refreshAuthToken(token) {
        // no-op since we just always call getToken.
    }
    /**
     * Performs a REST request to the given path, with the provided query string parameters,
     * and any auth credentials we have.
     */
    restRequest_(pathString, queryStringParameters = {}, callback) {
        queryStringParameters['format'] = 'export';
        return Promise.all([
            this.authTokenProvider_.getToken(/*forceRefresh=*/ false),
            this.appCheckTokenProvider_.getToken(/*forceRefresh=*/ false)
        ]).then(([authToken, appCheckToken]) => {
            if (authToken && authToken.accessToken) {
                queryStringParameters['auth'] = authToken.accessToken;
            }
            if (appCheckToken && appCheckToken.token) {
                queryStringParameters['ac'] = appCheckToken.token;
            }
            const url = (this.repoInfo_.secure ? 'https://' : 'http://') +
                this.repoInfo_.host +
                pathString +
                '?' +
                'ns=' +
                this.repoInfo_.namespace +
                util.querystring(queryStringParameters);
            this.log_('Sending REST request for ' + url);
            const xhr = new XMLHttpRequest();
            xhr.onreadystatechange = () => {
                if (callback && xhr.readyState === 4) {
                    this.log_('REST Response for ' + url + ' received. status:', xhr.status, 'response:', xhr.responseText);
                    let res = null;
                    if (xhr.status >= 200 && xhr.status < 300) {
                        try {
                            res = util.jsonEval(xhr.responseText);
                        }
                        catch (e) {
                            warn('Failed to parse JSON response for ' +
                                url +
                                ': ' +
                                xhr.responseText);
                        }
                        callback(null, res);
                    }
                    else {
                        // 401 and 404 are expected.
                        if (xhr.status !== 401 && xhr.status !== 404) {
                            warn('Got unsuccessful REST response for ' +
                                url +
                                ' Status: ' +
                                xhr.status);
                        }
                        callback(xhr.status);
                    }
                    callback = null;
                }
            };
            xhr.open('GET', url, /*asynchronous=*/ true);
            xhr.send();
        });
    }
}

/**
 * @license
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Applies a server range merge against locally cached data: every leaf whose
 * path lies strictly after `optExclusiveStart` and at-or-before
 * `optInclusiveEnd` is replaced by (or, when absent from the update, deleted
 * in favor of) the corresponding leaves of the update node; everything
 * outside the range is kept. A null bound is open (-/+ infinity). Priorities
 * of children nodes are treated as leaf children of that node.
 *
 * The server sends range merges when a listen carried a compound hash and
 * only some of its ranges differed — each merge covers one differing range.
 *
 * This is a port of the range merge support in the Android and iOS SDKs
 * (Android: com.google.firebase.database.snapshot.RangeMerge); the
 * semantics match them exactly.
 */
class RangeMerge {
    constructor(optExclusiveStart_, optInclusiveEnd_, snap_) {
        this.optExclusiveStart_ = optExclusiveStart_;
        this.optInclusiveEnd_ = optInclusiveEnd_;
        this.snap_ = snap_;
    }
    applyTo(node) {
        return this.updateRangeInNode_(newEmptyPath(), node, this.snap_);
    }
    updateRangeInNode_(currentPath, node, updateNode) {
        const startComparison = this.optExclusiveStart_ === null
            ? 1
            : pathCompare(currentPath, this.optExclusiveStart_);
        const endComparison = this.optInclusiveEnd_ === null
            ? -1
            : pathCompare(currentPath, this.optInclusiveEnd_);
        const startInNode = this.optExclusiveStart_ !== null &&
            pathContains(currentPath, this.optExclusiveStart_);
        const endInNode = this.optInclusiveEnd_ !== null &&
            pathContains(currentPath, this.optInclusiveEnd_);
        if (startComparison > 0 && endComparison < 0 && !endInNode) {
            // node is completely contained in the range
            return updateNode;
        }
        else if (startComparison > 0 && endInNode && updateNode.isLeafNode()) {
            return updateNode;
        }
        else if (startComparison > 0 && endComparison === 0) {
            // Exactly the inclusive end and the update is not a leaf: any leaf at
            // this position was consumed by the range (deleted); deeper structure
            // is outside the range.
            if (node.isLeafNode()) {
                return ChildrenNode.EMPTY_NODE;
            }
            return node;
        }
        else if (startInNode || endInNode) {
            // The range starts or ends within this node: update the union of both
            // nodes' children.
            const allChildren = new Set();
            node.forEachChild(KEY_INDEX, key => {
                allChildren.add(key);
            });
            updateNode.forEachChild(KEY_INDEX, key => {
                allChildren.add(key);
            });
            const inOrder = Array.from(allChildren);
            // Add priority last, so the node is not empty when it is applied.
            if (!updateNode.getPriority().isEmpty() ||
                !node.getPriority().isEmpty()) {
                inOrder.push('.priority');
            }
            let newNode = node;
            for (const key of inOrder) {
                const currentChild = node.getImmediateChild(key);
                const updatedChild = this.updateRangeInNode_(pathChild(currentPath, key), currentChild, updateNode.getImmediateChild(key));
                if (updatedChild !== currentChild) {
                    newNode = newNode.updateImmediateChild(key, updatedChild);
                }
            }
            return newNode;
        }
        else {
            // Unaffected by this range
            return node;
        }
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Mutable object which basically just stores a reference to the "latest" immutable snapshot.
 */
class SnapshotHolder {
    constructor() {
        this.rootNode_ = ChildrenNode.EMPTY_NODE;
    }
    getNode(path) {
        return this.rootNode_.getChild(path);
    }
    updateSnapshot(path, newSnapshotNode) {
        this.rootNode_ = this.rootNode_.updateChild(path, newSnapshotNode);
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
function newSparseSnapshotTree() {
    return {
        value: null,
        children: new Map()
    };
}
/**
 * Stores the given node at the specified path. If there is already a node
 * at a shallower path, it merges the new data into that snapshot node.
 *
 * @param path - Path to look up snapshot for.
 * @param data - The new data, or null.
 */
function sparseSnapshotTreeRemember(sparseSnapshotTree, path, data) {
    if (pathIsEmpty(path)) {
        sparseSnapshotTree.value = data;
        sparseSnapshotTree.children.clear();
    }
    else if (sparseSnapshotTree.value !== null) {
        sparseSnapshotTree.value = sparseSnapshotTree.value.updateChild(path, data);
    }
    else {
        const childKey = pathGetFront(path);
        if (!sparseSnapshotTree.children.has(childKey)) {
            sparseSnapshotTree.children.set(childKey, newSparseSnapshotTree());
        }
        const child = sparseSnapshotTree.children.get(childKey);
        path = pathPopFront(path);
        sparseSnapshotTreeRemember(child, path, data);
    }
}
/**
 * Purge the data at path from the cache.
 *
 * @param path - Path to look up snapshot for.
 * @returns True if this node should now be removed.
 */
function sparseSnapshotTreeForget(sparseSnapshotTree, path) {
    if (pathIsEmpty(path)) {
        sparseSnapshotTree.value = null;
        sparseSnapshotTree.children.clear();
        return true;
    }
    else {
        if (sparseSnapshotTree.value !== null) {
            if (sparseSnapshotTree.value.isLeafNode()) {
                // We're trying to forget a node that doesn't exist
                return false;
            }
            else {
                const value = sparseSnapshotTree.value;
                sparseSnapshotTree.value = null;
                value.forEachChild(PRIORITY_INDEX, (key, tree) => {
                    sparseSnapshotTreeRemember(sparseSnapshotTree, new Path(key), tree);
                });
                return sparseSnapshotTreeForget(sparseSnapshotTree, path);
            }
        }
        else if (sparseSnapshotTree.children.size > 0) {
            const childKey = pathGetFront(path);
            path = pathPopFront(path);
            if (sparseSnapshotTree.children.has(childKey)) {
                const safeToRemove = sparseSnapshotTreeForget(sparseSnapshotTree.children.get(childKey), path);
                if (safeToRemove) {
                    sparseSnapshotTree.children.delete(childKey);
                }
            }
            return sparseSnapshotTree.children.size === 0;
        }
        else {
            return true;
        }
    }
}
/**
 * Recursively iterates through all of the stored tree and calls the
 * callback on each one.
 *
 * @param prefixPath - Path to look up node for.
 * @param func - The function to invoke for each tree.
 */
function sparseSnapshotTreeForEachTree(sparseSnapshotTree, prefixPath, func) {
    if (sparseSnapshotTree.value !== null) {
        func(prefixPath, sparseSnapshotTree.value);
    }
    else {
        sparseSnapshotTreeForEachChild(sparseSnapshotTree, (key, tree) => {
            const path = new Path(prefixPath.toString() + '/' + key);
            sparseSnapshotTreeForEachTree(tree, path, func);
        });
    }
}
/**
 * Iterates through each immediate child and triggers the callback.
 * Only seems to be used in tests.
 *
 * @param func - The function to invoke for each child.
 */
function sparseSnapshotTreeForEachChild(sparseSnapshotTree, func) {
    sparseSnapshotTree.children.forEach((tree, key) => {
        func(key, tree);
    });
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Returns the delta from the previous call to get stats.
 *
 * @param collection_ - The collection to "listen" to.
 */
class StatsListener {
    constructor(collection_) {
        this.collection_ = collection_;
        this.last_ = null;
    }
    get() {
        const newStats = this.collection_.get();
        const delta = { ...newStats };
        if (this.last_) {
            each(this.last_, (stat, value) => {
                delta[stat] = delta[stat] - value;
            });
        }
        this.last_ = newStats;
        return delta;
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Assuming some apps may have a short amount of time on page, and a bulk of firebase operations probably
// happen on page load, we try to report our first set of stats pretty quickly, but we wait at least 10
// seconds to try to ensure the Firebase connection is established / settled.
const FIRST_STATS_MIN_TIME = 10 * 1000;
const FIRST_STATS_MAX_TIME = 30 * 1000;
// We'll continue to report stats on average every 5 minutes.
const REPORT_STATS_INTERVAL = 5 * 60 * 1000;
class StatsReporter {
    constructor(collection, server_) {
        this.server_ = server_;
        this.statsToReport_ = {};
        this.statsListener_ = new StatsListener(collection);
        const timeout = FIRST_STATS_MIN_TIME +
            (FIRST_STATS_MAX_TIME - FIRST_STATS_MIN_TIME) * Math.random();
        setTimeoutNonBlocking(this.reportStats_.bind(this), Math.floor(timeout));
    }
    reportStats_() {
        const stats = this.statsListener_.get();
        const reportedStats = {};
        let haveStatsToReport = false;
        each(stats, (stat, value) => {
            if (value > 0 && util.contains(this.statsToReport_, stat)) {
                reportedStats[stat] = value;
                haveStatsToReport = true;
            }
        });
        if (haveStatsToReport) {
            this.server_.reportStats(reportedStats);
        }
        // queue our next run.
        setTimeoutNonBlocking(this.reportStats_.bind(this), Math.floor(Math.random() * 2 * REPORT_STATS_INTERVAL));
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 *
 * @enum
 */
var OperationType;
(function (OperationType) {
    OperationType[OperationType["OVERWRITE"] = 0] = "OVERWRITE";
    OperationType[OperationType["MERGE"] = 1] = "MERGE";
    OperationType[OperationType["ACK_USER_WRITE"] = 2] = "ACK_USER_WRITE";
    OperationType[OperationType["LISTEN_COMPLETE"] = 3] = "LISTEN_COMPLETE";
})(OperationType || (OperationType = {}));
function newOperationSourceUser() {
    return {
        fromUser: true,
        fromServer: false,
        queryId: null,
        tagged: false
    };
}
function newOperationSourceServer() {
    return {
        fromUser: false,
        fromServer: true,
        queryId: null,
        tagged: false
    };
}
function newOperationSourceServerTaggedQuery(queryId) {
    return {
        fromUser: false,
        fromServer: true,
        queryId,
        tagged: true
    };
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class AckUserWrite {
    /**
     * @param affectedTree - A tree containing true for each affected path. Affected paths can't overlap.
     */
    constructor(
    /** @inheritDoc */ path, 
    /** @inheritDoc */ affectedTree, 
    /** @inheritDoc */ revert) {
        this.path = path;
        this.affectedTree = affectedTree;
        this.revert = revert;
        /** @inheritDoc */
        this.type = OperationType.ACK_USER_WRITE;
        /** @inheritDoc */
        this.source = newOperationSourceUser();
    }
    operationForChild(childName) {
        if (!pathIsEmpty(this.path)) {
            util.assert(pathGetFront(this.path) === childName, 'operationForChild called for unrelated child.');
            return new AckUserWrite(pathPopFront(this.path), this.affectedTree, this.revert);
        }
        else if (this.affectedTree.value != null) {
            util.assert(this.affectedTree.children.isEmpty(), 'affectedTree should not have overlapping affected paths.');
            // All child locations are affected as well; just return same operation.
            return this;
        }
        else {
            const childTree = this.affectedTree.subtree(new Path(childName));
            return new AckUserWrite(newEmptyPath(), childTree, this.revert);
        }
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class ListenComplete {
    constructor(source, path) {
        this.source = source;
        this.path = path;
        /** @inheritDoc */
        this.type = OperationType.LISTEN_COMPLETE;
    }
    operationForChild(childName) {
        if (pathIsEmpty(this.path)) {
            return new ListenComplete(this.source, newEmptyPath());
        }
        else {
            return new ListenComplete(this.source, pathPopFront(this.path));
        }
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class Overwrite {
    constructor(source, path, snap) {
        this.source = source;
        this.path = path;
        this.snap = snap;
        /** @inheritDoc */
        this.type = OperationType.OVERWRITE;
    }
    operationForChild(childName) {
        if (pathIsEmpty(this.path)) {
            return new Overwrite(this.source, newEmptyPath(), this.snap.getImmediateChild(childName));
        }
        else {
            return new Overwrite(this.source, pathPopFront(this.path), this.snap);
        }
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class Merge {
    constructor(
    /** @inheritDoc */ source, 
    /** @inheritDoc */ path, 
    /** @inheritDoc */ children) {
        this.source = source;
        this.path = path;
        this.children = children;
        /** @inheritDoc */
        this.type = OperationType.MERGE;
    }
    operationForChild(childName) {
        if (pathIsEmpty(this.path)) {
            const childTree = this.children.subtree(new Path(childName));
            if (childTree.isEmpty()) {
                // This child is unaffected
                return null;
            }
            else if (childTree.value) {
                // We have a snapshot for the child in question.  This becomes an overwrite of the child.
                return new Overwrite(this.source, newEmptyPath(), childTree.value);
            }
            else {
                // This is a merge at a deeper level
                return new Merge(this.source, newEmptyPath(), childTree);
            }
        }
        else {
            util.assert(pathGetFront(this.path) === childName, "Can't get a merge for a child not on the path of the operation");
            return new Merge(this.source, pathPopFront(this.path), this.children);
        }
    }
    toString() {
        return ('Operation(' +
            this.path +
            ': ' +
            this.source.toString() +
            ' merge: ' +
            this.children.toString() +
            ')');
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * A cache node only stores complete children. Additionally it holds a flag whether the node can be considered fully
 * initialized in the sense that we know at one point in time this represented a valid state of the world, e.g.
 * initialized with data from the server, or a complete overwrite by the client. The filtered flag also tracks
 * whether a node potentially had children removed due to a filter.
 */
class CacheNode {
    constructor(node_, fullyInitialized_, filtered_) {
        this.node_ = node_;
        this.fullyInitialized_ = fullyInitialized_;
        this.filtered_ = filtered_;
    }
    /**
     * Returns whether this node was fully initialized with either server data or a complete overwrite by the client
     */
    isFullyInitialized() {
        return this.fullyInitialized_;
    }
    /**
     * Returns whether this node is potentially missing children due to a filter applied to the node
     */
    isFiltered() {
        return this.filtered_;
    }
    isCompleteForPath(path) {
        if (pathIsEmpty(path)) {
            return this.isFullyInitialized() && !this.filtered_;
        }
        const childKey = pathGetFront(path);
        return this.isCompleteForChild(childKey);
    }
    isCompleteForChild(key) {
        return ((this.isFullyInitialized() && !this.filtered_) || this.node_.hasChild(key));
    }
    getNode() {
        return this.node_;
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * An EventGenerator is used to convert "raw" changes (Change) as computed by the
 * CacheDiffer into actual events (Event) that can be raised.  See generateEventsForChanges()
 * for details.
 *
 */
class EventGenerator {
    constructor(query_) {
        this.query_ = query_;
        this.index_ = this.query_._queryParams.getIndex();
    }
}
/**
 * Given a set of raw changes (no moved events and prevName not specified yet), and a set of
 * EventRegistrations that should be notified of these changes, generate the actual events to be raised.
 *
 * Notes:
 *  - child_moved events will be synthesized at this time for any child_changed events that affect
 *    our index.
 *  - prevName will be calculated based on the index ordering.
 */
function eventGeneratorGenerateEventsForChanges(eventGenerator, changes, eventCache, eventRegistrations) {
    const events = [];
    const moves = [];
    changes.forEach(change => {
        if (change.type === "child_changed" /* ChangeType.CHILD_CHANGED */ &&
            eventGenerator.index_.indexedValueChanged(change.oldSnap, change.snapshotNode)) {
            moves.push(changeChildMoved(change.childName, change.snapshotNode));
        }
    });
    eventGeneratorGenerateEventsForType(eventGenerator, events, "child_removed" /* ChangeType.CHILD_REMOVED */, changes, eventRegistrations, eventCache);
    eventGeneratorGenerateEventsForType(eventGenerator, events, "child_added" /* ChangeType.CHILD_ADDED */, changes, eventRegistrations, eventCache);
    eventGeneratorGenerateEventsForType(eventGenerator, events, "child_moved" /* ChangeType.CHILD_MOVED */, moves, eventRegistrations, eventCache);
    eventGeneratorGenerateEventsForType(eventGenerator, events, "child_changed" /* ChangeType.CHILD_CHANGED */, changes, eventRegistrations, eventCache);
    eventGeneratorGenerateEventsForType(eventGenerator, events, "value" /* ChangeType.VALUE */, changes, eventRegistrations, eventCache);
    return events;
}
/**
 * Given changes of a single change type, generate the corresponding events.
 */
function eventGeneratorGenerateEventsForType(eventGenerator, events, eventType, changes, registrations, eventCache) {
    const filteredChanges = changes.filter(change => change.type === eventType);
    filteredChanges.sort((a, b) => eventGeneratorCompareChanges(eventGenerator, a, b));
    filteredChanges.forEach(change => {
        const materializedChange = eventGeneratorMaterializeSingleChange(eventGenerator, change, eventCache);
        registrations.forEach(registration => {
            if (registration.respondsTo(change.type)) {
                events.push(registration.createEvent(materializedChange, eventGenerator.query_));
            }
        });
    });
}
function eventGeneratorMaterializeSingleChange(eventGenerator, change, eventCache) {
    if (change.type === 'value' || change.type === 'child_removed') {
        return change;
    }
    else {
        change.prevName = eventCache.getPredecessorChildName(change.childName, change.snapshotNode, eventGenerator.index_);
        return change;
    }
}
function eventGeneratorCompareChanges(eventGenerator, a, b) {
    if (a.childName == null || b.childName == null) {
        throw util.assertionError('Should only compare child_ events.');
    }
    const aWrapped = new NamedNode(a.childName, a.snapshotNode);
    const bWrapped = new NamedNode(b.childName, b.snapshotNode);
    return eventGenerator.index_.compare(aWrapped, bWrapped);
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
function newViewCache(eventCache, serverCache) {
    return { eventCache, serverCache };
}
function viewCacheUpdateEventSnap(viewCache, eventSnap, complete, filtered) {
    return newViewCache(new CacheNode(eventSnap, complete, filtered), viewCache.serverCache);
}
function viewCacheUpdateServerSnap(viewCache, serverSnap, complete, filtered) {
    return newViewCache(viewCache.eventCache, new CacheNode(serverSnap, complete, filtered));
}
function viewCacheGetCompleteEventSnap(viewCache) {
    return viewCache.eventCache.isFullyInitialized()
        ? viewCache.eventCache.getNode()
        : null;
}
function viewCacheGetCompleteServerSnap(viewCache) {
    return viewCache.serverCache.isFullyInitialized()
        ? viewCache.serverCache.getNode()
        : null;
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
let emptyChildrenSingleton;
/**
 * Singleton empty children collection.
 *
 */
const EmptyChildren = () => {
    if (!emptyChildrenSingleton) {
        emptyChildrenSingleton = new SortedMap(stringCompare);
    }
    return emptyChildrenSingleton;
};
/**
 * A tree with immutable elements.
 */
class ImmutableTree {
    static fromObject(obj) {
        let tree = new ImmutableTree(null);
        each(obj, (childPath, childSnap) => {
            tree = tree.set(new Path(childPath), childSnap);
        });
        return tree;
    }
    constructor(value, children = EmptyChildren()) {
        this.value = value;
        this.children = children;
    }
    /**
     * True if the value is empty and there are no children
     */
    isEmpty() {
        return this.value === null && this.children.isEmpty();
    }
    /**
     * Given a path and predicate, return the first node and the path to that node
     * where the predicate returns true.
     *
     * TODO Do a perf test -- If we're creating a bunch of `{path: value:}`
     * objects on the way back out, it may be better to pass down a pathSoFar obj.
     *
     * @param relativePath - The remainder of the path
     * @param predicate - The predicate to satisfy to return a node
     */
    findRootMostMatchingPathAndValue(relativePath, predicate) {
        if (this.value != null && predicate(this.value)) {
            return { path: newEmptyPath(), value: this.value };
        }
        else {
            if (pathIsEmpty(relativePath)) {
                return null;
            }
            else {
                const front = pathGetFront(relativePath);
                const child = this.children.get(front);
                if (child !== null) {
                    const childExistingPathAndValue = child.findRootMostMatchingPathAndValue(pathPopFront(relativePath), predicate);
                    if (childExistingPathAndValue != null) {
                        const fullPath = pathChild(new Path(front), childExistingPathAndValue.path);
                        return { path: fullPath, value: childExistingPathAndValue.value };
                    }
                    else {
                        return null;
                    }
                }
                else {
                    return null;
                }
            }
        }
    }
    /**
     * Find, if it exists, the shortest subpath of the given path that points a defined
     * value in the tree
     */
    findRootMostValueAndPath(relativePath) {
        return this.findRootMostMatchingPathAndValue(relativePath, () => true);
    }
    /**
     * @returns The subtree at the given path
     */
    subtree(relativePath) {
        if (pathIsEmpty(relativePath)) {
            return this;
        }
        else {
            const front = pathGetFront(relativePath);
            const childTree = this.children.get(front);
            if (childTree !== null) {
                return childTree.subtree(pathPopFront(relativePath));
            }
            else {
                return new ImmutableTree(null);
            }
        }
    }
    /**
     * Sets a value at the specified path.
     *
     * @param relativePath - Path to set value at.
     * @param toSet - Value to set.
     * @returns Resulting tree.
     */
    set(relativePath, toSet) {
        if (pathIsEmpty(relativePath)) {
            return new ImmutableTree(toSet, this.children);
        }
        else {
            const front = pathGetFront(relativePath);
            const child = this.children.get(front) || new ImmutableTree(null);
            const newChild = child.set(pathPopFront(relativePath), toSet);
            const newChildren = this.children.insert(front, newChild);
            return new ImmutableTree(this.value, newChildren);
        }
    }
    /**
     * Removes the value at the specified path.
     *
     * @param relativePath - Path to value to remove.
     * @returns Resulting tree.
     */
    remove(relativePath) {
        if (pathIsEmpty(relativePath)) {
            if (this.children.isEmpty()) {
                return new ImmutableTree(null);
            }
            else {
                return new ImmutableTree(null, this.children);
            }
        }
        else {
            const front = pathGetFront(relativePath);
            const child = this.children.get(front);
            if (child) {
                const newChild = child.remove(pathPopFront(relativePath));
                let newChildren;
                if (newChild.isEmpty()) {
                    newChildren = this.children.remove(front);
                }
                else {
                    newChildren = this.children.insert(front, newChild);
                }
                if (this.value === null && newChildren.isEmpty()) {
                    return new ImmutableTree(null);
                }
                else {
                    return new ImmutableTree(this.value, newChildren);
                }
            }
            else {
                return this;
            }
        }
    }
    /**
     * Gets a value from the tree.
     *
     * @param relativePath - Path to get value for.
     * @returns Value at path, or null.
     */
    get(relativePath) {
        if (pathIsEmpty(relativePath)) {
            return this.value;
        }
        else {
            const front = pathGetFront(relativePath);
            const child = this.children.get(front);
            if (child) {
                return child.get(pathPopFront(relativePath));
            }
            else {
                return null;
            }
        }
    }
    /**
     * Replace the subtree at the specified path with the given new tree.
     *
     * @param relativePath - Path to replace subtree for.
     * @param newTree - New tree.
     * @returns Resulting tree.
     */
    setTree(relativePath, newTree) {
        if (pathIsEmpty(relativePath)) {
            return newTree;
        }
        else {
            const front = pathGetFront(relativePath);
            const child = this.children.get(front) || new ImmutableTree(null);
            const newChild = child.setTree(pathPopFront(relativePath), newTree);
            let newChildren;
            if (newChild.isEmpty()) {
                newChildren = this.children.remove(front);
            }
            else {
                newChildren = this.children.insert(front, newChild);
            }
            return new ImmutableTree(this.value, newChildren);
        }
    }
    /**
     * Performs a depth first fold on this tree. Transforms a tree into a single
     * value, given a function that operates on the path to a node, an optional
     * current value, and a map of child names to folded subtrees
     */
    fold(fn) {
        return this.fold_(newEmptyPath(), fn);
    }
    /**
     * Recursive helper for public-facing fold() method
     */
    fold_(pathSoFar, fn) {
        const accum = {};
        this.children.inorderTraversal((childKey, childTree) => {
            accum[childKey] = childTree.fold_(pathChild(pathSoFar, childKey), fn);
        });
        return fn(pathSoFar, this.value, accum);
    }
    /**
     * Find the first matching value on the given path. Return the result of applying f to it.
     */
    findOnPath(path, f) {
        return this.findOnPath_(path, newEmptyPath(), f);
    }
    findOnPath_(pathToFollow, pathSoFar, f) {
        const result = this.value ? f(pathSoFar, this.value) : false;
        if (result) {
            return result;
        }
        else {
            if (pathIsEmpty(pathToFollow)) {
                return null;
            }
            else {
                const front = pathGetFront(pathToFollow);
                const nextChild = this.children.get(front);
                if (nextChild) {
                    return nextChild.findOnPath_(pathPopFront(pathToFollow), pathChild(pathSoFar, front), f);
                }
                else {
                    return null;
                }
            }
        }
    }
    foreachOnPath(path, f) {
        return this.foreachOnPath_(path, newEmptyPath(), f);
    }
    foreachOnPath_(pathToFollow, currentRelativePath, f) {
        if (pathIsEmpty(pathToFollow)) {
            return this;
        }
        else {
            if (this.value) {
                f(currentRelativePath, this.value);
            }
            const front = pathGetFront(pathToFollow);
            const nextChild = this.children.get(front);
            if (nextChild) {
                return nextChild.foreachOnPath_(pathPopFront(pathToFollow), pathChild(currentRelativePath, front), f);
            }
            else {
                return new ImmutableTree(null);
            }
        }
    }
    /**
     * Calls the given function for each node in the tree that has a value.
     *
     * @param f - A function to be called with the path from the root of the tree to
     * a node, and the value at that node. Called in depth-first order.
     */
    foreach(f) {
        this.foreach_(newEmptyPath(), f);
    }
    foreach_(currentRelativePath, f) {
        this.children.inorderTraversal((childName, childTree) => {
            childTree.foreach_(pathChild(currentRelativePath, childName), f);
        });
        if (this.value) {
            f(currentRelativePath, this.value);
        }
    }
    foreachChild(f) {
        this.children.inorderTraversal((childName, childTree) => {
            if (childTree.value) {
                f(childName, childTree.value);
            }
        });
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * This class holds a collection of writes that can be applied to nodes in unison. It abstracts away the logic with
 * dealing with priority writes and multiple nested writes. At any given path there is only allowed to be one write
 * modifying that path. Any write to an existing path or shadowing an existing path will modify that existing write
 * to reflect the write added.
 */
class CompoundWrite {
    constructor(writeTree_) {
        this.writeTree_ = writeTree_;
    }
    static empty() {
        return new CompoundWrite(new ImmutableTree(null));
    }
}
function compoundWriteAddWrite(compoundWrite, path, node) {
    if (pathIsEmpty(path)) {
        return new CompoundWrite(new ImmutableTree(node));
    }
    else {
        const rootmost = compoundWrite.writeTree_.findRootMostValueAndPath(path);
        if (rootmost != null) {
            const rootMostPath = rootmost.path;
            let value = rootmost.value;
            const relativePath = newRelativePath(rootMostPath, path);
            value = value.updateChild(relativePath, node);
            return new CompoundWrite(compoundWrite.writeTree_.set(rootMostPath, value));
        }
        else {
            const subtree = new ImmutableTree(node);
            const newWriteTree = compoundWrite.writeTree_.setTree(path, subtree);
            return new CompoundWrite(newWriteTree);
        }
    }
}
function compoundWriteAddWrites(compoundWrite, path, updates) {
    let newWrite = compoundWrite;
    each(updates, (childKey, node) => {
        newWrite = compoundWriteAddWrite(newWrite, pathChild(path, childKey), node);
    });
    return newWrite;
}
/**
 * Will remove a write at the given path and deeper paths. This will <em>not</em> modify a write at a higher
 * location, which must be removed by calling this method with that path.
 *
 * @param compoundWrite - The CompoundWrite to remove.
 * @param path - The path at which a write and all deeper writes should be removed
 * @returns The new CompoundWrite with the removed path
 */
function compoundWriteRemoveWrite(compoundWrite, path) {
    if (pathIsEmpty(path)) {
        return CompoundWrite.empty();
    }
    else {
        const newWriteTree = compoundWrite.writeTree_.setTree(path, new ImmutableTree(null));
        return new CompoundWrite(newWriteTree);
    }
}
/**
 * Returns whether this CompoundWrite will fully overwrite a node at a given location and can therefore be
 * considered "complete".
 *
 * @param compoundWrite - The CompoundWrite to check.
 * @param path - The path to check for
 * @returns Whether there is a complete write at that path
 */
function compoundWriteHasCompleteWrite(compoundWrite, path) {
    return compoundWriteGetCompleteNode(compoundWrite, path) != null;
}
/**
 * Returns a node for a path if and only if the node is a "complete" overwrite at that path. This will not aggregate
 * writes from deeper paths, but will return child nodes from a more shallow path.
 *
 * @param compoundWrite - The CompoundWrite to get the node from.
 * @param path - The path to get a complete write
 * @returns The node if complete at that path, or null otherwise.
 */
function compoundWriteGetCompleteNode(compoundWrite, path) {
    const rootmost = compoundWrite.writeTree_.findRootMostValueAndPath(path);
    if (rootmost != null) {
        return compoundWrite.writeTree_
            .get(rootmost.path)
            .getChild(newRelativePath(rootmost.path, path));
    }
    else {
        return null;
    }
}
/**
 * Returns all children that are guaranteed to be a complete overwrite.
 *
 * @param compoundWrite - The CompoundWrite to get children from.
 * @returns A list of all complete children.
 */
function compoundWriteGetCompleteChildren(compoundWrite) {
    const children = [];
    const node = compoundWrite.writeTree_.value;
    if (node != null) {
        // If it's a leaf node, it has no children; so nothing to do.
        if (!node.isLeafNode()) {
            node.forEachChild(PRIORITY_INDEX, (childName, childNode) => {
                children.push(new NamedNode(childName, childNode));
            });
        }
    }
    else {
        compoundWrite.writeTree_.children.inorderTraversal((childName, childTree) => {
            if (childTree.value != null) {
                children.push(new NamedNode(childName, childTree.value));
            }
        });
    }
    return children;
}
function compoundWriteChildCompoundWrite(compoundWrite, path) {
    if (pathIsEmpty(path)) {
        return compoundWrite;
    }
    else {
        const shadowingNode = compoundWriteGetCompleteNode(compoundWrite, path);
        if (shadowingNode != null) {
            return new CompoundWrite(new ImmutableTree(shadowingNode));
        }
        else {
            return new CompoundWrite(compoundWrite.writeTree_.subtree(path));
        }
    }
}
/**
 * Returns true if this CompoundWrite is empty and therefore does not modify any nodes.
 * @returns Whether this CompoundWrite is empty
 */
function compoundWriteIsEmpty(compoundWrite) {
    return compoundWrite.writeTree_.isEmpty();
}
/**
 * Applies this CompoundWrite to a node. The node is returned with all writes from this CompoundWrite applied to the
 * node
 * @param node - The node to apply this CompoundWrite to
 * @returns The node with all writes applied
 */
function compoundWriteApply(compoundWrite, node) {
    return applySubtreeWrite(newEmptyPath(), compoundWrite.writeTree_, node);
}
function applySubtreeWrite(relativePath, writeTree, node) {
    if (writeTree.value != null) {
        // Since there a write is always a leaf, we're done here
        return node.updateChild(relativePath, writeTree.value);
    }
    else {
        let priorityWrite = null;
        writeTree.children.inorderTraversal((childKey, childTree) => {
            if (childKey === '.priority') {
                // Apply priorities at the end so we don't update priorities for either empty nodes or forget
                // to apply priorities to empty nodes that are later filled
                util.assert(childTree.value !== null, 'Priority writes must always be leaf nodes');
                priorityWrite = childTree.value;
            }
            else {
                node = applySubtreeWrite(pathChild(relativePath, childKey), childTree, node);
            }
        });
        // If there was a priority write, we only apply it if the node is not empty
        if (!node.getChild(relativePath).isEmpty() && priorityWrite !== null) {
            node = node.updateChild(pathChild(relativePath, '.priority'), priorityWrite);
        }
        return node;
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Create a new WriteTreeRef for the given path. For use with a new sync point at the given path.
 *
 */
function writeTreeChildWrites(writeTree, path) {
    return newWriteTreeRef(path, writeTree);
}
/**
 * Record a new overwrite from user code.
 *
 * @param visible - This is set to false by some transactions. It should be excluded from event caches
 */
function writeTreeAddOverwrite(writeTree, path, snap, writeId, visible) {
    util.assert(writeId > writeTree.lastWriteId, 'Stacking an older write on top of newer ones');
    if (visible === undefined) {
        visible = true;
    }
    writeTree.allWrites.push({
        path,
        snap,
        writeId,
        visible
    });
    if (visible) {
        writeTree.visibleWrites = compoundWriteAddWrite(writeTree.visibleWrites, path, snap);
    }
    writeTree.lastWriteId = writeId;
}
/**
 * Record a new merge from user code.
 */
function writeTreeAddMerge(writeTree, path, changedChildren, writeId) {
    util.assert(writeId > writeTree.lastWriteId, 'Stacking an older merge on top of newer ones');
    writeTree.allWrites.push({
        path,
        children: changedChildren,
        writeId,
        visible: true
    });
    writeTree.visibleWrites = compoundWriteAddWrites(writeTree.visibleWrites, path, changedChildren);
    writeTree.lastWriteId = writeId;
}
function writeTreeGetWrite(writeTree, writeId) {
    for (let i = 0; i < writeTree.allWrites.length; i++) {
        const record = writeTree.allWrites[i];
        if (record.writeId === writeId) {
            return record;
        }
    }
    return null;
}
/**
 * Remove a write (either an overwrite or merge) that has been successfully acknowledge by the server. Recalculates
 * the tree if necessary.  We return true if it may have been visible, meaning views need to reevaluate.
 *
 * @returns true if the write may have been visible (meaning we'll need to reevaluate / raise
 * events as a result).
 */
function writeTreeRemoveWrite(writeTree, writeId) {
    // Note: disabling this check. It could be a transaction that preempted another transaction, and thus was applied
    // out of order.
    //const validClear = revert || this.allWrites_.length === 0 || writeId <= this.allWrites_[0].writeId;
    //assert(validClear, "Either we don't have this write, or it's the first one in the queue");
    const idx = writeTree.allWrites.findIndex(s => {
        return s.writeId === writeId;
    });
    util.assert(idx >= 0, 'removeWrite called with nonexistent writeId.');
    const writeToRemove = writeTree.allWrites[idx];
    writeTree.allWrites.splice(idx, 1);
    let removedWriteWasVisible = writeToRemove.visible;
    let removedWriteOverlapsWithOtherWrites = false;
    let i = writeTree.allWrites.length - 1;
    while (removedWriteWasVisible && i >= 0) {
        const currentWrite = writeTree.allWrites[i];
        if (currentWrite.visible) {
            if (i >= idx &&
                writeTreeRecordContainsPath_(currentWrite, writeToRemove.path)) {
                // The removed write was completely shadowed by a subsequent write.
                removedWriteWasVisible = false;
            }
            else if (pathContains(writeToRemove.path, currentWrite.path)) {
                // Either we're covering some writes or they're covering part of us (depending on which came first).
                removedWriteOverlapsWithOtherWrites = true;
            }
        }
        i--;
    }
    if (!removedWriteWasVisible) {
        return false;
    }
    else if (removedWriteOverlapsWithOtherWrites) {
        // There's some shadowing going on. Just rebuild the visible writes from scratch.
        writeTreeResetTree_(writeTree);
        return true;
    }
    else {
        // There's no shadowing.  We can safely just remove the write(s) from visibleWrites.
        if (writeToRemove.snap) {
            writeTree.visibleWrites = compoundWriteRemoveWrite(writeTree.visibleWrites, writeToRemove.path);
        }
        else {
            const children = writeToRemove.children;
            each(children, (childName) => {
                writeTree.visibleWrites = compoundWriteRemoveWrite(writeTree.visibleWrites, pathChild(writeToRemove.path, childName));
            });
        }
        return true;
    }
}
function writeTreeRecordContainsPath_(writeRecord, path) {
    if (writeRecord.snap) {
        return pathContains(writeRecord.path, path);
    }
    else {
        for (const childName in writeRecord.children) {
            if (writeRecord.children.hasOwnProperty(childName) &&
                pathContains(pathChild(writeRecord.path, childName), path)) {
                return true;
            }
        }
        return false;
    }
}
/**
 * Re-layer the writes and merges into a tree so we can efficiently calculate event snapshots
 */
function writeTreeResetTree_(writeTree) {
    writeTree.visibleWrites = writeTreeLayerTree_(writeTree.allWrites, writeTreeDefaultFilter_, newEmptyPath());
    if (writeTree.allWrites.length > 0) {
        writeTree.lastWriteId =
            writeTree.allWrites[writeTree.allWrites.length - 1].writeId;
    }
    else {
        writeTree.lastWriteId = -1;
    }
}
/**
 * The default filter used when constructing the tree. Keep everything that's visible.
 */
function writeTreeDefaultFilter_(write) {
    return write.visible;
}
/**
 * Static method. Given an array of WriteRecords, a filter for which ones to include, and a path, construct the tree of
 * event data at that path.
 */
function writeTreeLayerTree_(writes, filter, treeRoot) {
    let compoundWrite = CompoundWrite.empty();
    for (let i = 0; i < writes.length; ++i) {
        const write = writes[i];
        // Theory, a later set will either:
        // a) abort a relevant transaction, so no need to worry about excluding it from calculating that transaction
        // b) not be relevant to a transaction (separate branch), so again will not affect the data for that transaction
        if (filter(write)) {
            const writePath = write.path;
            let relativePath;
            if (write.snap) {
                if (pathContains(treeRoot, writePath)) {
                    relativePath = newRelativePath(treeRoot, writePath);
                    compoundWrite = compoundWriteAddWrite(compoundWrite, relativePath, write.snap);
                }
                else if (pathContains(writePath, treeRoot)) {
                    relativePath = newRelativePath(writePath, treeRoot);
                    compoundWrite = compoundWriteAddWrite(compoundWrite, newEmptyPath(), write.snap.getChild(relativePath));
                }
                else ;
            }
            else if (write.children) {
                if (pathContains(treeRoot, writePath)) {
                    relativePath = newRelativePath(treeRoot, writePath);
                    compoundWrite = compoundWriteAddWrites(compoundWrite, relativePath, write.children);
                }
                else if (pathContains(writePath, treeRoot)) {
                    relativePath = newRelativePath(writePath, treeRoot);
                    if (pathIsEmpty(relativePath)) {
                        compoundWrite = compoundWriteAddWrites(compoundWrite, newEmptyPath(), write.children);
                    }
                    else {
                        const child = util.safeGet(write.children, pathGetFront(relativePath));
                        if (child) {
                            // There exists a child in this node that matches the root path
                            const deepNode = child.getChild(pathPopFront(relativePath));
                            compoundWrite = compoundWriteAddWrite(compoundWrite, newEmptyPath(), deepNode);
                        }
                    }
                }
                else ;
            }
            else {
                throw util.assertionError('WriteRecord should have .snap or .children');
            }
        }
    }
    return compoundWrite;
}
/**
 * Given optional, underlying server data, and an optional set of constraints (exclude some sets, include hidden
 * writes), attempt to calculate a complete snapshot for the given path
 *
 * @param writeIdsToExclude - An optional set to be excluded
 * @param includeHiddenWrites - Defaults to false, whether or not to layer on writes with visible set to false
 */
function writeTreeCalcCompleteEventCache(writeTree, treePath, completeServerCache, writeIdsToExclude, includeHiddenWrites) {
    if (!writeIdsToExclude && !includeHiddenWrites) {
        const shadowingNode = compoundWriteGetCompleteNode(writeTree.visibleWrites, treePath);
        if (shadowingNode != null) {
            return shadowingNode;
        }
        else {
            const subMerge = compoundWriteChildCompoundWrite(writeTree.visibleWrites, treePath);
            if (compoundWriteIsEmpty(subMerge)) {
                return completeServerCache;
            }
            else if (completeServerCache == null &&
                !compoundWriteHasCompleteWrite(subMerge, newEmptyPath())) {
                // We wouldn't have a complete snapshot, since there's no underlying data and no complete shadow
                return null;
            }
            else {
                const layeredCache = completeServerCache || ChildrenNode.EMPTY_NODE;
                return compoundWriteApply(subMerge, layeredCache);
            }
        }
    }
    else {
        const merge = compoundWriteChildCompoundWrite(writeTree.visibleWrites, treePath);
        if (!includeHiddenWrites && compoundWriteIsEmpty(merge)) {
            return completeServerCache;
        }
        else {
            // If the server cache is null, and we don't have a complete cache, we need to return null
            if (!includeHiddenWrites &&
                completeServerCache == null &&
                !compoundWriteHasCompleteWrite(merge, newEmptyPath())) {
                return null;
            }
            else {
                const filter = function (write) {
                    return ((write.visible || includeHiddenWrites) &&
                        (!writeIdsToExclude ||
                            !~writeIdsToExclude.indexOf(write.writeId)) &&
                        (pathContains(write.path, treePath) ||
                            pathContains(treePath, write.path)));
                };
                const mergeAtPath = writeTreeLayerTree_(writeTree.allWrites, filter, treePath);
                const layeredCache = completeServerCache || ChildrenNode.EMPTY_NODE;
                return compoundWriteApply(mergeAtPath, layeredCache);
            }
        }
    }
}
/**
 * With optional, underlying server data, attempt to return a children node of children that we have complete data for.
 * Used when creating new views, to pre-fill their complete event children snapshot.
 */
function writeTreeCalcCompleteEventChildren(writeTree, treePath, completeServerChildren) {
    let completeChildren = ChildrenNode.EMPTY_NODE;
    const topLevelSet = compoundWriteGetCompleteNode(writeTree.visibleWrites, treePath);
    if (topLevelSet) {
        if (!topLevelSet.isLeafNode()) {
            // we're shadowing everything. Return the children.
            topLevelSet.forEachChild(PRIORITY_INDEX, (childName, childSnap) => {
                completeChildren = completeChildren.updateImmediateChild(childName, childSnap);
            });
        }
        return completeChildren;
    }
    else if (completeServerChildren) {
        // Layer any children we have on top of this
        // We know we don't have a top-level set, so just enumerate existing children
        const merge = compoundWriteChildCompoundWrite(writeTree.visibleWrites, treePath);
        completeServerChildren.forEachChild(PRIORITY_INDEX, (childName, childNode) => {
            const node = compoundWriteApply(compoundWriteChildCompoundWrite(merge, new Path(childName)), childNode);
            completeChildren = completeChildren.updateImmediateChild(childName, node);
        });
        // Add any complete children we have from the set
        compoundWriteGetCompleteChildren(merge).forEach(namedNode => {
            completeChildren = completeChildren.updateImmediateChild(namedNode.name, namedNode.node);
        });
        return completeChildren;
    }
    else {
        // We don't have anything to layer on top of. Layer on any children we have
        // Note that we can return an empty snap if we have a defined delete
        const merge = compoundWriteChildCompoundWrite(writeTree.visibleWrites, treePath);
        compoundWriteGetCompleteChildren(merge).forEach(namedNode => {
            completeChildren = completeChildren.updateImmediateChild(namedNode.name, namedNode.node);
        });
        return completeChildren;
    }
}
/**
 * Given that the underlying server data has updated, determine what, if anything, needs to be
 * applied to the event cache.
 *
 * Possibilities:
 *
 * 1. No writes are shadowing. Events should be raised, the snap to be applied comes from the server data
 *
 * 2. Some write is completely shadowing. No events to be raised
 *
 * 3. Is partially shadowed. Events
 *
 * Either existingEventSnap or existingServerSnap must exist
 */
function writeTreeCalcEventCacheAfterServerOverwrite(writeTree, treePath, childPath, existingEventSnap, existingServerSnap) {
    util.assert(existingEventSnap || existingServerSnap, 'Either existingEventSnap or existingServerSnap must exist');
    const path = pathChild(treePath, childPath);
    if (compoundWriteHasCompleteWrite(writeTree.visibleWrites, path)) {
        // At this point we can probably guarantee that we're in case 2, meaning no events
        // May need to check visibility while doing the findRootMostValueAndPath call
        return null;
    }
    else {
        // No complete shadowing. We're either partially shadowing or not shadowing at all.
        const childMerge = compoundWriteChildCompoundWrite(writeTree.visibleWrites, path);
        if (compoundWriteIsEmpty(childMerge)) {
            // We're not shadowing at all. Case 1
            return existingServerSnap.getChild(childPath);
        }
        else {
            // This could be more efficient if the serverNode + updates doesn't change the eventSnap
            // However this is tricky to find out, since user updates don't necessary change the server
            // snap, e.g. priority updates on empty nodes, or deep deletes. Another special case is if the server
            // adds nodes, but doesn't change any existing writes. It is therefore not enough to
            // only check if the updates change the serverNode.
            // Maybe check if the merge tree contains these special cases and only do a full overwrite in that case?
            return compoundWriteApply(childMerge, existingServerSnap.getChild(childPath));
        }
    }
}
/**
 * Returns a complete child for a given server snap after applying all user writes or null if there is no
 * complete child for this ChildKey.
 */
function writeTreeCalcCompleteChild(writeTree, treePath, childKey, existingServerSnap) {
    const path = pathChild(treePath, childKey);
    const shadowingNode = compoundWriteGetCompleteNode(writeTree.visibleWrites, path);
    if (shadowingNode != null) {
        return shadowingNode;
    }
    else {
        if (existingServerSnap.isCompleteForChild(childKey)) {
            const childMerge = compoundWriteChildCompoundWrite(writeTree.visibleWrites, path);
            return compoundWriteApply(childMerge, existingServerSnap.getNode().getImmediateChild(childKey));
        }
        else {
            return null;
        }
    }
}
/**
 * Returns a node if there is a complete overwrite for this path. More specifically, if there is a write at
 * a higher path, this will return the child of that write relative to the write and this path.
 * Returns null if there is no write at this path.
 */
function writeTreeShadowingWrite(writeTree, path) {
    return compoundWriteGetCompleteNode(writeTree.visibleWrites, path);
}
/**
 * This method is used when processing child remove events on a query. If we can, we pull in children that were outside
 * the window, but may now be in the window.
 */
function writeTreeCalcIndexedSlice(writeTree, treePath, completeServerData, startPost, count, reverse, index) {
    let toIterate;
    const merge = compoundWriteChildCompoundWrite(writeTree.visibleWrites, treePath);
    const shadowingNode = compoundWriteGetCompleteNode(merge, newEmptyPath());
    if (shadowingNode != null) {
        toIterate = shadowingNode;
    }
    else if (completeServerData != null) {
        toIterate = compoundWriteApply(merge, completeServerData);
    }
    else {
        // no children to iterate on
        return [];
    }
    toIterate = toIterate.withIndex(index);
    if (!toIterate.isEmpty() && !toIterate.isLeafNode()) {
        const nodes = [];
        const cmp = index.getCompare();
        const iter = reverse
            ? toIterate.getReverseIteratorFrom(startPost, index)
            : toIterate.getIteratorFrom(startPost, index);
        let next = iter.getNext();
        while (next && nodes.length < count) {
            if (cmp(next, startPost) !== 0) {
                nodes.push(next);
            }
            next = iter.getNext();
        }
        return nodes;
    }
    else {
        return [];
    }
}
function newWriteTree() {
    return {
        visibleWrites: CompoundWrite.empty(),
        allWrites: [],
        lastWriteId: -1
    };
}
/**
 * If possible, returns a complete event cache, using the underlying server data if possible. In addition, can be used
 * to get a cache that includes hidden writes, and excludes arbitrary writes. Note that customizing the returned node
 * can lead to a more expensive calculation.
 *
 * @param writeIdsToExclude - Optional writes to exclude.
 * @param includeHiddenWrites - Defaults to false, whether or not to layer on writes with visible set to false
 */
function writeTreeRefCalcCompleteEventCache(writeTreeRef, completeServerCache, writeIdsToExclude, includeHiddenWrites) {
    return writeTreeCalcCompleteEventCache(writeTreeRef.writeTree, writeTreeRef.treePath, completeServerCache, writeIdsToExclude, includeHiddenWrites);
}
/**
 * If possible, returns a children node containing all of the complete children we have data for. The returned data is a
 * mix of the given server data and write data.
 *
 */
function writeTreeRefCalcCompleteEventChildren(writeTreeRef, completeServerChildren) {
    return writeTreeCalcCompleteEventChildren(writeTreeRef.writeTree, writeTreeRef.treePath, completeServerChildren);
}
/**
 * Given that either the underlying server data has updated or the outstanding writes have updated, determine what,
 * if anything, needs to be applied to the event cache.
 *
 * Possibilities:
 *
 * 1. No writes are shadowing. Events should be raised, the snap to be applied comes from the server data
 *
 * 2. Some write is completely shadowing. No events to be raised
 *
 * 3. Is partially shadowed. Events should be raised
 *
 * Either existingEventSnap or existingServerSnap must exist, this is validated via an assert
 *
 *
 */
function writeTreeRefCalcEventCacheAfterServerOverwrite(writeTreeRef, path, existingEventSnap, existingServerSnap) {
    return writeTreeCalcEventCacheAfterServerOverwrite(writeTreeRef.writeTree, writeTreeRef.treePath, path, existingEventSnap, existingServerSnap);
}
/**
 * Returns a node if there is a complete overwrite for this path. More specifically, if there is a write at
 * a higher path, this will return the child of that write relative to the write and this path.
 * Returns null if there is no write at this path.
 *
 */
function writeTreeRefShadowingWrite(writeTreeRef, path) {
    return writeTreeShadowingWrite(writeTreeRef.writeTree, pathChild(writeTreeRef.treePath, path));
}
/**
 * This method is used when processing child remove events on a query. If we can, we pull in children that were outside
 * the window, but may now be in the window
 */
function writeTreeRefCalcIndexedSlice(writeTreeRef, completeServerData, startPost, count, reverse, index) {
    return writeTreeCalcIndexedSlice(writeTreeRef.writeTree, writeTreeRef.treePath, completeServerData, startPost, count, reverse, index);
}
/**
 * Returns a complete child for a given server snap after applying all user writes or null if there is no
 * complete child for this ChildKey.
 */
function writeTreeRefCalcCompleteChild(writeTreeRef, childKey, existingServerCache) {
    return writeTreeCalcCompleteChild(writeTreeRef.writeTree, writeTreeRef.treePath, childKey, existingServerCache);
}
/**
 * Return a WriteTreeRef for a child.
 */
function writeTreeRefChild(writeTreeRef, childName) {
    return newWriteTreeRef(pathChild(writeTreeRef.treePath, childName), writeTreeRef.writeTree);
}
function newWriteTreeRef(path, writeTree) {
    return {
        treePath: path,
        writeTree
    };
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class ChildChangeAccumulator {
    constructor() {
        this.changeMap = new Map();
    }
    trackChildChange(change) {
        const type = change.type;
        const childKey = change.childName;
        util.assert(type === "child_added" /* ChangeType.CHILD_ADDED */ ||
            type === "child_changed" /* ChangeType.CHILD_CHANGED */ ||
            type === "child_removed" /* ChangeType.CHILD_REMOVED */, 'Only child changes supported for tracking');
        util.assert(childKey !== '.priority', 'Only non-priority child changes can be tracked.');
        const oldChange = this.changeMap.get(childKey);
        if (oldChange) {
            const oldType = oldChange.type;
            if (type === "child_added" /* ChangeType.CHILD_ADDED */ &&
                oldType === "child_removed" /* ChangeType.CHILD_REMOVED */) {
                this.changeMap.set(childKey, changeChildChanged(childKey, change.snapshotNode, oldChange.snapshotNode));
            }
            else if (type === "child_removed" /* ChangeType.CHILD_REMOVED */ &&
                oldType === "child_added" /* ChangeType.CHILD_ADDED */) {
                this.changeMap.delete(childKey);
            }
            else if (type === "child_removed" /* ChangeType.CHILD_REMOVED */ &&
                oldType === "child_changed" /* ChangeType.CHILD_CHANGED */) {
                this.changeMap.set(childKey, changeChildRemoved(childKey, oldChange.oldSnap));
            }
            else if (type === "child_changed" /* ChangeType.CHILD_CHANGED */ &&
                oldType === "child_added" /* ChangeType.CHILD_ADDED */) {
                this.changeMap.set(childKey, changeChildAdded(childKey, change.snapshotNode));
            }
            else if (type === "child_changed" /* ChangeType.CHILD_CHANGED */ &&
                oldType === "child_changed" /* ChangeType.CHILD_CHANGED */) {
                this.changeMap.set(childKey, changeChildChanged(childKey, change.snapshotNode, oldChange.oldSnap));
            }
            else {
                throw util.assertionError('Illegal combination of changes: ' +
                    change +
                    ' occurred after ' +
                    oldChange);
            }
        }
        else {
            this.changeMap.set(childKey, change);
        }
    }
    getChanges() {
        return Array.from(this.changeMap.values());
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * An implementation of CompleteChildSource that never returns any additional children
 */
// eslint-disable-next-line @typescript-eslint/naming-convention
class NoCompleteChildSource_ {
    getCompleteChild(childKey) {
        return null;
    }
    getChildAfterChild(index, child, reverse) {
        return null;
    }
}
/**
 * Singleton instance.
 */
const NO_COMPLETE_CHILD_SOURCE = new NoCompleteChildSource_();
/**
 * An implementation of CompleteChildSource that uses a WriteTree in addition to any other server data or
 * old event caches available to calculate complete children.
 */
class WriteTreeCompleteChildSource {
    constructor(writes_, viewCache_, optCompleteServerCache_ = null) {
        this.writes_ = writes_;
        this.viewCache_ = viewCache_;
        this.optCompleteServerCache_ = optCompleteServerCache_;
    }
    getCompleteChild(childKey) {
        const node = this.viewCache_.eventCache;
        if (node.isCompleteForChild(childKey)) {
            return node.getNode().getImmediateChild(childKey);
        }
        else {
            const serverNode = this.optCompleteServerCache_ != null
                ? new CacheNode(this.optCompleteServerCache_, true, false)
                : this.viewCache_.serverCache;
            return writeTreeRefCalcCompleteChild(this.writes_, childKey, serverNode);
        }
    }
    getChildAfterChild(index, child, reverse) {
        const completeServerData = this.optCompleteServerCache_ != null
            ? this.optCompleteServerCache_
            : viewCacheGetCompleteServerSnap(this.viewCache_);
        const nodes = writeTreeRefCalcIndexedSlice(this.writes_, completeServerData, child, 1, reverse, index);
        if (nodes.length === 0) {
            return null;
        }
        else {
            return nodes[0];
        }
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
function newViewProcessor(filter) {
    return { filter };
}
function viewProcessorAssertIndexed(viewProcessor, viewCache) {
    util.assert(viewCache.eventCache.getNode().isIndexed(viewProcessor.filter.getIndex()), 'Event snap not indexed');
    util.assert(viewCache.serverCache.getNode().isIndexed(viewProcessor.filter.getIndex()), 'Server snap not indexed');
}
function viewProcessorApplyOperation(viewProcessor, oldViewCache, operation, writesCache, completeCache) {
    const accumulator = new ChildChangeAccumulator();
    let newViewCache, filterServerNode;
    if (operation.type === OperationType.OVERWRITE) {
        const overwrite = operation;
        if (overwrite.source.fromUser) {
            newViewCache = viewProcessorApplyUserOverwrite(viewProcessor, oldViewCache, overwrite.path, overwrite.snap, writesCache, completeCache, accumulator);
        }
        else {
            util.assert(overwrite.source.fromServer, 'Unknown source.');
            // We filter the node if it's a tagged update or the node has been previously filtered  and the
            // update is not at the root in which case it is ok (and necessary) to mark the node unfiltered
            // again
            filterServerNode =
                overwrite.source.tagged ||
                    (oldViewCache.serverCache.isFiltered() && !pathIsEmpty(overwrite.path));
            newViewCache = viewProcessorApplyServerOverwrite(viewProcessor, oldViewCache, overwrite.path, overwrite.snap, writesCache, completeCache, filterServerNode, accumulator);
        }
    }
    else if (operation.type === OperationType.MERGE) {
        const merge = operation;
        if (merge.source.fromUser) {
            newViewCache = viewProcessorApplyUserMerge(viewProcessor, oldViewCache, merge.path, merge.children, writesCache, completeCache, accumulator);
        }
        else {
            util.assert(merge.source.fromServer, 'Unknown source.');
            // We filter the node if it's a tagged update or the node has been previously filtered
            filterServerNode =
                merge.source.tagged || oldViewCache.serverCache.isFiltered();
            newViewCache = viewProcessorApplyServerMerge(viewProcessor, oldViewCache, merge.path, merge.children, writesCache, completeCache, filterServerNode, accumulator);
        }
    }
    else if (operation.type === OperationType.ACK_USER_WRITE) {
        const ackUserWrite = operation;
        if (!ackUserWrite.revert) {
            newViewCache = viewProcessorAckUserWrite(viewProcessor, oldViewCache, ackUserWrite.path, ackUserWrite.affectedTree, writesCache, completeCache, accumulator);
        }
        else {
            newViewCache = viewProcessorRevertUserWrite(viewProcessor, oldViewCache, ackUserWrite.path, writesCache, completeCache, accumulator);
        }
    }
    else if (operation.type === OperationType.LISTEN_COMPLETE) {
        newViewCache = viewProcessorListenComplete(viewProcessor, oldViewCache, operation.path, writesCache, accumulator);
    }
    else {
        throw util.assertionError('Unknown operation type: ' + operation.type);
    }
    const changes = accumulator.getChanges();
    viewProcessorMaybeAddValueEvent(oldViewCache, newViewCache, changes);
    return { viewCache: newViewCache, changes };
}
function viewProcessorMaybeAddValueEvent(oldViewCache, newViewCache, accumulator) {
    const eventSnap = newViewCache.eventCache;
    if (eventSnap.isFullyInitialized()) {
        const isLeafOrEmpty = eventSnap.getNode().isLeafNode() || eventSnap.getNode().isEmpty();
        const oldCompleteSnap = viewCacheGetCompleteEventSnap(oldViewCache);
        if (accumulator.length > 0 ||
            !oldViewCache.eventCache.isFullyInitialized() ||
            (isLeafOrEmpty && !eventSnap.getNode().equals(oldCompleteSnap)) ||
            !eventSnap.getNode().getPriority().equals(oldCompleteSnap.getPriority())) {
            accumulator.push(changeValue(viewCacheGetCompleteEventSnap(newViewCache)));
        }
    }
}
function viewProcessorGenerateEventCacheAfterServerEvent(viewProcessor, viewCache, changePath, writesCache, source, accumulator) {
    const oldEventSnap = viewCache.eventCache;
    if (writeTreeRefShadowingWrite(writesCache, changePath) != null) {
        // we have a shadowing write, ignore changes
        return viewCache;
    }
    else {
        let newEventCache, serverNode;
        if (pathIsEmpty(changePath)) {
            // TODO: figure out how this plays with "sliding ack windows"
            util.assert(viewCache.serverCache.isFullyInitialized(), 'If change path is empty, we must have complete server data');
            if (viewCache.serverCache.isFiltered()) {
                // We need to special case this, because we need to only apply writes to complete children, or
                // we might end up raising events for incomplete children. If the server data is filtered deep
                // writes cannot be guaranteed to be complete
                const serverCache = viewCacheGetCompleteServerSnap(viewCache);
                const completeChildren = serverCache instanceof ChildrenNode
                    ? serverCache
                    : ChildrenNode.EMPTY_NODE;
                const completeEventChildren = writeTreeRefCalcCompleteEventChildren(writesCache, completeChildren);
                newEventCache = viewProcessor.filter.updateFullNode(viewCache.eventCache.getNode(), completeEventChildren, accumulator);
            }
            else {
                const completeNode = writeTreeRefCalcCompleteEventCache(writesCache, viewCacheGetCompleteServerSnap(viewCache));
                newEventCache = viewProcessor.filter.updateFullNode(viewCache.eventCache.getNode(), completeNode, accumulator);
            }
        }
        else {
            const childKey = pathGetFront(changePath);
            if (childKey === '.priority') {
                util.assert(pathGetLength(changePath) === 1, "Can't have a priority with additional path components");
                const oldEventNode = oldEventSnap.getNode();
                serverNode = viewCache.serverCache.getNode();
                // we might have overwrites for this priority
                const updatedPriority = writeTreeRefCalcEventCacheAfterServerOverwrite(writesCache, changePath, oldEventNode, serverNode);
                if (updatedPriority != null) {
                    newEventCache = viewProcessor.filter.updatePriority(oldEventNode, updatedPriority);
                }
                else {
                    // priority didn't change, keep old node
                    newEventCache = oldEventSnap.getNode();
                }
            }
            else {
                const childChangePath = pathPopFront(changePath);
                // update child
                let newEventChild;
                if (oldEventSnap.isCompleteForChild(childKey)) {
                    serverNode = viewCache.serverCache.getNode();
                    const eventChildUpdate = writeTreeRefCalcEventCacheAfterServerOverwrite(writesCache, changePath, oldEventSnap.getNode(), serverNode);
                    if (eventChildUpdate != null) {
                        newEventChild = oldEventSnap
                            .getNode()
                            .getImmediateChild(childKey)
                            .updateChild(childChangePath, eventChildUpdate);
                    }
                    else {
                        // Nothing changed, just keep the old child
                        newEventChild = oldEventSnap.getNode().getImmediateChild(childKey);
                    }
                }
                else {
                    newEventChild = writeTreeRefCalcCompleteChild(writesCache, childKey, viewCache.serverCache);
                }
                if (newEventChild != null) {
                    newEventCache = viewProcessor.filter.updateChild(oldEventSnap.getNode(), childKey, newEventChild, childChangePath, source, accumulator);
                }
                else {
                    // no complete child available or no change
                    newEventCache = oldEventSnap.getNode();
                }
            }
        }
        return viewCacheUpdateEventSnap(viewCache, newEventCache, oldEventSnap.isFullyInitialized() || pathIsEmpty(changePath), viewProcessor.filter.filtersNodes());
    }
}
function viewProcessorApplyServerOverwrite(viewProcessor, oldViewCache, changePath, changedSnap, writesCache, completeCache, filterServerNode, accumulator) {
    const oldServerSnap = oldViewCache.serverCache;
    let newServerCache;
    const serverFilter = filterServerNode
        ? viewProcessor.filter
        : viewProcessor.filter.getIndexedFilter();
    if (pathIsEmpty(changePath)) {
        newServerCache = serverFilter.updateFullNode(oldServerSnap.getNode(), changedSnap, null);
    }
    else if (serverFilter.filtersNodes() && !oldServerSnap.isFiltered()) {
        // we want to filter the server node, but we didn't filter the server node yet, so simulate a full update
        const newServerNode = oldServerSnap
            .getNode()
            .updateChild(changePath, changedSnap);
        newServerCache = serverFilter.updateFullNode(oldServerSnap.getNode(), newServerNode, null);
    }
    else {
        const childKey = pathGetFront(changePath);
        if (!oldServerSnap.isCompleteForPath(changePath) &&
            pathGetLength(changePath) > 1) {
            // We don't update incomplete nodes with updates intended for other listeners
            return oldViewCache;
        }
        const childChangePath = pathPopFront(changePath);
        const childNode = oldServerSnap.getNode().getImmediateChild(childKey);
        const newChildNode = childNode.updateChild(childChangePath, changedSnap);
        if (childKey === '.priority') {
            newServerCache = serverFilter.updatePriority(oldServerSnap.getNode(), newChildNode);
        }
        else {
            newServerCache = serverFilter.updateChild(oldServerSnap.getNode(), childKey, newChildNode, childChangePath, NO_COMPLETE_CHILD_SOURCE, null);
        }
    }
    const newViewCache = viewCacheUpdateServerSnap(oldViewCache, newServerCache, oldServerSnap.isFullyInitialized() || pathIsEmpty(changePath), serverFilter.filtersNodes());
    const source = new WriteTreeCompleteChildSource(writesCache, newViewCache, completeCache);
    return viewProcessorGenerateEventCacheAfterServerEvent(viewProcessor, newViewCache, changePath, writesCache, source, accumulator);
}
function viewProcessorApplyUserOverwrite(viewProcessor, oldViewCache, changePath, changedSnap, writesCache, completeCache, accumulator) {
    const oldEventSnap = oldViewCache.eventCache;
    let newViewCache, newEventCache;
    const source = new WriteTreeCompleteChildSource(writesCache, oldViewCache, completeCache);
    if (pathIsEmpty(changePath)) {
        newEventCache = viewProcessor.filter.updateFullNode(oldViewCache.eventCache.getNode(), changedSnap, accumulator);
        newViewCache = viewCacheUpdateEventSnap(oldViewCache, newEventCache, true, viewProcessor.filter.filtersNodes());
    }
    else {
        const childKey = pathGetFront(changePath);
        if (childKey === '.priority') {
            newEventCache = viewProcessor.filter.updatePriority(oldViewCache.eventCache.getNode(), changedSnap);
            newViewCache = viewCacheUpdateEventSnap(oldViewCache, newEventCache, oldEventSnap.isFullyInitialized(), oldEventSnap.isFiltered());
        }
        else {
            const childChangePath = pathPopFront(changePath);
            const oldChild = oldEventSnap.getNode().getImmediateChild(childKey);
            let newChild;
            if (pathIsEmpty(childChangePath)) {
                // Child overwrite, we can replace the child
                newChild = changedSnap;
            }
            else {
                const childNode = source.getCompleteChild(childKey);
                if (childNode != null) {
                    if (pathGetBack(childChangePath) === '.priority' &&
                        childNode.getChild(pathParent(childChangePath)).isEmpty()) {
                        // This is a priority update on an empty node. If this node exists on the server, the
                        // server will send down the priority in the update, so ignore for now
                        newChild = childNode;
                    }
                    else {
                        newChild = childNode.updateChild(childChangePath, changedSnap);
                    }
                }
                else {
                    // There is no complete child node available
                    newChild = ChildrenNode.EMPTY_NODE;
                }
            }
            if (!oldChild.equals(newChild)) {
                const newEventSnap = viewProcessor.filter.updateChild(oldEventSnap.getNode(), childKey, newChild, childChangePath, source, accumulator);
                newViewCache = viewCacheUpdateEventSnap(oldViewCache, newEventSnap, oldEventSnap.isFullyInitialized(), viewProcessor.filter.filtersNodes());
            }
            else {
                newViewCache = oldViewCache;
            }
        }
    }
    return newViewCache;
}
function viewProcessorCacheHasChild(viewCache, childKey) {
    return viewCache.eventCache.isCompleteForChild(childKey);
}
function viewProcessorApplyUserMerge(viewProcessor, viewCache, path, changedChildren, writesCache, serverCache, accumulator) {
    // HACK: In the case of a limit query, there may be some changes that bump things out of the
    // window leaving room for new items.  It's important we process these changes first, so we
    // iterate the changes twice, first processing any that affect items currently in view.
    // TODO: I consider an item "in view" if cacheHasChild is true, which checks both the server
    // and event snap.  I'm not sure if this will result in edge cases when a child is in one but
    // not the other.
    let curViewCache = viewCache;
    changedChildren.foreach((relativePath, childNode) => {
        const writePath = pathChild(path, relativePath);
        if (viewProcessorCacheHasChild(viewCache, pathGetFront(writePath))) {
            curViewCache = viewProcessorApplyUserOverwrite(viewProcessor, curViewCache, writePath, childNode, writesCache, serverCache, accumulator);
        }
    });
    changedChildren.foreach((relativePath, childNode) => {
        const writePath = pathChild(path, relativePath);
        if (!viewProcessorCacheHasChild(viewCache, pathGetFront(writePath))) {
            curViewCache = viewProcessorApplyUserOverwrite(viewProcessor, curViewCache, writePath, childNode, writesCache, serverCache, accumulator);
        }
    });
    return curViewCache;
}
function viewProcessorApplyMerge(viewProcessor, node, merge) {
    merge.foreach((relativePath, childNode) => {
        node = node.updateChild(relativePath, childNode);
    });
    return node;
}
function viewProcessorApplyServerMerge(viewProcessor, viewCache, path, changedChildren, writesCache, serverCache, filterServerNode, accumulator) {
    // If we don't have a cache yet, this merge was intended for a previously listen in the same location. Ignore it and
    // wait for the complete data update coming soon.
    if (viewCache.serverCache.getNode().isEmpty() &&
        !viewCache.serverCache.isFullyInitialized()) {
        return viewCache;
    }
    // HACK: In the case of a limit query, there may be some changes that bump things out of the
    // window leaving room for new items.  It's important we process these changes first, so we
    // iterate the changes twice, first processing any that affect items currently in view.
    // TODO: I consider an item "in view" if cacheHasChild is true, which checks both the server
    // and event snap.  I'm not sure if this will result in edge cases when a child is in one but
    // not the other.
    let curViewCache = viewCache;
    let viewMergeTree;
    if (pathIsEmpty(path)) {
        viewMergeTree = changedChildren;
    }
    else {
        viewMergeTree = new ImmutableTree(null).setTree(path, changedChildren);
    }
    const serverNode = viewCache.serverCache.getNode();
    viewMergeTree.children.inorderTraversal((childKey, childTree) => {
        if (serverNode.hasChild(childKey)) {
            const serverChild = viewCache.serverCache
                .getNode()
                .getImmediateChild(childKey);
            const newChild = viewProcessorApplyMerge(viewProcessor, serverChild, childTree);
            curViewCache = viewProcessorApplyServerOverwrite(viewProcessor, curViewCache, new Path(childKey), newChild, writesCache, serverCache, filterServerNode, accumulator);
        }
    });
    viewMergeTree.children.inorderTraversal((childKey, childMergeTree) => {
        const isUnknownDeepMerge = !viewCache.serverCache.isCompleteForChild(childKey) &&
            childMergeTree.value === null;
        if (!serverNode.hasChild(childKey) && !isUnknownDeepMerge) {
            const serverChild = viewCache.serverCache
                .getNode()
                .getImmediateChild(childKey);
            const newChild = viewProcessorApplyMerge(viewProcessor, serverChild, childMergeTree);
            curViewCache = viewProcessorApplyServerOverwrite(viewProcessor, curViewCache, new Path(childKey), newChild, writesCache, serverCache, filterServerNode, accumulator);
        }
    });
    return curViewCache;
}
function viewProcessorAckUserWrite(viewProcessor, viewCache, ackPath, affectedTree, writesCache, completeCache, accumulator) {
    if (writeTreeRefShadowingWrite(writesCache, ackPath) != null) {
        return viewCache;
    }
    // Only filter server node if it is currently filtered
    const filterServerNode = viewCache.serverCache.isFiltered();
    // Essentially we'll just get our existing server cache for the affected paths and re-apply it as a server update
    // now that it won't be shadowed.
    const serverCache = viewCache.serverCache;
    if (affectedTree.value != null) {
        // This is an overwrite.
        if ((pathIsEmpty(ackPath) && serverCache.isFullyInitialized()) ||
            serverCache.isCompleteForPath(ackPath)) {
            return viewProcessorApplyServerOverwrite(viewProcessor, viewCache, ackPath, serverCache.getNode().getChild(ackPath), writesCache, completeCache, filterServerNode, accumulator);
        }
        else if (pathIsEmpty(ackPath)) {
            // This is a goofy edge case where we are acking data at this location but don't have full data.  We
            // should just re-apply whatever we have in our cache as a merge.
            let changedChildren = new ImmutableTree(null);
            serverCache.getNode().forEachChild(KEY_INDEX, (name, node) => {
                changedChildren = changedChildren.set(new Path(name), node);
            });
            return viewProcessorApplyServerMerge(viewProcessor, viewCache, ackPath, changedChildren, writesCache, completeCache, filterServerNode, accumulator);
        }
        else {
            return viewCache;
        }
    }
    else {
        // This is a merge.
        let changedChildren = new ImmutableTree(null);
        affectedTree.foreach((mergePath, value) => {
            const serverCachePath = pathChild(ackPath, mergePath);
            if (serverCache.isCompleteForPath(serverCachePath)) {
                changedChildren = changedChildren.set(mergePath, serverCache.getNode().getChild(serverCachePath));
            }
        });
        return viewProcessorApplyServerMerge(viewProcessor, viewCache, ackPath, changedChildren, writesCache, completeCache, filterServerNode, accumulator);
    }
}
function viewProcessorListenComplete(viewProcessor, viewCache, path, writesCache, accumulator) {
    const oldServerNode = viewCache.serverCache;
    const newViewCache = viewCacheUpdateServerSnap(viewCache, oldServerNode.getNode(), oldServerNode.isFullyInitialized() || pathIsEmpty(path), oldServerNode.isFiltered());
    return viewProcessorGenerateEventCacheAfterServerEvent(viewProcessor, newViewCache, path, writesCache, NO_COMPLETE_CHILD_SOURCE, accumulator);
}
function viewProcessorRevertUserWrite(viewProcessor, viewCache, path, writesCache, completeServerCache, accumulator) {
    let complete;
    if (writeTreeRefShadowingWrite(writesCache, path) != null) {
        return viewCache;
    }
    else {
        const source = new WriteTreeCompleteChildSource(writesCache, viewCache, completeServerCache);
        const oldEventCache = viewCache.eventCache.getNode();
        let newEventCache;
        if (pathIsEmpty(path) || pathGetFront(path) === '.priority') {
            let newNode;
            if (viewCache.serverCache.isFullyInitialized()) {
                newNode = writeTreeRefCalcCompleteEventCache(writesCache, viewCacheGetCompleteServerSnap(viewCache));
            }
            else {
                const serverChildren = viewCache.serverCache.getNode();
                util.assert(serverChildren instanceof ChildrenNode, 'serverChildren would be complete if leaf node');
                newNode = writeTreeRefCalcCompleteEventChildren(writesCache, serverChildren);
            }
            newNode = newNode;
            newEventCache = viewProcessor.filter.updateFullNode(oldEventCache, newNode, accumulator);
        }
        else {
            const childKey = pathGetFront(path);
            let newChild = writeTreeRefCalcCompleteChild(writesCache, childKey, viewCache.serverCache);
            if (newChild == null &&
                viewCache.serverCache.isCompleteForChild(childKey)) {
                newChild = oldEventCache.getImmediateChild(childKey);
            }
            if (newChild != null) {
                newEventCache = viewProcessor.filter.updateChild(oldEventCache, childKey, newChild, pathPopFront(path), source, accumulator);
            }
            else if (viewCache.eventCache.getNode().hasChild(childKey)) {
                // No complete child available, delete the existing one, if any
                newEventCache = viewProcessor.filter.updateChild(oldEventCache, childKey, ChildrenNode.EMPTY_NODE, pathPopFront(path), source, accumulator);
            }
            else {
                newEventCache = oldEventCache;
            }
            if (newEventCache.isEmpty() &&
                viewCache.serverCache.isFullyInitialized()) {
                // We might have reverted all child writes. Maybe the old event was a leaf node
                complete = writeTreeRefCalcCompleteEventCache(writesCache, viewCacheGetCompleteServerSnap(viewCache));
                if (complete.isLeafNode()) {
                    newEventCache = viewProcessor.filter.updateFullNode(newEventCache, complete, accumulator);
                }
            }
        }
        complete =
            viewCache.serverCache.isFullyInitialized() ||
                writeTreeRefShadowingWrite(writesCache, newEmptyPath()) != null;
        return viewCacheUpdateEventSnap(viewCache, newEventCache, complete, viewProcessor.filter.filtersNodes());
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * A view represents a specific location and query that has 1 or more event registrations.
 *
 * It does several things:
 *  - Maintains the list of event registrations for this location/query.
 *  - Maintains a cache of the data visible for this location/query.
 *  - Applies new operations (via applyOperation), updates the cache, and based on the event
 *    registrations returns the set of events to be raised.
 */
class View {
    constructor(query_, initialViewCache) {
        this.query_ = query_;
        this.eventRegistrations_ = [];
        const params = this.query_._queryParams;
        const indexFilter = new IndexedFilter(params.getIndex());
        const filter = queryParamsGetNodeFilter(params);
        this.processor_ = newViewProcessor(filter);
        const initialServerCache = initialViewCache.serverCache;
        const initialEventCache = initialViewCache.eventCache;
        // Don't filter server node with other filter than index, wait for tagged listen
        const serverSnap = indexFilter.updateFullNode(ChildrenNode.EMPTY_NODE, initialServerCache.getNode(), null);
        const eventSnap = filter.updateFullNode(ChildrenNode.EMPTY_NODE, initialEventCache.getNode(), null);
        const newServerCache = new CacheNode(serverSnap, initialServerCache.isFullyInitialized(), indexFilter.filtersNodes());
        const newEventCache = new CacheNode(eventSnap, initialEventCache.isFullyInitialized(), filter.filtersNodes());
        this.viewCache_ = newViewCache(newEventCache, newServerCache);
        this.eventGenerator_ = new EventGenerator(this.query_);
    }
    get query() {
        return this.query_;
    }
}
function viewGetServerCache(view) {
    return view.viewCache_.serverCache.getNode();
}
function viewGetCompleteNode(view) {
    return viewCacheGetCompleteEventSnap(view.viewCache_);
}
function viewGetCompleteServerCache(view, path) {
    const cache = viewCacheGetCompleteServerSnap(view.viewCache_);
    if (cache) {
        // If this isn't a "loadsAllData" view, then cache isn't actually a complete cache and
        // we need to see if it contains the child we're interested in.
        if (view.query._queryParams.loadsAllData() ||
            (!pathIsEmpty(path) &&
                !cache.getImmediateChild(pathGetFront(path)).isEmpty())) {
            return cache.getChild(path);
        }
    }
    return null;
}
function viewIsEmpty(view) {
    return view.eventRegistrations_.length === 0;
}
function viewAddEventRegistration(view, eventRegistration) {
    view.eventRegistrations_.push(eventRegistration);
}
/**
 * @param eventRegistration - If null, remove all callbacks.
 * @param cancelError - If a cancelError is provided, appropriate cancel events will be returned.
 * @returns Cancel events, if cancelError was provided.
 */
function viewRemoveEventRegistration(view, eventRegistration, cancelError) {
    const cancelEvents = [];
    if (cancelError) {
        util.assert(eventRegistration == null, 'A cancel should cancel all event registrations.');
        const path = view.query._path;
        view.eventRegistrations_.forEach(registration => {
            const maybeEvent = registration.createCancelEvent(cancelError, path);
            if (maybeEvent) {
                cancelEvents.push(maybeEvent);
            }
        });
    }
    if (eventRegistration) {
        let remaining = [];
        for (let i = 0; i < view.eventRegistrations_.length; ++i) {
            const existing = view.eventRegistrations_[i];
            if (!existing.matches(eventRegistration)) {
                remaining.push(existing);
            }
            else {
                existing.onRemove?.();
                if (eventRegistration.hasAnyCallback()) {
                    // We're removing just this one
                    remaining = remaining.concat(view.eventRegistrations_.slice(i + 1));
                    break;
                }
            }
        }
        view.eventRegistrations_ = remaining;
    }
    else {
        for (const existing of view.eventRegistrations_) {
            existing.onRemove?.();
        }
        view.eventRegistrations_ = [];
    }
    return cancelEvents;
}
/**
 * Applies the given Operation, updates our cache, and returns the appropriate events.
 */
function viewApplyOperation(view, operation, writesCache, completeServerCache) {
    if (operation.type === OperationType.MERGE &&
        operation.source.queryId !== null) {
        util.assert(viewCacheGetCompleteServerSnap(view.viewCache_), 'We should always have a full cache before handling merges');
        util.assert(viewCacheGetCompleteEventSnap(view.viewCache_), 'Missing event cache, even though we have a server cache');
    }
    const oldViewCache = view.viewCache_;
    const result = viewProcessorApplyOperation(view.processor_, oldViewCache, operation, writesCache, completeServerCache);
    viewProcessorAssertIndexed(view.processor_, result.viewCache);
    util.assert(result.viewCache.serverCache.isFullyInitialized() ||
        !oldViewCache.serverCache.isFullyInitialized(), 'Once a server snap is complete, it should never go back');
    view.viewCache_ = result.viewCache;
    return viewGenerateEventsForChanges_(view, result.changes, result.viewCache.eventCache.getNode(), null);
}
function viewGetInitialEvents(view, registration) {
    const eventSnap = view.viewCache_.eventCache;
    const initialChanges = [];
    if (!eventSnap.getNode().isLeafNode()) {
        const eventNode = eventSnap.getNode();
        eventNode.forEachChild(PRIORITY_INDEX, (key, childNode) => {
            initialChanges.push(changeChildAdded(key, childNode));
        });
    }
    if (eventSnap.isFullyInitialized()) {
        initialChanges.push(changeValue(eventSnap.getNode()));
    }
    return viewGenerateEventsForChanges_(view, initialChanges, eventSnap.getNode(), registration);
}
function viewGenerateEventsForChanges_(view, changes, eventCache, eventRegistration) {
    const registrations = eventRegistration
        ? [eventRegistration]
        : view.eventRegistrations_;
    return eventGeneratorGenerateEventsForChanges(view.eventGenerator_, changes, eventCache, registrations);
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
let referenceConstructor$1;
/**
 * SyncPoint represents a single location in a SyncTree with 1 or more event registrations, meaning we need to
 * maintain 1 or more Views at this location to cache server data and raise appropriate events for server changes
 * and user writes (set, transaction, update).
 *
 * It's responsible for:
 *  - Maintaining the set of 1 or more views necessary at this location (a SyncPoint with 0 views should be removed).
 *  - Proxying user / server operations to the views as appropriate (i.e. applyServerOverwrite,
 *    applyUserOverwrite, etc.)
 */
class SyncPoint {
    constructor() {
        /**
         * The Views being tracked at this location in the tree, stored as a map where the key is a
         * queryId and the value is the View for that query.
         *
         * NOTE: This list will be quite small (usually 1, but perhaps 2 or 3; any more is an odd use case).
         */
        this.views = new Map();
    }
}
function syncPointSetReferenceConstructor(val) {
    util.assert(!referenceConstructor$1, '__referenceConstructor has already been defined');
    referenceConstructor$1 = val;
}
function syncPointGetReferenceConstructor() {
    util.assert(referenceConstructor$1, 'Reference.ts has not been loaded');
    return referenceConstructor$1;
}
function syncPointIsEmpty(syncPoint) {
    return syncPoint.views.size === 0;
}
function syncPointApplyOperation(syncPoint, operation, writesCache, optCompleteServerCache) {
    const queryId = operation.source.queryId;
    if (queryId !== null) {
        const view = syncPoint.views.get(queryId);
        util.assert(view != null, 'SyncTree gave us an op for an invalid query.');
        return viewApplyOperation(view, operation, writesCache, optCompleteServerCache);
    }
    else {
        let events = [];
        for (const view of syncPoint.views.values()) {
            events = events.concat(viewApplyOperation(view, operation, writesCache, optCompleteServerCache));
        }
        return events;
    }
}
/**
 * Get a view for the specified query.
 *
 * @param query - The query to return a view for
 * @param writesCache
 * @param serverCache
 * @param serverCacheComplete
 * @returns Events to raise.
 */
function syncPointGetView(syncPoint, query, writesCache, serverCache, serverCacheComplete) {
    const queryId = query._queryIdentifier;
    const view = syncPoint.views.get(queryId);
    if (!view) {
        // TODO: make writesCache take flag for complete server node
        let eventCache = writeTreeRefCalcCompleteEventCache(writesCache, serverCacheComplete ? serverCache : null);
        let eventCacheComplete = false;
        if (eventCache) {
            eventCacheComplete = true;
        }
        else if (serverCache instanceof ChildrenNode) {
            eventCache = writeTreeRefCalcCompleteEventChildren(writesCache, serverCache);
            eventCacheComplete = false;
        }
        else {
            eventCache = ChildrenNode.EMPTY_NODE;
            eventCacheComplete = false;
        }
        const viewCache = newViewCache(new CacheNode(eventCache, eventCacheComplete, false), new CacheNode(serverCache, serverCacheComplete, false));
        return new View(query, viewCache);
    }
    return view;
}
/**
 * Add an event callback for the specified query.
 *
 * @param query
 * @param eventRegistration
 * @param writesCache
 * @param serverCache - Complete server cache, if we have it.
 * @param serverCacheComplete
 * @returns Events to raise.
 */
function syncPointAddEventRegistration(syncPoint, query, eventRegistration, writesCache, serverCache, serverCacheComplete) {
    const view = syncPointGetView(syncPoint, query, writesCache, serverCache, serverCacheComplete);
    if (!syncPoint.views.has(query._queryIdentifier)) {
        syncPoint.views.set(query._queryIdentifier, view);
    }
    // This is guaranteed to exist now, we just created anything that was missing
    viewAddEventRegistration(view, eventRegistration);
    return viewGetInitialEvents(view, eventRegistration);
}
/**
 * Remove event callback(s).  Return cancelEvents if a cancelError is specified.
 *
 * If query is the default query, we'll check all views for the specified eventRegistration.
 * If eventRegistration is null, we'll remove all callbacks for the specified view(s).
 *
 * @param eventRegistration - If null, remove all callbacks.
 * @param cancelError - If a cancelError is provided, appropriate cancel events will be returned.
 * @returns removed queries and any cancel events
 */
function syncPointRemoveEventRegistration(syncPoint, query, eventRegistration, cancelError) {
    const queryId = query._queryIdentifier;
    const removed = [];
    let cancelEvents = [];
    const hadCompleteView = syncPointHasCompleteView(syncPoint);
    if (queryId === 'default') {
        // When you do ref.off(...), we search all views for the registration to remove.
        for (const [viewQueryId, view] of syncPoint.views.entries()) {
            cancelEvents = cancelEvents.concat(viewRemoveEventRegistration(view, eventRegistration, cancelError));
            if (viewIsEmpty(view)) {
                syncPoint.views.delete(viewQueryId);
                // We'll deal with complete views later.
                if (!view.query._queryParams.loadsAllData()) {
                    removed.push(view.query);
                }
            }
        }
    }
    else {
        // remove the callback from the specific view.
        const view = syncPoint.views.get(queryId);
        if (view) {
            cancelEvents = cancelEvents.concat(viewRemoveEventRegistration(view, eventRegistration, cancelError));
            if (viewIsEmpty(view)) {
                syncPoint.views.delete(queryId);
                // We'll deal with complete views later.
                if (!view.query._queryParams.loadsAllData()) {
                    removed.push(view.query);
                }
            }
        }
    }
    if (hadCompleteView && !syncPointHasCompleteView(syncPoint)) {
        // We removed our last complete view.
        removed.push(new (syncPointGetReferenceConstructor())(query._repo, query._path));
    }
    return { removed, events: cancelEvents };
}
function syncPointGetQueryViews(syncPoint) {
    const result = [];
    for (const view of syncPoint.views.values()) {
        if (!view.query._queryParams.loadsAllData()) {
            result.push(view);
        }
    }
    return result;
}
/**
 * @param path - The path to the desired complete snapshot
 * @returns A complete cache, if it exists
 */
function syncPointGetCompleteServerCache(syncPoint, path) {
    let serverCache = null;
    for (const view of syncPoint.views.values()) {
        serverCache = serverCache || viewGetCompleteServerCache(view, path);
    }
    return serverCache;
}
function syncPointViewForQuery(syncPoint, query) {
    const params = query._queryParams;
    if (params.loadsAllData()) {
        return syncPointGetCompleteView(syncPoint);
    }
    else {
        const queryId = query._queryIdentifier;
        return syncPoint.views.get(queryId);
    }
}
function syncPointViewExistsForQuery(syncPoint, query) {
    return syncPointViewForQuery(syncPoint, query) != null;
}
function syncPointHasCompleteView(syncPoint) {
    return syncPointGetCompleteView(syncPoint) != null;
}
function syncPointGetCompleteView(syncPoint) {
    for (const view of syncPoint.views.values()) {
        if (view.query._queryParams.loadsAllData()) {
            return view;
        }
    }
    return null;
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
let referenceConstructor;
function syncTreeSetReferenceConstructor(val) {
    util.assert(!referenceConstructor, '__referenceConstructor has already been defined');
    referenceConstructor = val;
}
function syncTreeGetReferenceConstructor() {
    util.assert(referenceConstructor, 'Reference.ts has not been loaded');
    return referenceConstructor;
}
/**
 * Static tracker for next query tag.
 */
let syncTreeNextQueryTag_ = 1;
/**
 * SyncTree is the central class for managing event callback registration, data caching, views
 * (query processing), and event generation.  There are typically two SyncTree instances for
 * each Repo, one for the normal Firebase data, and one for the .info data.
 *
 * It has a number of responsibilities, including:
 *  - Tracking all user event callbacks (registered via addEventRegistration() and removeEventRegistration()).
 *  - Applying and caching data changes for user set(), transaction(), and update() calls
 *    (applyUserOverwrite(), applyUserMerge()).
 *  - Applying and caching data changes for server data changes (applyServerOverwrite(),
 *    applyServerMerge()).
 *  - Generating user-facing events for server and user changes (all of the apply* methods
 *    return the set of events that need to be raised as a result).
 *  - Maintaining the appropriate set of server listens to ensure we are always subscribed
 *    to the correct set of paths and queries to satisfy the current set of user event
 *    callbacks (listens are started/stopped using the provided listenProvider).
 *
 * NOTE: Although SyncTree tracks event callbacks and calculates events to raise, the actual
 * events are returned to the caller rather than raised synchronously.
 *
 */
class SyncTree {
    /**
     * @param listenProvider_ - Used by SyncTree to start / stop listening
     *   to server data.
     */
    constructor(listenProvider_) {
        this.listenProvider_ = listenProvider_;
        /**
         * Tree of SyncPoints.  There's a SyncPoint at any location that has 1 or more views.
         */
        this.syncPointTree_ = new ImmutableTree(null);
        /**
         * A tree of all pending user writes (user-initiated set()'s, transaction()'s, update()'s, etc.).
         */
        this.pendingWriteTree_ = newWriteTree();
        this.tagToQueryMap = new Map();
        this.queryToTagMap = new Map();
    }
}
/**
 * Apply the data changes for a user-generated set() or transaction() call.
 *
 * @returns Events to raise.
 */
function syncTreeApplyUserOverwrite(syncTree, path, newData, writeId, visible) {
    // Record pending write.
    writeTreeAddOverwrite(syncTree.pendingWriteTree_, path, newData, writeId, visible);
    if (!visible) {
        return [];
    }
    else {
        return syncTreeApplyOperationToSyncPoints_(syncTree, new Overwrite(newOperationSourceUser(), path, newData));
    }
}
/**
 * Apply the data from a user-generated update() call
 *
 * @returns Events to raise.
 */
function syncTreeApplyUserMerge(syncTree, path, changedChildren, writeId) {
    // Record pending merge.
    writeTreeAddMerge(syncTree.pendingWriteTree_, path, changedChildren, writeId);
    const changeTree = ImmutableTree.fromObject(changedChildren);
    return syncTreeApplyOperationToSyncPoints_(syncTree, new Merge(newOperationSourceUser(), path, changeTree));
}
/**
 * Acknowledge a pending user write that was previously registered with applyUserOverwrite() or applyUserMerge().
 *
 * @param revert - True if the given write failed and needs to be reverted
 * @returns Events to raise.
 */
function syncTreeAckUserWrite(syncTree, writeId, revert = false) {
    const write = writeTreeGetWrite(syncTree.pendingWriteTree_, writeId);
    const needToReevaluate = writeTreeRemoveWrite(syncTree.pendingWriteTree_, writeId);
    if (!needToReevaluate) {
        return [];
    }
    else {
        let affectedTree = new ImmutableTree(null);
        if (write.snap != null) {
            // overwrite
            affectedTree = affectedTree.set(newEmptyPath(), true);
        }
        else {
            each(write.children, (pathString) => {
                affectedTree = affectedTree.set(new Path(pathString), true);
            });
        }
        return syncTreeApplyOperationToSyncPoints_(syncTree, new AckUserWrite(write.path, affectedTree, revert));
    }
}
/**
 * Apply new server data for the specified path..
 *
 * @returns Events to raise.
 */
function syncTreeApplyServerOverwrite(syncTree, path, newData) {
    return syncTreeApplyOperationToSyncPoints_(syncTree, new Overwrite(newOperationSourceServer(), path, newData));
}
/**
 * Apply new server data to be merged in at the specified path.
 *
 * @returns Events to raise.
 */
function syncTreeApplyServerMerge(syncTree, path, changedChildren) {
    const changeTree = ImmutableTree.fromObject(changedChildren);
    return syncTreeApplyOperationToSyncPoints_(syncTree, new Merge(newOperationSourceServer(), path, changeTree));
}
/**
 * Apply a listen complete for a query
 *
 * @returns Events to raise.
 */
function syncTreeApplyListenComplete(syncTree, path) {
    return syncTreeApplyOperationToSyncPoints_(syncTree, new ListenComplete(newOperationSourceServer(), path));
}
/**
 * Apply a listen complete for a tagged query
 *
 * @returns Events to raise.
 */
function syncTreeApplyTaggedListenComplete(syncTree, path, tag) {
    const queryKey = syncTreeQueryKeyForTag_(syncTree, tag);
    if (queryKey) {
        const r = syncTreeParseQueryKey_(queryKey);
        const queryPath = r.path, queryId = r.queryId;
        const relativePath = newRelativePath(queryPath, path);
        const op = new ListenComplete(newOperationSourceServerTaggedQuery(queryId), relativePath);
        return syncTreeApplyTaggedOperation_(syncTree, queryPath, op);
    }
    else {
        // We've already removed the query. No big deal, ignore the update
        return [];
    }
}
/**
 * The complete (default) view's server cache at `path`, or null when no
 * complete view exists there. Used by persistence write-through to read the
 * tree the server just confirmed.
 */
function syncTreeGetCompleteServerCache(syncTree, path) {
    const syncPoint = syncTree.syncPointTree_.get(path);
    if (!syncPoint) {
        return null;
    }
    const view = syncPointGetCompleteView(syncPoint);
    if (!view) {
        return null;
    }
    // The CERTIFIED cache only (fully server-initialized) — never a seeded or
    // child-assembled partial tree: this feeds the persistence write-through,
    // and persisting uncertified data would surface it as server truth on the
    // next boot.
    return viewGetCompleteServerCache(view, newEmptyPath());
}
/**
 * The server-cache state of every view at or below `path`, except the
 * complete default view at `path` itself (the caller's own). Used by the
 * persistence restore path to decide what a stored tree may be applied
 * over: a view with a COMPLETE server cache contributes its certified tree
 * (to graft over the restored bytes); a view holding server data it cannot
 * certify as complete — a filtered query, a partially filled cache — is
 * reported as partial, because grafting it is impossible and overwriting it
 * would replace live server data with stale bytes.
 *
 * Ordered shallowest-first, so grafting in order lets deeper (more
 * specific) trees win where they nest.
 */
function syncTreeGetDescendantServerCacheStates(syncTree, path) {
    const states = [];
    syncTree.syncPointTree_.subtree(path).foreach((relativePath, syncPoint) => {
        for (const view of syncPoint.views.values()) {
            if (pathIsEmpty(relativePath) && view.query._queryParams.loadsAllData()) {
                // The default view at `path` is the one being restored into; the
                // caller checks its state separately.
                continue;
            }
            const complete = viewGetCompleteServerCache(view, newEmptyPath());
            if (complete !== null) {
                states.push({ path: relativePath, complete, hasPartialData: false });
            }
            else {
                const raw = viewGetServerCache(view);
                if (raw !== null &&
                    (!raw.isEmpty() || !view.query._queryParams.loadsAllData())) {
                    states.push({
                        path: relativePath,
                        complete: null,
                        hasPartialData: true
                    });
                }
            }
        }
    });
    states.sort((a, b) => pathGetLength(a.path) - pathGetLength(b.path));
    return states;
}
/**
 * Applies server range merges against the complete (default) view at the
 * given path and promotes the merged tree through the standard
 * server-overwrite path.
 *
 * @returns Events to raise.
 */
function syncTreeApplyServerRangeMerges(syncTree, path, merges) {
    const syncPoint = syncTree.syncPointTree_.get(path);
    if (!syncPoint) {
        // Removed view, so it's safe to just ignore this update
        return [];
    }
    const view = syncPointGetCompleteView(syncPoint);
    if (!view) {
        // No complete view for this update: it was removed, ignore
        return [];
    }
    return syncTreeApplyServerOverwrite(syncTree, path, applyRangeMergesToView(view, merges));
}
/**
 * Applies tagged-query server range merges against the query's view.
 *
 * @returns Events to raise.
 */
function syncTreeApplyTaggedRangeMerges(syncTree, path, merges, tag) {
    const queryKey = syncTreeQueryKeyForTag_(syncTree, tag);
    if (queryKey === undefined) {
        // Query was removed. No big deal, ignore the update
        return [];
    }
    const r = syncTreeParseQueryKey_(queryKey);
    const syncPoint = syncTree.syncPointTree_.get(r.path);
    if (!syncPoint) {
        return [];
    }
    const view = syncPoint.views.get(r.queryId);
    if (!view) {
        return [];
    }
    return syncTreeApplyTaggedQueryOverwrite(syncTree, r.path, applyRangeMergesToView(view, merges), tag);
}
/**
 * Folds server range merges over a view's raw server cache — the tree the
 * listen's hashes were computed from, certified or not — producing the tree
 * the overwrite paths then promote.
 */
function applyRangeMergesToView(view, merges) {
    let serverNode = viewGetServerCache(view) || ChildrenNode.EMPTY_NODE;
    for (const merge of merges) {
        serverNode = merge.applyTo(serverNode);
    }
    return serverNode;
}
/**
 * Remove event callback(s).
 *
 * If query is the default query, we'll check all queries for the specified eventRegistration.
 * If eventRegistration is null, we'll remove all callbacks for the specified query/queries.
 *
 * @param eventRegistration - If null, all callbacks are removed.
 * @param cancelError - If a cancelError is provided, appropriate cancel events will be returned.
 * @param skipListenerDedup - When performing a `get()`, we don't add any new listeners, so no
 *  deduping needs to take place. This flag allows toggling of that behavior
 * @returns Cancel events, if cancelError was provided.
 */
function syncTreeRemoveEventRegistration(syncTree, query, eventRegistration, cancelError, skipListenerDedup = false) {
    // Find the syncPoint first. Then deal with whether or not it has matching listeners
    const path = query._path;
    const maybeSyncPoint = syncTree.syncPointTree_.get(path);
    let cancelEvents = [];
    // A removal on a default query affects all queries at that location. A removal on an indexed query, even one without
    // other query constraints, does *not* affect all queries at that location. So this check must be for 'default', and
    // not loadsAllData().
    if (maybeSyncPoint &&
        (query._queryIdentifier === 'default' ||
            syncPointViewExistsForQuery(maybeSyncPoint, query))) {
        const removedAndEvents = syncPointRemoveEventRegistration(maybeSyncPoint, query, eventRegistration, cancelError);
        if (syncPointIsEmpty(maybeSyncPoint)) {
            syncTree.syncPointTree_ = syncTree.syncPointTree_.remove(path);
        }
        const removed = removedAndEvents.removed;
        cancelEvents = removedAndEvents.events;
        if (!skipListenerDedup) {
            /**
             * We may have just removed one of many listeners and can short-circuit this whole process
             * We may also not have removed a default listener, in which case all of the descendant listeners should already be
             * properly set up.
             */
            // Since indexed queries can shadow if they don't have other query constraints, check for loadsAllData(), instead of
            // queryId === 'default'
            const removingDefault = -1 !==
                removed.findIndex(query => {
                    return query._queryParams.loadsAllData();
                });
            const covered = syncTree.syncPointTree_.findOnPath(path, (relativePath, parentSyncPoint) => syncPointHasCompleteView(parentSyncPoint));
            if (removingDefault && !covered) {
                const subtree = syncTree.syncPointTree_.subtree(path);
                // There are potentially child listeners. Determine what if any listens we need to send before executing the
                // removal
                if (!subtree.isEmpty()) {
                    // We need to fold over our subtree and collect the listeners to send
                    const newViews = syncTreeCollectDistinctViewsForSubTree_(subtree);
                    // Ok, we've collected all the listens we need. Set them up.
                    for (let i = 0; i < newViews.length; ++i) {
                        const view = newViews[i], newQuery = view.query;
                        const listener = syncTreeCreateListenerForView_(syncTree, view);
                        syncTree.listenProvider_.startListening(syncTreeQueryForListening_(newQuery), syncTreeTagForQuery(syncTree, newQuery), listener.hashFn, listener.onComplete);
                    }
                }
                // Otherwise there's nothing below us, so nothing we need to start listening on
            }
            // If we removed anything and we're not covered by a higher up listen, we need to stop listening on this query
            // The above block has us covered in terms of making sure we're set up on listens lower in the tree.
            // Also, note that if we have a cancelError, it's already been removed at the provider level.
            if (!covered && removed.length > 0 && !cancelError) {
                // If we removed a default, then we weren't listening on any of the other queries here. Just cancel the one
                // default. Otherwise, we need to iterate through and cancel each individual query
                if (removingDefault) {
                    // We don't tag default listeners
                    const defaultTag = null;
                    syncTree.listenProvider_.stopListening(syncTreeQueryForListening_(query), defaultTag);
                }
                else {
                    removed.forEach((queryToRemove) => {
                        const tagToRemove = syncTree.queryToTagMap.get(syncTreeMakeQueryKey_(queryToRemove));
                        syncTree.listenProvider_.stopListening(syncTreeQueryForListening_(queryToRemove), tagToRemove);
                    });
                }
            }
        }
        // Now, clear all of the tags we're tracking for the removed listens
        syncTreeRemoveTags_(syncTree, removed);
    }
    return cancelEvents;
}
/**
 * Apply new server data for the specified tagged query.
 *
 * @returns Events to raise.
 */
function syncTreeApplyTaggedQueryOverwrite(syncTree, path, snap, tag) {
    const queryKey = syncTreeQueryKeyForTag_(syncTree, tag);
    if (queryKey != null) {
        const r = syncTreeParseQueryKey_(queryKey);
        const queryPath = r.path, queryId = r.queryId;
        const relativePath = newRelativePath(queryPath, path);
        const op = new Overwrite(newOperationSourceServerTaggedQuery(queryId), relativePath, snap);
        return syncTreeApplyTaggedOperation_(syncTree, queryPath, op);
    }
    else {
        // Query must have been removed already
        return [];
    }
}
/**
 * Apply server data to be merged in for the specified tagged query.
 *
 * @returns Events to raise.
 */
function syncTreeApplyTaggedQueryMerge(syncTree, path, changedChildren, tag) {
    const queryKey = syncTreeQueryKeyForTag_(syncTree, tag);
    if (queryKey) {
        const r = syncTreeParseQueryKey_(queryKey);
        const queryPath = r.path, queryId = r.queryId;
        const relativePath = newRelativePath(queryPath, path);
        const changeTree = ImmutableTree.fromObject(changedChildren);
        const op = new Merge(newOperationSourceServerTaggedQuery(queryId), relativePath, changeTree);
        return syncTreeApplyTaggedOperation_(syncTree, queryPath, op);
    }
    else {
        // We've already removed the query. No big deal, ignore the update
        return [];
    }
}
/**
 * Add an event callback for the specified query.
 *
 * @returns Events to raise.
 */
function syncTreeAddEventRegistration(syncTree, query, eventRegistration, skipSetupListener = false) {
    const path = query._path;
    let serverCache = null;
    let foundAncestorDefaultView = false;
    // Any covering writes will necessarily be at the root, so really all we need to find is the server cache.
    // Consider optimizing this once there's a better understanding of what actual behavior will be.
    syncTree.syncPointTree_.foreachOnPath(path, (pathToSyncPoint, sp) => {
        const relativePath = newRelativePath(pathToSyncPoint, path);
        serverCache =
            serverCache || syncPointGetCompleteServerCache(sp, relativePath);
        foundAncestorDefaultView =
            foundAncestorDefaultView || syncPointHasCompleteView(sp);
    });
    let syncPoint = syncTree.syncPointTree_.get(path);
    if (!syncPoint) {
        syncPoint = new SyncPoint();
        syncTree.syncPointTree_ = syncTree.syncPointTree_.set(path, syncPoint);
    }
    else {
        foundAncestorDefaultView =
            foundAncestorDefaultView || syncPointHasCompleteView(syncPoint);
        serverCache =
            serverCache || syncPointGetCompleteServerCache(syncPoint, newEmptyPath());
    }
    let serverCacheComplete;
    if (serverCache != null) {
        serverCacheComplete = true;
    }
    else {
        serverCacheComplete = false;
        // If the app registered persisted data for this path, install it as the
        // INCOMPLETE initial server cache: the listen then carries the seeded
        // tree's hash instead of the empty hash, but no value event is raised
        // until the server certifies it (see ServerCacheSeed).
        serverCache = ChildrenNode.EMPTY_NODE;
        const subtree = syncTree.syncPointTree_.subtree(path);
        subtree.foreachChild((childName, childSyncPoint) => {
            const completeCache = syncPointGetCompleteServerCache(childSyncPoint, newEmptyPath());
            if (completeCache) {
                serverCache = serverCache.updateImmediateChild(childName, completeCache);
            }
        });
    }
    const viewAlreadyExists = syncPointViewExistsForQuery(syncPoint, query);
    if (!viewAlreadyExists && !query._queryParams.loadsAllData()) {
        // We need to track a tag for this query
        const queryKey = syncTreeMakeQueryKey_(query);
        util.assert(!syncTree.queryToTagMap.has(queryKey), 'View does not exist, but we have a tag');
        const tag = syncTreeGetNextQueryTag_();
        syncTree.queryToTagMap.set(queryKey, tag);
        syncTree.tagToQueryMap.set(tag, queryKey);
    }
    const writesCache = writeTreeChildWrites(syncTree.pendingWriteTree_, path);
    let events = syncPointAddEventRegistration(syncPoint, query, eventRegistration, writesCache, serverCache, serverCacheComplete);
    if (!viewAlreadyExists && !foundAncestorDefaultView && !skipSetupListener) {
        const view = syncPointViewForQuery(syncPoint, query);
        events = events.concat(syncTreeSetupListener_(syncTree, query, view));
    }
    return events;
}
/**
 * Returns a complete cache, if we have one, of the data at a particular path. If the location does not have a
 * listener above it, we will get a false "null". This shouldn't be a problem because transactions will always
 * have a listener above, and atomic operations would correctly show a jitter of <increment value> ->
 *     <incremented total> as the write is applied locally and then acknowledged at the server.
 *
 * Note: this method will *include* hidden writes from transaction with applyLocally set to false.
 *
 * @param path - The path to the data we want
 * @param writeIdsToExclude - A specific set to be excluded
 */
function syncTreeCalcCompleteEventCache(syncTree, path, writeIdsToExclude) {
    const includeHiddenSets = true;
    const writeTree = syncTree.pendingWriteTree_;
    const serverCache = syncTree.syncPointTree_.findOnPath(path, (pathSoFar, syncPoint) => {
        const relativePath = newRelativePath(pathSoFar, path);
        const serverCache = syncPointGetCompleteServerCache(syncPoint, relativePath);
        if (serverCache) {
            return serverCache;
        }
    });
    return writeTreeCalcCompleteEventCache(writeTree, path, serverCache, writeIdsToExclude, includeHiddenSets);
}
function syncTreeGetServerValue(syncTree, query) {
    const path = query._path;
    let serverCache = null;
    // Any covering writes will necessarily be at the root, so really all we need to find is the server cache.
    // Consider optimizing this once there's a better understanding of what actual behavior will be.
    syncTree.syncPointTree_.foreachOnPath(path, (pathToSyncPoint, sp) => {
        const relativePath = newRelativePath(pathToSyncPoint, path);
        serverCache =
            serverCache || syncPointGetCompleteServerCache(sp, relativePath);
    });
    let syncPoint = syncTree.syncPointTree_.get(path);
    if (!syncPoint) {
        syncPoint = new SyncPoint();
        syncTree.syncPointTree_ = syncTree.syncPointTree_.set(path, syncPoint);
    }
    else {
        serverCache =
            serverCache || syncPointGetCompleteServerCache(syncPoint, newEmptyPath());
    }
    const serverCacheComplete = serverCache != null;
    const serverCacheNode = serverCacheComplete
        ? new CacheNode(serverCache, true, false)
        : null;
    const writesCache = writeTreeChildWrites(syncTree.pendingWriteTree_, query._path);
    const view = syncPointGetView(syncPoint, query, writesCache, serverCacheComplete ? serverCacheNode.getNode() : ChildrenNode.EMPTY_NODE, serverCacheComplete);
    return viewGetCompleteNode(view);
}
/**
 * A helper method that visits all descendant and ancestor SyncPoints, applying the operation.
 *
 * NOTES:
 * - Descendant SyncPoints will be visited first (since we raise events depth-first).
 *
 * - We call applyOperation() on each SyncPoint passing three things:
 *   1. A version of the Operation that has been made relative to the SyncPoint location.
 *   2. A WriteTreeRef of any writes we have cached at the SyncPoint location.
 *   3. A snapshot Node with cached server data, if we have it.
 *
 * - We concatenate all of the events returned by each SyncPoint and return the result.
 */
function syncTreeApplyOperationToSyncPoints_(syncTree, operation) {
    return syncTreeApplyOperationHelper_(operation, syncTree.syncPointTree_, 
    /*serverCache=*/ null, writeTreeChildWrites(syncTree.pendingWriteTree_, newEmptyPath()));
}
/**
 * Recursive helper for applyOperationToSyncPoints_
 */
function syncTreeApplyOperationHelper_(operation, syncPointTree, serverCache, writesCache) {
    if (pathIsEmpty(operation.path)) {
        return syncTreeApplyOperationDescendantsHelper_(operation, syncPointTree, serverCache, writesCache);
    }
    else {
        const syncPoint = syncPointTree.get(newEmptyPath());
        // If we don't have cached server data, see if we can get it from this SyncPoint.
        if (serverCache == null && syncPoint != null) {
            serverCache = syncPointGetCompleteServerCache(syncPoint, newEmptyPath());
        }
        let events = [];
        const childName = pathGetFront(operation.path);
        const childOperation = operation.operationForChild(childName);
        const childTree = syncPointTree.children.get(childName);
        if (childTree && childOperation) {
            const childServerCache = serverCache
                ? serverCache.getImmediateChild(childName)
                : null;
            const childWritesCache = writeTreeRefChild(writesCache, childName);
            events = events.concat(syncTreeApplyOperationHelper_(childOperation, childTree, childServerCache, childWritesCache));
        }
        if (syncPoint) {
            events = events.concat(syncPointApplyOperation(syncPoint, operation, writesCache, serverCache));
        }
        return events;
    }
}
/**
 * Recursive helper for applyOperationToSyncPoints_
 */
function syncTreeApplyOperationDescendantsHelper_(operation, syncPointTree, serverCache, writesCache) {
    const syncPoint = syncPointTree.get(newEmptyPath());
    // If we don't have cached server data, see if we can get it from this SyncPoint.
    if (serverCache == null && syncPoint != null) {
        serverCache = syncPointGetCompleteServerCache(syncPoint, newEmptyPath());
    }
    let events = [];
    syncPointTree.children.inorderTraversal((childName, childTree) => {
        const childServerCache = serverCache
            ? serverCache.getImmediateChild(childName)
            : null;
        const childWritesCache = writeTreeRefChild(writesCache, childName);
        const childOperation = operation.operationForChild(childName);
        if (childOperation) {
            events = events.concat(syncTreeApplyOperationDescendantsHelper_(childOperation, childTree, childServerCache, childWritesCache));
        }
    });
    if (syncPoint) {
        events = events.concat(syncPointApplyOperation(syncPoint, operation, writesCache, serverCache));
    }
    return events;
}
function syncTreeCreateListenerForView_(syncTree, view) {
    const query = view.query;
    const tag = syncTreeTagForQuery(syncTree, query);
    const pathString = query._path.toString();
    const hashFn = () => {
        // Manifest-first boot: the persisted hashes are stamped for this path
        // before the restored tree exists in SyncTree (see stampNextListenHashes).
        const pending = syncTree.listenProvider_.getPendingListenHashes?.(pathString);
        if (pending !== undefined) {
            return pending.hash;
        }
        const cache = viewGetServerCache(view) || ChildrenNode.EMPTY_NODE;
        return getNodeCanonicalHash(cache) ?? cache.hash();
    };
    // The compound hash rides as a property on hashFn so it threads through
    // the existing listen-provider chain untouched. Only a seeded node carries
    // one; once a server update replaces the cache it is gone, and re-listens
    // send only the simple hash.
    hashFn.compoundHash = () => {
        const pending = syncTree.listenProvider_.getPendingListenHashes?.(pathString);
        if (pending !== undefined) {
            return pending.compoundHash;
        }
        const cache = viewGetServerCache(view) || ChildrenNode.EMPTY_NODE;
        return getNodeCompoundHash(cache);
    };
    return {
        hashFn,
        onComplete: (status) => {
            if (status === 'ok') {
                if (tag) {
                    return syncTreeApplyTaggedListenComplete(syncTree, query._path, tag);
                }
                else {
                    return syncTreeApplyListenComplete(syncTree, query._path);
                }
            }
            else {
                // If a listen failed, kill all of the listeners here, not just the one that triggered the error.
                // Note that this may need to be scoped to just this listener if we change permissions on filtered children
                const error = errorForServerCode(status, query);
                return syncTreeRemoveEventRegistration(syncTree, query, 
                /*eventRegistration*/ null, error);
            }
        }
    };
}
/**
 * Return the tag associated with the given query.
 */
function syncTreeTagForQuery(syncTree, query) {
    const queryKey = syncTreeMakeQueryKey_(query);
    return syncTree.queryToTagMap.get(queryKey);
}
/**
 * Given a query, computes a "queryKey" suitable for use in our queryToTagMap_.
 */
function syncTreeMakeQueryKey_(query) {
    return query._path.toString() + '$' + query._queryIdentifier;
}
/**
 * Return the query associated with the given tag, if we have one
 */
function syncTreeQueryKeyForTag_(syncTree, tag) {
    return syncTree.tagToQueryMap.get(tag);
}
/**
 * Given a queryKey (created by makeQueryKey), parse it back into a path and queryId.
 */
function syncTreeParseQueryKey_(queryKey) {
    const splitIndex = queryKey.indexOf('$');
    util.assert(splitIndex !== -1 && splitIndex < queryKey.length - 1, 'Bad queryKey.');
    return {
        queryId: queryKey.substr(splitIndex + 1),
        path: new Path(queryKey.substr(0, splitIndex))
    };
}
/**
 * A helper method to apply tagged operations
 */
function syncTreeApplyTaggedOperation_(syncTree, queryPath, operation) {
    const syncPoint = syncTree.syncPointTree_.get(queryPath);
    util.assert(syncPoint, "Missing sync point for query tag that we're tracking");
    const writesCache = writeTreeChildWrites(syncTree.pendingWriteTree_, queryPath);
    return syncPointApplyOperation(syncPoint, operation, writesCache, null);
}
/**
 * This collapses multiple unfiltered views into a single view, since we only need a single
 * listener for them.
 */
function syncTreeCollectDistinctViewsForSubTree_(subtree) {
    return subtree.fold((relativePath, maybeChildSyncPoint, childMap) => {
        if (maybeChildSyncPoint && syncPointHasCompleteView(maybeChildSyncPoint)) {
            const completeView = syncPointGetCompleteView(maybeChildSyncPoint);
            return [completeView];
        }
        else {
            // No complete view here, flatten any deeper listens into an array
            let views = [];
            if (maybeChildSyncPoint) {
                views = syncPointGetQueryViews(maybeChildSyncPoint);
            }
            each(childMap, (_key, childViews) => {
                views = views.concat(childViews);
            });
            return views;
        }
    });
}
/**
 * Normalizes a query to a query we send the server for listening
 *
 * @returns The normalized query
 */
function syncTreeQueryForListening_(query) {
    if (query._queryParams.loadsAllData() && !query._queryParams.isDefault()) {
        // We treat queries that load all data as default queries
        // Cast is necessary because ref() technically returns Firebase which is actually fb.api.Firebase which inherits
        // from Query
        return new (syncTreeGetReferenceConstructor())(query._repo, query._path);
    }
    else {
        return query;
    }
}
function syncTreeRemoveTags_(syncTree, queries) {
    for (let j = 0; j < queries.length; ++j) {
        const removedQuery = queries[j];
        if (!removedQuery._queryParams.loadsAllData()) {
            // We should have a tag for this
            const removedQueryKey = syncTreeMakeQueryKey_(removedQuery);
            const removedQueryTag = syncTree.queryToTagMap.get(removedQueryKey);
            syncTree.queryToTagMap.delete(removedQueryKey);
            syncTree.tagToQueryMap.delete(removedQueryTag);
        }
    }
}
/**
 * Static accessor for query tags.
 */
function syncTreeGetNextQueryTag_() {
    return syncTreeNextQueryTag_++;
}
/**
 * For a given new listen, manage the de-duplication of outstanding subscriptions.
 *
 * @returns This method can return events to support synchronous data sources
 */
function syncTreeSetupListener_(syncTree, query, view) {
    const path = query._path;
    const tag = syncTreeTagForQuery(syncTree, query);
    const listener = syncTreeCreateListenerForView_(syncTree, view);
    const events = syncTree.listenProvider_.startListening(syncTreeQueryForListening_(query), tag, listener.hashFn, listener.onComplete);
    const subtree = syncTree.syncPointTree_.subtree(path);
    // The root of this subtree has our query. We're here because we definitely need to send a listen for that, but we
    // may need to shadow other listens as well.
    if (tag) {
        util.assert(!syncPointHasCompleteView(subtree.value), "If we're adding a query, it shouldn't be shadowed");
    }
    else {
        // Shadow everything at or below this location, this is a default listener.
        const queriesToStop = subtree.fold((relativePath, maybeChildSyncPoint, childMap) => {
            if (!pathIsEmpty(relativePath) &&
                maybeChildSyncPoint &&
                syncPointHasCompleteView(maybeChildSyncPoint)) {
                return [syncPointGetCompleteView(maybeChildSyncPoint).query];
            }
            else {
                // No default listener here, flatten any deeper queries into an array
                let queries = [];
                if (maybeChildSyncPoint) {
                    queries = queries.concat(syncPointGetQueryViews(maybeChildSyncPoint).map(view => view.query));
                }
                each(childMap, (_key, childQueries) => {
                    queries = queries.concat(childQueries);
                });
                return queries;
            }
        });
        for (let i = 0; i < queriesToStop.length; ++i) {
            const queryToStop = queriesToStop[i];
            syncTree.listenProvider_.stopListening(syncTreeQueryForListening_(queryToStop), syncTreeTagForQuery(syncTree, queryToStop));
        }
    }
    return events;
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class ExistingValueProvider {
    constructor(node_) {
        this.node_ = node_;
    }
    getImmediateChild(childName) {
        const child = this.node_.getImmediateChild(childName);
        return new ExistingValueProvider(child);
    }
    node() {
        return this.node_;
    }
}
class DeferredValueProvider {
    constructor(syncTree, path) {
        this.syncTree_ = syncTree;
        this.path_ = path;
    }
    getImmediateChild(childName) {
        const childPath = pathChild(this.path_, childName);
        return new DeferredValueProvider(this.syncTree_, childPath);
    }
    node() {
        return syncTreeCalcCompleteEventCache(this.syncTree_, this.path_);
    }
}
/**
 * Generate placeholders for deferred values.
 */
const generateWithValues = function (values) {
    values = values || {};
    values['timestamp'] = values['timestamp'] || new Date().getTime();
    return values;
};
/**
 * Value to use when firing local events. When writing server values, fire
 * local events with an approximate value, otherwise return value as-is.
 */
const resolveDeferredLeafValue = function (value, existingVal, serverValues) {
    if (!value || typeof value !== 'object') {
        return value;
    }
    util.assert('.sv' in value, 'Unexpected leaf node or priority contents');
    if (typeof value['.sv'] === 'string') {
        return resolveScalarDeferredValue(value['.sv'], existingVal, serverValues);
    }
    else if (typeof value['.sv'] === 'object') {
        return resolveComplexDeferredValue(value['.sv'], existingVal);
    }
    else {
        util.assert(false, 'Unexpected server value: ' + JSON.stringify(value, null, 2));
    }
};
const resolveScalarDeferredValue = function (op, existing, serverValues) {
    switch (op) {
        case 'timestamp':
            return serverValues['timestamp'];
        default:
            util.assert(false, 'Unexpected server value: ' + op);
    }
};
const resolveComplexDeferredValue = function (op, existing, unused) {
    if (!op.hasOwnProperty('increment')) {
        util.assert(false, 'Unexpected server value: ' + JSON.stringify(op, null, 2));
    }
    const delta = op['increment'];
    if (typeof delta !== 'number') {
        util.assert(false, 'Unexpected increment value: ' + delta);
    }
    const existingNode = existing.node();
    util.assert(existingNode !== null && typeof existingNode !== 'undefined', 'Expected ChildrenNode.EMPTY_NODE for nulls');
    // Incrementing a non-number sets the value to the incremented amount
    if (!existingNode.isLeafNode()) {
        return delta;
    }
    const leaf = existingNode;
    const existingVal = leaf.getValue();
    if (typeof existingVal !== 'number') {
        return delta;
    }
    // No need to do over/underflow arithmetic here because JS only handles floats under the covers
    return existingVal + delta;
};
/**
 * Recursively replace all deferred values and priorities in the tree with the
 * specified generated replacement values.
 * @param path - path to which write is relative
 * @param node - new data written at path
 * @param syncTree - current data
 */
const resolveDeferredValueTree = function (path, node, syncTree, serverValues) {
    return resolveDeferredValue(node, new DeferredValueProvider(syncTree, path), serverValues);
};
/**
 * Recursively replace all deferred values and priorities in the node with the
 * specified generated replacement values.  If there are no server values in the node,
 * it'll be returned as-is.
 */
const resolveDeferredValueSnapshot = function (node, existing, serverValues) {
    return resolveDeferredValue(node, new ExistingValueProvider(existing), serverValues);
};
function resolveDeferredValue(node, existingVal, serverValues) {
    const rawPri = node.getPriority().val();
    const priority = resolveDeferredLeafValue(rawPri, existingVal.getImmediateChild('.priority'), serverValues);
    let newNode;
    if (node.isLeafNode()) {
        const leafNode = node;
        const value = resolveDeferredLeafValue(leafNode.getValue(), existingVal, serverValues);
        if (value !== leafNode.getValue() ||
            priority !== leafNode.getPriority().val()) {
            return new LeafNode(value, nodeFromJSON(priority));
        }
        else {
            return node;
        }
    }
    else {
        const childrenNode = node;
        newNode = childrenNode;
        if (priority !== childrenNode.getPriority().val()) {
            newNode = newNode.updatePriority(new LeafNode(priority));
        }
        childrenNode.forEachChild(PRIORITY_INDEX, (childName, childNode) => {
            const newChildNode = resolveDeferredValue(childNode, existingVal.getImmediateChild(childName), serverValues);
            if (newChildNode !== childNode) {
                newNode = newNode.updateImmediateChild(childName, newChildNode);
            }
        });
        return newNode;
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * A light-weight tree, traversable by path.  Nodes can have both values and children.
 * Nodes are not enumerated (by forEachChild) unless they have a value or non-empty
 * children.
 */
class Tree {
    /**
     * @param name - Optional name of the node.
     * @param parent - Optional parent node.
     * @param node - Optional node to wrap.
     */
    constructor(name = '', parent = null, node = { children: {}, childCount: 0 }) {
        this.name = name;
        this.parent = parent;
        this.node = node;
    }
}
/**
 * Returns a sub-Tree for the given path.
 *
 * @param pathObj - Path to look up.
 * @returns Tree for path.
 */
function treeSubTree(tree, pathObj) {
    // TODO: Require pathObj to be Path?
    let path = pathObj instanceof Path ? pathObj : new Path(pathObj);
    let child = tree, next = pathGetFront(path);
    while (next !== null) {
        const childNode = util.safeGet(child.node.children, next) || {
            children: {},
            childCount: 0
        };
        child = new Tree(next, child, childNode);
        path = pathPopFront(path);
        next = pathGetFront(path);
    }
    return child;
}
/**
 * Returns the data associated with this tree node.
 *
 * @returns The data or null if no data exists.
 */
function treeGetValue(tree) {
    return tree.node.value;
}
/**
 * Sets data to this tree node.
 *
 * @param value - Value to set.
 */
function treeSetValue(tree, value) {
    tree.node.value = value;
    treeUpdateParents(tree);
}
/**
 * @returns Whether the tree has any children.
 */
function treeHasChildren(tree) {
    return tree.node.childCount > 0;
}
/**
 * @returns Whether the tree is empty (no value or children).
 */
function treeIsEmpty(tree) {
    return treeGetValue(tree) === undefined && !treeHasChildren(tree);
}
/**
 * Calls action for each child of this tree node.
 *
 * @param action - Action to be called for each child.
 */
function treeForEachChild(tree, action) {
    each(tree.node.children, (child, childTree) => {
        action(new Tree(child, tree, childTree));
    });
}
/**
 * Does a depth-first traversal of this node's descendants, calling action for each one.
 *
 * @param action - Action to be called for each child.
 * @param includeSelf - Whether to call action on this node as well. Defaults to
 *   false.
 * @param childrenFirst - Whether to call action on children before calling it on
 *   parent.
 */
function treeForEachDescendant(tree, action, includeSelf, childrenFirst) {
    if (includeSelf && !childrenFirst) {
        action(tree);
    }
    treeForEachChild(tree, child => {
        treeForEachDescendant(child, action, true, childrenFirst);
    });
    if (includeSelf && childrenFirst) {
        action(tree);
    }
}
/**
 * Calls action on each ancestor node.
 *
 * @param action - Action to be called on each parent; return
 *   true to abort.
 * @param includeSelf - Whether to call action on this node as well.
 * @returns true if the action callback returned true.
 */
function treeForEachAncestor(tree, action, includeSelf) {
    let node = includeSelf ? tree : tree.parent;
    while (node !== null) {
        if (action(node)) {
            return true;
        }
        node = node.parent;
    }
    return false;
}
/**
 * @returns The path of this tree node, as a Path.
 */
function treeGetPath(tree) {
    return new Path(tree.parent === null
        ? tree.name
        : treeGetPath(tree.parent) + '/' + tree.name);
}
/**
 * Adds or removes this child from its parent based on whether it's empty or not.
 */
function treeUpdateParents(tree) {
    if (tree.parent !== null) {
        treeUpdateChild(tree.parent, tree.name, tree);
    }
}
/**
 * Adds or removes the passed child to this tree node, depending on whether it's empty.
 *
 * @param childName - The name of the child to update.
 * @param child - The child to update.
 */
function treeUpdateChild(tree, childName, child) {
    const childEmpty = treeIsEmpty(child);
    const childExists = util.contains(tree.node.children, childName);
    if (childEmpty && childExists) {
        delete tree.node.children[childName];
        tree.node.childCount--;
        treeUpdateParents(tree);
    }
    else if (!childEmpty && !childExists) {
        tree.node.children[childName] = child.node;
        tree.node.childCount++;
        treeUpdateParents(tree);
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * True for invalid Firebase keys
 */
const INVALID_KEY_REGEX_ = /[\[\].#$\/\u0000-\u001F\u007F]/;
/**
 * True for invalid Firebase paths.
 * Allows '/' in paths.
 */
const INVALID_PATH_REGEX_ = /[\[\].#$\u0000-\u001F\u007F]/;
/**
 * Maximum number of characters to allow in leaf value
 */
const MAX_LEAF_SIZE_ = 10 * 1024 * 1024;
const isValidKey = function (key) {
    return (typeof key === 'string' && key.length !== 0 && !INVALID_KEY_REGEX_.test(key));
};
const isValidPathString = function (pathString) {
    return (typeof pathString === 'string' &&
        pathString.length !== 0 &&
        !INVALID_PATH_REGEX_.test(pathString));
};
const isValidRootPathString = function (pathString) {
    if (pathString) {
        // Allow '/.info/' at the beginning.
        pathString = pathString.replace(/^\/*\.info(\/|$)/, '/');
    }
    return isValidPathString(pathString);
};
const isValidPriority = function (priority) {
    return (priority === null ||
        typeof priority === 'string' ||
        (typeof priority === 'number' && !isInvalidJSONNumber(priority)) ||
        (priority &&
            typeof priority === 'object' &&
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            util.contains(priority, '.sv')));
};
/**
 * Pre-validate a datum passed as an argument to Firebase function.
 */
const validateFirebaseDataArg = function (fnName, value, path, optional) {
    if (optional && value === undefined) {
        return;
    }
    validateFirebaseData(util.errorPrefix(fnName, 'value'), value, path);
};
/**
 * Validate a data object client-side before sending to server.
 */
const validateFirebaseData = function (errorPrefix, data, path_) {
    const path = path_ instanceof Path ? new ValidationPath(path_, errorPrefix) : path_;
    if (data === undefined) {
        throw new Error(errorPrefix + 'contains undefined ' + validationPathToErrorString(path));
    }
    if (typeof data === 'function') {
        throw new Error(errorPrefix +
            'contains a function ' +
            validationPathToErrorString(path) +
            ' with contents = ' +
            data.toString());
    }
    if (isInvalidJSONNumber(data)) {
        throw new Error(errorPrefix +
            'contains ' +
            data.toString() +
            ' ' +
            validationPathToErrorString(path));
    }
    // Check max leaf size, but try to avoid the utf8 conversion if we can.
    if (typeof data === 'string' &&
        data.length > MAX_LEAF_SIZE_ / 3 &&
        util.stringLength(data) > MAX_LEAF_SIZE_) {
        throw new Error(errorPrefix +
            'contains a string greater than ' +
            MAX_LEAF_SIZE_ +
            ' utf8 bytes ' +
            validationPathToErrorString(path) +
            " ('" +
            data.substring(0, 50) +
            "...')");
    }
    // TODO = Perf = Consider combining the recursive validation of keys into NodeFromJSON
    // to save extra walking of large objects.
    if (data && typeof data === 'object') {
        let hasDotValue = false;
        let hasActualChild = false;
        each(data, (key, value) => {
            if (key === '.value') {
                hasDotValue = true;
            }
            else if (key !== '.priority' && key !== '.sv') {
                hasActualChild = true;
                if (!isValidKey(key)) {
                    throw new Error(errorPrefix +
                        ' contains an invalid key (' +
                        key +
                        ') ' +
                        validationPathToErrorString(path) +
                        '.  Keys must be non-empty strings ' +
                        'and can\'t contain ".", "#", "$", "/", "[", or "]"');
                }
            }
            validationPathPush(path, key);
            validateFirebaseData(errorPrefix, value, path);
            validationPathPop(path);
        });
        if (hasDotValue && hasActualChild) {
            throw new Error(errorPrefix +
                ' contains ".value" child ' +
                validationPathToErrorString(path) +
                ' in addition to actual children.');
        }
    }
};
/**
 * Pre-validate paths passed in the firebase function.
 */
const validateFirebaseMergePaths = function (errorPrefix, mergePaths) {
    let i, curPath;
    for (i = 0; i < mergePaths.length; i++) {
        curPath = mergePaths[i];
        const keys = pathSlice(curPath);
        for (let j = 0; j < keys.length; j++) {
            if (keys[j] === '.priority' && j === keys.length - 1) ;
            else if (!isValidKey(keys[j])) {
                throw new Error(errorPrefix +
                    'contains an invalid key (' +
                    keys[j] +
                    ') in path ' +
                    curPath.toString() +
                    '. Keys must be non-empty strings ' +
                    'and can\'t contain ".", "#", "$", "/", "[", or "]"');
            }
        }
    }
    // Check that update keys are not descendants of each other.
    // We rely on the property that sorting guarantees that ancestors come
    // right before descendants.
    mergePaths.sort(pathCompare);
    let prevPath = null;
    for (i = 0; i < mergePaths.length; i++) {
        curPath = mergePaths[i];
        if (prevPath !== null && pathContains(prevPath, curPath)) {
            throw new Error(errorPrefix +
                'contains a path ' +
                prevPath.toString() +
                ' that is ancestor of another path ' +
                curPath.toString());
        }
        prevPath = curPath;
    }
};
/**
 * pre-validate an object passed as an argument to firebase function (
 * must be an object - e.g. for firebase.update()).
 */
const validateFirebaseMergeDataArg = function (fnName, data, path, optional) {
    if (optional && data === undefined) {
        return;
    }
    const errorPrefix = util.errorPrefix(fnName, 'values');
    if (!(data && typeof data === 'object') || Array.isArray(data)) {
        throw new Error(errorPrefix + ' must be an object containing the children to replace.');
    }
    const mergePaths = [];
    each(data, (key, value) => {
        const curPath = new Path(key);
        validateFirebaseData(errorPrefix, value, pathChild(path, curPath));
        if (pathGetBack(curPath) === '.priority') {
            if (!isValidPriority(value)) {
                throw new Error(errorPrefix +
                    "contains an invalid value for '" +
                    curPath.toString() +
                    "', which must be a valid " +
                    'Firebase priority (a string, finite number, server value, or null).');
            }
        }
        mergePaths.push(curPath);
    });
    validateFirebaseMergePaths(errorPrefix, mergePaths);
};
const validatePriority = function (fnName, priority, optional) {
    if (optional && priority === undefined) {
        return;
    }
    if (isInvalidJSONNumber(priority)) {
        throw new Error(util.errorPrefix(fnName, 'priority') +
            'is ' +
            priority.toString() +
            ', but must be a valid Firebase priority (a string, finite number, ' +
            'server value, or null).');
    }
    // Special case to allow importing data with a .sv.
    if (!isValidPriority(priority)) {
        throw new Error(util.errorPrefix(fnName, 'priority') +
            'must be a valid Firebase priority ' +
            '(a string, finite number, server value, or null).');
    }
};
const validateKey = function (fnName, argumentName, key, optional) {
    if (optional && key === undefined) {
        return;
    }
    if (!isValidKey(key)) {
        throw new Error(util.errorPrefix(fnName, argumentName) +
            'was an invalid key = "' +
            key +
            '".  Firebase keys must be non-empty strings and ' +
            'can\'t contain ".", "#", "$", "/", "[", or "]").');
    }
};
/**
 * @internal
 */
const validatePathString = function (fnName, argumentName, pathString, optional) {
    if (optional && pathString === undefined) {
        return;
    }
    if (!isValidPathString(pathString)) {
        throw new Error(util.errorPrefix(fnName, argumentName) +
            'was an invalid path = "' +
            pathString +
            '". Paths must be non-empty strings and ' +
            'can\'t contain ".", "#", "$", "[", or "]"');
    }
};
const validateRootPathString = function (fnName, argumentName, pathString, optional) {
    // Non-string input falls through to validatePathString's descriptive
    // error instead of throwing a bare TypeError here.
    if (typeof pathString === 'string' && pathString) {
        // Allow '/.info/' at the beginning.
        pathString = pathString.replace(/^\/*\.info(\/|$)/, '/');
    }
    validatePathString(fnName, argumentName, pathString, optional);
};
/**
 * @internal
 */
const validateWritablePath = function (fnName, path) {
    if (pathGetFront(path) === '.info') {
        throw new Error(fnName + " failed = Can't modify data under /.info/");
    }
};
const validateUrl = function (fnName, parsedUrl) {
    // TODO = Validate server better.
    const pathString = parsedUrl.path.toString();
    if (!(typeof parsedUrl.repoInfo.host === 'string') ||
        parsedUrl.repoInfo.host.length === 0 ||
        (!isValidKey(parsedUrl.repoInfo.namespace) &&
            parsedUrl.repoInfo.host.split(':')[0] !== 'localhost') ||
        (pathString.length !== 0 && !isValidRootPathString(pathString))) {
        throw new Error(util.errorPrefix(fnName, 'url') +
            'must be a valid firebase URL and ' +
            'the path can\'t contain ".", "#", "$", "[", or "]".');
    }
};

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * The event queue serves a few purposes:
 * 1. It ensures we maintain event order in the face of event callbacks doing operations that result in more
 *    events being queued.
 * 2. raiseQueuedEvents() handles being called reentrantly nicely.  That is, if in the course of raising events,
 *    raiseQueuedEvents() is called again, the "inner" call will pick up raising events where the "outer" call
 *    left off, ensuring that the events are still raised synchronously and in order.
 * 3. You can use raiseEventsAtPath and raiseEventsForChangedPath to ensure only relevant previously-queued
 *    events are raised synchronously.
 *
 * NOTE: This can all go away if/when we move to async events.
 *
 */
class EventQueue {
    constructor() {
        this.eventLists_ = [];
        /**
         * Tracks recursion depth of raiseQueuedEvents_, for debugging purposes.
         */
        this.recursionDepth_ = 0;
    }
}
/**
 * @param eventDataList - The new events to queue.
 */
function eventQueueQueueEvents(eventQueue, eventDataList) {
    // We group events by path, storing them in a single EventList, to make it easier to skip over them quickly.
    let currList = null;
    for (let i = 0; i < eventDataList.length; i++) {
        const data = eventDataList[i];
        const path = data.getPath();
        if (currList !== null && !pathEquals(path, currList.path)) {
            eventQueue.eventLists_.push(currList);
            currList = null;
        }
        if (currList === null) {
            currList = { events: [], path };
        }
        currList.events.push(data);
    }
    if (currList) {
        eventQueue.eventLists_.push(currList);
    }
}
/**
 * Queues the specified events and synchronously raises all events (including previously queued ones)
 * for the specified path.
 *
 * It is assumed that the new events are all for the specified path.
 *
 * @param path - The path to raise events for.
 * @param eventDataList - The new events to raise.
 */
function eventQueueRaiseEventsAtPath(eventQueue, path, eventDataList) {
    eventQueueQueueEvents(eventQueue, eventDataList);
    eventQueueRaiseQueuedEventsMatchingPredicate(eventQueue, eventPath => pathEquals(eventPath, path));
}
/**
 * Queues the specified events and synchronously raises all events (including previously queued ones) for
 * locations related to the specified change path (i.e. all ancestors and descendants).
 *
 * It is assumed that the new events are all related (ancestor or descendant) to the specified path.
 *
 * @param changedPath - The path to raise events for.
 * @param eventDataList - The events to raise
 */
function eventQueueRaiseEventsForChangedPath(eventQueue, changedPath, eventDataList) {
    eventQueueQueueEvents(eventQueue, eventDataList);
    eventQueueRaiseQueuedEventsMatchingPredicate(eventQueue, eventPath => pathContains(eventPath, changedPath) ||
        pathContains(changedPath, eventPath));
}
function eventQueueRaiseQueuedEventsMatchingPredicate(eventQueue, predicate) {
    eventQueue.recursionDepth_++;
    let sentAll = true;
    for (let i = 0; i < eventQueue.eventLists_.length; i++) {
        const eventList = eventQueue.eventLists_[i];
        if (eventList) {
            const eventPath = eventList.path;
            if (predicate(eventPath)) {
                eventListRaise(eventQueue.eventLists_[i]);
                eventQueue.eventLists_[i] = null;
            }
            else {
                sentAll = false;
            }
        }
    }
    if (sentAll) {
        eventQueue.eventLists_ = [];
    }
    eventQueue.recursionDepth_--;
}
/**
 * Iterates through the list and raises each event
 */
function eventListRaise(eventList) {
    for (let i = 0; i < eventList.events.length; i++) {
        const eventData = eventList.events[i];
        if (eventData !== null) {
            eventList.events[i] = null;
            const eventFn = eventData.getEventRunner();
            if (logger) {
                log('event: ' + eventData.toString());
            }
            exceptionGuard(eventFn);
        }
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const INTERRUPT_REASON = 'repo_interrupt';
/**
 * If a transaction does not succeed after 25 retries, we abort it. Among other
 * things this ensure that if there's ever a bug causing a mismatch between
 * client / server hashes for some data, we won't retry indefinitely.
 */
const MAX_TRANSACTION_RETRIES = 25;
/**
 * The boot-buffer root covering `pathString`, if any: operations at or under
 * a buffering root are held until its cached base applies.
 */
function repoBootBufferRootFor(repo, pathString) {
    if (repo.bootBuffers_.size === 0) {
        return null;
    }
    for (const root of repo.bootBuffers_.keys()) {
        if (pathString === root ||
            root === '/' ||
            (pathString.length > root.length && pathString.startsWith(root + '/'))) {
            return root;
        }
    }
    return null;
}
function emitPersistenceTrace(event) {
    const sink = globalThis
        .__firebaseDatabasePersistenceTrace;
    if (typeof sink === 'function') {
        exceptionGuard(() => sink(event));
    }
}
function repoCancelPendingSeedRestore(pending) {
    pending.cancelled = true;
    pending.authScopeUnsubscribe?.();
    if (pending.authScopeTimer !== undefined) {
        clearTimeout(pending.authScopeTimer);
    }
}
class Repo {
    constructor(repoInfo_, forceRestClient_, authTokenProvider_, appCheckProvider_) {
        this.repoInfo_ = repoInfo_;
        this.forceRestClient_ = forceRestClient_;
        this.authTokenProvider_ = authTokenProvider_;
        this.appCheckProvider_ = appCheckProvider_;
        this.dataUpdateCount = 0;
        this.statsListener_ = null;
        this.eventQueue_ = new EventQueue();
        this.nextWriteId_ = 1;
        this.interceptServerDataCallback_ = null;
        /** A list of data pieces and paths to be set when this client disconnects. */
        this.onDisconnect_ = newSparseSnapshotTree();
        /** Stores queues of outstanding transactions for Firebase locations. */
        this.transactionQueueTree_ = new Tree();
        // TODO: This should be @private but it's used by test_access.js and internal.js
        this.persistentConnection_ = null;
        /**
         * Server-cache persistence (see core/Persistence.ts); null unless the app
         * enabled it before this Repo's first listen.
         */
        this.persistence_ = null;
        /**
         * The application-provided identity scope currently bound to persistence.
         * `undefined` means Auth has not resolved yet; null means signed out.
         */
        this.persistenceAuthScope_ = undefined;
        this.persistenceAuthScopeListeners_ = new Set();
        /**
         * Listens held back while their persisted root restores, keyed by path.
         * stopListening flips the token so a listen whose last registration was
         * removed mid-restore is never sent (see repoStartServerListen).
         */
        this.pendingSeedRestores_ = new Map();
        /** Manifest-first hashes scoped to this Repo, never process-global. */
        this.pendingListenHashes_ = new PendingListenHashStore();
        /**
         * Server operations buffered during a manifest-first boot window: the
         * range listen is on the wire before the cached base has been applied to
         * SyncTree, so anything the server sends for that root (range merges —
         * deltas against the base — or full pushes) is held, in arrival order,
         * until the base applies, then replayed. Keyed by the listened root path.
         */
        this.bootBuffers_ = new Map();
        /**
         * Listen-complete state per default complete listen, keyed by path: whether
         * the current listen has received its initial server response, and waiters
         * to publish its certification outcome (see onListenOutcome in api/Database.ts).
         */
        this.listenOutcomes_ = new Map();
        // This key is intentionally not updated if RepoInfo is later changed or replaced
        this.key = this.repoInfo_.toURLString();
    }
    /**
     * @returns The URL corresponding to the root of this Firebase.
     */
    toString() {
        return ((this.repoInfo_.secure ? 'https://' : 'http://') + this.repoInfo_.host);
    }
}
function repoStart(repo, appId, authOverride) {
    repo.stats_ = statsManagerGetCollection(repo.repoInfo_);
    if (repo.forceRestClient_ || beingCrawled()) {
        repo.server_ = new ReadonlyRestClient(repo.repoInfo_, (pathString, data, isMerge, tag) => {
            repoOnDataUpdate(repo, pathString, data, isMerge, tag);
        }, repo.authTokenProvider_, repo.appCheckProvider_);
        // Minor hack: Fire onConnect immediately, since there's no actual connection.
        setTimeout(() => repoOnConnectStatus(repo, /* connectStatus= */ true), 0);
    }
    else {
        // Validate authOverride
        if (typeof authOverride !== 'undefined' && authOverride !== null) {
            if (typeof authOverride !== 'object') {
                throw new Error('Only objects are supported for option databaseAuthVariableOverride');
            }
            try {
                util.stringify(authOverride);
            }
            catch (e) {
                throw new Error('Invalid authOverride provided: ' + e);
            }
        }
        repo.persistentConnection_ = new PersistentConnection(repo.repoInfo_, appId, (pathString, data, isMerge, tag) => {
            repoOnDataUpdate(repo, pathString, data, isMerge, tag);
        }, (connectStatus) => {
            repoOnConnectStatus(repo, connectStatus);
        }, (updates) => {
            repoOnServerInfoUpdate(repo, updates);
        }, repo.authTokenProvider_, repo.appCheckProvider_, authOverride, (pathString, ranges, tag) => {
            repoOnRangeMergeUpdate(repo, pathString, ranges, tag);
        });
        repo.server_ = repo.persistentConnection_;
    }
    repo.authTokenProvider_.addTokenChangeListener(token => {
        repo.server_.refreshAuthToken(token);
    });
    repo.appCheckProvider_.addTokenChangeListener(result => {
        repo.server_.refreshAppCheckToken(result.token);
    });
    // In the case of multiple Repos for the same repoInfo (i.e. there are multiple Firebase.Contexts being used),
    // we only want to create one StatsReporter.  As such, we'll report stats over the first Repo created.
    repo.statsReporter_ = statsManagerGetOrCreateReporter(repo.repoInfo_, () => new StatsReporter(repo.stats_, repo.server_));
    // Used for .info.
    repo.infoData_ = new SnapshotHolder();
    repo.infoSyncTree_ = new SyncTree({
        startListening: (query, tag, currentHashFn, onComplete) => {
            let infoEvents = [];
            const node = repo.infoData_.getNode(query._path);
            // This is possibly a hack, but we have different semantics for .info endpoints. We don't raise null events
            // on initial data...
            if (!node.isEmpty()) {
                infoEvents = syncTreeApplyServerOverwrite(repo.infoSyncTree_, query._path, node);
                setTimeout(() => {
                    onComplete('ok');
                }, 0);
            }
            return infoEvents;
        },
        stopListening: () => { }
    });
    repoUpdateInfo(repo, 'connected', false);
    repo.serverSyncTree_ = new SyncTree({
        startListening: (query, tag, currentHashFn, onComplete) => {
            repoStartServerListen(repo, query, tag, currentHashFn, onComplete);
            // No synchronous events for network-backed sync trees
            return [];
        },
        stopListening: (query, tag) => {
            repoStopServerListen(repo, query, tag);
        },
        getPendingListenHashes: pathString => repo.pendingListenHashes_.get(pathString)
    });
}
/**
 * @returns The time in milliseconds, taking the server offset into account if we have one.
 */
function repoServerTime(repo) {
    const offsetNode = repo.infoData_.getNode(new Path('.info/serverTimeOffset'));
    const offset = offsetNode.val() || 0;
    return new Date().getTime() + offset;
}
/**
 * Generate ServerValues using some variables from the repo object.
 */
function repoGenerateServerValues(repo) {
    return generateWithValues({
        timestamp: repoServerTime(repo)
    });
}
function repoOnDataUpdate(repo, pathString, data, isMerge, tag) {
    // For testing.
    repo.dataUpdateCount++;
    {
        // Manifest-first boot window: the listen went out before the cached base
        // applied. Hold server data for that root — in arrival order with range
        // merges — until the base is in SyncTree (see repoStartServerListen).
        const bufferRoot = repoBootBufferRootFor(repo, pathString);
        if (bufferRoot !== null) {
            repo.bootBuffers_
                .get(bufferRoot)
                .push({ kind: 'data', pathString, data, isMerge, tag });
            return;
        }
    }
    const path = new Path(pathString);
    data = repo.interceptServerDataCallback_
        ? repo.interceptServerDataCallback_(pathString, data)
        : data;
    let events = [];
    if (tag) {
        if (isMerge) {
            const taggedChildren = util.map(data, (raw) => nodeFromJSON(raw));
            events = syncTreeApplyTaggedQueryMerge(repo.serverSyncTree_, path, taggedChildren, tag);
        }
        else {
            const taggedSnap = nodeFromJSON(data);
            events = syncTreeApplyTaggedQueryOverwrite(repo.serverSyncTree_, path, taggedSnap, tag);
        }
    }
    else if (isMerge) {
        const changedChildren = util.map(data, (raw) => nodeFromJSON(raw));
        events = syncTreeApplyServerMerge(repo.serverSyncTree_, path, changedChildren);
    }
    else {
        const snap = nodeFromJSON(data);
        events = syncTreeApplyServerOverwrite(repo.serverSyncTree_, path, snap);
    }
    let affectedPath = path;
    if (events.length > 0) {
        // Since we have a listener outstanding for each transaction, receiving any events
        // is a proxy for some change having occurred.
        affectedPath = repoRerunTransactions(repo, path);
    }
    eventQueueRaiseEventsForChangedPath(repo.eventQueue_, affectedPath, events);
    if (tag == null) {
        // Overwrite and merge both change only subtrees under `path`.
        repoPersistAfterServerUpdate(repo, path, 'at-path');
    }
}
/**
 * Sends a listen for the server sync tree, restoring the persisted server
 * cache first where applicable: a complete default listen on a persisted
 * root is held until the stored tree restores (bounded inside restore()),
 * the restored tree is applied as server data — raising cached events
 * immediately, mobile-persistence semantics — and the listen then goes out
 * carrying the restored tree's hashes. Roots that were never persisted
 * resolve null instantly and attach exactly as before.
 */
function repoPublishListenOutcome(repo, pathString, outcome) {
    const state = repo.listenOutcomes_.get(pathString);
    if (!state) {
        return;
    }
    state.outcome = outcome;
    emitPersistenceTrace({ type: 'listen-outcome', path: pathString, outcome });
    for (const subscriber of state.subscribers) {
        // Observability callbacks must never abort authoritative wire processing.
        exceptionGuard(() => subscriber(outcome));
    }
}
function repoStartServerListen(repo, query, tag, currentHashFn, onComplete, skipPersistence = false, authScopeTimeoutMs = PERSISTENCE_RESTORE_TIMEOUT_MS) {
    const pathString = query._path.toString();
    const isDefaultComplete = tag == null && query._queryParams.loadsAllData();
    if (isDefaultComplete) {
        const prior = repo.listenOutcomes_.get(pathString);
        repo.listenOutcomes_.set(pathString, {
            outcome: null,
            subscribers: prior?.subscribers ?? new Set()
        });
    }
    let activeMode = 'cold';
    let activeReason;
    const processListenComplete = (status, data, wire) => {
        const events = onComplete(status, data);
        eventQueueRaiseEventsForChangedPath(repo.eventQueue_, query._path, events);
        if (!isDefaultComplete) {
            return;
        }
        repoPublishListenOutcome(repo, pathString, {
            mode: activeMode,
            certified: status === 'ok',
            bytes: wire.bytes,
            reason: status === 'ok' ? activeReason : 'auth'
        });
        if (repo.persistence_ !== null) {
            if (status === 'ok') {
                // The certification confirms state whose changes (pushes / range
                // merges before it) were already reported individually.
                repoPersistAfterServerUpdate(repo, query._path, 'confirmed');
            }
            else {
                repo.persistence_.evict(query._path);
            }
        }
    };
    const sendListen = (mode, reason) => {
        activeMode = mode;
        activeReason = reason;
        if (isDefaultComplete) {
            repoPublishListenOutcome(repo, pathString, {
                mode,
                certified: false,
                bytes: 0,
                reason
            });
        }
        repo.server_.listen(query, currentHashFn, tag, (status, data, wire) => {
            const bufferRoot = repoBootBufferRootFor(repo, pathString);
            if (bufferRoot !== null) {
                repo.bootBuffers_.get(bufferRoot).push({
                    kind: 'complete',
                    apply: () => processListenComplete(status, data, wire)
                });
                return;
            }
            processListenComplete(status, data, wire);
        }, wire => {
            if (!isDefaultComplete) {
                return;
            }
            // Range merges preserve the restored base (incremental/cyan). A normal
            // data push replaces it, so expose a full authoritative fallback
            // (amber) before the listen response arrives.
            if (activeMode === 'restored' &&
                wire.dataReceived &&
                !wire.rangeMerged) {
                activeMode = 'fallback';
            }
            repoPublishListenOutcome(repo, pathString, {
                mode: activeMode,
                certified: false,
                bytes: wire.bytes,
                reason
            });
        });
    };
    const persistence = repo.persistence_;
    if (skipPersistence ||
        persistence === null ||
        !isDefaultComplete ||
        !persistence.isPersistentPath(pathString)) {
        sendListen('cold');
        return;
    }
    if (!persistence.isAuthScopeConfigured()) {
        // Identity hydration is local (Firebase Auth's persisted user), but it is
        // asynchronous. Keep the subscription inside the SDK until that identity
        // reaches persistence: restoring before then would either miss the cache
        // or, worse, replay another account's record. A bounded timeout preserves
        // the normal live-listen liveness contract if Auth integration wedges.
        const token = { cancelled: false };
        // Nothing is on the wire while waiting for the identity: a bulk cancel
        // (account switch) just restarts this subscription as a plain cold
        // listen so it cannot be stranded silent behind the cancelled token.
        token.reattachCold = () => {
            repoStartServerListen(repo, query, tag, currentHashFn, onComplete, true, authScopeTimeoutMs);
        };
        repo.pendingSeedRestores_.set(pathString, token);
        const isCurrent = () => !token.cancelled && repo.pendingSeedRestores_.get(pathString) === token;
        const cleanup = () => {
            token.authScopeUnsubscribe?.();
            if (token.authScopeTimer !== undefined) {
                clearTimeout(token.authScopeTimer);
            }
        };
        const resume = () => {
            if (!isCurrent() || !persistence.isAuthScopeConfigured()) {
                return;
            }
            cleanup();
            repo.pendingSeedRestores_.delete(pathString);
            repoStartServerListen(repo, query, tag, currentHashFn, onComplete, false, authScopeTimeoutMs);
        };
        repo.persistenceAuthScopeListeners_.add(resume);
        token.authScopeUnsubscribe = () => repo.persistenceAuthScopeListeners_.delete(resume);
        token.authScopeTimer = setTimeout(() => {
            if (!isCurrent()) {
                return;
            }
            cleanup();
            repo.pendingSeedRestores_.delete(pathString);
            // No identity means no safe cache namespace. Fall open to the ordinary
            // network listener rather than stranding the subscription forever.
            repoStartServerListen(repo, query, tag, currentHashFn, onComplete, true, authScopeTimeoutMs);
        }, authScopeTimeoutMs);
        // Close the check-to-subscribe race if a pre-auth peek configured the
        // manager between the first readiness check and listener registration.
        resume();
        return;
    }
    persistence.track(pathString);
    const token = { cancelled: false };
    // A bulk cancel (account switch) lands in one of two shapes: pre-manifest
    // (no listen sent yet — just start cold), or manifest-first (a seeded wire
    // listen is out and its boot buffer is about to be dropped — tear the
    // seeded listen down first, then start cold). Restore resolution observes
    // the cancelled token and exits without touching the new listen.
    token.reattachCold = () => {
        repo.pendingListenHashes_.clear(pathString);
        if (repo.bootBuffers_.has(pathString)) {
            repo.bootBuffers_.delete(pathString);
            repo.server_.unlisten(query, tag);
        }
        repoStartServerListen(repo, query, tag, currentHashFn, onComplete, true, authScopeTimeoutMs);
    };
    repo.pendingSeedRestores_.set(pathString, token);
    const isCurrent = () => !token.cancelled && repo.pendingSeedRestores_.get(pathString) === token;
    const finish = (mode, reason) => {
        if (!isCurrent()) {
            return;
        }
        repo.pendingSeedRestores_.delete(pathString);
        sendListen(mode, reason);
    };
    // MANIFEST-FIRST LISTEN. The stored manifest alone carries the protocol
    // hashes, so the listen goes out the moment it is read (milliseconds) —
    // the server's round-trip overlaps the tree record's read and Node
    // construction. Server pushes that arrive before the cached base has been
    // applied are buffered (repoOnDataUpdate/repoOnRangeMergeUpdate) and
    // replayed against the base, preserving arrival order. If anything about
    // the restore then fails, the buffered data is authoritative anyway — it
    // is applied and the listen simply behaves as an unseeded one.
    let sentFromManifest = false;
    let restartedCold = false;
    const restartCold = (reason = 'corrupt') => {
        if (!sentFromManifest || restartedCold || !isCurrent()) {
            return;
        }
        restartedCold = true;
        repo.pendingSeedRestores_.delete(pathString);
        repo.pendingListenHashes_.clear(pathString);
        repo.bootBuffers_.delete(pathString);
        // The compound response may omit every matching range, so buffered data
        // cannot reconstruct a missing base. Tear down the seeded listen and send
        // exactly one ordinary full listen.
        repo.server_.unlisten(query, tag);
        sendListen('fallback', reason);
    };
    const onManifest = () => {
        if (!isCurrent() || sentFromManifest) {
            return;
        }
        if (syncTreeGetCompleteServerCache(repo.serverSyncTree_, query._path) !==
            null ||
            syncTreeGetDescendantServerCacheStates(repo.serverSyncTree_, query._path)
                .length > 0) {
            // Existing server-cache state changes the exact tree the persisted
            // hashes describe; let the restore resolution pick the cold path.
            return;
        }
        sentFromManifest = true;
        repo.bootBuffers_.set(pathString, []);
        // Keep the pending token until range assembly finishes. A stop after this
        // send must cancel replay/restart as well as unlisten the wire request.
        sendListen('restored');
    };
    const drainBootBuffer = () => {
        const buffered = repo.bootBuffers_.get(pathString);
        if (buffered === undefined) {
            return;
        }
        repo.bootBuffers_.delete(pathString);
        for (const op of buffered) {
            if (op.kind === 'data') {
                repoOnDataUpdate(repo, op.pathString, op.data, op.isMerge, op.tag);
            }
            else if (op.kind === 'rm') {
                repoOnRangeMergeUpdate(repo, op.pathString, op.ranges, op.tag);
            }
            else {
                op.apply();
            }
        }
    };
    void persistence
        .restoreForListen(pathString, hashes => {
        repo.pendingListenHashes_.set(pathString, hashes.hash, hashes.compoundHash);
        onManifest();
    })
        .then(result => {
        if (!isCurrent()) {
            repo.pendingListenHashes_.clear(pathString);
            repo.bootBuffers_.delete(pathString);
            return;
        }
        const { record, reason } = result;
        if (record === null) {
            // A manifest-first listen may have omitted matching ranges. If
            // any referenced local payload is missing/corrupt, buffered deltas
            // are not a complete base: restart exactly once with a cold listen.
            repo.pendingListenHashes_.clear(pathString);
            if (sentFromManifest) {
                restartCold(reason ?? 'corrupt');
                return;
            }
            const fallback = reason === 'corrupt' || reason === 'timeout';
            finish(fallback ? 'fallback' : 'cold', reason);
            return;
        }
        if (!sentFromManifest) {
            // Manifest callback never fired usable (pre-existing server cache,
            // or a race); apply-then-listen, the pre-manifest-first sequence.
            if (syncTreeGetCompleteServerCache(repo.serverSyncTree_, query._path) !== null ||
                syncTreeGetDescendantServerCacheStates(repo.serverSyncTree_, query._path).length > 0) {
                repo.pendingListenHashes_.clear(pathString);
                finish('cold');
                return;
            }
        }
        try {
            const restored = stampSeedHashes(record.node, record.hash, record.compoundHash);
            const events = syncTreeApplyServerOverwrite(repo.serverSyncTree_, query._path, restored);
            eventQueueRaiseEventsForChangedPath(repo.eventQueue_, query._path, events);
            if (!isCurrent()) {
                repo.pendingListenHashes_.clear(pathString);
                repo.bootBuffers_.delete(pathString);
                return;
            }
            if (sentFromManifest) {
                repo.pendingSeedRestores_.delete(pathString);
            }
            // The hashes ride the seeded node from here on; the pending stamp
            // must not outlive the boot window (a re-listen after real server
            // updates must send the CURRENT tree's hashes, not the stored ones).
            repo.pendingListenHashes_.clear(pathString);
            if (sentFromManifest) {
                drainBootBuffer();
            }
            else {
                finish('restored');
            }
        }
        catch {
            repo.pendingListenHashes_.clear(pathString);
            persistence.invalidate(query._path);
            if (sentFromManifest) {
                restartCold('corrupt');
            }
            else {
                finish('fallback', 'corrupt');
            }
        }
    }, () => {
        repo.pendingListenHashes_.clear(pathString);
        if (sentFromManifest) {
            drainBootBuffer();
        }
        else {
            finish('fallback', 'corrupt');
        }
    });
}
/**
 * Stops a server listen. With persistence, a complete default listen may
 * still be waiting on its restore — cancel it so it never attaches — and its
 * root leaves write-through tracking (flushing the final tree to IndexedDB),
 * releasing the in-memory copy that only live listens need.
 */
function repoStopServerListen(repo, query, tag) {
    const pathString = query._path.toString();
    if (tag != null || !query._queryParams.loadsAllData()) {
        repo.server_.unlisten(query, tag);
        return;
    }
    const pending = repo.pendingSeedRestores_.get(pathString);
    if (pending && !repo.bootBuffers_.has(pathString)) {
        // Still waiting on the auth scope or manifest: the listen was never sent.
        repoCancelPendingSeedRestore(pending);
        repo.pendingSeedRestores_.delete(pathString);
    }
    else {
        if (pending) {
            repoCancelPendingSeedRestore(pending);
            repo.pendingSeedRestores_.delete(pathString);
        }
        repo.server_.unlisten(query, tag);
    }
    repo.bootBuffers_.delete(pathString);
    repo.pendingListenHashes_.clear(pathString);
    repo.listenOutcomes_.delete(pathString);
    repo.persistence_?.untrack(pathString);
}
/** Observe the outcome of one exact default listen. @internal */
function repoOnListenOutcome(repo, pathString, subscriber) {
    let state = repo.listenOutcomes_.get(pathString);
    if (!state) {
        // Subscribing before the listen starts is the natural ordering for a
        // caller that wires observability alongside its onValue().
        // repoStartServerListen preserves the subscriber set from a prior state,
        // so an empty pre-created state is carried into the real listen.
        state = { outcome: null, subscribers: new Set() };
        repo.listenOutcomes_.set(pathString, state);
    }
    state.subscribers.add(subscriber);
    if (state.outcome) {
        subscriber(state.outcome);
    }
    return () => state.subscribers.delete(subscriber);
}
/**
 * Activates persistence for a `{ persistent: true }` registration that JOINED
 * an already-listening default query (repoStartServerListen does not re-run
 * for it, so nothing else would ever track the root). Tracking is idempotent;
 * when the live listen has already certified a complete server cache, that
 * exact tree is seeded through the normal write-through path so the root is
 * warm on the next boot. A still-loading listen needs nothing here — its own
 * listen-complete certification write-through covers the root once tracked.
 * @internal
 */
function repoActivatePersistenceForJoinedListen(repo, path) {
    const persistence = repo.persistence_;
    const pathString = path.toString();
    if (persistence === null ||
        !persistence.isPersistentPath(pathString) ||
        persistence.trackedRootFor(pathString) === pathString ||
        repo.pendingSeedRestores_?.has(pathString)) {
        // No manager, not selected, already tracked by its own start path, or the
        // start path is still in flight (it will track on resolution).
        return;
    }
    persistence.track(pathString);
    const serverCache = syncTreeGetCompleteServerCache(repo.serverSyncTree_, path);
    if (serverCache !== null) {
        persistence.serverCacheUpdated(path, serverCache);
    }
}
function repoCancelPendingSeedRestores(repo, reattach = true) {
    repo.pendingListenHashes_.clearAll();
    const pendings = [...repo.pendingSeedRestores_.values()];
    repo.pendingSeedRestores_.clear();
    for (const pending of pendings) {
        repoCancelPendingSeedRestore(pending);
        if (reattach) {
            // The subscription outlives the cancelled restore (account switch,
            // persistence disabled): a pre-manifest token never sent its listen, a
            // manifest-first token has a seeded wire listen whose boot buffer is
            // about to be dropped. Either way, restart it as one ordinary cold
            // listen so the registration keeps receiving data. Dispose passes
            // false — there is no live subscription left to serve.
            pending.reattachCold?.();
        }
    }
}
function repoClearListenOutcomes(repo) {
    repo.listenOutcomes_.clear();
}
function repoNotifyPersistenceAuthScope(repo) {
    for (const listener of repo.persistenceAuthScopeListeners_) {
        exceptionGuard(listener);
    }
}
function repoDispose(repo) {
    repoInterrupt(repo);
    repoCancelPendingSeedRestores(repo, false);
    repoClearListenOutcomes(repo);
    repo.persistenceAuthScopeListeners_.clear();
    repo.persistence_?.dispose();
}
/**
 * Persistence write-through: after the server updated `path` (overwrite,
 * merge, range merge, or listen-complete certification), re-persist the
 * nearest persistence-tracked root containing it. Reads the SyncTree's own
 * complete server cache — the exact tree the SDK now holds as server truth —
 * so what is stored is always what was applied, never a re-derivation.
 */
/**
 * `preciseChange` distinguishes how much the caller knows about what this
 * update touched, so the flush can mark dirty ranges from the server-named
 * path instead of re-discovering it with a full identity diff:
 *   'at-path'   — an ordinary data push changed exactly the subtree at `path`
 *   'confirmed' — a listen certification: state already accounted, nothing new
 *   'unknown'   — a range merge or any update whose shape isn't named here
 */
function repoPersistAfterServerUpdate(repo, path, preciseChange) {
    const persistence = repo.persistence_;
    if (persistence === null) {
        return;
    }
    const rootString = persistence.trackedRootFor(path.toString());
    if (rootString === null) {
        return;
    }
    const rootPath = new Path(rootString);
    const serverCache = syncTreeGetCompleteServerCache(repo.serverSyncTree_, rootPath);
    if (serverCache !== null) {
        let changedPaths;
        if (preciseChange === 'at-path') {
            changedPaths = [pathSlice(newRelativePath(rootPath, path))];
        }
        else if (preciseChange === 'confirmed') {
            changedPaths = [];
        }
        persistence.serverCacheUpdated(rootPath, serverCache, changedPaths);
    }
}
/**
 * Handles a server range-merge push: the listen carried a compound hash and
 * only some of its ranges differed. Each wire range is
 * `{ s?, e?, m }` — exclusive-start path, inclusive-end path (either bound
 * may be missing, meaning open), and the update tree for that range —
 * applied in order against the locally cached server data.
 */
function repoOnRangeMergeUpdate(repo, pathString, ranges, tag) {
    // For testing.
    repo.dataUpdateCount++;
    {
        const bufferRoot = repoBootBufferRootFor(repo, pathString);
        if (bufferRoot !== null) {
            repo.bootBuffers_
                .get(bufferRoot)
                .push({ kind: 'rm', pathString, ranges, tag });
            return;
        }
    }
    const path = new Path(pathString);
    const merges = ranges.map(range => new RangeMerge(typeof range.s === 'string' ? new Path(range.s) : null, typeof range.e === 'string' ? new Path(range.e) : null, nodeFromJSON(range.m)));
    let events;
    if (tag) {
        events = syncTreeApplyTaggedRangeMerges(repo.serverSyncTree_, path, merges, tag);
    }
    else {
        events = syncTreeApplyServerRangeMerges(repo.serverSyncTree_, path, merges);
    }
    let affectedPath = path;
    if (events.length > 0) {
        // Since we have a listener outstanding for each transaction, receiving any events
        // is a proxy for some change having occurred.
        affectedPath = repoRerunTransactions(repo, path);
    }
    eventQueueRaiseEventsForChangedPath(repo.eventQueue_, affectedPath, events);
    if (tag == null) {
        // A range merge names leaf INTERVALS, not subtrees; the flush falls back
        // to the identity diff for this baseline.
        repoPersistAfterServerUpdate(repo, path, 'unknown');
    }
}
function repoOnConnectStatus(repo, connectStatus) {
    repoUpdateInfo(repo, 'connected', connectStatus);
    if (connectStatus === false) {
        repoRunOnDisconnectEvents(repo);
    }
}
function repoOnServerInfoUpdate(repo, updates) {
    each(updates, (key, value) => {
        repoUpdateInfo(repo, key, value);
    });
}
function repoUpdateInfo(repo, pathString, value) {
    const path = new Path('/.info/' + pathString);
    const newNode = nodeFromJSON(value);
    repo.infoData_.updateSnapshot(path, newNode);
    const events = syncTreeApplyServerOverwrite(repo.infoSyncTree_, path, newNode);
    eventQueueRaiseEventsForChangedPath(repo.eventQueue_, path, events);
}
function repoGetNextWriteId(repo) {
    return repo.nextWriteId_++;
}
/**
 * The purpose of `getValue` is to return the latest known value
 * satisfying `query`.
 *
 * This method will first check for in-memory cached values
 * belonging to active listeners. If they are found, such values
 * are considered to be the most up-to-date.
 *
 * If the client is not connected, this method will wait until the
 *  repo has established a connection and then request the value for `query`.
 * If the client is not able to retrieve the query result for another reason,
 * it reports an error.
 *
 * @param query - The query to surface a value for.
 */
function repoGetValue(repo, query, eventRegistration) {
    // Only active queries are cached. There is no persisted cache.
    const cached = syncTreeGetServerValue(repo.serverSyncTree_, query);
    if (cached != null) {
        return Promise.resolve(cached);
    }
    return repo.server_.get(query).then(payload => {
        const node = nodeFromJSON(payload).withIndex(query._queryParams.getIndex());
        /**
         * Below we simulate the actions of an `onlyOnce` `onValue()` event where:
         * Add an event registration,
         * Update data at the path,
         * Raise any events,
         * Cleanup the SyncTree
         */
        syncTreeAddEventRegistration(repo.serverSyncTree_, query, eventRegistration, true);
        let events;
        if (query._queryParams.loadsAllData()) {
            events = syncTreeApplyServerOverwrite(repo.serverSyncTree_, query._path, node);
        }
        else {
            const tag = syncTreeTagForQuery(repo.serverSyncTree_, query);
            events = syncTreeApplyTaggedQueryOverwrite(repo.serverSyncTree_, query._path, node, tag);
        }
        /*
         * We need to raise events in the scenario where `get()` is called at a parent path, and
         * while the `get()` is pending, `onValue` is called at a child location. While get() is waiting
         * for the data, `onValue` will register a new event. Then, get() will come back, and update the syncTree
         * and its corresponding serverCache, including the child location where `onValue` is called. Then,
         * `onValue` will receive the event from the server, but look at the syncTree and see that the data received
         * from the server is already at the SyncPoint, and so the `onValue` callback will never get fired.
         * Calling `eventQueueRaiseEventsForChangedPath()` is the correct way to propagate the events and
         * ensure the corresponding child events will get fired.
         */
        eventQueueRaiseEventsForChangedPath(repo.eventQueue_, query._path, events);
        syncTreeRemoveEventRegistration(repo.serverSyncTree_, query, eventRegistration, null, true);
        return node;
    }, err => {
        repoLog(repo, 'get for query ' + util.stringify(query) + ' failed: ' + err);
        return Promise.reject(new Error(err));
    });
}
function repoSetWithPriority(repo, path, newVal, newPriority, onComplete) {
    repoLog(repo, 'set', {
        path: path.toString(),
        value: newVal,
        priority: newPriority
    });
    // TODO: Optimize this behavior to either (a) store flag to skip resolving where possible and / or
    // (b) store unresolved paths on JSON parse
    const serverValues = repoGenerateServerValues(repo);
    const newNodeUnresolved = nodeFromJSON(newVal, newPriority);
    const existing = syncTreeCalcCompleteEventCache(repo.serverSyncTree_, path);
    const newNode = resolveDeferredValueSnapshot(newNodeUnresolved, existing, serverValues);
    const writeId = repoGetNextWriteId(repo);
    const events = syncTreeApplyUserOverwrite(repo.serverSyncTree_, path, newNode, writeId, true);
    eventQueueQueueEvents(repo.eventQueue_, events);
    repo.server_.put(path.toString(), newNodeUnresolved.val(/*export=*/ true), (status, errorReason) => {
        const success = status === 'ok';
        if (!success) {
            warn('set at ' + path + ' failed: ' + status);
        }
        const clearEvents = syncTreeAckUserWrite(repo.serverSyncTree_, writeId, !success);
        eventQueueRaiseEventsForChangedPath(repo.eventQueue_, path, clearEvents);
        repoCallOnCompleteCallback(repo, onComplete, status, errorReason);
    });
    const affectedPath = repoAbortTransactions(repo, path);
    repoRerunTransactions(repo, affectedPath);
    // We queued the events above, so just flush the queue here
    eventQueueRaiseEventsForChangedPath(repo.eventQueue_, affectedPath, []);
}
function repoUpdate(repo, path, childrenToMerge, onComplete) {
    repoLog(repo, 'update', { path: path.toString(), value: childrenToMerge });
    // Start with our existing data and merge each child into it.
    let empty = true;
    const serverValues = repoGenerateServerValues(repo);
    const changedChildren = {};
    each(childrenToMerge, (changedKey, changedValue) => {
        empty = false;
        changedChildren[changedKey] = resolveDeferredValueTree(pathChild(path, changedKey), nodeFromJSON(changedValue), repo.serverSyncTree_, serverValues);
    });
    if (!empty) {
        const writeId = repoGetNextWriteId(repo);
        const events = syncTreeApplyUserMerge(repo.serverSyncTree_, path, changedChildren, writeId);
        eventQueueQueueEvents(repo.eventQueue_, events);
        repo.server_.merge(path.toString(), childrenToMerge, (status, errorReason) => {
            const success = status === 'ok';
            if (!success) {
                warn('update at ' + path + ' failed: ' + status);
            }
            const clearEvents = syncTreeAckUserWrite(repo.serverSyncTree_, writeId, !success);
            const affectedPath = clearEvents.length > 0 ? repoRerunTransactions(repo, path) : path;
            eventQueueRaiseEventsForChangedPath(repo.eventQueue_, affectedPath, clearEvents);
            repoCallOnCompleteCallback(repo, onComplete, status, errorReason);
        });
        each(childrenToMerge, (changedPath) => {
            const affectedPath = repoAbortTransactions(repo, pathChild(path, changedPath));
            repoRerunTransactions(repo, affectedPath);
        });
        // We queued the events above, so just flush the queue here
        eventQueueRaiseEventsForChangedPath(repo.eventQueue_, path, []);
    }
    else {
        log("update() called with empty data.  Don't do anything.");
        repoCallOnCompleteCallback(repo, onComplete, 'ok', undefined);
    }
}
/**
 * Applies all of the changes stored up in the onDisconnect_ tree.
 */
function repoRunOnDisconnectEvents(repo) {
    repoLog(repo, 'onDisconnectEvents');
    const serverValues = repoGenerateServerValues(repo);
    const resolvedOnDisconnectTree = newSparseSnapshotTree();
    sparseSnapshotTreeForEachTree(repo.onDisconnect_, newEmptyPath(), (path, node) => {
        const resolved = resolveDeferredValueTree(path, node, repo.serverSyncTree_, serverValues);
        sparseSnapshotTreeRemember(resolvedOnDisconnectTree, path, resolved);
    });
    let events = [];
    sparseSnapshotTreeForEachTree(resolvedOnDisconnectTree, newEmptyPath(), (path, snap) => {
        events = events.concat(syncTreeApplyServerOverwrite(repo.serverSyncTree_, path, snap));
        const affectedPath = repoAbortTransactions(repo, path);
        repoRerunTransactions(repo, affectedPath);
    });
    repo.onDisconnect_ = newSparseSnapshotTree();
    eventQueueRaiseEventsForChangedPath(repo.eventQueue_, newEmptyPath(), events);
}
function repoOnDisconnectCancel(repo, path, onComplete) {
    repo.server_.onDisconnectCancel(path.toString(), (status, errorReason) => {
        if (status === 'ok') {
            sparseSnapshotTreeForget(repo.onDisconnect_, path);
        }
        repoCallOnCompleteCallback(repo, onComplete, status, errorReason);
    });
}
function repoOnDisconnectSet(repo, path, value, onComplete) {
    const newNode = nodeFromJSON(value);
    repo.server_.onDisconnectPut(path.toString(), newNode.val(/*export=*/ true), (status, errorReason) => {
        if (status === 'ok') {
            sparseSnapshotTreeRemember(repo.onDisconnect_, path, newNode);
        }
        repoCallOnCompleteCallback(repo, onComplete, status, errorReason);
    });
}
function repoOnDisconnectSetWithPriority(repo, path, value, priority, onComplete) {
    const newNode = nodeFromJSON(value, priority);
    repo.server_.onDisconnectPut(path.toString(), newNode.val(/*export=*/ true), (status, errorReason) => {
        if (status === 'ok') {
            sparseSnapshotTreeRemember(repo.onDisconnect_, path, newNode);
        }
        repoCallOnCompleteCallback(repo, onComplete, status, errorReason);
    });
}
function repoOnDisconnectUpdate(repo, path, childrenToMerge, onComplete) {
    if (util.isEmpty(childrenToMerge)) {
        log("onDisconnect().update() called with empty data.  Don't do anything.");
        repoCallOnCompleteCallback(repo, onComplete, 'ok', undefined);
        return;
    }
    repo.server_.onDisconnectMerge(path.toString(), childrenToMerge, (status, errorReason) => {
        if (status === 'ok') {
            each(childrenToMerge, (childName, childNode) => {
                const newChildNode = nodeFromJSON(childNode);
                sparseSnapshotTreeRemember(repo.onDisconnect_, pathChild(path, childName), newChildNode);
            });
        }
        repoCallOnCompleteCallback(repo, onComplete, status, errorReason);
    });
}
function repoAddEventCallbackForQuery(repo, query, eventRegistration) {
    let events;
    if (pathGetFront(query._path) === '.info') {
        events = syncTreeAddEventRegistration(repo.infoSyncTree_, query, eventRegistration);
    }
    else {
        events = syncTreeAddEventRegistration(repo.serverSyncTree_, query, eventRegistration);
    }
    eventQueueRaiseEventsAtPath(repo.eventQueue_, query._path, events);
}
function repoRemoveEventCallbackForQuery(repo, query, eventRegistration) {
    // These are guaranteed not to raise events, since we're not passing in a cancelError. However, we can future-proof
    // a little bit by handling the return values anyways.
    let events;
    if (pathGetFront(query._path) === '.info') {
        events = syncTreeRemoveEventRegistration(repo.infoSyncTree_, query, eventRegistration);
    }
    else {
        events = syncTreeRemoveEventRegistration(repo.serverSyncTree_, query, eventRegistration);
    }
    eventQueueRaiseEventsAtPath(repo.eventQueue_, query._path, events);
}
function repoInterrupt(repo) {
    if (repo.persistentConnection_) {
        repo.persistentConnection_.interrupt(INTERRUPT_REASON);
    }
    // Liveness is not eligibility: an offline tab keeps running (and
    // heartbeating), but its server cache is frozen — it must stop being any
    // root's persisted writer so an online tab can take over. Roots stay
    // tracked; repoResume re-acquires (see setNetworkSuspended).
    repo.persistence_?.setNetworkSuspended(true);
}
function repoResume(repo) {
    if (repo.persistentConnection_) {
        repo.persistentConnection_.resume(INTERRUPT_REASON);
    }
    repo.persistence_?.setNetworkSuspended(false);
}
function repoLog(repo, ...varArgs) {
    let prefix = '';
    if (repo.persistentConnection_) {
        prefix = repo.persistentConnection_.id + ':';
    }
    log(prefix, ...varArgs);
}
function repoCallOnCompleteCallback(repo, callback, status, errorReason) {
    if (callback) {
        exceptionGuard(() => {
            if (status === 'ok') {
                callback(null);
            }
            else {
                const code = (status || 'error').toUpperCase();
                let message = code;
                if (errorReason) {
                    message += ': ' + errorReason;
                }
                const error = new Error(message);
                // eslint-disable-next-line @typescript-eslint/no-explicit-any
                error.code = code;
                callback(error);
            }
        });
    }
}
/**
 * Creates a new transaction, adds it to the transactions we're tracking, and
 * sends it to the server if possible.
 *
 * @param path - Path at which to do transaction.
 * @param transactionUpdate - Update callback.
 * @param onComplete - Completion callback.
 * @param unwatcher - Function that will be called when the transaction no longer
 * need data updates for `path`.
 * @param applyLocally - Whether or not to make intermediate results visible
 */
function repoStartTransaction(repo, path, transactionUpdate, onComplete, unwatcher, applyLocally) {
    repoLog(repo, 'transaction on ' + path);
    // Initialize transaction.
    const transaction = {
        path,
        update: transactionUpdate,
        onComplete,
        // One of TransactionStatus enums.
        status: null,
        // Used when combining transactions at different locations to figure out
        // which one goes first.
        order: LUIDGenerator(),
        // Whether to raise local events for this transaction.
        applyLocally,
        // Count of how many times we've retried the transaction.
        retryCount: 0,
        // Function to call to clean up our .on() listener.
        unwatcher,
        // Stores why a transaction was aborted.
        abortReason: null,
        currentWriteId: null,
        currentInputSnapshot: null,
        currentOutputSnapshotRaw: null,
        currentOutputSnapshotResolved: null
    };
    // Run transaction initially.
    const currentState = repoGetLatestState(repo, path, undefined);
    transaction.currentInputSnapshot = currentState;
    const newVal = transaction.update(currentState.val());
    if (newVal === undefined) {
        // Abort transaction.
        transaction.unwatcher();
        transaction.currentOutputSnapshotRaw = null;
        transaction.currentOutputSnapshotResolved = null;
        if (transaction.onComplete) {
            transaction.onComplete(null, false, transaction.currentInputSnapshot);
        }
    }
    else {
        validateFirebaseData('transaction failed: Data returned ', newVal, transaction.path);
        // Mark as run and add to our queue.
        transaction.status = 0 /* TransactionStatus.RUN */;
        const queueNode = treeSubTree(repo.transactionQueueTree_, path);
        const nodeQueue = treeGetValue(queueNode) || [];
        nodeQueue.push(transaction);
        treeSetValue(queueNode, nodeQueue);
        // Update visibleData and raise events
        // Note: We intentionally raise events after updating all of our
        // transaction state, since the user could start new transactions from the
        // event callbacks.
        let priorityForNode;
        if (typeof newVal === 'object' &&
            newVal !== null &&
            util.contains(newVal, '.priority')) {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            priorityForNode = util.safeGet(newVal, '.priority');
            util.assert(isValidPriority(priorityForNode), 'Invalid priority returned by transaction. ' +
                'Priority must be a valid string, finite number, server value, or null.');
        }
        else {
            const currentNode = syncTreeCalcCompleteEventCache(repo.serverSyncTree_, path) ||
                ChildrenNode.EMPTY_NODE;
            priorityForNode = currentNode.getPriority().val();
        }
        const serverValues = repoGenerateServerValues(repo);
        const newNodeUnresolved = nodeFromJSON(newVal, priorityForNode);
        const newNode = resolveDeferredValueSnapshot(newNodeUnresolved, currentState, serverValues);
        transaction.currentOutputSnapshotRaw = newNodeUnresolved;
        transaction.currentOutputSnapshotResolved = newNode;
        transaction.currentWriteId = repoGetNextWriteId(repo);
        const events = syncTreeApplyUserOverwrite(repo.serverSyncTree_, path, newNode, transaction.currentWriteId, transaction.applyLocally);
        eventQueueRaiseEventsForChangedPath(repo.eventQueue_, path, events);
        repoSendReadyTransactions(repo, repo.transactionQueueTree_);
    }
}
/**
 * @param excludeSets - A specific set to exclude
 */
function repoGetLatestState(repo, path, excludeSets) {
    return (syncTreeCalcCompleteEventCache(repo.serverSyncTree_, path, excludeSets) ||
        ChildrenNode.EMPTY_NODE);
}
/**
 * Sends any already-run transactions that aren't waiting for outstanding
 * transactions to complete.
 *
 * Externally it's called with no arguments, but it calls itself recursively
 * with a particular transactionQueueTree node to recurse through the tree.
 *
 * @param node - transactionQueueTree node to start at.
 */
function repoSendReadyTransactions(repo, node = repo.transactionQueueTree_) {
    // Before recursing, make sure any completed transactions are removed.
    if (!node) {
        repoPruneCompletedTransactionsBelowNode(repo, node);
    }
    if (treeGetValue(node)) {
        const queue = repoBuildTransactionQueue(repo, node);
        util.assert(queue.length > 0, 'Sending zero length transaction queue');
        const allRun = queue.every((transaction) => transaction.status === 0 /* TransactionStatus.RUN */);
        // If they're all run (and not sent), we can send them.  Else, we must wait.
        if (allRun) {
            repoSendTransactionQueue(repo, treeGetPath(node), queue);
        }
    }
    else if (treeHasChildren(node)) {
        treeForEachChild(node, childNode => {
            repoSendReadyTransactions(repo, childNode);
        });
    }
}
/**
 * Given a list of run transactions, send them to the server and then handle
 * the result (success or failure).
 *
 * @param path - The location of the queue.
 * @param queue - Queue of transactions under the specified location.
 */
function repoSendTransactionQueue(repo, path, queue) {
    // Mark transactions as sent and increment retry count!
    const setsToIgnore = queue.map(txn => {
        return txn.currentWriteId;
    });
    const latestState = repoGetLatestState(repo, path, setsToIgnore);
    let snapToSend = latestState;
    const latestHash = latestState.hash();
    for (let i = 0; i < queue.length; i++) {
        const txn = queue[i];
        util.assert(txn.status === 0 /* TransactionStatus.RUN */, 'tryToSendTransactionQueue_: items in queue should all be run.');
        txn.status = 1 /* TransactionStatus.SENT */;
        txn.retryCount++;
        const relativePath = newRelativePath(path, txn.path);
        // If we've gotten to this point, the output snapshot must be defined.
        snapToSend = snapToSend.updateChild(relativePath /** @type {!Node} */, txn.currentOutputSnapshotRaw);
    }
    const dataToSend = snapToSend.val(true);
    const pathToSend = path;
    // Send the put.
    repo.server_.put(pathToSend.toString(), dataToSend, (status) => {
        repoLog(repo, 'transaction put response', {
            path: pathToSend.toString(),
            status
        });
        let events = [];
        if (status === 'ok') {
            // Queue up the callbacks and fire them after cleaning up all of our
            // transaction state, since the callback could trigger more
            // transactions or sets.
            const callbacks = [];
            for (let i = 0; i < queue.length; i++) {
                queue[i].status = 2 /* TransactionStatus.COMPLETED */;
                events = events.concat(syncTreeAckUserWrite(repo.serverSyncTree_, queue[i].currentWriteId));
                if (queue[i].onComplete) {
                    // We never unset the output snapshot, and given that this
                    // transaction is complete, it should be set
                    callbacks.push(() => queue[i].onComplete(null, true, queue[i].currentOutputSnapshotResolved));
                }
                queue[i].unwatcher();
            }
            // Now remove the completed transactions.
            repoPruneCompletedTransactionsBelowNode(repo, treeSubTree(repo.transactionQueueTree_, path));
            // There may be pending transactions that we can now send.
            repoSendReadyTransactions(repo, repo.transactionQueueTree_);
            eventQueueRaiseEventsForChangedPath(repo.eventQueue_, path, events);
            // Finally, trigger onComplete callbacks.
            for (let i = 0; i < callbacks.length; i++) {
                exceptionGuard(callbacks[i]);
            }
        }
        else {
            // transactions are no longer sent.  Update their status appropriately.
            if (status === 'datastale') {
                for (let i = 0; i < queue.length; i++) {
                    if (queue[i].status === 3 /* TransactionStatus.SENT_NEEDS_ABORT */) {
                        queue[i].status = 4 /* TransactionStatus.NEEDS_ABORT */;
                    }
                    else {
                        queue[i].status = 0 /* TransactionStatus.RUN */;
                    }
                }
            }
            else {
                warn('transaction at ' + pathToSend.toString() + ' failed: ' + status);
                for (let i = 0; i < queue.length; i++) {
                    queue[i].status = 4 /* TransactionStatus.NEEDS_ABORT */;
                    queue[i].abortReason = status;
                }
            }
            repoRerunTransactions(repo, path);
        }
    }, latestHash);
}
/**
 * Finds all transactions dependent on the data at changedPath and reruns them.
 *
 * Should be called any time cached data changes.
 *
 * Return the highest path that was affected by rerunning transactions. This
 * is the path at which events need to be raised for.
 *
 * @param changedPath - The path in mergedData that changed.
 * @returns The rootmost path that was affected by rerunning transactions.
 */
function repoRerunTransactions(repo, changedPath) {
    const rootMostTransactionNode = repoGetAncestorTransactionNode(repo, changedPath);
    const path = treeGetPath(rootMostTransactionNode);
    const queue = repoBuildTransactionQueue(repo, rootMostTransactionNode);
    repoRerunTransactionQueue(repo, queue, path);
    return path;
}
/**
 * Does all the work of rerunning transactions (as well as cleans up aborted
 * transactions and whatnot).
 *
 * @param queue - The queue of transactions to run.
 * @param path - The path the queue is for.
 */
function repoRerunTransactionQueue(repo, queue, path) {
    if (queue.length === 0) {
        return; // Nothing to do!
    }
    // Queue up the callbacks and fire them after cleaning up all of our
    // transaction state, since the callback could trigger more transactions or
    // sets.
    const callbacks = [];
    let events = [];
    // Ignore all of the sets we're going to re-run.
    const txnsToRerun = queue.filter(q => {
        return q.status === 0 /* TransactionStatus.RUN */;
    });
    const setsToIgnore = txnsToRerun.map(q => {
        return q.currentWriteId;
    });
    for (let i = 0; i < queue.length; i++) {
        const transaction = queue[i];
        const relativePath = newRelativePath(path, transaction.path);
        let abortTransaction = false, abortReason;
        util.assert(relativePath !== null, 'rerunTransactionsUnderNode_: relativePath should not be null.');
        if (transaction.status === 4 /* TransactionStatus.NEEDS_ABORT */) {
            abortTransaction = true;
            abortReason = transaction.abortReason;
            events = events.concat(syncTreeAckUserWrite(repo.serverSyncTree_, transaction.currentWriteId, true));
        }
        else if (transaction.status === 0 /* TransactionStatus.RUN */) {
            if (transaction.retryCount >= MAX_TRANSACTION_RETRIES) {
                abortTransaction = true;
                abortReason = 'maxretry';
                events = events.concat(syncTreeAckUserWrite(repo.serverSyncTree_, transaction.currentWriteId, true));
            }
            else {
                // This code reruns a transaction
                const currentNode = repoGetLatestState(repo, transaction.path, setsToIgnore);
                transaction.currentInputSnapshot = currentNode;
                const newData = queue[i].update(currentNode.val());
                if (newData !== undefined) {
                    validateFirebaseData('transaction failed: Data returned ', newData, transaction.path);
                    let newDataNode = nodeFromJSON(newData);
                    const hasExplicitPriority = typeof newData === 'object' &&
                        newData != null &&
                        util.contains(newData, '.priority');
                    if (!hasExplicitPriority) {
                        // Keep the old priority if there wasn't a priority explicitly specified.
                        newDataNode = newDataNode.updatePriority(currentNode.getPriority());
                    }
                    const oldWriteId = transaction.currentWriteId;
                    const serverValues = repoGenerateServerValues(repo);
                    const newNodeResolved = resolveDeferredValueSnapshot(newDataNode, currentNode, serverValues);
                    transaction.currentOutputSnapshotRaw = newDataNode;
                    transaction.currentOutputSnapshotResolved = newNodeResolved;
                    transaction.currentWriteId = repoGetNextWriteId(repo);
                    // Mutates setsToIgnore in place
                    setsToIgnore.splice(setsToIgnore.indexOf(oldWriteId), 1);
                    events = events.concat(syncTreeApplyUserOverwrite(repo.serverSyncTree_, transaction.path, newNodeResolved, transaction.currentWriteId, transaction.applyLocally));
                    events = events.concat(syncTreeAckUserWrite(repo.serverSyncTree_, oldWriteId, true));
                }
                else {
                    abortTransaction = true;
                    abortReason = 'nodata';
                    events = events.concat(syncTreeAckUserWrite(repo.serverSyncTree_, transaction.currentWriteId, true));
                }
            }
        }
        eventQueueRaiseEventsForChangedPath(repo.eventQueue_, path, events);
        events = [];
        if (abortTransaction) {
            // Abort.
            queue[i].status = 2 /* TransactionStatus.COMPLETED */;
            // Removing a listener can trigger pruning which can muck with
            // mergedData/visibleData (as it prunes data). So defer the unwatcher
            // until we're done.
            (function (unwatcher) {
                setTimeout(unwatcher, Math.floor(0));
            })(queue[i].unwatcher);
            if (queue[i].onComplete) {
                if (abortReason === 'nodata') {
                    callbacks.push(() => queue[i].onComplete(null, false, queue[i].currentInputSnapshot));
                }
                else {
                    callbacks.push(() => queue[i].onComplete(new Error(abortReason), false, null));
                }
            }
        }
    }
    // Clean up completed transactions.
    repoPruneCompletedTransactionsBelowNode(repo, repo.transactionQueueTree_);
    // Now fire callbacks, now that we're in a good, known state.
    for (let i = 0; i < callbacks.length; i++) {
        exceptionGuard(callbacks[i]);
    }
    // Try to send the transaction result to the server.
    repoSendReadyTransactions(repo, repo.transactionQueueTree_);
}
/**
 * Returns the rootmost ancestor node of the specified path that has a pending
 * transaction on it, or just returns the node for the given path if there are
 * no pending transactions on any ancestor.
 *
 * @param path - The location to start at.
 * @returns The rootmost node with a transaction.
 */
function repoGetAncestorTransactionNode(repo, path) {
    let front;
    // Start at the root and walk deeper into the tree towards path until we
    // find a node with pending transactions.
    let transactionNode = repo.transactionQueueTree_;
    front = pathGetFront(path);
    while (front !== null && treeGetValue(transactionNode) === undefined) {
        transactionNode = treeSubTree(transactionNode, front);
        path = pathPopFront(path);
        front = pathGetFront(path);
    }
    return transactionNode;
}
/**
 * Builds the queue of all transactions at or below the specified
 * transactionNode.
 *
 * @param transactionNode
 * @returns The generated queue.
 */
function repoBuildTransactionQueue(repo, transactionNode) {
    // Walk any child transaction queues and aggregate them into a single queue.
    const transactionQueue = [];
    repoAggregateTransactionQueuesForNode(repo, transactionNode, transactionQueue);
    // Sort them by the order the transactions were created.
    transactionQueue.sort((a, b) => a.order - b.order);
    return transactionQueue;
}
function repoAggregateTransactionQueuesForNode(repo, node, queue) {
    const nodeQueue = treeGetValue(node);
    if (nodeQueue) {
        for (let i = 0; i < nodeQueue.length; i++) {
            queue.push(nodeQueue[i]);
        }
    }
    treeForEachChild(node, child => {
        repoAggregateTransactionQueuesForNode(repo, child, queue);
    });
}
/**
 * Remove COMPLETED transactions at or below this node in the transactionQueueTree_.
 */
function repoPruneCompletedTransactionsBelowNode(repo, node) {
    const queue = treeGetValue(node);
    if (queue) {
        let to = 0;
        for (let from = 0; from < queue.length; from++) {
            if (queue[from].status !== 2 /* TransactionStatus.COMPLETED */) {
                queue[to] = queue[from];
                to++;
            }
        }
        queue.length = to;
        treeSetValue(node, queue.length > 0 ? queue : undefined);
    }
    treeForEachChild(node, childNode => {
        repoPruneCompletedTransactionsBelowNode(repo, childNode);
    });
}
/**
 * Aborts all transactions on ancestors or descendants of the specified path.
 * Called when doing a set() or update() since we consider them incompatible
 * with transactions.
 *
 * @param path - Path for which we want to abort related transactions.
 */
function repoAbortTransactions(repo, path) {
    const affectedPath = treeGetPath(repoGetAncestorTransactionNode(repo, path));
    const transactionNode = treeSubTree(repo.transactionQueueTree_, path);
    treeForEachAncestor(transactionNode, (node) => {
        repoAbortTransactionsOnNode(repo, node);
    });
    repoAbortTransactionsOnNode(repo, transactionNode);
    treeForEachDescendant(transactionNode, (node) => {
        repoAbortTransactionsOnNode(repo, node);
    });
    return affectedPath;
}
/**
 * Abort transactions stored in this transaction queue node.
 *
 * @param node - Node to abort transactions for.
 */
function repoAbortTransactionsOnNode(repo, node) {
    const queue = treeGetValue(node);
    if (queue) {
        // Queue up the callbacks and fire them after cleaning up all of our
        // transaction state, since the callback could trigger more transactions
        // or sets.
        const callbacks = [];
        // Go through queue.  Any already-sent transactions must be marked for
        // abort, while the unsent ones can be immediately aborted and removed.
        let events = [];
        let lastSent = -1;
        for (let i = 0; i < queue.length; i++) {
            if (queue[i].status === 3 /* TransactionStatus.SENT_NEEDS_ABORT */) ;
            else if (queue[i].status === 1 /* TransactionStatus.SENT */) {
                util.assert(lastSent === i - 1, 'All SENT items should be at beginning of queue.');
                lastSent = i;
                // Mark transaction for abort when it comes back.
                queue[i].status = 3 /* TransactionStatus.SENT_NEEDS_ABORT */;
                queue[i].abortReason = 'set';
            }
            else {
                util.assert(queue[i].status === 0 /* TransactionStatus.RUN */, 'Unexpected transaction status in abort');
                // We can abort it immediately.
                queue[i].unwatcher();
                events = events.concat(syncTreeAckUserWrite(repo.serverSyncTree_, queue[i].currentWriteId, true));
                if (queue[i].onComplete) {
                    callbacks.push(queue[i].onComplete.bind(null, new Error('set'), false, null));
                }
            }
        }
        if (lastSent === -1) {
            // We're not waiting for any sent transactions.  We can clear the queue.
            treeSetValue(node, undefined);
        }
        else {
            // Remove the transactions we aborted.
            queue.length = lastSent + 1;
        }
        // Now fire the callbacks.
        eventQueueRaiseEventsForChangedPath(repo.eventQueue_, treeGetPath(node), events);
        for (let i = 0; i < callbacks.length; i++) {
            exceptionGuard(callbacks[i]);
        }
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
function decodePath(pathString) {
    let pathStringDecoded = '';
    const pieces = pathString.split('/');
    for (let i = 0; i < pieces.length; i++) {
        if (pieces[i].length > 0) {
            let piece = pieces[i];
            try {
                piece = decodeURIComponent(piece.replace(/\+/g, ' '));
            }
            catch (e) { }
            pathStringDecoded += '/' + piece;
        }
    }
    return pathStringDecoded;
}
/**
 * @returns key value hash
 */
function decodeQuery(queryString) {
    const results = {};
    if (queryString.charAt(0) === '?') {
        queryString = queryString.substring(1);
    }
    for (const segment of queryString.split('&')) {
        if (segment.length === 0) {
            continue;
        }
        const kv = segment.split('=');
        if (kv.length === 2) {
            results[decodeURIComponent(kv[0])] = decodeURIComponent(kv[1]);
        }
        else {
            warn(`Invalid query segment '${segment}' in query '${queryString}'`);
        }
    }
    return results;
}
const parseRepoInfo = function (dataURL, nodeAdmin) {
    const parsedUrl = parseDatabaseURL(dataURL), namespace = parsedUrl.namespace;
    if (parsedUrl.domain === 'firebase.com') {
        fatal(parsedUrl.host +
            ' is no longer supported. ' +
            'Please use <YOUR FIREBASE>.firebaseio.com instead');
    }
    // Catch common error of uninitialized namespace value.
    if ((!namespace || namespace === 'undefined') &&
        parsedUrl.domain !== 'localhost') {
        fatal('Cannot parse Firebase url. Please use https://<YOUR FIREBASE>.firebaseio.com');
    }
    if (!parsedUrl.secure) {
        warnIfPageIsSecure();
    }
    const webSocketOnly = parsedUrl.scheme === 'ws' || parsedUrl.scheme === 'wss';
    return {
        repoInfo: new RepoInfo(parsedUrl.host, parsedUrl.secure, namespace, webSocketOnly, nodeAdmin, 
        /*persistenceKey=*/ '', 
        /*includeNamespaceInQueryParams=*/ namespace !== parsedUrl.subdomain),
        path: new Path(parsedUrl.pathString)
    };
};
const parseDatabaseURL = function (dataURL) {
    // Default to empty strings in the event of a malformed string.
    let host = '', domain = '', subdomain = '', pathString = '', namespace = '';
    // Always default to SSL, unless otherwise specified.
    let secure = true, scheme = 'https', port = 443;
    // Don't do any validation here. The caller is responsible for validating the result of parsing.
    if (typeof dataURL === 'string') {
        // Parse scheme.
        let colonInd = dataURL.indexOf('//');
        if (colonInd >= 0) {
            scheme = dataURL.substring(0, colonInd - 1);
            dataURL = dataURL.substring(colonInd + 2);
        }
        // Parse host, path, and query string.
        let slashInd = dataURL.indexOf('/');
        if (slashInd === -1) {
            slashInd = dataURL.length;
        }
        let questionMarkInd = dataURL.indexOf('?');
        if (questionMarkInd === -1) {
            questionMarkInd = dataURL.length;
        }
        host = dataURL.substring(0, Math.min(slashInd, questionMarkInd));
        if (slashInd < questionMarkInd) {
            // For pathString, questionMarkInd will always come after slashInd
            pathString = decodePath(dataURL.substring(slashInd, questionMarkInd));
        }
        const queryParams = decodeQuery(dataURL.substring(Math.min(dataURL.length, questionMarkInd)));
        // If we have a port, use scheme for determining if it's secure.
        colonInd = host.indexOf(':');
        if (colonInd >= 0) {
            secure = scheme === 'https' || scheme === 'wss';
            port = parseInt(host.substring(colonInd + 1), 10);
        }
        else {
            colonInd = host.length;
        }
        const hostWithoutPort = host.slice(0, colonInd);
        if (hostWithoutPort.toLowerCase() === 'localhost') {
            domain = 'localhost';
        }
        else if (hostWithoutPort.split('.').length <= 2) {
            domain = hostWithoutPort;
        }
        else {
            // Interpret the subdomain of a 3 or more component URL as the namespace name.
            const dotInd = host.indexOf('.');
            subdomain = host.substring(0, dotInd).toLowerCase();
            domain = host.substring(dotInd + 1);
            // Normalize namespaces to lowercase to share storage / connection.
            namespace = subdomain;
        }
        // Always treat the value of the `ns` as the namespace name if it is present.
        if ('ns' in queryParams) {
            namespace = queryParams['ns'];
        }
    }
    return {
        host,
        port,
        domain,
        subdomain,
        secure,
        scheme,
        pathString,
        namespace
    };
};

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// Modeled after base64 web-safe chars, but ordered by ASCII.
const PUSH_CHARS = '-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz';
/**
 * Fancy ID generator that creates 20-character string identifiers with the
 * following properties:
 *
 * 1. They're based on timestamp so that they sort *after* any existing ids.
 * 2. They contain 72-bits of random data after the timestamp so that IDs won't
 *    collide with other clients' IDs.
 * 3. They sort *lexicographically* (so the timestamp is converted to characters
 *    that will sort properly).
 * 4. They're monotonically increasing. Even if you generate more than one in
 *    the same timestamp, the latter ones will sort after the former ones. We do
 *    this by using the previous random bits but "incrementing" them by 1 (only
 *    in the case of a timestamp collision).
 */
const nextPushId = (function () {
    // Timestamp of last push, used to prevent local collisions if you push twice
    // in one ms.
    let lastPushTime = 0;
    // We generate 72-bits of randomness which get turned into 12 characters and
    // appended to the timestamp to prevent collisions with other clients. We
    // store the last characters we generated because in the event of a collision,
    // we'll use those same characters except "incremented" by one.
    const lastRandChars = [];
    return function (now) {
        const duplicateTime = now === lastPushTime;
        lastPushTime = now;
        let i;
        const timeStampChars = new Array(8);
        for (i = 7; i >= 0; i--) {
            timeStampChars[i] = PUSH_CHARS.charAt(now % 64);
            // NOTE: Can't use << here because javascript will convert to int and lose
            // the upper bits.
            now = Math.floor(now / 64);
        }
        util.assert(now === 0, 'Cannot push at time == 0');
        let id = timeStampChars.join('');
        if (!duplicateTime) {
            for (i = 0; i < 12; i++) {
                lastRandChars[i] = Math.floor(Math.random() * 64);
            }
        }
        else {
            // If the timestamp hasn't changed since last push, use the same random
            // number, except incremented by 1.
            for (i = 11; i >= 0 && lastRandChars[i] === 63; i--) {
                lastRandChars[i] = 0;
            }
            lastRandChars[i]++;
        }
        for (i = 0; i < 12; i++) {
            id += PUSH_CHARS.charAt(lastRandChars[i]);
        }
        util.assert(id.length === 20, 'nextPushId: Length should be 20.');
        return id;
    };
})();

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Encapsulates the data needed to raise an event
 */
class DataEvent {
    /**
     * @param eventType - One of: value, child_added, child_changed, child_moved, child_removed
     * @param eventRegistration - The function to call to with the event data. User provided
     * @param snapshot - The data backing the event
     * @param prevName - Optional, the name of the previous child for child_* events.
     */
    constructor(eventType, eventRegistration, snapshot, prevName) {
        this.eventType = eventType;
        this.eventRegistration = eventRegistration;
        this.snapshot = snapshot;
        this.prevName = prevName;
    }
    getPath() {
        const ref = this.snapshot.ref;
        if (this.eventType === 'value') {
            return ref._path;
        }
        else {
            return ref.parent._path;
        }
    }
    getEventType() {
        return this.eventType;
    }
    getEventRunner() {
        return this.eventRegistration.getEventRunner(this);
    }
    toString() {
        return (this.getPath().toString() +
            ':' +
            this.eventType +
            ':' +
            util.stringify(this.snapshot.exportVal()));
    }
}
class CancelEvent {
    constructor(eventRegistration, error, path) {
        this.eventRegistration = eventRegistration;
        this.error = error;
        this.path = path;
    }
    getPath() {
        return this.path;
    }
    getEventType() {
        return 'cancel';
    }
    getEventRunner() {
        return this.eventRegistration.getEventRunner(this);
    }
    toString() {
        return this.path.toString() + ':cancel';
    }
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * A wrapper class that converts events from the database@exp SDK to the legacy
 * Database SDK. Events are not converted directly as event registration relies
 * on reference comparison of the original user callback (see `matches()`) and
 * relies on equality of the legacy SDK's `context` object.
 */
class CallbackContext {
    constructor(snapshotCallback, cancelCallback) {
        this.snapshotCallback = snapshotCallback;
        this.cancelCallback = cancelCallback;
    }
    onValue(expDataSnapshot, previousChildName) {
        this.snapshotCallback.call(null, expDataSnapshot, previousChildName);
    }
    onCancel(error) {
        util.assert(this.hasCancelCallback, 'Raising a cancel event on a listener with no cancel callback');
        return this.cancelCallback.call(null, error);
    }
    get hasCancelCallback() {
        return !!this.cancelCallback;
    }
    matches(other) {
        return (this.snapshotCallback === other.snapshotCallback ||
            (this.snapshotCallback.userCallback !== undefined &&
                this.snapshotCallback.userCallback ===
                    other.snapshotCallback.userCallback &&
                this.snapshotCallback.context === other.snapshotCallback.context));
    }
}

/**
 * @license
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * The `onDisconnect` class allows you to write or clear data when your client
 * disconnects from the Database server. These updates occur whether your
 * client disconnects cleanly or not, so you can rely on them to clean up data
 * even if a connection is dropped or a client crashes.
 *
 * The `onDisconnect` class is most commonly used to manage presence in
 * applications where it is useful to detect how many clients are connected and
 * when other clients disconnect. See
 * {@link https://firebase.google.com/docs/database/web/offline-capabilities | Enabling Offline Capabilities in JavaScript}
 * for more information.
 *
 * To avoid problems when a connection is dropped before the requests can be
 * transferred to the Database server, these functions should be called before
 * writing any data.
 *
 * Note that `onDisconnect` operations are only triggered once. If you want an
 * operation to occur each time a disconnect occurs, you'll need to re-establish
 * the `onDisconnect` operations each time you reconnect.
 */
class OnDisconnect {
    /** @hideconstructor */
    constructor(_repo, _path) {
        this._repo = _repo;
        this._path = _path;
    }
    /**
     * Cancels all previously queued `onDisconnect()` set or update events for this
     * location and all children.
     *
     * If a write has been queued for this location via a `set()` or `update()` at a
     * parent location, the write at this location will be canceled, though writes
     * to sibling locations will still occur.
     *
     * @returns Resolves when synchronization to the server is complete.
     */
    cancel() {
        const deferred = new util.Deferred();
        repoOnDisconnectCancel(this._repo, this._path, deferred.wrapCallback(() => { }));
        return deferred.promise;
    }
    /**
     * Ensures the data at this location is deleted when the client is disconnected
     * (due to closing the browser, navigating to a new page, or network issues).
     *
     * @returns Resolves when synchronization to the server is complete.
     */
    remove() {
        validateWritablePath('OnDisconnect.remove', this._path);
        const deferred = new util.Deferred();
        repoOnDisconnectSet(this._repo, this._path, null, deferred.wrapCallback(() => { }));
        return deferred.promise;
    }
    /**
     * Ensures the data at this location is set to the specified value when the
     * client is disconnected (due to closing the browser, navigating to a new page,
     * or network issues).
     *
     * `set()` is especially useful for implementing "presence" systems, where a
     * value should be changed or cleared when a user disconnects so that they
     * appear "offline" to other users. See
     * {@link https://firebase.google.com/docs/database/web/offline-capabilities | Enabling Offline Capabilities in JavaScript}
     * for more information.
     *
     * Note that `onDisconnect` operations are only triggered once. If you want an
     * operation to occur each time a disconnect occurs, you'll need to re-establish
     * the `onDisconnect` operations each time.
     *
     * @param value - The value to be written to this location on disconnect (can
     * be an object, array, string, number, boolean, or null).
     * @returns Resolves when synchronization to the Database is complete.
     */
    set(value) {
        validateWritablePath('OnDisconnect.set', this._path);
        validateFirebaseDataArg('OnDisconnect.set', value, this._path, false);
        const deferred = new util.Deferred();
        repoOnDisconnectSet(this._repo, this._path, value, deferred.wrapCallback(() => { }));
        return deferred.promise;
    }
    /**
     * Ensures the data at this location is set to the specified value and priority
     * when the client is disconnected (due to closing the browser, navigating to a
     * new page, or network issues).
     *
     * @param value - The value to be written to this location on disconnect (can
     * be an object, array, string, number, boolean, or null).
     * @param priority - The priority to be written (string, number, or null).
     * @returns Resolves when synchronization to the Database is complete.
     */
    setWithPriority(value, priority) {
        validateWritablePath('OnDisconnect.setWithPriority', this._path);
        validateFirebaseDataArg('OnDisconnect.setWithPriority', value, this._path, false);
        validatePriority('OnDisconnect.setWithPriority', priority, false);
        const deferred = new util.Deferred();
        repoOnDisconnectSetWithPriority(this._repo, this._path, value, priority, deferred.wrapCallback(() => { }));
        return deferred.promise;
    }
    /**
     * Writes multiple values at this location when the client is disconnected (due
     * to closing the browser, navigating to a new page, or network issues).
     *
     * The `values` argument contains multiple property-value pairs that will be
     * written to the Database together. Each child property can either be a simple
     * property (for example, "name") or a relative path (for example, "name/first")
     * from the current location to the data to update.
     *
     * As opposed to the `set()` method, `update()` can be use to selectively update
     * only the referenced properties at the current location (instead of replacing
     * all the child properties at the current location).
     *
     * @param values - Object containing multiple values.
     * @returns Resolves when synchronization to the Database is complete.
     */
    update(values) {
        validateWritablePath('OnDisconnect.update', this._path);
        validateFirebaseMergeDataArg('OnDisconnect.update', values, this._path, false);
        const deferred = new util.Deferred();
        repoOnDisconnectUpdate(this._repo, this._path, values, deferred.wrapCallback(() => { }));
        return deferred.promise;
    }
}

/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * @internal
 */
class QueryImpl {
    /**
     * @hideconstructor
     */
    constructor(_repo, _path, _queryParams, _orderByCalled) {
        this._repo = _repo;
        this._path = _path;
        this._queryParams = _queryParams;
        this._orderByCalled = _orderByCalled;
    }
    get key() {
        if (pathIsEmpty(this._path)) {
            return null;
        }
        else {
            return pathGetBack(this._path);
        }
    }
    get ref() {
        return new ReferenceImpl(this._repo, this._path);
    }
    get _queryIdentifier() {
        const obj = queryParamsGetQueryObject(this._queryParams);
        const id = ObjectToUniqueKey(obj);
        return id === '{}' ? 'default' : id;
    }
    /**
     * An object representation of the query parameters used by this Query.
     */
    get _queryObject() {
        return queryParamsGetQueryObject(this._queryParams);
    }
    isEqual(other) {
        other = util.getModularInstance(other);
        if (!(other instanceof QueryImpl)) {
            return false;
        }
        const sameRepo = this._repo === other._repo;
        const samePath = pathEquals(this._path, other._path);
        const sameQueryIdentifier = this._queryIdentifier === other._queryIdentifier;
        return sameRepo && samePath && sameQueryIdentifier;
    }
    toJSON() {
        return this.toString();
    }
    toString() {
        return this._repo.toString() + pathToUrlEncodedString(this._path);
    }
}
/**
 * Validates that no other order by call has been made
 */
function validateNoPreviousOrderByCall(query, fnName) {
    if (query._orderByCalled === true) {
        throw new Error(fnName + ": You can't combine multiple orderBy calls.");
    }
}
/**
 * Validates start/end values for queries.
 */
function validateQueryEndpoints(params) {
    let startNode = null;
    let endNode = null;
    if (params.hasStart()) {
        startNode = params.getIndexStartValue();
    }
    if (params.hasEnd()) {
        endNode = params.getIndexEndValue();
    }
    if (params.getIndex() === KEY_INDEX) {
        const tooManyArgsError = 'Query: When ordering by key, you may only pass one argument to ' +
            'startAt(), endAt(), or equalTo().';
        const wrongArgTypeError = 'Query: When ordering by key, the argument passed to startAt(), startAfter(), ' +
            'endAt(), endBefore(), or equalTo() must be a string.';
        if (params.hasStart()) {
            const startName = params.getIndexStartName();
            if (startName !== MIN_NAME) {
                throw new Error(tooManyArgsError);
            }
            else if (typeof startNode !== 'string') {
                throw new Error(wrongArgTypeError);
            }
        }
        if (params.hasEnd()) {
            const endName = params.getIndexEndName();
            if (endName !== MAX_NAME) {
                throw new Error(tooManyArgsError);
            }
            else if (typeof endNode !== 'string') {
                throw new Error(wrongArgTypeError);
            }
        }
    }
    else if (params.getIndex() === PRIORITY_INDEX) {
        if ((startNode != null && !isValidPriority(startNode)) ||
            (endNode != null && !isValidPriority(endNode))) {
            throw new Error('Query: When ordering by priority, the first argument passed to startAt(), ' +
                'startAfter() endAt(), endBefore(), or equalTo() must be a valid priority value ' +
                '(null, a number, or a string).');
        }
    }
    else {
        util.assert(params.getIndex() instanceof PathIndex ||
            params.getIndex() === VALUE_INDEX, 'unknown index type.');
        if ((startNode != null && typeof startNode === 'object') ||
            (endNode != null && typeof endNode === 'object')) {
            throw new Error('Query: First argument passed to startAt(), startAfter(), endAt(), endBefore(), or ' +
                'equalTo() cannot be an object.');
        }
    }
}
/**
 * Validates that limit* has been called with the correct combination of parameters
 */
function validateLimit(params) {
    if (params.hasStart() &&
        params.hasEnd() &&
        params.hasLimit() &&
        !params.hasAnchoredLimit()) {
        throw new Error("Query: Can't combine startAt(), startAfter(), endAt(), endBefore(), and limit(). Use " +
            'limitToFirst() or limitToLast() instead.');
    }
}
/**
 * @internal
 */
class ReferenceImpl extends QueryImpl {
    /** @hideconstructor */
    constructor(repo, path) {
        super(repo, path, new QueryParams(), false);
    }
    get parent() {
        const parentPath = pathParent(this._path);
        return parentPath === null
            ? null
            : new ReferenceImpl(this._repo, parentPath);
    }
    get root() {
        let ref = this;
        while (ref.parent !== null) {
            ref = ref.parent;
        }
        return ref;
    }
}
/**
 * A `DataSnapshot` contains data from a Database location.
 *
 * Any time you read data from the Database, you receive the data as a
 * `DataSnapshot`. A `DataSnapshot` is passed to the event callbacks you attach
 * with `on()` or `once()`. You can extract the contents of the snapshot as a
 * JavaScript object by calling the `val()` method. Alternatively, you can
 * traverse into the snapshot by calling `child()` to return child snapshots
 * (which you could then call `val()` on).
 *
 * A `DataSnapshot` is an efficiently generated, immutable copy of the data at
 * a Database location. It cannot be modified and will never change (to modify
 * data, you always call the `set()` method on a `Reference` directly).
 */
class DataSnapshot {
    /**
     * @param _node - A SnapshotNode to wrap.
     * @param ref - The location this snapshot came from.
     * @param _index - The iteration order for this snapshot
     * @hideconstructor
     */
    constructor(_node, 
    /**
     * The location of this DataSnapshot.
     */
    ref, _index) {
        this._node = _node;
        this.ref = ref;
        this._index = _index;
    }
    /**
     * Gets the priority value of the data in this `DataSnapshot`.
     *
     * Applications need not use priority but can order collections by
     * ordinary properties (see
     * {@link https://firebase.google.com/docs/database/web/lists-of-data#sorting_and_filtering_data |Sorting and filtering data}
     * ).
     */
    get priority() {
        // typecast here because we never return deferred values or internal priorities (MAX_PRIORITY)
        return this._node.getPriority().val();
    }
    /**
     * The key (last part of the path) of the location of this `DataSnapshot`.
     *
     * The last token in a Database location is considered its key. For example,
     * "ada" is the key for the /users/ada/ node. Accessing the key on any
     * `DataSnapshot` will return the key for the location that generated it.
     * However, accessing the key on the root URL of a Database will return
     * `null`.
     */
    get key() {
        return this.ref.key;
    }
    /** Returns the number of child properties of this `DataSnapshot`. */
    get size() {
        return this._node.numChildren();
    }
    /**
     * Gets another `DataSnapshot` for the location at the specified relative path.
     *
     * Passing a relative path to the `child()` method of a DataSnapshot returns
     * another `DataSnapshot` for the location at the specified relative path. The
     * relative path can either be a simple child name (for example, "ada") or a
     * deeper, slash-separated path (for example, "ada/name/first"). If the child
     * location has no data, an empty `DataSnapshot` (that is, a `DataSnapshot`
     * whose value is `null`) is returned.
     *
     * @param path - A relative path to the location of child data.
     */
    child(path) {
        const childPath = new Path(path);
        const childRef = child(this.ref, path);
        return new DataSnapshot(this._node.getChild(childPath), childRef, PRIORITY_INDEX);
    }
    /**
     * Returns true if this `DataSnapshot` contains any data. It is slightly more
     * efficient than using `snapshot.val() !== null`.
     */
    exists() {
        return !this._node.isEmpty();
    }
    /**
     * Exports the entire contents of the DataSnapshot as a JavaScript object.
     *
     * The `exportVal()` method is similar to `val()`, except priority information
     * is included (if available), making it suitable for backing up your data.
     *
     * @returns The DataSnapshot's contents as a JavaScript value (Object,
     *   Array, string, number, boolean, or `null`).
     */
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    exportVal() {
        return this._node.val(true);
    }
    /**
     * Enumerates the top-level children in the `IteratedDataSnapshot`.
     *
     * Because of the way JavaScript objects work, the ordering of data in the
     * JavaScript object returned by `val()` is not guaranteed to match the
     * ordering on the server nor the ordering of `onChildAdded()` events. That is
     * where `forEach()` comes in handy. It guarantees the children of a
     * `DataSnapshot` will be iterated in their query order.
     *
     * If no explicit `orderBy*()` method is used, results are returned
     * ordered by key (unless priorities are used, in which case, results are
     * returned by priority).
     *
     * @param action - A function that will be called for each child DataSnapshot.
     * The callback can return true to cancel further enumeration.
     * @returns true if enumeration was canceled due to your callback returning
     * true.
     */
    forEach(action) {
        if (this._node.isLeafNode()) {
            return false;
        }
        const childrenNode = this._node;
        // Sanitize the return value to a boolean. ChildrenNode.forEachChild has a weird return type...
        return !!childrenNode.forEachChild(this._index, (key, node) => {
            return action(new DataSnapshot(node, child(this.ref, key), PRIORITY_INDEX));
        });
    }
    /**
     * Returns true if the specified child path has (non-null) data.
     *
     * @param path - A relative path to the location of a potential child.
     * @returns `true` if data exists at the specified child path; else
     *  `false`.
     */
    hasChild(path) {
        const childPath = new Path(path);
        return !this._node.getChild(childPath).isEmpty();
    }
    /**
     * Returns whether or not the `DataSnapshot` has any non-`null` child
     * properties.
     *
     * You can use `hasChildren()` to determine if a `DataSnapshot` has any
     * children. If it does, you can enumerate them using `forEach()`. If it
     * doesn't, then either this snapshot contains a primitive value (which can be
     * retrieved with `val()`) or it is empty (in which case, `val()` will return
     * `null`).
     *
     * @returns true if this snapshot has any children; else false.
     */
    hasChildren() {
        if (this._node.isLeafNode()) {
            return false;
        }
        else {
            return !this._node.isEmpty();
        }
    }
    /**
     * Returns a JSON-serializable representation of this object.
     */
    toJSON() {
        return this.exportVal();
    }
    /**
     * Extracts a JavaScript value from a `DataSnapshot`.
     *
     * Depending on the data in a `DataSnapshot`, the `val()` method may return a
     * scalar type (string, number, or boolean), an array, or an object. It may
     * also return null, indicating that the `DataSnapshot` is empty (contains no
     * data).
     *
     * @returns The DataSnapshot's contents as a JavaScript value (Object,
     *   Array, string, number, boolean, or `null`).
     */
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    val() {
        return this._node.val();
    }
}
/**
 * Consumes the optimistic peek's one-boot materialization for exactly this
 * snapshot's immutable node, or returns `undefined` when none exists (no
 * peek, a different node, or already consumed — each stamp is returned at
 * most once).
 *
 * This is the deliberate opt-in half of the peek→listener handoff (see
 * ServerCacheSeed): `getPersistedValue()` materializes the restored tree
 * once, and the listener that replays the SAME immutable nodes can adopt
 * that materialization instead of walking the tree a second time.
 * Correctness is by construction — a Node is immutable, so a stamp can only
 * be returned for exactly the data it was computed from; any server delta
 * between peek and replay creates a new node, which misses.
 *
 * The returned object is the SAME object `getPersistedValue()` returned to
 * the application — shared by design, so an optimistic paint and the live
 * tree keep child identity (memoized consumers see unchanged branches as
 * unchanged). Treat it as immutable. `snapshot.val()` itself never consumes
 * a stamp and always returns fresh objects.
 *
 * @public
 */
function consumePersistedMaterialization(snapshot) {
    return consumeMaterializedValue(snapshot._node);
}
/**
 *
 * Returns a `Reference` representing the location in the Database
 * corresponding to the provided path. If no path is provided, the `Reference`
 * will point to the root of the Database.
 *
 * @param db - The database instance to obtain a reference for.
 * @param path - Optional path representing the location the returned
 *   `Reference` will point. If not provided, the returned `Reference` will
 *   point to the root of the Database.
 * @returns If a path is provided, a `Reference`
 *   pointing to the provided path. Otherwise, a `Reference` pointing to the
 *   root of the Database.
 */
function ref(db, path) {
    db = util.getModularInstance(db);
    db._checkNotDeleted('ref');
    return path !== undefined ? child(db._root, path) : db._root;
}
/**
 * Returns a `Reference` representing the location in the Database
 * corresponding to the provided Firebase URL.
 *
 * An exception is thrown if the URL is not a valid Firebase Database URL or it
 * has a different domain than the current `Database` instance.
 *
 * Note that all query parameters (`orderBy`, `limitToLast`, etc.) are ignored
 * and are not applied to the returned `Reference`.
 *
 * @param db - The database instance to obtain a reference for.
 * @param url - The Firebase URL at which the returned `Reference` will
 *   point.
 * @returns A `Reference` pointing to the provided
 *   Firebase URL.
 */
function refFromURL(db, url) {
    db = util.getModularInstance(db);
    db._checkNotDeleted('refFromURL');
    const parsedURL = parseRepoInfo(url, db._repo.repoInfo_.nodeAdmin);
    validateUrl('refFromURL', parsedURL);
    const repoInfo = parsedURL.repoInfo;
    if (!db._repo.repoInfo_.isCustomHost() &&
        repoInfo.host !== db._repo.repoInfo_.host) {
        fatal('refFromURL' +
            ': Host name does not match the current database: ' +
            '(found ' +
            repoInfo.host +
            ' but expected ' +
            db._repo.repoInfo_.host +
            ')');
    }
    return ref(db, parsedURL.path.toString());
}
/**
 * Gets a `Reference` for the location at the specified relative path.
 *
 * The relative path can either be a simple child name (for example, "ada") or
 * a deeper slash-separated path (for example, "ada/name/first").
 *
 * @param parent - The parent location.
 * @param path - A relative path from this location to the desired child
 *   location.
 * @returns The specified child location.
 */
function child(parent, path) {
    parent = util.getModularInstance(parent);
    if (pathGetFront(parent._path) === null) {
        validateRootPathString('child', 'path', path, false);
    }
    else {
        validatePathString('child', 'path', path, false);
    }
    return new ReferenceImpl(parent._repo, pathChild(parent._path, path));
}
/**
 * Returns an `OnDisconnect` object - see
 * {@link https://firebase.google.com/docs/database/web/offline-capabilities | Enabling Offline Capabilities in JavaScript}
 * for more information on how to use it.
 *
 * @param ref - The reference to add OnDisconnect triggers for.
 */
function onDisconnect(ref) {
    ref = util.getModularInstance(ref);
    return new OnDisconnect(ref._repo, ref._path);
}
/**
 * Generates a new child location using a unique key and returns its
 * `Reference`.
 *
 * This is the most common pattern for adding data to a collection of items.
 *
 * If you provide a value to `push()`, the value is written to the
 * generated location. If you don't pass a value, nothing is written to the
 * database and the child remains empty (but you can use the `Reference`
 * elsewhere).
 *
 * The unique keys generated by `push()` are ordered by the current time, so the
 * resulting list of items is chronologically sorted. The keys are also
 * designed to be unguessable (they contain 72 random bits of entropy).
 *
 * See {@link https://firebase.google.com/docs/database/web/lists-of-data#append_to_a_list_of_data | Append to a list of data}.
 * See {@link https://firebase.googleblog.com/2015/02/the-2120-ways-to-ensure-unique_68.html | The 2^120 Ways to Ensure Unique Identifiers}.
 *
 * @param parent - The parent location.
 * @param value - Optional value to be written at the generated location.
 * @returns Combined `Promise` and `Reference`; resolves when write is complete,
 * but can be used immediately as the `Reference` to the child location.
 */
function push(parent, value) {
    parent = util.getModularInstance(parent);
    validateWritablePath('push', parent._path);
    validateFirebaseDataArg('push', value, parent._path, true);
    const now = repoServerTime(parent._repo);
    const name = nextPushId(now);
    // push() returns a ThennableReference whose promise is fulfilled with a
    // regular Reference. We use child() to create handles to two different
    // references. The first is turned into a ThennableReference below by adding
    // then() and catch() methods and is used as the return value of push(). The
    // second remains a regular Reference and is used as the fulfilled value of
    // the first ThennableReference.
    const thenablePushRef = child(parent, name);
    const pushRef = child(parent, name);
    let promise;
    if (value != null) {
        promise = set(pushRef, value).then(() => pushRef);
    }
    else {
        promise = Promise.resolve(pushRef);
    }
    thenablePushRef.then = promise.then.bind(promise);
    thenablePushRef.catch = promise.then.bind(promise, undefined);
    return thenablePushRef;
}
/**
 * Removes the data at this Database location.
 *
 * Any data at child locations will also be deleted.
 *
 * The effect of the remove will be visible immediately and the corresponding
 * event 'value' will be triggered. Synchronization of the remove to the
 * Firebase servers will also be started, and the returned Promise will resolve
 * when complete. If provided, the onComplete callback will be called
 * asynchronously after synchronization has finished.
 *
 * @param ref - The location to remove.
 * @returns Resolves when remove on server is complete.
 */
function remove(ref) {
    validateWritablePath('remove', ref._path);
    return set(ref, null);
}
/**
 * Writes data to this Database location.
 *
 * This will overwrite any data at this location and all child locations.
 *
 * The effect of the write will be visible immediately, and the corresponding
 * events ("value", "child_added", etc.) will be triggered. Synchronization of
 * the data to the Firebase servers will also be started, and the returned
 * Promise will resolve when complete. If provided, the `onComplete` callback
 * will be called asynchronously after synchronization has finished.
 *
 * Passing `null` for the new value is equivalent to calling `remove()`; namely,
 * all data at this location and all child locations will be deleted.
 *
 * `set()` will remove any priority stored at this location, so if priority is
 * meant to be preserved, you need to use `setWithPriority()` instead.
 *
 * Note that modifying data with `set()` will cancel any pending transactions
 * at that location, so extreme care should be taken if mixing `set()` and
 * `transaction()` to modify the same data.
 *
 * A single `set()` will generate a single "value" event at the location where
 * the `set()` was performed.
 *
 * @param ref - The location to write to.
 * @param value - The value to be written (string, number, boolean, object,
 *   array, or null).
 * @returns Resolves when write to server is complete.
 */
function set(ref, value) {
    ref = util.getModularInstance(ref);
    validateWritablePath('set', ref._path);
    validateFirebaseDataArg('set', value, ref._path, false);
    const deferred = new util.Deferred();
    repoSetWithPriority(ref._repo, ref._path, value, 
    /*priority=*/ null, deferred.wrapCallback(() => { }));
    return deferred.promise;
}
/**
 * Sets a priority for the data at this Database location.
 *
 * Applications need not use priority but can order collections by
 * ordinary properties (see
 * {@link https://firebase.google.com/docs/database/web/lists-of-data#sorting_and_filtering_data | Sorting and filtering data}
 * ).
 *
 * @param ref - The location to write to.
 * @param priority - The priority to be written (string, number, or null).
 * @returns Resolves when write to server is complete.
 */
function setPriority(ref, priority) {
    ref = util.getModularInstance(ref);
    validateWritablePath('setPriority', ref._path);
    validatePriority('setPriority', priority, false);
    const deferred = new util.Deferred();
    repoSetWithPriority(ref._repo, pathChild(ref._path, '.priority'), priority, null, deferred.wrapCallback(() => { }));
    return deferred.promise;
}
/**
 * Writes data the Database location. Like `set()` but also specifies the
 * priority for that data.
 *
 * Applications need not use priority but can order collections by
 * ordinary properties (see
 * {@link https://firebase.google.com/docs/database/web/lists-of-data#sorting_and_filtering_data | Sorting and filtering data}
 * ).
 *
 * @param ref - The location to write to.
 * @param value - The value to be written (string, number, boolean, object,
 *   array, or null).
 * @param priority - The priority to be written (string, number, or null).
 * @returns Resolves when write to server is complete.
 */
function setWithPriority(ref, value, priority) {
    validateWritablePath('setWithPriority', ref._path);
    validateFirebaseDataArg('setWithPriority', value, ref._path, false);
    validatePriority('setWithPriority', priority, false);
    if (ref.key === '.length' || ref.key === '.keys') {
        throw 'setWithPriority failed: ' + ref.key + ' is a read-only object.';
    }
    const deferred = new util.Deferred();
    repoSetWithPriority(ref._repo, ref._path, value, priority, deferred.wrapCallback(() => { }));
    return deferred.promise;
}
/**
 * Writes multiple values to the Database at once.
 *
 * The `values` argument contains multiple property-value pairs that will be
 * written to the Database together. Each child property can either be a simple
 * property (for example, "name") or a relative path (for example,
 * "name/first") from the current location to the data to update.
 *
 * As opposed to the `set()` method, `update()` can be use to selectively update
 * only the referenced properties at the current location (instead of replacing
 * all the child properties at the current location).
 *
 * The effect of the write will be visible immediately, and the corresponding
 * events ('value', 'child_added', etc.) will be triggered. Synchronization of
 * the data to the Firebase servers will also be started, and the returned
 * Promise will resolve when complete. If provided, the `onComplete` callback
 * will be called asynchronously after synchronization has finished.
 *
 * A single `update()` will generate a single "value" event at the location
 * where the `update()` was performed, regardless of how many children were
 * modified.
 *
 * Note that modifying data with `update()` will cancel any pending
 * transactions at that location, so extreme care should be taken if mixing
 * `update()` and `transaction()` to modify the same data.
 *
 * Passing `null` to `update()` will remove the data at this location.
 *
 * See
 * {@link https://firebase.googleblog.com/2015/09/introducing-multi-location-updates-and_86.html | Introducing multi-location updates and more}.
 *
 * @param ref - The location to write to.
 * @param values - Object containing multiple values.
 * @returns Resolves when update on server is complete.
 */
function update(ref, values) {
    validateFirebaseMergeDataArg('update', values, ref._path, false);
    const deferred = new util.Deferred();
    repoUpdate(ref._repo, ref._path, values, deferred.wrapCallback(() => { }));
    return deferred.promise;
}
/**
 * Gets the most up-to-date result for this query.
 *
 * @param query - The query to run.
 * @returns A `Promise` which resolves to the resulting DataSnapshot if a value is
 * available, or rejects if the client is unable to return a value (e.g., if the
 * server is unreachable and there is nothing cached).
 */
function get(query) {
    query = util.getModularInstance(query);
    const callbackContext = new CallbackContext(() => { });
    const container = new ValueEventRegistration(callbackContext);
    return repoGetValue(query._repo, query, container).then(node => {
        return new DataSnapshot(node, new ReferenceImpl(query._repo, query._path), query._queryParams.getIndex());
    });
}
/**
 * Represents registration for 'value' events.
 */
class ValueEventRegistration {
    constructor(callbackContext, onRemove) {
        this.callbackContext = callbackContext;
        this.onRemove = onRemove;
    }
    respondsTo(eventType) {
        return eventType === 'value';
    }
    createEvent(change, query) {
        const index = query._queryParams.getIndex();
        return new DataEvent('value', this, new DataSnapshot(change.snapshotNode, new ReferenceImpl(query._repo, query._path), index));
    }
    getEventRunner(eventData) {
        if (eventData.getEventType() === 'cancel') {
            return () => this.callbackContext.onCancel(eventData.error);
        }
        else {
            return () => this.callbackContext.onValue(eventData.snapshot, null);
        }
    }
    createCancelEvent(error, path) {
        if (this.callbackContext.hasCancelCallback) {
            return new CancelEvent(this, error, path);
        }
        else {
            return null;
        }
    }
    matches(other) {
        if (!(other instanceof ValueEventRegistration)) {
            return false;
        }
        else if (!other.callbackContext || !this.callbackContext) {
            // If no callback specified, we consider it to match any callback.
            return true;
        }
        else {
            return other.callbackContext.matches(this.callbackContext);
        }
    }
    hasAnyCallback() {
        return this.callbackContext !== null;
    }
}
/**
 * Represents the registration of a child_x event.
 */
class ChildEventRegistration {
    constructor(eventType, callbackContext, onRemove) {
        this.eventType = eventType;
        this.callbackContext = callbackContext;
        this.onRemove = onRemove;
    }
    respondsTo(eventType) {
        let eventToCheck = eventType === 'children_added' ? 'child_added' : eventType;
        eventToCheck =
            eventToCheck === 'children_removed' ? 'child_removed' : eventToCheck;
        return this.eventType === eventToCheck;
    }
    createCancelEvent(error, path) {
        if (this.callbackContext.hasCancelCallback) {
            return new CancelEvent(this, error, path);
        }
        else {
            return null;
        }
    }
    createEvent(change, query) {
        util.assert(change.childName != null, 'Child events should have a childName.');
        const childRef = child(new ReferenceImpl(query._repo, query._path), change.childName);
        const index = query._queryParams.getIndex();
        return new DataEvent(change.type, this, new DataSnapshot(change.snapshotNode, childRef, index), change.prevName);
    }
    getEventRunner(eventData) {
        if (eventData.getEventType() === 'cancel') {
            return () => this.callbackContext.onCancel(eventData.error);
        }
        else {
            return () => this.callbackContext.onValue(eventData.snapshot, eventData.prevName);
        }
    }
    matches(other) {
        if (other instanceof ChildEventRegistration) {
            return (this.eventType === other.eventType &&
                (!this.callbackContext ||
                    !other.callbackContext ||
                    this.callbackContext.matches(other.callbackContext)));
        }
        return false;
    }
    hasAnyCallback() {
        return !!this.callbackContext;
    }
}
function addEventListener(query, eventType, callback, cancelCallbackOrListenOptions, options) {
    let cancelCallback;
    if (typeof cancelCallbackOrListenOptions === 'object') {
        cancelCallback = undefined;
        options = cancelCallbackOrListenOptions;
    }
    if (typeof cancelCallbackOrListenOptions === 'function') {
        cancelCallback = cancelCallbackOrListenOptions;
    }
    if (options?.persistent && query._queryIdentifier !== 'default') {
        throw new Error('persistent listener option is only supported for complete, unfiltered references.');
    }
    if (options && options.onlyOnce) {
        const userCallback = callback;
        const onceCallback = (dataSnapshot, previousChildName) => {
            repoRemoveEventCallbackForQuery(query._repo, query, container);
            userCallback(dataSnapshot, previousChildName);
        };
        onceCallback.userCallback = callback.userCallback;
        onceCallback.context = callback.context;
        callback = onceCallback;
    }
    let persistenceReleased = false;
    const releasePersistence = options?.persistent
        ? () => {
            if (persistenceReleased) {
                return;
            }
            persistenceReleased = true;
            query._repo.persistence_?.setPersistentPath(query._path.toString(), false);
        }
        : undefined;
    if (options?.persistent) {
        // Select before adding the registration: the first registration starts
        // the wire listen synchronously, and persistence must already own it.
        query._repo.persistence_?.setPersistentPath(query._path.toString(), true);
    }
    const callbackContext = new CallbackContext(callback, cancelCallback || undefined);
    const container = eventType === 'value'
        ? new ValueEventRegistration(callbackContext, releasePersistence)
        : new ChildEventRegistration(eventType, callbackContext, releasePersistence);
    try {
        repoAddEventCallbackForQuery(query._repo, query, container);
    }
    catch (error) {
        releasePersistence?.();
        throw error;
    }
    if (options?.persistent) {
        // Selecting before registration covers the registration that CREATES the
        // wire listen (repoStartServerListen tracks the root). When this
        // registration JOINED an already-listening default query instead, that
        // start path never re-runs — activate persistence against the live
        // listen: track the root and seed write-through from the complete server
        // cache the listen already certified (nothing to do while it is still
        // loading; the listen-complete certification write-through covers that
        // ordering once tracked).
        repoActivatePersistenceForJoinedListen(query._repo, query._path);
    }
    return () => repoRemoveEventCallbackForQuery(query._repo, query, container);
}
function onValue(query, callback, cancelCallbackOrListenOptions, options) {
    return addEventListener(query, 'value', callback, cancelCallbackOrListenOptions, options);
}
function onChildAdded(query, callback, cancelCallbackOrListenOptions, options) {
    return addEventListener(query, 'child_added', callback, cancelCallbackOrListenOptions, options);
}
function onChildChanged(query, callback, cancelCallbackOrListenOptions, options) {
    return addEventListener(query, 'child_changed', callback, cancelCallbackOrListenOptions, options);
}
function onChildMoved(query, callback, cancelCallbackOrListenOptions, options) {
    return addEventListener(query, 'child_moved', callback, cancelCallbackOrListenOptions, options);
}
function onChildRemoved(query, callback, cancelCallbackOrListenOptions, options) {
    return addEventListener(query, 'child_removed', callback, cancelCallbackOrListenOptions, options);
}
/**
 * Detaches a callback previously attached with the corresponding `on*()` (`onValue`, `onChildAdded`) listener.
 * Note: This is not the recommended way to remove a listener. Instead, please use the returned callback function from
 * the respective `on*` callbacks.
 *
 * Detach a callback previously attached with `on*()`. Calling `off()` on a parent listener
 * will not automatically remove listeners registered on child nodes, `off()`
 * must also be called on any child listeners to remove the callback.
 *
 * If a callback is not specified, all callbacks for the specified eventType
 * will be removed. Similarly, if no eventType is specified, all callbacks
 * for the `Reference` will be removed.
 *
 * Individual listeners can also be removed by invoking their unsubscribe
 * callbacks.
 *
 * @param query - The query that the listener was registered with.
 * @param eventType - One of the following strings: "value", "child_added",
 * "child_changed", "child_removed", or "child_moved." If omitted, all callbacks
 * for the `Reference` will be removed.
 * @param callback - The callback function that was passed to `on()` or
 * `undefined` to remove all callbacks.
 */
function off(query, eventType, callback) {
    let container = null;
    const expCallback = callback ? new CallbackContext(callback) : null;
    if (eventType === 'value') {
        container = new ValueEventRegistration(expCallback);
    }
    else if (eventType) {
        container = new ChildEventRegistration(eventType, expCallback);
    }
    repoRemoveEventCallbackForQuery(query._repo, query, container);
}
/**
 * A `QueryConstraint` is used to narrow the set of documents returned by a
 * Database query. `QueryConstraint`s are created by invoking {@link endAt},
 * {@link endBefore}, {@link startAt}, {@link startAfter}, {@link
 * limitToFirst}, {@link limitToLast}, {@link orderByChild},
 * {@link orderByChild}, {@link orderByKey} , {@link orderByPriority} ,
 * {@link orderByValue}  or {@link equalTo} and
 * can then be passed to {@link query} to create a new query instance that
 * also contains this `QueryConstraint`.
 */
class QueryConstraint {
}
class QueryEndAtConstraint extends QueryConstraint {
    constructor(_value, _key) {
        super();
        this._value = _value;
        this._key = _key;
        this.type = 'endAt';
    }
    _apply(query) {
        validateFirebaseDataArg('endAt', this._value, query._path, true);
        const newParams = queryParamsEndAt(query._queryParams, this._value, this._key);
        validateLimit(newParams);
        validateQueryEndpoints(newParams);
        if (query._queryParams.hasEnd()) {
            throw new Error('endAt: Starting point was already set (by another call to endAt, ' +
                'endBefore or equalTo).');
        }
        return new QueryImpl(query._repo, query._path, newParams, query._orderByCalled);
    }
}
/**
 * Creates a `QueryConstraint` with the specified ending point.
 *
 * Using `startAt()`, `startAfter()`, `endBefore()`, `endAt()` and `equalTo()`
 * allows you to choose arbitrary starting and ending points for your queries.
 *
 * The ending point is inclusive, so children with exactly the specified value
 * will be included in the query. The optional key argument can be used to
 * further limit the range of the query. If it is specified, then children that
 * have exactly the specified value must also have a key name less than or equal
 * to the specified key.
 *
 * You can read more about `endAt()` in
 * {@link https://firebase.google.com/docs/database/web/lists-of-data#filtering_data | Filtering data}.
 *
 * @param value - The value to end at. The argument type depends on which
 * `orderBy*()` function was used in this query. Specify a value that matches
 * the `orderBy*()` type. When used in combination with `orderByKey()`, the
 * value must be a string.
 * @param key - The child key to end at, among the children with the previously
 * specified priority. This argument is only allowed if ordering by child,
 * value, or priority.
 */
function endAt(value, key) {
    validateKey('endAt', 'key', key, true);
    return new QueryEndAtConstraint(value, key);
}
class QueryEndBeforeConstraint extends QueryConstraint {
    constructor(_value, _key) {
        super();
        this._value = _value;
        this._key = _key;
        this.type = 'endBefore';
    }
    _apply(query) {
        validateFirebaseDataArg('endBefore', this._value, query._path, false);
        const newParams = queryParamsEndBefore(query._queryParams, this._value, this._key);
        validateLimit(newParams);
        validateQueryEndpoints(newParams);
        if (query._queryParams.hasEnd()) {
            throw new Error('endBefore: Starting point was already set (by another call to endAt, ' +
                'endBefore or equalTo).');
        }
        return new QueryImpl(query._repo, query._path, newParams, query._orderByCalled);
    }
}
/**
 * Creates a `QueryConstraint` with the specified ending point (exclusive).
 *
 * Using `startAt()`, `startAfter()`, `endBefore()`, `endAt()` and `equalTo()`
 * allows you to choose arbitrary starting and ending points for your queries.
 *
 * The ending point is exclusive. If only a value is provided, children
 * with a value less than the specified value will be included in the query.
 * If a key is specified, then children must have a value less than or equal
 * to the specified value and a key name less than the specified key.
 *
 * @param value - The value to end before. The argument type depends on which
 * `orderBy*()` function was used in this query. Specify a value that matches
 * the `orderBy*()` type. When used in combination with `orderByKey()`, the
 * value must be a string.
 * @param key - The child key to end before, among the children with the
 * previously specified priority. This argument is only allowed if ordering by
 * child, value, or priority.
 */
function endBefore(value, key) {
    validateKey('endBefore', 'key', key, true);
    return new QueryEndBeforeConstraint(value, key);
}
class QueryStartAtConstraint extends QueryConstraint {
    constructor(_value, _key) {
        super();
        this._value = _value;
        this._key = _key;
        this.type = 'startAt';
    }
    _apply(query) {
        validateFirebaseDataArg('startAt', this._value, query._path, true);
        const newParams = queryParamsStartAt(query._queryParams, this._value, this._key);
        validateLimit(newParams);
        validateQueryEndpoints(newParams);
        if (query._queryParams.hasStart()) {
            throw new Error('startAt: Starting point was already set (by another call to startAt, ' +
                'startBefore or equalTo).');
        }
        return new QueryImpl(query._repo, query._path, newParams, query._orderByCalled);
    }
}
/**
 * Creates a `QueryConstraint` with the specified starting point.
 *
 * Using `startAt()`, `startAfter()`, `endBefore()`, `endAt()` and `equalTo()`
 * allows you to choose arbitrary starting and ending points for your queries.
 *
 * The starting point is inclusive, so children with exactly the specified value
 * will be included in the query. The optional key argument can be used to
 * further limit the range of the query. If it is specified, then children that
 * have exactly the specified value must also have a key name greater than or
 * equal to the specified key.
 *
 * You can read more about `startAt()` in
 * {@link https://firebase.google.com/docs/database/web/lists-of-data#filtering_data | Filtering data}.
 *
 * @param value - The value to start at. The argument type depends on which
 * `orderBy*()` function was used in this query. Specify a value that matches
 * the `orderBy*()` type. When used in combination with `orderByKey()`, the
 * value must be a string.
 * @param key - The child key to start at. This argument is only allowed if
 * ordering by child, value, or priority.
 */
function startAt(value = null, key) {
    validateKey('startAt', 'key', key, true);
    return new QueryStartAtConstraint(value, key);
}
class QueryStartAfterConstraint extends QueryConstraint {
    constructor(_value, _key) {
        super();
        this._value = _value;
        this._key = _key;
        this.type = 'startAfter';
    }
    _apply(query) {
        validateFirebaseDataArg('startAfter', this._value, query._path, false);
        const newParams = queryParamsStartAfter(query._queryParams, this._value, this._key);
        validateLimit(newParams);
        validateQueryEndpoints(newParams);
        if (query._queryParams.hasStart()) {
            throw new Error('startAfter: Starting point was already set (by another call to startAt, ' +
                'startAfter, or equalTo).');
        }
        return new QueryImpl(query._repo, query._path, newParams, query._orderByCalled);
    }
}
/**
 * Creates a `QueryConstraint` with the specified starting point (exclusive).
 *
 * Using `startAt()`, `startAfter()`, `endBefore()`, `endAt()` and `equalTo()`
 * allows you to choose arbitrary starting and ending points for your queries.
 *
 * The starting point is exclusive. If only a value is provided, children
 * with a value greater than the specified value will be included in the query.
 * If a key is specified, then children must have a value greater than or equal
 * to the specified value and a a key name greater than the specified key.
 *
 * @param value - The value to start after. The argument type depends on which
 * `orderBy*()` function was used in this query. Specify a value that matches
 * the `orderBy*()` type. When used in combination with `orderByKey()`, the
 * value must be a string.
 * @param key - The child key to start after. This argument is only allowed if
 * ordering by child, value, or priority.
 */
function startAfter(value, key) {
    validateKey('startAfter', 'key', key, true);
    return new QueryStartAfterConstraint(value, key);
}
class QueryLimitToFirstConstraint extends QueryConstraint {
    constructor(_limit) {
        super();
        this._limit = _limit;
        this.type = 'limitToFirst';
    }
    _apply(query) {
        if (query._queryParams.hasLimit()) {
            throw new Error('limitToFirst: Limit was already set (by another call to limitToFirst ' +
                'or limitToLast).');
        }
        return new QueryImpl(query._repo, query._path, queryParamsLimitToFirst(query._queryParams, this._limit), query._orderByCalled);
    }
}
/**
 * Creates a new `QueryConstraint` that if limited to the first specific number
 * of children.
 *
 * The `limitToFirst()` method is used to set a maximum number of children to be
 * synced for a given callback. If we set a limit of 100, we will initially only
 * receive up to 100 `child_added` events. If we have fewer than 100 messages
 * stored in our Database, a `child_added` event will fire for each message.
 * However, if we have over 100 messages, we will only receive a `child_added`
 * event for the first 100 ordered messages. As items change, we will receive
 * `child_removed` events for each item that drops out of the active list so
 * that the total number stays at 100.
 *
 * You can read more about `limitToFirst()` in
 * {@link https://firebase.google.com/docs/database/web/lists-of-data#filtering_data | Filtering data}.
 *
 * @param limit - The maximum number of nodes to include in this query.
 */
function limitToFirst(limit) {
    if (typeof limit !== 'number' || Math.floor(limit) !== limit || limit <= 0) {
        throw new Error('limitToFirst: First argument must be a positive integer.');
    }
    return new QueryLimitToFirstConstraint(limit);
}
class QueryLimitToLastConstraint extends QueryConstraint {
    constructor(_limit) {
        super();
        this._limit = _limit;
        this.type = 'limitToLast';
    }
    _apply(query) {
        if (query._queryParams.hasLimit()) {
            throw new Error('limitToLast: Limit was already set (by another call to limitToFirst ' +
                'or limitToLast).');
        }
        return new QueryImpl(query._repo, query._path, queryParamsLimitToLast(query._queryParams, this._limit), query._orderByCalled);
    }
}
/**
 * Creates a new `QueryConstraint` that is limited to return only the last
 * specified number of children.
 *
 * The `limitToLast()` method is used to set a maximum number of children to be
 * synced for a given callback. If we set a limit of 100, we will initially only
 * receive up to 100 `child_added` events. If we have fewer than 100 messages
 * stored in our Database, a `child_added` event will fire for each message.
 * However, if we have over 100 messages, we will only receive a `child_added`
 * event for the last 100 ordered messages. As items change, we will receive
 * `child_removed` events for each item that drops out of the active list so
 * that the total number stays at 100.
 *
 * You can read more about `limitToLast()` in
 * {@link https://firebase.google.com/docs/database/web/lists-of-data#filtering_data | Filtering data}.
 *
 * @param limit - The maximum number of nodes to include in this query.
 */
function limitToLast(limit) {
    if (typeof limit !== 'number' || Math.floor(limit) !== limit || limit <= 0) {
        throw new Error('limitToLast: First argument must be a positive integer.');
    }
    return new QueryLimitToLastConstraint(limit);
}
class QueryOrderByChildConstraint extends QueryConstraint {
    constructor(_path) {
        super();
        this._path = _path;
        this.type = 'orderByChild';
    }
    _apply(query) {
        validateNoPreviousOrderByCall(query, 'orderByChild');
        const parsedPath = new Path(this._path);
        if (pathIsEmpty(parsedPath)) {
            throw new Error('orderByChild: cannot pass in empty path. Use orderByValue() instead.');
        }
        const index = new PathIndex(parsedPath);
        const newParams = queryParamsOrderBy(query._queryParams, index);
        validateQueryEndpoints(newParams);
        return new QueryImpl(query._repo, query._path, newParams, 
        /*orderByCalled=*/ true);
    }
}
/**
 * Creates a new `QueryConstraint` that orders by the specified child key.
 *
 * Queries can only order by one key at a time. Calling `orderByChild()`
 * multiple times on the same query is an error.
 *
 * Firebase queries allow you to order your data by any child key on the fly.
 * However, if you know in advance what your indexes will be, you can define
 * them via the .indexOn rule in your Security Rules for better performance. See
 * the{@link https://firebase.google.com/docs/database/security/indexing-data}
 * rule for more information.
 *
 * You can read more about `orderByChild()` in
 * {@link https://firebase.google.com/docs/database/web/lists-of-data#sort_data | Sort data}.
 *
 * @param path - The path to order by.
 */
function orderByChild(path) {
    if (path === '$key') {
        throw new Error('orderByChild: "$key" is invalid.  Use orderByKey() instead.');
    }
    else if (path === '$priority') {
        throw new Error('orderByChild: "$priority" is invalid.  Use orderByPriority() instead.');
    }
    else if (path === '$value') {
        throw new Error('orderByChild: "$value" is invalid.  Use orderByValue() instead.');
    }
    validatePathString('orderByChild', 'path', path, false);
    return new QueryOrderByChildConstraint(path);
}
class QueryOrderByKeyConstraint extends QueryConstraint {
    constructor() {
        super(...arguments);
        this.type = 'orderByKey';
    }
    _apply(query) {
        validateNoPreviousOrderByCall(query, 'orderByKey');
        const newParams = queryParamsOrderBy(query._queryParams, KEY_INDEX);
        validateQueryEndpoints(newParams);
        return new QueryImpl(query._repo, query._path, newParams, 
        /*orderByCalled=*/ true);
    }
}
/**
 * Creates a new `QueryConstraint` that orders by the key.
 *
 * Sorts the results of a query by their (ascending) key values.
 *
 * You can read more about `orderByKey()` in
 * {@link https://firebase.google.com/docs/database/web/lists-of-data#sort_data | Sort data}.
 */
function orderByKey() {
    return new QueryOrderByKeyConstraint();
}
class QueryOrderByPriorityConstraint extends QueryConstraint {
    constructor() {
        super(...arguments);
        this.type = 'orderByPriority';
    }
    _apply(query) {
        validateNoPreviousOrderByCall(query, 'orderByPriority');
        const newParams = queryParamsOrderBy(query._queryParams, PRIORITY_INDEX);
        validateQueryEndpoints(newParams);
        return new QueryImpl(query._repo, query._path, newParams, 
        /*orderByCalled=*/ true);
    }
}
/**
 * Creates a new `QueryConstraint` that orders by priority.
 *
 * Applications need not use priority but can order collections by
 * ordinary properties (see
 * {@link https://firebase.google.com/docs/database/web/lists-of-data#sort_data | Sort data}
 * for alternatives to priority.
 */
function orderByPriority() {
    return new QueryOrderByPriorityConstraint();
}
class QueryOrderByValueConstraint extends QueryConstraint {
    constructor() {
        super(...arguments);
        this.type = 'orderByValue';
    }
    _apply(query) {
        validateNoPreviousOrderByCall(query, 'orderByValue');
        const newParams = queryParamsOrderBy(query._queryParams, VALUE_INDEX);
        validateQueryEndpoints(newParams);
        return new QueryImpl(query._repo, query._path, newParams, 
        /*orderByCalled=*/ true);
    }
}
/**
 * Creates a new `QueryConstraint` that orders by value.
 *
 * If the children of a query are all scalar values (string, number, or
 * boolean), you can order the results by their (ascending) values.
 *
 * You can read more about `orderByValue()` in
 * {@link https://firebase.google.com/docs/database/web/lists-of-data#sort_data | Sort data}.
 */
function orderByValue() {
    return new QueryOrderByValueConstraint();
}
class QueryEqualToValueConstraint extends QueryConstraint {
    constructor(_value, _key) {
        super();
        this._value = _value;
        this._key = _key;
        this.type = 'equalTo';
    }
    _apply(query) {
        validateFirebaseDataArg('equalTo', this._value, query._path, false);
        if (query._queryParams.hasStart()) {
            throw new Error('equalTo: Starting point was already set (by another call to startAt/startAfter or ' +
                'equalTo).');
        }
        if (query._queryParams.hasEnd()) {
            throw new Error('equalTo: Ending point was already set (by another call to endAt/endBefore or ' +
                'equalTo).');
        }
        return new QueryEndAtConstraint(this._value, this._key)._apply(new QueryStartAtConstraint(this._value, this._key)._apply(query));
    }
}
/**
 * Creates a `QueryConstraint` that includes children that match the specified
 * value.
 *
 * Using `startAt()`, `startAfter()`, `endBefore()`, `endAt()` and `equalTo()`
 * allows you to choose arbitrary starting and ending points for your queries.
 *
 * The optional key argument can be used to further limit the range of the
 * query. If it is specified, then children that have exactly the specified
 * value must also have exactly the specified key as their key name. This can be
 * used to filter result sets with many matches for the same value.
 *
 * You can read more about `equalTo()` in
 * {@link https://firebase.google.com/docs/database/web/lists-of-data#filtering_data | Filtering data}.
 *
 * @param value - The value to match for. The argument type depends on which
 * `orderBy*()` function was used in this query. Specify a value that matches
 * the `orderBy*()` type. When used in combination with `orderByKey()`, the
 * value must be a string.
 * @param key - The child key to start at, among the children with the
 * previously specified priority. This argument is only allowed if ordering by
 * child, value, or priority.
 */
function equalTo(value, key) {
    validateKey('equalTo', 'key', key, true);
    return new QueryEqualToValueConstraint(value, key);
}
/**
 * Creates a new immutable instance of `Query` that is extended to also include
 * additional query constraints.
 *
 * @param query - The Query instance to use as a base for the new constraints.
 * @param queryConstraints - The list of `QueryConstraint`s to apply.
 * @throws if any of the provided query constraints cannot be combined with the
 * existing or new constraints.
 */
function query(query, ...queryConstraints) {
    let queryImpl = util.getModularInstance(query);
    for (const constraint of queryConstraints) {
        queryImpl = constraint._apply(queryImpl);
    }
    return queryImpl;
}
/**
 * Define reference constructor in various modules
 *
 * We are doing this here to avoid several circular
 * dependency issues
 */
syncPointSetReferenceConstructor(ReferenceImpl);
syncTreeSetReferenceConstructor(ReferenceImpl);

/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * This variable is also defined in the firebase Node.js Admin SDK. Before
 * modifying this definition, consult the definition in:
 *
 * https://github.com/firebase/firebase-admin-node
 *
 * and make sure the two are consistent.
 */
const FIREBASE_DATABASE_EMULATOR_HOST_VAR = 'FIREBASE_DATABASE_EMULATOR_HOST';
/**
 * Creates and caches `Repo` instances.
 */
const repos = {};
/**
 * If true, any new `Repo` will be created to use `ReadonlyRestClient` (for testing purposes).
 */
let useRestClient = false;
/**
 * Update an existing `Repo` in place to point to a new host/port.
 */
function repoManagerApplyEmulatorSettings(repo, hostAndPort, emulatorOptions, tokenProvider) {
    const portIndex = hostAndPort.lastIndexOf(':');
    const host = hostAndPort.substring(0, portIndex);
    const useSsl = util.isCloudWorkstation(host);
    repo.repoInfo_ = new RepoInfo(hostAndPort, 
    /* secure= */ useSsl, repo.repoInfo_.namespace, repo.repoInfo_.webSocketOnly, repo.repoInfo_.nodeAdmin, repo.repoInfo_.persistenceKey, repo.repoInfo_.includeNamespaceInQueryParams, 
    /*isUsingEmulator=*/ true, emulatorOptions);
    if (tokenProvider) {
        repo.authTokenProvider_ = tokenProvider;
    }
}
/**
 * This function should only ever be called to CREATE a new database instance.
 * @internal
 */
function repoManagerDatabaseFromApp(app, authProvider, appCheckProvider, url, nodeAdmin) {
    let dbUrl = url || app.options.databaseURL;
    if (dbUrl === undefined) {
        if (!app.options.projectId) {
            fatal("Can't determine Firebase Database URL. Be sure to include " +
                ' a Project ID when calling firebase.initializeApp().');
        }
        log('Using default host for project ', app.options.projectId);
        dbUrl = `${app.options.projectId}-default-rtdb.firebaseio.com`;
    }
    let parsedUrl = parseRepoInfo(dbUrl, nodeAdmin);
    let repoInfo = parsedUrl.repoInfo;
    let isEmulator;
    let dbEmulatorHost = undefined;
    if (typeof process !== 'undefined' && process.env) {
        dbEmulatorHost = process.env[FIREBASE_DATABASE_EMULATOR_HOST_VAR];
    }
    if (dbEmulatorHost) {
        isEmulator = true;
        dbUrl = `http://${dbEmulatorHost}?ns=${repoInfo.namespace}`;
        parsedUrl = parseRepoInfo(dbUrl, nodeAdmin);
        repoInfo = parsedUrl.repoInfo;
    }
    else {
        isEmulator = !parsedUrl.repoInfo.secure;
    }
    const authTokenProvider = nodeAdmin && isEmulator
        ? new EmulatorTokenProvider(EmulatorTokenProvider.OWNER)
        : new FirebaseAuthTokenProvider(app.name, app.options, authProvider);
    validateUrl('Invalid Firebase Database URL', parsedUrl);
    if (!pathIsEmpty(parsedUrl.path)) {
        fatal('Database URL must point to the root of a Firebase Database ' +
            '(not including a child path).');
    }
    const repo = repoManagerCreateRepo(repoInfo, app, authTokenProvider, new AppCheckTokenProvider(app, appCheckProvider));
    return new Database(repo, app);
}
/**
 * Remove the repo and make sure it is disconnected.
 *
 */
function repoManagerDeleteRepo(repo, appName) {
    const appRepos = repos[appName];
    // This should never happen...
    if (!appRepos || appRepos[repo.key] !== repo) {
        fatal(`Database ${appName}(${repo.repoInfo_}) has already been deleted.`);
    }
    repoDispose(repo);
    delete appRepos[repo.key];
}
/**
 * Ensures a repo doesn't already exist and then creates one using the
 * provided app.
 *
 * @param repoInfo - The metadata about the Repo
 * @returns The Repo object for the specified server / repoName.
 */
function repoManagerCreateRepo(repoInfo, app, authTokenProvider, appCheckProvider) {
    let appRepos = repos[app.name];
    if (!appRepos) {
        appRepos = {};
        repos[app.name] = appRepos;
    }
    let repo = appRepos[repoInfo.toURLString()];
    if (repo) {
        fatal('Database initialized multiple times. Please make sure the format of the database URL matches with each database() call.');
    }
    repo = new Repo(repoInfo, useRestClient, authTokenProvider, appCheckProvider);
    appRepos[repoInfo.toURLString()] = repo;
    return repo;
}
/**
 * Forces us to use ReadonlyRestClient instead of PersistentConnection for new Repos.
 */
function repoManagerForceRestClient(forceRestClient) {
    useRestClient = forceRestClient;
}
/**
 * Class representing a Firebase Realtime Database.
 */
class Database {
    /** @hideconstructor */
    constructor(_repoInternal, 
    /** The {@link @firebase/app#FirebaseApp} associated with this Realtime Database instance. */
    app) {
        this._repoInternal = _repoInternal;
        this.app = app;
        /** Represents a `Database` instance. */
        this['type'] = 'database';
        /** Track if the instance has been used (root or repo accessed) */
        this._instanceStarted = false;
    }
    get _repo() {
        if (!this._instanceStarted) {
            repoStart(this._repoInternal, this.app.options.appId, this.app.options['databaseAuthVariableOverride']);
            this._instanceStarted = true;
        }
        return this._repoInternal;
    }
    get _root() {
        if (!this._rootInternal) {
            this._rootInternal = new ReferenceImpl(this._repo, newEmptyPath());
        }
        return this._rootInternal;
    }
    _delete() {
        if (this._rootInternal !== null) {
            repoManagerDeleteRepo(this._repo, this.app.name);
            this._repoInternal = null;
            this._rootInternal = null;
        }
        return Promise.resolve();
    }
    _checkNotDeleted(apiName) {
        if (this._rootInternal === null) {
            fatal('Cannot call ' + apiName + ' on a deleted database.');
        }
    }
}
function checkTransportInit() {
    if (TransportManager.IS_TRANSPORT_INITIALIZED) {
        warn('Transport has already been initialized. Please call this function before calling ref or setting up a listener');
    }
}
/**
 * Force the use of websockets instead of longPolling.
 */
function forceWebSockets() {
    checkTransportInit();
    BrowserPollConnection.forceDisallow();
}
/**
 * Force the use of longPolling instead of websockets. This will be ignored if websocket protocol is used in databaseURL.
 */
function forceLongPolling() {
    checkTransportInit();
    WebSocketConnection.forceDisallow();
    BrowserPollConnection.forceAllow();
}
/**
 * Modify the provided instance to communicate with the Realtime Database
 * emulator.
 *
 * <p>Note: This method must be called before performing any other operation.
 *
 * @param db - The instance to modify.
 * @param host - The emulator host (ex: localhost)
 * @param port - The emulator port (ex: 8080)
 * @param options.mockUserToken - the mock auth token to use for unit testing Security Rules
 */
function connectDatabaseEmulator(db, host, port, options = {}) {
    db = util.getModularInstance(db);
    db._checkNotDeleted('useEmulator');
    const hostAndPort = `${host}:${port}`;
    const repo = db._repoInternal;
    if (db._instanceStarted) {
        // If the instance has already been started, then silenty fail if this function is called again
        // with the same parameters. If the parameters differ then assert.
        if (hostAndPort === db._repoInternal.repoInfo_.host &&
            util.deepEqual(options, repo.repoInfo_.emulatorOptions)) {
            return;
        }
        fatal('connectDatabaseEmulator() cannot initialize or alter the emulator configuration after the database instance has started.');
    }
    let tokenProvider = undefined;
    if (repo.repoInfo_.nodeAdmin) {
        if (options.mockUserToken) {
            fatal('mockUserToken is not supported by the Admin SDK. For client access with mock users, please use the "firebase" package instead of "firebase-admin".');
        }
        tokenProvider = new EmulatorTokenProvider(EmulatorTokenProvider.OWNER);
    }
    else if (options.mockUserToken) {
        const token = typeof options.mockUserToken === 'string'
            ? options.mockUserToken
            : util.createMockUserToken(options.mockUserToken, db.app.options.projectId);
        tokenProvider = new EmulatorTokenProvider(token);
    }
    // Workaround to get cookies in Firebase Studio
    if (util.isCloudWorkstation(host)) {
        void util.pingServer(host);
    }
    // Modify the repo to apply emulator settings
    repoManagerApplyEmulatorSettings(repo, hostAndPort, options, tokenProvider);
    // Persistence enabled before this call captured the production URL as its
    // storage prefix; rebind it to the emulator's so emulator sessions never
    // restore production records or write emulator data under the production
    // namespace. (Emulator config only happens pre-start, so no listens or
    // tracked roots exist yet.)
    if (repo.persistence_ !== null) {
        repo.persistence_ = repo.persistence_.rebindTo(repo.repoInfo_.toURLString());
    }
}
/**
 * Disconnects from the server (all Database operations will be completed
 * offline).
 *
 * The client automatically maintains a persistent connection to the Database
 * server, which will remain active indefinitely and reconnect when
 * disconnected. However, the `goOffline()` and `goOnline()` methods may be used
 * to control the client connection in cases where a persistent connection is
 * undesirable.
 *
 * While offline, the client will no longer receive data updates from the
 * Database. However, all Database operations performed locally will continue to
 * immediately fire events, allowing your application to continue behaving
 * normally. Additionally, each operation performed locally will automatically
 * be queued and retried upon reconnection to the Database server.
 *
 * To reconnect to the Database and begin receiving remote events, see
 * `goOnline()`.
 *
 * @param db - The instance to disconnect.
 */
function goOffline(db) {
    db = util.getModularInstance(db);
    db._checkNotDeleted('goOffline');
    repoInterrupt(db._repo);
}
/**
 * Per-child work units charged per main-thread slice of the peek walk (one
 * charge per child pulled from a node's iterator and per array-coercion
 * copy). Sized so one slice stays well inside a frame budget on mobile
 * hardware while keeping the total slice count (and its scheduling overhead)
 * low on large workspaces.
 * @internal
 */
const _PEEK_MATERIALIZE_SLICE_VISITS = 4000;
let peekYieldChannel = null;
const peekYieldResolvers = [];
function setPeekPortsReferenced(referenced) {
    for (const port of [peekYieldChannel.port1, peekYieldChannel.port2]) {
        const p = port;
        if (referenced) {
            p.ref?.();
        }
        else {
            p.unref?.();
        }
    }
}
function yieldMacrotask() {
    if (typeof MessageChannel === 'undefined') {
        return new Promise(resolve => setTimeout(resolve, 0));
    }
    if (peekYieldChannel === null) {
        peekYieldChannel = new MessageChannel();
        // Installing onmessage references the port in Node; start idle-unref'd.
        peekYieldChannel.port1.onmessage = () => {
            peekYieldResolvers.shift()?.();
            if (peekYieldResolvers.length === 0) {
                setPeekPortsReferenced(false);
            }
        };
        setPeekPortsReferenced(false);
    }
    return new Promise(resolve => {
        peekYieldResolvers.push(resolve);
        setPeekPortsReferenced(true);
        peekYieldChannel.port2.postMessage(null);
    });
}
// ChildrenNode.val()'s integer-key grammar (private there; replicated for the
// sliced walk's array coercion, which must match val() exactly).
const PEEK_INTEGER_REGEXP = /^(0|[1-9]\d*)$/;
/**
 * Materializes `node` to the exact JS value `node.val()` returns — same child
 * ordering (PRIORITY_INDEX), same array coercion, same leaf semantics — in
 * yielded slices under one budget invariant: EVERY per-child unit of work
 * (a child pulled from a node's lazy iterator, an array-coercion copy)
 * charges the shared slice budget, and no uncharged loop is unbounded. After
 * _PEEK_MATERIALIZE_SLICE_VISITS charges the walk yields a macrotask so the
 * main thread can paint and GC.
 *
 * Children are pulled from ChildrenNode.getIterator(PRIORITY_INDEX) — the
 * identical resolveIndex_ order forEachChild/val() traverse — one at a time,
 * so a wide flat node never enumerates (or buffers) its whole child set
 * between two yields. An eager per-node child copy would itself be the
 * unbounded synchronous walk this function exists to prevent.
 *
 * Why not node.val(): the pre-auth peek materializes the ENTIRE persisted
 * workspace at boot, and one synchronous walk over a large root is a
 * multi-second main-thread block plus an allocation spike at exactly the
 * moment mobile WebKit is quickest to kill the page. The IndexedDB decode
 * before this point is already sliced (decodeFragmentsSliced_); this closes
 * the remaining monolithic walk on the boot path.
 */
async function materializeNodeSliced(node) {
    let visitsSinceYield = 0;
    const walk = async (current) => {
        if (current.isLeafNode()) {
            return current.val();
        }
        if (current.isEmpty()) {
            return null;
        }
        // Lazy child iteration: same PRIORITY_INDEX order as forEachChild (both
        // resolve the identical index), pulled one child per budget charge.
        const iterator = current.getIterator(PRIORITY_INDEX);
        const obj = {};
        let numKeys = 0;
        let maxKey = 0;
        let allIntegerKeys = true;
        let child = iterator.getNext();
        while (child !== null) {
            if (++visitsSinceYield >= _PEEK_MATERIALIZE_SLICE_VISITS) {
                visitsSinceYield = 0;
                await yieldMacrotask();
            }
            const key = child.name;
            // Inline leaves: a promise per leaf (via recursion) would dominate
            // allocation on exactly the wide flat collections this bounds.
            obj[key] = child.node.isLeafNode()
                ? child.node.val()
                : await walk(child.node);
            numKeys++;
            // charCode fast-reject mirrors ChildrenNode.val().
            if (allIntegerKeys &&
                key.charCodeAt(0) >= 48 /* '0' */ &&
                key.charCodeAt(0) <= 57 /* '9' */ &&
                PEEK_INTEGER_REGEXP.test(key)) {
                maxKey = Math.max(maxKey, Number(key));
            }
            else {
                allIntegerKeys = false;
            }
            child = iterator.getNext();
        }
        if (allIntegerKeys && maxKey < 2 * numKeys) {
            const array = [];
            // eslint-disable-next-line guard-for-in
            for (const key in obj) {
                // Charged like any other per-child unit: a huge dense array must not
                // replay its whole length in one uncharged task after the last yield.
                if (++visitsSinceYield >= _PEEK_MATERIALIZE_SLICE_VISITS) {
                    visitsSinceYield = 0;
                    await yieldMacrotask();
                }
                array[key] = obj[key];
            }
            return array;
        }
        return obj;
    };
    return walk(node);
}
/**
 * Reads the exact persisted server cache root at `path` WITHOUT attaching a
 * listener — the pre-auth boot peek: apps that paint an optimistic shell before sign-in
 * completes can render the persisted tree, then let the real (authenticated)
 * listener attach and reconcile. Resolves null when persistence is disabled,
 * nothing is stored, or the record expired.
 *
 * @public
 */
function getPersistedValue(db, pathString, expectedAuthScope = null) {
    db = util.getModularInstance(db);
    db._checkNotDeleted('getPersistedValue');
    validateRootPathString('getPersistedValue', 'path', pathString, false);
    // _repoInternal, not the _repo getter: the boot peek runs before sign-in,
    // and reading a stored record must not start the instance (which would
    // lock out later transport/emulator configuration).
    const repo = db._repoInternal;
    const persistence = repo.persistence_;
    if (persistence === null) {
        return Promise.resolve(null);
    }
    // Prime the manager with the trusted expected identity so the later auth
    // callback for that same user can reuse this physical decode. A different
    // real auth uid changes scope and cancels it before any listener consumes
    // it. Priming is NOT app confirmation (confirmedByApp=false): until real
    // auth confirms this scope via setPersistenceAuthScope, the peek's decoded
    // tree is retained under the long pre-auth backstop instead of the short
    // handoff grace — auth hydration can be arbitrarily slow, and expiring the
    // handoff before it completes forces a full second restore alongside the
    // first (the double-tree boot-memory spike).
    if (expectedAuthScope !== null) {
        persistence.setAuthScope(expectedAuthScope, false);
    }
    // Exact-root by design: callers peek the same path they are about to
    // listen to. This lets the authenticated listener consume the same decoded
    // Node and prevents a fresher ancestor record from being mistaken for the
    // exact listener's initial replay.
    const normalizedPath = new Path(pathString).toString();
    // peek() revalidates the identity scope when its record promise resolves,
    // but the sliced walk below opens a multi-macrotask window AFTER that
    // check. Capture the generation here and re-check once the walk is done,
    // so a scope switch or sign-out mid-walk invalidates this peek exactly
    // like one that lands before resolution — a public-API caller must never
    // receive the previous account's cached tree.
    const authGeneration = persistence.authGeneration();
    return persistence.peek(normalizedPath, expectedAuthScope).then(async (record) => {
        if (record === null) {
            return null;
        }
        // Sliced val(): identical result, but the walk yields macrotasks so a
        // large workspace cannot block boot in one multi-second task.
        const value = await materializeNodeSliced(record.node);
        if (persistence.authGeneration() !== authGeneration) {
            return null;
        }
        // One-boot materialization handoff (see ServerCacheSeed), installed
        // ATOMICALLY after the walk and only while THE read that decoded this
        // exact node is still retained for a future listener join — the only
        // window with a consumer. Identity-bound (record.node, not just the
        // path): a listener consuming the read mid-walk, or a replacement
        // peek retained since, must leave zero stamps behind — a stamp
        // without a taker pins a full JS copy of its subtree for the session.
        // When the check fails, the joined/next listener simply
        // re-materializes via val(), the ordinary cold path.
        //
        // This pass is deliberately synchronous (not budget-charged): it
        // allocates nothing — the values already live in `value` — and its
        // per-child cost is a WeakMap set for object children only
        // (stampMaterializedValue ignores primitives itself), strictly
        // cheaper than the listener replay burst over the same children.
        // Charging it would reopen the mid-pass interleaving the atomicity
        // exists to prevent.
        if (value !== null &&
            typeof value === 'object' &&
            persistence.hasRetainedPeek(normalizedPath, record.node)) {
            const byKey = value;
            record.node.forEachChild(PRIORITY_INDEX, (key, childNode) => {
                stampMaterializedValue(childNode, byKey[key]);
            });
            stampMaterializedValue(record.node, value);
        }
        return value;
    });
}
/**
 * Enables client-side persistence of the server cache for this Database
 * instance (see core/Persistence.ts): listened roots are stored in IndexedDB
 * and restored on the next startup, where they paint immediately and
 * revalidate with the server via the hash protocol — an unchanged tree costs
 * a handshake, a changed one costs range-merge deltas.
 *
 * Must be called before the first listener attaches (matching the mobile
 * SDKs' setPersistenceEnabled contract); listens attached earlier simply
 * bypass persistence. No-ops where IndexedDB is unavailable.
 *
 * @public
 */
function setPersistenceEnabled(db, enabled) {
    db = util.getModularInstance(db);
    db._checkNotDeleted('setPersistenceEnabled');
    if (db._instanceStarted) {
        fatal('setPersistenceEnabled() must be called before the first Database operation.');
    }
    // _repoInternal, not the _repo getter: configuration must not start the
    // instance, or a later connectDatabaseEmulator() would refuse to run.
    const repo = db._repoInternal;
    if (enabled) {
        if (repo.persistence_ === null) {
            repo.persistence_ = new PersistenceManager(repo.repoInfo_.toURLString());
        }
    }
    else {
        if (repo.persistence_ !== null) {
            repoCancelPendingSeedRestores(repo);
            repo.persistence_.dispose();
            repo.persistence_ = null;
        }
    }
}
/**
 * Sets the identity scope used to read and write persisted cache records.
 * @public
 */
function setPersistenceAuthScope(db, scope) {
    db = util.getModularInstance(db);
    db._checkNotDeleted('setPersistenceAuthScope');
    const repo = db._repoInternal;
    const persistenceWasScoped = repo.persistence_?.isAuthScopeConfigured() ?? false;
    if (repo.persistence_?.setAuthScope(scope) && persistenceWasScoped) {
        repoCancelPendingSeedRestores(repo);
    }
    const scopeChanged = repo.persistenceAuthScope_ !== scope;
    repo.persistenceAuthScope_ = scope;
    if (scopeChanged || (!persistenceWasScoped && repo.persistence_ !== null)) {
        repoNotifyPersistenceAuthScope(repo);
    }
}
/**
 * Observes the restore/cold/fallback state and final server certification for
 * one exact default listen. The callback is invoked first when the local path
 * choice is known (`certified: false`), then once the server responds.
 *
 * @public
 */
function onListenOutcome(db, pathString, callback) {
    db = util.getModularInstance(db);
    db._checkNotDeleted('onListenOutcome');
    validateRootPathString('onListenOutcome', 'path', pathString, false);
    return repoOnListenOutcome(db._repoInternal, new Path(pathString).toString(), callback);
}
/**
 * Reconnects to the server and synchronizes the offline Database state
 * with the server state.
 *
 * This method should be used after disabling the active connection with
 * `goOffline()`. Once reconnected, the client will transmit the proper data
 * and fire the appropriate events so that your client "catches up"
 * automatically.
 *
 * @param db - The instance to reconnect.
 */
function goOnline(db) {
    db = util.getModularInstance(db);
    db._checkNotDeleted('goOnline');
    repoResume(db._repo);
}
function enableLogging(logger, persistent) {
    enableLogging$1(logger, persistent);
}

/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
const SERVER_TIMESTAMP = {
    '.sv': 'timestamp'
};
/**
 * Returns a placeholder value for auto-populating the current timestamp (time
 * since the Unix epoch, in milliseconds) as determined by the Firebase
 * servers.
 */
function serverTimestamp() {
    return SERVER_TIMESTAMP;
}
/**
 * Returns a placeholder value that can be used to atomically increment the
 * current database value by the provided delta.
 *
 * @param delta - the amount to modify the current value atomically.
 * @returns A placeholder value for modifying data atomically server-side.
 */
function increment(delta) {
    return {
        '.sv': {
            'increment': delta
        }
    };
}

/**
 * @license
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * A type for the resolve value of {@link runTransaction}.
 */
class TransactionResult {
    /** @hideconstructor */
    constructor(
    /** Whether the transaction was successfully committed. */
    committed, 
    /** The resulting data snapshot. */
    snapshot) {
        this.committed = committed;
        this.snapshot = snapshot;
    }
    /** Returns a JSON-serializable representation of this object. */
    toJSON() {
        return { committed: this.committed, snapshot: this.snapshot.toJSON() };
    }
}
/**
 * Atomically modifies the data at this location.
 *
 * Atomically modify the data at this location. Unlike a normal `set()`, which
 * just overwrites the data regardless of its previous value, `runTransaction()` is
 * used to modify the existing value to a new value, ensuring there are no
 * conflicts with other clients writing to the same location at the same time.
 *
 * To accomplish this, you pass `runTransaction()` an update function which is
 * used to transform the current value into a new value. If another client
 * writes to the location before your new value is successfully written, your
 * update function will be called again with the new current value, and the
 * write will be retried. This will happen repeatedly until your write succeeds
 * without conflict or you abort the transaction by not returning a value from
 * your update function.
 *
 * Note: Modifying data with `set()` will cancel any pending transactions at
 * that location, so extreme care should be taken if mixing `set()` and
 * `runTransaction()` to update the same data.
 *
 * Note: When using transactions with Security and Firebase Rules in place, be
 * aware that a client needs `.read` access in addition to `.write` access in
 * order to perform a transaction. This is because the client-side nature of
 * transactions requires the client to read the data in order to transactionally
 * update it.
 *
 * @param ref - The location to atomically modify.
 * @param transactionUpdate - A developer-supplied function which will be passed
 * the current data stored at this location (as a JavaScript object). The
 * function should return the new value it would like written (as a JavaScript
 * object). If `undefined` is returned (i.e. you return with no arguments) the
 * transaction will be aborted and the data at this location will not be
 * modified.
 * @param options - An options object to configure transactions.
 * @returns A `Promise` that can optionally be used instead of the `onComplete`
 * callback to handle success and failure.
 */
function runTransaction(ref, 
// eslint-disable-next-line @typescript-eslint/no-explicit-any
transactionUpdate, options) {
    ref = util.getModularInstance(ref);
    validateWritablePath('Reference.transaction', ref._path);
    if (ref.key === '.length' || ref.key === '.keys') {
        throw ('Reference.transaction failed: ' + ref.key + ' is a read-only object.');
    }
    const applyLocally = options?.applyLocally ?? true;
    const deferred = new util.Deferred();
    const promiseComplete = (error, committed, node) => {
        let dataSnapshot = null;
        if (error) {
            deferred.reject(error);
        }
        else {
            dataSnapshot = new DataSnapshot(node, new ReferenceImpl(ref._repo, ref._path), PRIORITY_INDEX);
            deferred.resolve(new TransactionResult(committed, dataSnapshot));
        }
    };
    // Add a watch to make sure we get server updates.
    const unwatcher = onValue(ref, () => { });
    repoStartTransaction(ref._repo, ref._path, transactionUpdate, promiseComplete, unwatcher, applyLocally);
    return deferred.promise;
}

/**
 * @license
 * Copyright 2017 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
PersistentConnection;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
PersistentConnection.prototype.simpleListen = function (pathString, onComplete) {
    this.sendRequest('q', { p: pathString }, onComplete);
};
// eslint-disable-next-line @typescript-eslint/no-explicit-any
PersistentConnection.prototype.echo = function (data, onEcho) {
    this.sendRequest('echo', { d: data }, onEcho);
};
// RealTimeConnection properties that we use in tests.
Connection;
/**
 * @internal
 */
const hijackHash = function (newHash) {
    const oldPut = PersistentConnection.prototype.put;
    PersistentConnection.prototype.put = function (pathString, data, onComplete, hash) {
        if (hash !== undefined) {
            hash = newHash();
        }
        oldPut.call(this, pathString, data, onComplete, hash);
    };
    return function () {
        PersistentConnection.prototype.put = oldPut;
    };
};
RepoInfo;
/**
 * Forces the RepoManager to create Repos that use ReadonlyRestClient instead of PersistentConnection.
 * @internal
 */
const forceRestClient = function (forceRestClient) {
    repoManagerForceRestClient(forceRestClient);
};

/**
 * @license
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Used by console to create a database based on the app,
 * passed database URL and a custom auth implementation.
 * @internal
 * @param app - A valid FirebaseApp-like object
 * @param url - A valid Firebase databaseURL
 * @param version - custom version e.g. firebase-admin version
 * @param customAppCheckImpl - custom app check implementation
 * @param customAuthImpl - custom auth implementation
 */
function _initStandalone({ app, url, version, customAuthImpl, customAppCheckImpl, nodeAdmin = false }) {
    setSDKVersion(version);
    /**
     * ComponentContainer('database-standalone') is just a placeholder that doesn't perform
     * any actual function.
     */
    const componentContainer = new component.ComponentContainer('database-standalone');
    const authProvider = new component.Provider('auth-internal', componentContainer);
    let appCheckProvider;
    if (customAppCheckImpl) {
        appCheckProvider = new component.Provider('app-check-internal', componentContainer);
        appCheckProvider.setComponent(new component.Component('app-check-internal', () => customAppCheckImpl, "PRIVATE" /* ComponentType.PRIVATE */));
    }
    authProvider.setComponent(new component.Component('auth-internal', () => customAuthImpl, "PRIVATE" /* ComponentType.PRIVATE */));
    return repoManagerDatabaseFromApp(app, authProvider, appCheckProvider, url, nodeAdmin);
}

/**
 * @license
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
setWebSocketImpl(Websocket__default["default"].Client);

exports.DataSnapshot = DataSnapshot;
exports.Database = Database;
exports.OnDisconnect = OnDisconnect;
exports.QueryConstraint = QueryConstraint;
exports.TransactionResult = TransactionResult;
exports._PERSISTENCE_WRITE_DEBOUNCE_MS = PERSISTENCE_WRITE_DEBOUNCE_MS;
exports._QueryImpl = QueryImpl;
exports._QueryParams = QueryParams;
exports._ReferenceImpl = ReferenceImpl;
exports._TEST_ACCESS_forceRestClient = forceRestClient;
exports._TEST_ACCESS_hijackHash = hijackHash;
exports._initStandalone = _initStandalone;
exports._repoManagerDatabaseFromApp = repoManagerDatabaseFromApp;
exports._setSDKVersion = setSDKVersion;
exports._validatePathString = validatePathString;
exports._validateWritablePath = validateWritablePath;
exports.child = child;
exports.connectDatabaseEmulator = connectDatabaseEmulator;
exports.consumePersistedMaterialization = consumePersistedMaterialization;
exports.enableLogging = enableLogging;
exports.endAt = endAt;
exports.endBefore = endBefore;
exports.equalTo = equalTo;
exports.forceLongPolling = forceLongPolling;
exports.forceWebSockets = forceWebSockets;
exports.get = get;
exports.getPersistedValue = getPersistedValue;
exports.goOffline = goOffline;
exports.goOnline = goOnline;
exports.increment = increment;
exports.limitToFirst = limitToFirst;
exports.limitToLast = limitToLast;
exports.off = off;
exports.onChildAdded = onChildAdded;
exports.onChildChanged = onChildChanged;
exports.onChildMoved = onChildMoved;
exports.onChildRemoved = onChildRemoved;
exports.onDisconnect = onDisconnect;
exports.onListenOutcome = onListenOutcome;
exports.onValue = onValue;
exports.orderByChild = orderByChild;
exports.orderByKey = orderByKey;
exports.orderByPriority = orderByPriority;
exports.orderByValue = orderByValue;
exports.push = push;
exports.query = query;
exports.ref = ref;
exports.refFromURL = refFromURL;
exports.remove = remove;
exports.runTransaction = runTransaction;
exports.serverTimestamp = serverTimestamp;
exports.set = set;
exports.setPersistenceAuthScope = setPersistenceAuthScope;
exports.setPersistenceEnabled = setPersistenceEnabled;
exports.setPriority = setPriority;
exports.setWithPriority = setWithPriority;
exports.startAfter = startAfter;
exports.startAt = startAt;
exports.update = update;
//# sourceMappingURL=index.standalone.js.map
