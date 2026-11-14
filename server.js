"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf, __hasOwnProp = Object.prototype.hasOwnProperty;
var __esm = (fn, res, err) => function() {
  if (err) throw err[0];
  try {
    return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
  } catch (e) {
    throw err = [e], e;
  }
};
var __commonJS = (cb, mod) => function() {
  try {
    return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
  } catch (e) {
    throw mod = 0, e;
  }
};
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: !0 });
}, __copyProps = (to, from, except, desc) => {
  if (from && typeof from == "object" || typeof from == "function")
    for (let key of __getOwnPropNames(from))
      !__hasOwnProp.call(to, key) && key !== except && __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: !0 }) : target,
  mod
)), __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: !0 }), mod);

// node_modules/ws/lib/constants.js
var require_constants = __commonJS({
  "node_modules/ws/lib/constants.js"(exports2, module2) {
    "use strict";
    var BINARY_TYPES = ["nodebuffer", "arraybuffer", "fragments"], hasBlob = typeof Blob < "u";
    hasBlob && BINARY_TYPES.push("blob");
    module2.exports = {
      BINARY_TYPES,
      CLOSE_TIMEOUT: 3e4,
      EMPTY_BUFFER: Buffer.alloc(0),
      GUID: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
      hasBlob,
      kForOnEventAttribute: /* @__PURE__ */ Symbol("kIsForOnEventAttribute"),
      kListener: /* @__PURE__ */ Symbol("kListener"),
      kStatusCode: /* @__PURE__ */ Symbol("status-code"),
      kWebSocket: /* @__PURE__ */ Symbol("websocket"),
      NOOP: () => {
      }
    };
  }
});

// node_modules/ws/lib/buffer-util.js
var require_buffer_util = __commonJS({
  "node_modules/ws/lib/buffer-util.js"(exports2, module2) {
    "use strict";
    var { websocketMask, websocketUnmask } = require("http"), { EMPTY_BUFFER } = require_constants(), FastBuffer = Buffer[Symbol.species];
    function concat(list, totalLength) {
      if (list.length === 0) return EMPTY_BUFFER;
      if (list.length === 1) return list[0];
      let target = Buffer.allocUnsafe(totalLength), offset = 0;
      for (let i = 0; i < list.length; i++) {
        let buf = list[i];
        target.set(buf, offset), offset += buf.length;
      }
      return offset < totalLength ? new FastBuffer(target.buffer, target.byteOffset, offset) : target;
    }
    function _mask(source, mask, output, offset, length) {
      for (let i = 0; i < length; i++)
        output[offset + i] = source[i] ^ mask[i & 3];
    }
    function _unmask(buffer, mask) {
      for (let i = 0; i < buffer.length; i++)
        buffer[i] ^= mask[i & 3];
    }
    function toArrayBuffer(buf) {
      return buf.length === buf.buffer.byteLength ? buf.buffer : buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.length);
    }
    function toBuffer(data) {
      if (toBuffer.readOnly = !0, Buffer.isBuffer(data)) return data;
      let buf;
      return data instanceof ArrayBuffer ? buf = new FastBuffer(data) : ArrayBuffer.isView(data) ? buf = new FastBuffer(data.buffer, data.byteOffset, data.byteLength) : (buf = Buffer.from(data), toBuffer.readOnly = !1), buf;
    }
    module2.exports = {
      concat,
      mask: _mask,
      toArrayBuffer,
      toBuffer,
      unmask: _unmask
    };
    if (websocketMask)
      module2.exports.mask = function(source, mask, output, offset, length) {
        length < 24 ? _mask(source, mask, output, offset, length) : websocketMask(source, mask, output, offset, length);
      }, module2.exports.unmask = function(buffer, mask) {
        buffer.length < 24 ? _unmask(buffer, mask) : websocketUnmask(buffer, mask);
      };
    else if (!process.env.WS_NO_BUFFER_UTIL)
      try {
        let bufferUtil = require("bufferutil");
        module2.exports.mask = function(source, mask, output, offset, length) {
          length < 48 ? _mask(source, mask, output, offset, length) : bufferUtil.mask(source, mask, output, offset, length);
        }, module2.exports.unmask = function(buffer, mask) {
          buffer.length < 32 ? _unmask(buffer, mask) : bufferUtil.unmask(buffer, mask);
        };
      } catch {
      }
  }
});

// node_modules/ws/lib/limiter.js
var require_limiter = __commonJS({
  "node_modules/ws/lib/limiter.js"(exports2, module2) {
    "use strict";
    var kDone = /* @__PURE__ */ Symbol("kDone"), kRun = /* @__PURE__ */ Symbol("kRun"), Limiter = class {
      /**
       * Creates a new `Limiter`.
       *
       * @param {Number} [concurrency=Infinity] The maximum number of jobs allowed
       *     to run concurrently
       */
      constructor(concurrency) {
        this[kDone] = () => {
          this.pending--, this[kRun]();
        }, this.concurrency = concurrency || 1 / 0, this.jobs = [], this.pending = 0;
      }
      /**
       * Adds a job to the queue.
       *
       * @param {Function} job The job to run
       * @public
       */
      add(job) {
        this.jobs.push(job), this[kRun]();
      }
      /**
       * Removes a job from the queue and runs it if possible.
       *
       * @private
       */
      [kRun]() {
        if (this.pending !== this.concurrency && this.jobs.length) {
          let job = this.jobs.shift();
          this.pending++, job(this[kDone]);
        }
      }
    };
    module2.exports = Limiter;
  }
});

// node_modules/ws/lib/permessage-deflate.js
var require_permessage_deflate = __commonJS({
  "node_modules/ws/lib/permessage-deflate.js"(exports2, module2) {
    "use strict";
    var zlib2 = require("zlib"), bufferUtil = require_buffer_util(), Limiter = require_limiter(), { kStatusCode } = require_constants(), FastBuffer = Buffer[Symbol.species], TRAILER = Buffer.from([0, 0, 255, 255]), kPerMessageDeflate = /* @__PURE__ */ Symbol("permessage-deflate"), kTotalLength = /* @__PURE__ */ Symbol("total-length"), kCallback = /* @__PURE__ */ Symbol("callback"), kBuffers = /* @__PURE__ */ Symbol("buffers"), kError = /* @__PURE__ */ Symbol("error"), zlibLimiter, PerMessageDeflate2 = class {
      /**
       * Creates a PerMessageDeflate instance.
       *
       * @param {Object} [options] Configuration options
       * @param {(Boolean|Number)} [options.clientMaxWindowBits] Advertise support
       *     for, or request, a custom client window size
       * @param {Boolean} [options.clientNoContextTakeover=false] Advertise/
       *     acknowledge disabling of client context takeover
       * @param {Number} [options.concurrencyLimit=10] The number of concurrent
       *     calls to zlib
       * @param {Boolean} [options.isServer=false] Create the instance in either
       *     server or client mode
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {(Boolean|Number)} [options.serverMaxWindowBits] Request/confirm the
       *     use of a custom server window size
       * @param {Boolean} [options.serverNoContextTakeover=false] Request/accept
       *     disabling of server context takeover
       * @param {Number} [options.threshold=1024] Size (in bytes) below which
       *     messages should not be compressed if context takeover is disabled
       * @param {Object} [options.zlibDeflateOptions] Options to pass to zlib on
       *     deflate
       * @param {Object} [options.zlibInflateOptions] Options to pass to zlib on
       *     inflate
       */
      constructor(options) {
        if (this._options = options || {}, this._threshold = this._options.threshold !== void 0 ? this._options.threshold : 1024, this._maxPayload = this._options.maxPayload | 0, this._isServer = !!this._options.isServer, this._deflate = null, this._inflate = null, this.params = null, !zlibLimiter) {
          let concurrency = this._options.concurrencyLimit !== void 0 ? this._options.concurrencyLimit : 10;
          zlibLimiter = new Limiter(concurrency);
        }
      }
      /**
       * @type {String}
       */
      static get extensionName() {
        return "permessage-deflate";
      }
      /**
       * Create an extension negotiation offer.
       *
       * @return {Object} Extension parameters
       * @public
       */
      offer() {
        let params = {};
        return this._options.serverNoContextTakeover && (params.server_no_context_takeover = !0), this._options.clientNoContextTakeover && (params.client_no_context_takeover = !0), this._options.serverMaxWindowBits && (params.server_max_window_bits = this._options.serverMaxWindowBits), this._options.clientMaxWindowBits ? params.client_max_window_bits = this._options.clientMaxWindowBits : this._options.clientMaxWindowBits == null && (params.client_max_window_bits = !0), params;
      }
      /**
       * Accept an extension negotiation offer/response.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Object} Accepted configuration
       * @public
       */
      accept(configurations) {
        return configurations = this.normalizeParams(configurations), this.params = this._isServer ? this.acceptAsServer(configurations) : this.acceptAsClient(configurations), this.params;
      }
      /**
       * Releases all resources used by the extension.
       *
       * @public
       */
      cleanup() {
        if (this._inflate && (this._inflate.close(), this._inflate = null), this._deflate) {
          let callback = this._deflate[kCallback];
          this._deflate.close(), this._deflate = null, callback && callback(
            new Error(
              "The deflate stream was closed while data was being processed"
            )
          );
        }
      }
      /**
       *  Accept an extension negotiation offer.
       *
       * @param {Array} offers The extension negotiation offers
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsServer(offers) {
        let opts = this._options, accepted = offers.find((params) => !(opts.serverNoContextTakeover === !1 && params.server_no_context_takeover || params.server_max_window_bits && (opts.serverMaxWindowBits === !1 || typeof opts.serverMaxWindowBits == "number" && opts.serverMaxWindowBits > params.server_max_window_bits) || typeof opts.clientMaxWindowBits == "number" && (typeof params.client_max_window_bits == "number" ? opts.clientMaxWindowBits > params.client_max_window_bits : !params.client_max_window_bits)));
        if (!accepted)
          throw new Error("None of the extension offers can be accepted");
        return opts.serverNoContextTakeover && (accepted.server_no_context_takeover = !0), opts.clientNoContextTakeover && (accepted.client_no_context_takeover = !0), typeof opts.serverMaxWindowBits == "number" && (accepted.server_max_window_bits = opts.serverMaxWindowBits), typeof opts.clientMaxWindowBits == "number" ? accepted.client_max_window_bits = opts.clientMaxWindowBits : (accepted.client_max_window_bits === !0 || opts.clientMaxWindowBits === !1) && delete accepted.client_max_window_bits, accepted;
      }
      /**
       * Accept the extension negotiation response.
       *
       * @param {Array} response The extension negotiation response
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsClient(response) {
        let params = response[0];
        if (this._options.clientNoContextTakeover === !1 && params.client_no_context_takeover)
          throw new Error('Unexpected parameter "client_no_context_takeover"');
        if (!params.client_max_window_bits)
          typeof this._options.clientMaxWindowBits == "number" && (params.client_max_window_bits = this._options.clientMaxWindowBits);
        else if (this._options.clientMaxWindowBits === !1 || typeof this._options.clientMaxWindowBits == "number" && params.client_max_window_bits > this._options.clientMaxWindowBits)
          throw new Error(
            'Unexpected or invalid parameter "client_max_window_bits"'
          );
        return params;
      }
      /**
       * Normalize parameters.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Array} The offers/response with normalized parameters
       * @private
       */
      normalizeParams(configurations) {
        return configurations.forEach((params) => {
          Object.keys(params).forEach((key) => {
            let value = params[key];
            if (value.length > 1)
              throw new Error(`Parameter "${key}" must have only a single value`);
            if (value = value[0], key === "client_max_window_bits") {
              if (value !== !0) {
                let num = +value;
                if (!Number.isInteger(num) || num < 8 || num > 15)
                  throw new TypeError(
                    `Invalid value for parameter "${key}": ${value}`
                  );
                value = num;
              } else if (!this._isServer)
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
            } else if (key === "server_max_window_bits") {
              let num = +value;
              if (!Number.isInteger(num) || num < 8 || num > 15)
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              value = num;
            } else if (key === "client_no_context_takeover" || key === "server_no_context_takeover") {
              if (value !== !0)
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
            } else
              throw new Error(`Unknown parameter "${key}"`);
            params[key] = value;
          });
        }), configurations;
      }
      /**
       * Decompress data. Concurrency limited.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      decompress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._decompress(data, fin, (err, result) => {
            done(), callback(err, result);
          });
        });
      }
      /**
       * Compress data. Concurrency limited.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      compress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._compress(data, fin, (err, result) => {
            done(), callback(err, result);
          });
        });
      }
      /**
       * Decompress data.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _decompress(data, fin, callback) {
        let endpoint = this._isServer ? "client" : "server";
        if (!this._inflate) {
          let key = `${endpoint}_max_window_bits`, windowBits = typeof this.params[key] != "number" ? zlib2.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._inflate = zlib2.createInflateRaw({
            ...this._options.zlibInflateOptions,
            windowBits
          }), this._inflate[kPerMessageDeflate] = this, this._inflate[kTotalLength] = 0, this._inflate[kBuffers] = [], this._inflate.on("error", inflateOnError), this._inflate.on("data", inflateOnData);
        }
        this._inflate[kCallback] = callback, this._inflate.write(data), fin && this._inflate.write(TRAILER), this._inflate.flush(() => {
          let err = this._inflate[kError];
          if (err) {
            this._inflate.close(), this._inflate = null, callback(err);
            return;
          }
          let data2 = bufferUtil.concat(
            this._inflate[kBuffers],
            this._inflate[kTotalLength]
          );
          this._inflate._readableState.endEmitted ? (this._inflate.close(), this._inflate = null) : (this._inflate[kTotalLength] = 0, this._inflate[kBuffers] = [], fin && this.params[`${endpoint}_no_context_takeover`] && this._inflate.reset()), callback(null, data2);
        });
      }
      /**
       * Compress data.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _compress(data, fin, callback) {
        let endpoint = this._isServer ? "server" : "client";
        if (!this._deflate) {
          let key = `${endpoint}_max_window_bits`, windowBits = typeof this.params[key] != "number" ? zlib2.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._deflate = zlib2.createDeflateRaw({
            ...this._options.zlibDeflateOptions,
            windowBits
          }), this._deflate[kTotalLength] = 0, this._deflate[kBuffers] = [], this._deflate.on("data", deflateOnData);
        }
        this._deflate[kCallback] = callback, this._deflate.write(data), this._deflate.flush(zlib2.Z_SYNC_FLUSH, () => {
          if (!this._deflate)
            return;
          let data2 = bufferUtil.concat(
            this._deflate[kBuffers],
            this._deflate[kTotalLength]
          );
          fin && (data2 = new FastBuffer(data2.buffer, data2.byteOffset, data2.length - 4)), this._deflate[kCallback] = null, this._deflate[kTotalLength] = 0, this._deflate[kBuffers] = [], fin && this.params[`${endpoint}_no_context_takeover`] && this._deflate.reset(), callback(null, data2);
        });
      }
    };
    module2.exports = PerMessageDeflate2;
    function deflateOnData(chunk) {
      this[kBuffers].push(chunk), this[kTotalLength] += chunk.length;
    }
    function inflateOnData(chunk) {
      if (this[kTotalLength] += chunk.length, this[kPerMessageDeflate]._maxPayload < 1 || this[kTotalLength] <= this[kPerMessageDeflate]._maxPayload) {
        this[kBuffers].push(chunk);
        return;
      }
      this[kError] = new RangeError("Max payload size exceeded"), this[kError].code = "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH", this[kError][kStatusCode] = 1009, this.removeListener("data", inflateOnData), this.reset();
    }
    function inflateOnError(err) {
      if (this[kPerMessageDeflate]._inflate = null, this[kError]) {
        this[kCallback](this[kError]);
        return;
      }
      err[kStatusCode] = 1007, this[kCallback](err);
    }
  }
});

// node_modules/ws/lib/validation.js
var require_validation = __commonJS({
  "node_modules/ws/lib/validation.js"(exports2, module2) {
    "use strict";
    var { isUtf8 } = require("buffer"), { hasBlob } = require_constants(), tokenChars = [
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 0 - 15
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 16 - 31
      0,
      1,
      0,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      1,
      1,
      0,
      1,
      1,
      0,
      // 32 - 47
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      // 48 - 63
      0,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 64 - 79
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      1,
      1,
      // 80 - 95
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 96 - 111
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      1,
      0,
      1,
      0
      // 112 - 127
    ];
    function isValidStatusCode(code) {
      return code >= 1e3 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006 || code >= 3e3 && code <= 4999;
    }
    function _isValidUTF8(buf) {
      let len = buf.length, i = 0;
      for (; i < len; )
        if ((buf[i] & 128) === 0)
          i++;
        else if ((buf[i] & 224) === 192) {
          if (i + 1 === len || (buf[i + 1] & 192) !== 128 || (buf[i] & 254) === 192)
            return !1;
          i += 2;
        } else if ((buf[i] & 240) === 224) {
          if (i + 2 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || buf[i] === 224 && (buf[i + 1] & 224) === 128 || // Overlong
          buf[i] === 237 && (buf[i + 1] & 224) === 160)
            return !1;
          i += 3;
        } else if ((buf[i] & 248) === 240) {
          if (i + 3 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || (buf[i + 3] & 192) !== 128 || buf[i] === 240 && (buf[i + 1] & 240) === 128 || // Overlong
          buf[i] === 244 && buf[i + 1] > 143 || buf[i] > 244)
            return !1;
          i += 4;
        } else
          return !1;
      return !0;
    }
    function isBlob(value) {
      return hasBlob && typeof value == "object" && typeof value.arrayBuffer == "function" && typeof value.type == "string" && typeof value.stream == "function" && (value[Symbol.toStringTag] === "Blob" || value[Symbol.toStringTag] === "File");
    }
    module2.exports = {
      isBlob,
      isValidStatusCode,
      isValidUTF8: _isValidUTF8,
      tokenChars
    };
    if (isUtf8)
      module2.exports.isValidUTF8 = function(buf) {
        return buf.length < 24 ? _isValidUTF8(buf) : isUtf8(buf);
      };
    else if (!process.env.WS_NO_UTF_8_VALIDATE)
      try {
        let isValidUTF8 = require("utf-8-validate");
        module2.exports.isValidUTF8 = function(buf) {
          return buf.length < 32 ? _isValidUTF8(buf) : isValidUTF8(buf);
        };
      } catch {
      }
  }
});

// node_modules/ws/lib/receiver.js
var require_receiver = __commonJS({
  "node_modules/ws/lib/receiver.js"(exports2, module2) {
    "use strict";
    var { Writable } = require("stream"), PerMessageDeflate2 = require_permessage_deflate(), {
      BINARY_TYPES,
      EMPTY_BUFFER,
      kStatusCode,
      kWebSocket
    } = require_constants(), { concat, toArrayBuffer, unmask } = require_buffer_util(), { isValidStatusCode, isValidUTF8 } = require_validation(), FastBuffer = Buffer[Symbol.species], GET_INFO = 0, GET_PAYLOAD_LENGTH_16 = 1, GET_PAYLOAD_LENGTH_64 = 2, GET_MASK = 3, GET_DATA = 4, INFLATING = 5, DEFER_EVENT = 6, Receiver2 = class extends Writable {
      /**
       * Creates a Receiver instance.
       *
       * @param {Object} [options] Options object
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {String} [options.binaryType=nodebuffer] The type for binary data
       * @param {Object} [options.extensions] An object containing the negotiated
       *     extensions
       * @param {Boolean} [options.isServer=false] Specifies whether to operate in
       *     client or server mode
       * @param {Number} [options.maxBufferedChunks=0] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=0] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       */
      constructor(options = {}) {
        super(), this._allowSynchronousEvents = options.allowSynchronousEvents !== void 0 ? options.allowSynchronousEvents : !0, this._binaryType = options.binaryType || BINARY_TYPES[0], this._extensions = options.extensions || {}, this._isServer = !!options.isServer, this._maxBufferedChunks = options.maxBufferedChunks | 0, this._maxFragments = options.maxFragments | 0, this._maxPayload = options.maxPayload | 0, this._skipUTF8Validation = !!options.skipUTF8Validation, this[kWebSocket] = void 0, this._bufferedBytes = 0, this._buffers = [], this._compressed = !1, this._payloadLength = 0, this._mask = void 0, this._fragmented = 0, this._masked = !1, this._fin = !1, this._opcode = 0, this._totalPayloadLength = 0, this._messageLength = 0, this._numFragments = 0, this._fragments = [], this._errored = !1, this._loop = !1, this._state = GET_INFO;
      }
      /**
       * Implements `Writable.prototype._write()`.
       *
       * @param {Buffer} chunk The chunk of data to write
       * @param {String} encoding The character encoding of `chunk`
       * @param {Function} cb Callback
       * @private
       */
      _write(chunk, encoding, cb) {
        if (this._opcode === 8 && this._state == GET_INFO) return cb();
        if (this._maxBufferedChunks > 0 && this._buffers.length >= this._maxBufferedChunks) {
          cb(
            this.createError(
              RangeError,
              "Too many buffered chunks",
              !1,
              1008,
              "WS_ERR_TOO_MANY_BUFFERED_PARTS"
            )
          );
          return;
        }
        this._bufferedBytes += chunk.length, this._buffers.push(chunk), this.startLoop(cb);
      }
      /**
       * Consumes `n` bytes from the buffered data.
       *
       * @param {Number} n The number of bytes to consume
       * @return {Buffer} The consumed bytes
       * @private
       */
      consume(n) {
        if (this._bufferedBytes -= n, n === this._buffers[0].length) return this._buffers.shift();
        if (n < this._buffers[0].length) {
          let buf = this._buffers[0];
          return this._buffers[0] = new FastBuffer(
            buf.buffer,
            buf.byteOffset + n,
            buf.length - n
          ), new FastBuffer(buf.buffer, buf.byteOffset, n);
        }
        let dst = Buffer.allocUnsafe(n);
        do {
          let buf = this._buffers[0], offset = dst.length - n;
          n >= buf.length ? dst.set(this._buffers.shift(), offset) : (dst.set(new Uint8Array(buf.buffer, buf.byteOffset, n), offset), this._buffers[0] = new FastBuffer(
            buf.buffer,
            buf.byteOffset + n,
            buf.length - n
          )), n -= buf.length;
        } while (n > 0);
        return dst;
      }
      /**
       * Starts the parsing loop.
       *
       * @param {Function} cb Callback
       * @private
       */
      startLoop(cb) {
        this._loop = !0;
        do
          switch (this._state) {
            case GET_INFO:
              this.getInfo(cb);
              break;
            case GET_PAYLOAD_LENGTH_16:
              this.getPayloadLength16(cb);
              break;
            case GET_PAYLOAD_LENGTH_64:
              this.getPayloadLength64(cb);
              break;
            case GET_MASK:
              this.getMask();
              break;
            case GET_DATA:
              this.getData(cb);
              break;
            case INFLATING:
            case DEFER_EVENT:
              this._loop = !1;
              return;
          }
        while (this._loop);
        this._errored || cb();
      }
      /**
       * Reads the first two bytes of a frame.
       *
       * @param {Function} cb Callback
       * @private
       */
      getInfo(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = !1;
          return;
        }
        let buf = this.consume(2);
        if ((buf[0] & 48) !== 0) {
          let error = this.createError(
            RangeError,
            "RSV2 and RSV3 must be clear",
            !0,
            1002,
            "WS_ERR_UNEXPECTED_RSV_2_3"
          );
          cb(error);
          return;
        }
        let compressed = (buf[0] & 64) === 64;
        if (compressed && !this._extensions[PerMessageDeflate2.extensionName]) {
          let error = this.createError(
            RangeError,
            "RSV1 must be clear",
            !0,
            1002,
            "WS_ERR_UNEXPECTED_RSV_1"
          );
          cb(error);
          return;
        }
        if (this._fin = (buf[0] & 128) === 128, this._opcode = buf[0] & 15, this._payloadLength = buf[1] & 127, this._opcode === 0) {
          if (compressed) {
            let error = this.createError(
              RangeError,
              "RSV1 must be clear",
              !0,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (!this._fragmented) {
            let error = this.createError(
              RangeError,
              "invalid opcode 0",
              !0,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._opcode = this._fragmented;
        } else if (this._opcode === 1 || this._opcode === 2) {
          if (this._fragmented) {
            let error = this.createError(
              RangeError,
              `invalid opcode ${this._opcode}`,
              !0,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._compressed = compressed;
        } else if (this._opcode > 7 && this._opcode < 11) {
          if (!this._fin) {
            let error = this.createError(
              RangeError,
              "FIN must be set",
              !0,
              1002,
              "WS_ERR_EXPECTED_FIN"
            );
            cb(error);
            return;
          }
          if (compressed) {
            let error = this.createError(
              RangeError,
              "RSV1 must be clear",
              !0,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (this._payloadLength > 125 || this._opcode === 8 && this._payloadLength === 1) {
            let error = this.createError(
              RangeError,
              `invalid payload length ${this._payloadLength}`,
              !0,
              1002,
              "WS_ERR_INVALID_CONTROL_PAYLOAD_LENGTH"
            );
            cb(error);
            return;
          }
        } else {
          let error = this.createError(
            RangeError,
            `invalid opcode ${this._opcode}`,
            !0,
            1002,
            "WS_ERR_INVALID_OPCODE"
          );
          cb(error);
          return;
        }
        if (!this._fin && !this._fragmented && (this._fragmented = this._opcode), this._masked = (buf[1] & 128) === 128, this._isServer) {
          if (!this._masked) {
            let error = this.createError(
              RangeError,
              "MASK must be set",
              !0,
              1002,
              "WS_ERR_EXPECTED_MASK"
            );
            cb(error);
            return;
          }
        } else if (this._masked) {
          let error = this.createError(
            RangeError,
            "MASK must be clear",
            !0,
            1002,
            "WS_ERR_UNEXPECTED_MASK"
          );
          cb(error);
          return;
        }
        this._payloadLength === 126 ? this._state = GET_PAYLOAD_LENGTH_16 : this._payloadLength === 127 ? this._state = GET_PAYLOAD_LENGTH_64 : this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+16).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength16(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = !1;
          return;
        }
        this._payloadLength = this.consume(2).readUInt16BE(0), this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+64).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength64(cb) {
        if (this._bufferedBytes < 8) {
          this._loop = !1;
          return;
        }
        let buf = this.consume(8), num = buf.readUInt32BE(0);
        if (num > Math.pow(2, 21) - 1) {
          let error = this.createError(
            RangeError,
            "Unsupported WebSocket frame: payload length > 2^53 - 1",
            !1,
            1009,
            "WS_ERR_UNSUPPORTED_DATA_PAYLOAD_LENGTH"
          );
          cb(error);
          return;
        }
        this._payloadLength = num * Math.pow(2, 32) + buf.readUInt32BE(4), this.haveLength(cb);
      }
      /**
       * Payload length has been read.
       *
       * @param {Function} cb Callback
       * @private
       */
      haveLength(cb) {
        if (this._payloadLength && this._opcode < 8 && (this._totalPayloadLength += this._payloadLength, this._totalPayloadLength > this._maxPayload && this._maxPayload > 0)) {
          let error = this.createError(
            RangeError,
            "Max payload size exceeded",
            !1,
            1009,
            "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
          );
          cb(error);
          return;
        }
        this._masked ? this._state = GET_MASK : this._state = GET_DATA;
      }
      /**
       * Reads mask bytes.
       *
       * @private
       */
      getMask() {
        if (this._bufferedBytes < 4) {
          this._loop = !1;
          return;
        }
        this._mask = this.consume(4), this._state = GET_DATA;
      }
      /**
       * Reads data bytes.
       *
       * @param {Function} cb Callback
       * @private
       */
      getData(cb) {
        let data = EMPTY_BUFFER;
        if (this._payloadLength) {
          if (this._bufferedBytes < this._payloadLength) {
            this._loop = !1;
            return;
          }
          data = this.consume(this._payloadLength), this._masked && (this._mask[0] | this._mask[1] | this._mask[2] | this._mask[3]) !== 0 && unmask(data, this._mask);
        }
        if (this._mask = void 0, this._opcode > 7) {
          this.controlMessage(data, cb);
          return;
        }
        if (this._maxFragments > 0 && ++this._numFragments > this._maxFragments) {
          let error = this.createError(
            RangeError,
            "Too many message fragments",
            !1,
            1008,
            "WS_ERR_TOO_MANY_BUFFERED_PARTS"
          );
          cb(error);
          return;
        }
        if (this._compressed) {
          this._state = INFLATING, this.decompress(data, cb);
          return;
        }
        data.length && (this._messageLength = this._totalPayloadLength, this._fragments.push(data)), this.dataMessage(cb);
      }
      /**
       * Decompresses data.
       *
       * @param {Buffer} data Compressed data
       * @param {Function} cb Callback
       * @private
       */
      decompress(data, cb) {
        this._extensions[PerMessageDeflate2.extensionName].decompress(data, this._fin, (err, buf) => {
          if (err) return cb(err);
          if (buf.length) {
            if (this._messageLength += buf.length, this._messageLength > this._maxPayload && this._maxPayload > 0) {
              let error = this.createError(
                RangeError,
                "Max payload size exceeded",
                !1,
                1009,
                "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
              );
              cb(error);
              return;
            }
            this._fragments.push(buf);
          }
          this.dataMessage(cb), this._state === GET_INFO && this.startLoop(cb);
        });
      }
      /**
       * Handles a data message.
       *
       * @param {Function} cb Callback
       * @private
       */
      dataMessage(cb) {
        if (!this._fin) {
          this._state = GET_INFO;
          return;
        }
        let messageLength = this._messageLength, fragments = this._fragments;
        if (this._totalPayloadLength = 0, this._messageLength = 0, this._fragmented = 0, this._numFragments = 0, this._fragments = [], this._opcode === 2) {
          let data;
          this._binaryType === "nodebuffer" ? data = concat(fragments, messageLength) : this._binaryType === "arraybuffer" ? data = toArrayBuffer(concat(fragments, messageLength)) : this._binaryType === "blob" ? data = new Blob(fragments) : data = fragments, this._allowSynchronousEvents ? (this.emit("message", data, !0), this._state = GET_INFO) : (this._state = DEFER_EVENT, setImmediate(() => {
            this.emit("message", data, !0), this._state = GET_INFO, this.startLoop(cb);
          }));
        } else {
          let buf = concat(fragments, messageLength);
          if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
            let error = this.createError(
              Error,
              "invalid UTF-8 sequence",
              !0,
              1007,
              "WS_ERR_INVALID_UTF8"
            );
            cb(error);
            return;
          }
          this._state === INFLATING || this._allowSynchronousEvents ? (this.emit("message", buf, !1), this._state = GET_INFO) : (this._state = DEFER_EVENT, setImmediate(() => {
            this.emit("message", buf, !1), this._state = GET_INFO, this.startLoop(cb);
          }));
        }
      }
      /**
       * Handles a control message.
       *
       * @param {Buffer} data Data to handle
       * @return {(Error|RangeError|undefined)} A possible error
       * @private
       */
      controlMessage(data, cb) {
        if (this._opcode === 8) {
          if (data.length === 0)
            this._loop = !1, this.emit("conclude", 1005, EMPTY_BUFFER), this.end();
          else {
            let code = data.readUInt16BE(0);
            if (!isValidStatusCode(code)) {
              let error = this.createError(
                RangeError,
                `invalid status code ${code}`,
                !0,
                1002,
                "WS_ERR_INVALID_CLOSE_CODE"
              );
              cb(error);
              return;
            }
            let buf = new FastBuffer(
              data.buffer,
              data.byteOffset + 2,
              data.length - 2
            );
            if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
              let error = this.createError(
                Error,
                "invalid UTF-8 sequence",
                !0,
                1007,
                "WS_ERR_INVALID_UTF8"
              );
              cb(error);
              return;
            }
            this._loop = !1, this.emit("conclude", code, buf), this.end();
          }
          this._state = GET_INFO;
          return;
        }
        this._allowSynchronousEvents ? (this.emit(this._opcode === 9 ? "ping" : "pong", data), this._state = GET_INFO) : (this._state = DEFER_EVENT, setImmediate(() => {
          this.emit(this._opcode === 9 ? "ping" : "pong", data), this._state = GET_INFO, this.startLoop(cb);
        }));
      }
      /**
       * Builds an error object.
       *
       * @param {function(new:Error|RangeError)} ErrorCtor The error constructor
       * @param {String} message The error message
       * @param {Boolean} prefix Specifies whether or not to add a default prefix to
       *     `message`
       * @param {Number} statusCode The status code
       * @param {String} errorCode The exposed error code
       * @return {(Error|RangeError)} The error
       * @private
       */
      createError(ErrorCtor, message, prefix, statusCode, errorCode) {
        this._loop = !1, this._errored = !0;
        let err = new ErrorCtor(
          prefix ? `Invalid WebSocket frame: ${message}` : message
        );
        return Error.captureStackTrace(err, this.createError), err.code = errorCode, err[kStatusCode] = statusCode, err;
      }
    };
    module2.exports = Receiver2;
  }
});

// node_modules/ws/lib/sender.js
var require_sender = __commonJS({
  "node_modules/ws/lib/sender.js"(exports2, module2) {
    "use strict";
    var { Duplex } = require("stream"), { randomFillSync } = require("crypto"), {
      types: { isUint8Array }
    } = require("util"), PerMessageDeflate2 = require_permessage_deflate(), { EMPTY_BUFFER, kWebSocket, NOOP } = require_constants(), { isBlob, isValidStatusCode } = require_validation(), { mask: applyMask, toBuffer } = require_buffer_util(), kByteLength = /* @__PURE__ */ Symbol("kByteLength"), maskBuffer = Buffer.alloc(4), RANDOM_POOL_SIZE = 8 * 1024, randomPool, randomPoolPointer = RANDOM_POOL_SIZE, DEFAULT = 0, DEFLATING = 1, GET_BLOB_DATA = 2, Sender2 = class _Sender {
      /**
       * Creates a Sender instance.
       *
       * @param {Duplex} socket The connection socket
       * @param {Object} [extensions] An object containing the negotiated extensions
       * @param {Function} [generateMask] The function used to generate the masking
       *     key
       */
      constructor(socket, extensions, generateMask) {
        this._extensions = extensions || {}, generateMask && (this._generateMask = generateMask, this._maskBuffer = Buffer.alloc(4)), this._socket = socket, this._firstFragment = !0, this._compress = !1, this._bufferedBytes = 0, this._queue = [], this._state = DEFAULT, this.onerror = NOOP, this[kWebSocket] = void 0;
      }
      /**
       * Frames a piece of data according to the HyBi WebSocket protocol.
       *
       * @param {(Buffer|String)} data The data to frame
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @return {(Buffer|String)[]} The framed data
       * @public
       */
      static frame(data, options) {
        let mask, merge = !1, offset = 2, skipMasking = !1;
        options.mask && (mask = options.maskBuffer || maskBuffer, options.generateMask ? options.generateMask(mask) : (randomPoolPointer === RANDOM_POOL_SIZE && (randomPool === void 0 && (randomPool = Buffer.alloc(RANDOM_POOL_SIZE)), randomFillSync(randomPool, 0, RANDOM_POOL_SIZE), randomPoolPointer = 0), mask[0] = randomPool[randomPoolPointer++], mask[1] = randomPool[randomPoolPointer++], mask[2] = randomPool[randomPoolPointer++], mask[3] = randomPool[randomPoolPointer++]), skipMasking = (mask[0] | mask[1] | mask[2] | mask[3]) === 0, offset = 6);
        let dataLength;
        typeof data == "string" ? (!options.mask || skipMasking) && options[kByteLength] !== void 0 ? dataLength = options[kByteLength] : (data = Buffer.from(data), dataLength = data.length) : (dataLength = data.length, merge = options.mask && options.readOnly && !skipMasking);
        let payloadLength = dataLength;
        dataLength >= 65536 ? (offset += 8, payloadLength = 127) : dataLength > 125 && (offset += 2, payloadLength = 126);
        let target = Buffer.allocUnsafe(merge ? dataLength + offset : offset);
        return target[0] = options.fin ? options.opcode | 128 : options.opcode, options.rsv1 && (target[0] |= 64), target[1] = payloadLength, payloadLength === 126 ? target.writeUInt16BE(dataLength, 2) : payloadLength === 127 && (target[2] = target[3] = 0, target.writeUIntBE(dataLength, 4, 6)), options.mask ? (target[1] |= 128, target[offset - 4] = mask[0], target[offset - 3] = mask[1], target[offset - 2] = mask[2], target[offset - 1] = mask[3], dataLength ? skipMasking ? [target, data] : merge ? (applyMask(data, mask, target, offset, dataLength), [target]) : (applyMask(data, mask, data, 0, dataLength), [target, data]) : [target]) : dataLength ? [target, data] : [target];
      }
      /**
       * Sends a close message to the other peer.
       *
       * @param {Number} [code] The status code component of the body
       * @param {(String|Buffer)} [data] The message component of the body
       * @param {Boolean} [mask=false] Specifies whether or not to mask the message
       * @param {Function} [cb] Callback
       * @public
       */
      close(code, data, mask, cb) {
        let buf;
        if (code === void 0)
          buf = EMPTY_BUFFER;
        else {
          if (typeof code != "number" || !isValidStatusCode(code))
            throw new TypeError("First argument must be a valid error code number");
          if (data === void 0 || !data.length)
            buf = Buffer.allocUnsafe(2), buf.writeUInt16BE(code, 0);
          else {
            let length = Buffer.byteLength(data);
            if (length > 123)
              throw new RangeError("The message must not be greater than 123 bytes");
            if (buf = Buffer.allocUnsafe(2 + length), buf.writeUInt16BE(code, 0), typeof data == "string")
              buf.write(data, 2);
            else if (isUint8Array(data))
              buf.set(data, 2);
            else
              throw new TypeError("Second argument must be a string or a Uint8Array");
          }
        }
        let options = {
          [kByteLength]: buf.length,
          fin: !0,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 8,
          readOnly: !1,
          rsv1: !1
        };
        this._state !== DEFAULT ? this.enqueue([this.dispatch, buf, !1, options, cb]) : this.sendFrame(_Sender.frame(buf, options), cb);
      }
      /**
       * Sends a ping message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      ping(data, mask, cb) {
        let byteLength, readOnly;
        if (typeof data == "string" ? (byteLength = Buffer.byteLength(data), readOnly = !1) : isBlob(data) ? (byteLength = data.size, readOnly = !1) : (data = toBuffer(data), byteLength = data.length, readOnly = toBuffer.readOnly), byteLength > 125)
          throw new RangeError("The data size must not be greater than 125 bytes");
        let options = {
          [kByteLength]: byteLength,
          fin: !0,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 9,
          readOnly,
          rsv1: !1
        };
        isBlob(data) ? this._state !== DEFAULT ? this.enqueue([this.getBlobData, data, !1, options, cb]) : this.getBlobData(data, !1, options, cb) : this._state !== DEFAULT ? this.enqueue([this.dispatch, data, !1, options, cb]) : this.sendFrame(_Sender.frame(data, options), cb);
      }
      /**
       * Sends a pong message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      pong(data, mask, cb) {
        let byteLength, readOnly;
        if (typeof data == "string" ? (byteLength = Buffer.byteLength(data), readOnly = !1) : isBlob(data) ? (byteLength = data.size, readOnly = !1) : (data = toBuffer(data), byteLength = data.length, readOnly = toBuffer.readOnly), byteLength > 125)
          throw new RangeError("The data size must not be greater than 125 bytes");
        let options = {
          [kByteLength]: byteLength,
          fin: !0,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 10,
          readOnly,
          rsv1: !1
        };
        isBlob(data) ? this._state !== DEFAULT ? this.enqueue([this.getBlobData, data, !1, options, cb]) : this.getBlobData(data, !1, options, cb) : this._state !== DEFAULT ? this.enqueue([this.dispatch, data, !1, options, cb]) : this.sendFrame(_Sender.frame(data, options), cb);
      }
      /**
       * Sends a data message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Object} options Options object
       * @param {Boolean} [options.binary=false] Specifies whether `data` is binary
       *     or text
       * @param {Boolean} [options.compress=false] Specifies whether or not to
       *     compress `data`
       * @param {Boolean} [options.fin=false] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Function} [cb] Callback
       * @public
       */
      send(data, options, cb) {
        let perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName], opcode = options.binary ? 2 : 1, rsv1 = options.compress, byteLength, readOnly;
        typeof data == "string" ? (byteLength = Buffer.byteLength(data), readOnly = !1) : isBlob(data) ? (byteLength = data.size, readOnly = !1) : (data = toBuffer(data), byteLength = data.length, readOnly = toBuffer.readOnly), this._firstFragment ? (this._firstFragment = !1, rsv1 && perMessageDeflate && perMessageDeflate.params[perMessageDeflate._isServer ? "server_no_context_takeover" : "client_no_context_takeover"] && (rsv1 = byteLength >= perMessageDeflate._threshold), this._compress = rsv1) : (rsv1 = !1, opcode = 0), options.fin && (this._firstFragment = !0);
        let opts = {
          [kByteLength]: byteLength,
          fin: options.fin,
          generateMask: this._generateMask,
          mask: options.mask,
          maskBuffer: this._maskBuffer,
          opcode,
          readOnly,
          rsv1
        };
        isBlob(data) ? this._state !== DEFAULT ? this.enqueue([this.getBlobData, data, this._compress, opts, cb]) : this.getBlobData(data, this._compress, opts, cb) : this._state !== DEFAULT ? this.enqueue([this.dispatch, data, this._compress, opts, cb]) : this.dispatch(data, this._compress, opts, cb);
      }
      /**
       * Gets the contents of a blob as binary data.
       *
       * @param {Blob} blob The blob
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     the data
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      getBlobData(blob, compress, options, cb) {
        this._bufferedBytes += options[kByteLength], this._state = GET_BLOB_DATA, blob.arrayBuffer().then((arrayBuffer) => {
          if (this._socket.destroyed) {
            let err = new Error(
              "The socket was closed while the blob was being read"
            );
            process.nextTick(callCallbacks, this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          let data = toBuffer(arrayBuffer);
          compress ? this.dispatch(data, compress, options, cb) : (this._state = DEFAULT, this.sendFrame(_Sender.frame(data, options), cb), this.dequeue());
        }).catch((err) => {
          process.nextTick(onError, this, err, cb);
        });
      }
      /**
       * Dispatches a message.
       *
       * @param {(Buffer|String)} data The message to send
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     `data`
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      dispatch(data, compress, options, cb) {
        if (!compress) {
          this.sendFrame(_Sender.frame(data, options), cb);
          return;
        }
        let perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        this._bufferedBytes += options[kByteLength], this._state = DEFLATING, perMessageDeflate.compress(data, options.fin, (_, buf) => {
          if (this._socket.destroyed) {
            let err = new Error(
              "The socket was closed while data was being compressed"
            );
            callCallbacks(this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength], this._state = DEFAULT, options.readOnly = !1, this.sendFrame(_Sender.frame(buf, options), cb), this.dequeue();
        });
      }
      /**
       * Executes queued send operations.
       *
       * @private
       */
      dequeue() {
        for (; this._state === DEFAULT && this._queue.length; ) {
          let params = this._queue.shift();
          this._bufferedBytes -= params[3][kByteLength], Reflect.apply(params[0], this, params.slice(1));
        }
      }
      /**
       * Enqueues a send operation.
       *
       * @param {Array} params Send operation parameters.
       * @private
       */
      enqueue(params) {
        this._bufferedBytes += params[3][kByteLength], this._queue.push(params);
      }
      /**
       * Sends a frame.
       *
       * @param {(Buffer | String)[]} list The frame to send
       * @param {Function} [cb] Callback
       * @private
       */
      sendFrame(list, cb) {
        list.length === 2 ? (this._socket.cork(), this._socket.write(list[0]), this._socket.write(list[1], cb), this._socket.uncork()) : this._socket.write(list[0], cb);
      }
    };
    module2.exports = Sender2;
    function callCallbacks(sender, err, cb) {
      typeof cb == "function" && cb(err);
      for (let i = 0; i < sender._queue.length; i++) {
        let params = sender._queue[i], callback = params[params.length - 1];
        typeof callback == "function" && callback(err);
      }
    }
    function onError(sender, err, cb) {
      callCallbacks(sender, err, cb), sender.onerror(err);
    }
  }
});

// node_modules/ws/lib/event-target.js
var require_event_target = __commonJS({
  "node_modules/ws/lib/event-target.js"(exports2, module2) {
    "use strict";
    var { kForOnEventAttribute, kListener } = require_constants(), kCode = /* @__PURE__ */ Symbol("kCode"), kData = /* @__PURE__ */ Symbol("kData"), kError = /* @__PURE__ */ Symbol("kError"), kMessage = /* @__PURE__ */ Symbol("kMessage"), kReason = /* @__PURE__ */ Symbol("kReason"), kTarget = /* @__PURE__ */ Symbol("kTarget"), kType = /* @__PURE__ */ Symbol("kType"), kWasClean = /* @__PURE__ */ Symbol("kWasClean"), Event = class {
      /**
       * Create a new `Event`.
       *
       * @param {String} type The name of the event
       * @throws {TypeError} If the `type` argument is not specified
       */
      constructor(type) {
        this[kTarget] = null, this[kType] = type;
      }
      /**
       * @type {*}
       */
      get target() {
        return this[kTarget];
      }
      /**
       * @type {String}
       */
      get type() {
        return this[kType];
      }
    };
    Object.defineProperty(Event.prototype, "target", { enumerable: !0 });
    Object.defineProperty(Event.prototype, "type", { enumerable: !0 });
    var CloseEvent = class extends Event {
      /**
       * Create a new `CloseEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {Number} [options.code=0] The status code explaining why the
       *     connection was closed
       * @param {String} [options.reason=''] A human-readable string explaining why
       *     the connection was closed
       * @param {Boolean} [options.wasClean=false] Indicates whether or not the
       *     connection was cleanly closed
       */
      constructor(type, options = {}) {
        super(type), this[kCode] = options.code === void 0 ? 0 : options.code, this[kReason] = options.reason === void 0 ? "" : options.reason, this[kWasClean] = options.wasClean === void 0 ? !1 : options.wasClean;
      }
      /**
       * @type {Number}
       */
      get code() {
        return this[kCode];
      }
      /**
       * @type {String}
       */
      get reason() {
        return this[kReason];
      }
      /**
       * @type {Boolean}
       */
      get wasClean() {
        return this[kWasClean];
      }
    };
    Object.defineProperty(CloseEvent.prototype, "code", { enumerable: !0 });
    Object.defineProperty(CloseEvent.prototype, "reason", { enumerable: !0 });
    Object.defineProperty(CloseEvent.prototype, "wasClean", { enumerable: !0 });
    var ErrorEvent = class extends Event {
      /**
       * Create a new `ErrorEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.error=null] The error that generated this event
       * @param {String} [options.message=''] The error message
       */
      constructor(type, options = {}) {
        super(type), this[kError] = options.error === void 0 ? null : options.error, this[kMessage] = options.message === void 0 ? "" : options.message;
      }
      /**
       * @type {*}
       */
      get error() {
        return this[kError];
      }
      /**
       * @type {String}
       */
      get message() {
        return this[kMessage];
      }
    };
    Object.defineProperty(ErrorEvent.prototype, "error", { enumerable: !0 });
    Object.defineProperty(ErrorEvent.prototype, "message", { enumerable: !0 });
    var MessageEvent = class extends Event {
      /**
       * Create a new `MessageEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.data=null] The message content
       */
      constructor(type, options = {}) {
        super(type), this[kData] = options.data === void 0 ? null : options.data;
      }
      /**
       * @type {*}
       */
      get data() {
        return this[kData];
      }
    };
    Object.defineProperty(MessageEvent.prototype, "data", { enumerable: !0 });
    var EventTarget = {
      /**
       * Register an event listener.
       *
       * @param {String} type A string representing the event type to listen for
       * @param {(Function|Object)} handler The listener to add
       * @param {Object} [options] An options object specifies characteristics about
       *     the event listener
       * @param {Boolean} [options.once=false] A `Boolean` indicating that the
       *     listener should be invoked at most once after being added. If `true`,
       *     the listener would be automatically removed when invoked.
       * @public
       */
      addEventListener(type, handler, options = {}) {
        for (let listener of this.listeners(type))
          if (!options[kForOnEventAttribute] && listener[kListener] === handler && !listener[kForOnEventAttribute])
            return;
        let wrapper;
        if (type === "message")
          wrapper = function(data, isBinary) {
            let event = new MessageEvent("message", {
              data: isBinary ? data : data.toString()
            });
            event[kTarget] = this, callListener(handler, this, event);
          };
        else if (type === "close")
          wrapper = function(code, message) {
            let event = new CloseEvent("close", {
              code,
              reason: message.toString(),
              wasClean: this._closeFrameReceived && this._closeFrameSent
            });
            event[kTarget] = this, callListener(handler, this, event);
          };
        else if (type === "error")
          wrapper = function(error) {
            let event = new ErrorEvent("error", {
              error,
              message: error.message
            });
            event[kTarget] = this, callListener(handler, this, event);
          };
        else if (type === "open")
          wrapper = function() {
            let event = new Event("open");
            event[kTarget] = this, callListener(handler, this, event);
          };
        else
          return;
        wrapper[kForOnEventAttribute] = !!options[kForOnEventAttribute], wrapper[kListener] = handler, options.once ? this.once(type, wrapper) : this.on(type, wrapper);
      },
      /**
       * Remove an event listener.
       *
       * @param {String} type A string representing the event type to remove
       * @param {(Function|Object)} handler The listener to remove
       * @public
       */
      removeEventListener(type, handler) {
        for (let listener of this.listeners(type))
          if (listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            this.removeListener(type, listener);
            break;
          }
      }
    };
    module2.exports = {
      CloseEvent,
      ErrorEvent,
      Event,
      EventTarget,
      MessageEvent
    };
    function callListener(listener, thisArg, event) {
      typeof listener == "object" && listener.handleEvent ? listener.handleEvent.call(listener, event) : listener.call(thisArg, event);
    }
  }
});

// node_modules/ws/lib/extension.js
var require_extension = __commonJS({
  "node_modules/ws/lib/extension.js"(exports2, module2) {
    "use strict";
    var { tokenChars } = require_validation();
    function push(dest, name, elem) {
      dest[name] === void 0 ? dest[name] = [elem] : dest[name].push(elem);
    }
    function parse(header) {
      let offers = /* @__PURE__ */ Object.create(null), params = /* @__PURE__ */ Object.create(null), mustUnescape = !1, isEscaping = !1, inQuotes = !1, extensionName, paramName, start = -1, code = -1, end = -1, i = 0;
      for (; i < header.length; i++)
        if (code = header.charCodeAt(i), extensionName === void 0)
          if (end === -1 && tokenChars[code] === 1)
            start === -1 && (start = i);
          else if (i !== 0 && (code === 32 || code === 9))
            end === -1 && start !== -1 && (end = i);
          else if (code === 59 || code === 44) {
            if (start === -1)
              throw new SyntaxError(`Unexpected character at index ${i}`);
            end === -1 && (end = i);
            let name = header.slice(start, end);
            code === 44 ? (push(offers, name, params), params = /* @__PURE__ */ Object.create(null)) : extensionName = name, start = end = -1;
          } else
            throw new SyntaxError(`Unexpected character at index ${i}`);
        else if (paramName === void 0)
          if (end === -1 && tokenChars[code] === 1)
            start === -1 && (start = i);
          else if (code === 32 || code === 9)
            end === -1 && start !== -1 && (end = i);
          else if (code === 59 || code === 44) {
            if (start === -1)
              throw new SyntaxError(`Unexpected character at index ${i}`);
            end === -1 && (end = i), push(params, header.slice(start, end), !0), code === 44 && (push(offers, extensionName, params), params = /* @__PURE__ */ Object.create(null), extensionName = void 0), start = end = -1;
          } else if (code === 61 && start !== -1 && end === -1)
            paramName = header.slice(start, i), start = end = -1;
          else
            throw new SyntaxError(`Unexpected character at index ${i}`);
        else if (isEscaping) {
          if (tokenChars[code] !== 1)
            throw new SyntaxError(`Unexpected character at index ${i}`);
          start === -1 ? start = i : mustUnescape || (mustUnescape = !0), isEscaping = !1;
        } else if (inQuotes)
          if (tokenChars[code] === 1)
            start === -1 && (start = i);
          else if (code === 34 && start !== -1)
            inQuotes = !1, end = i;
          else if (code === 92)
            isEscaping = !0;
          else
            throw new SyntaxError(`Unexpected character at index ${i}`);
        else if (code === 34 && header.charCodeAt(i - 1) === 61)
          inQuotes = !0;
        else if (end === -1 && tokenChars[code] === 1)
          start === -1 && (start = i);
        else if (start !== -1 && (code === 32 || code === 9))
          end === -1 && (end = i);
        else if (code === 59 || code === 44) {
          if (start === -1)
            throw new SyntaxError(`Unexpected character at index ${i}`);
          end === -1 && (end = i);
          let value = header.slice(start, end);
          mustUnescape && (value = value.replace(/\\/g, ""), mustUnescape = !1), push(params, paramName, value), code === 44 && (push(offers, extensionName, params), params = /* @__PURE__ */ Object.create(null), extensionName = void 0), paramName = void 0, start = end = -1;
        } else
          throw new SyntaxError(`Unexpected character at index ${i}`);
      if (start === -1 || inQuotes || code === 32 || code === 9)
        throw new SyntaxError("Unexpected end of input");
      end === -1 && (end = i);
      let token2 = header.slice(start, end);
      return extensionName === void 0 ? push(offers, token2, params) : (paramName === void 0 ? push(params, token2, !0) : mustUnescape ? push(params, paramName, token2.replace(/\\/g, "")) : push(params, paramName, token2), push(offers, extensionName, params)), offers;
    }
    function format(extensions) {
      return Object.keys(extensions).map((extension2) => {
        let configurations = extensions[extension2];
        return Array.isArray(configurations) || (configurations = [configurations]), configurations.map((params) => [extension2].concat(
          Object.keys(params).map((k) => {
            let values = params[k];
            return Array.isArray(values) || (values = [values]), values.map((v) => v === !0 ? k : `${k}=${v}`).join("; ");
          })
        ).join("; ")).join(", ");
      }).join(", ");
    }
    module2.exports = { format, parse };
  }
});

// node_modules/ws/lib/websocket.js
var require_websocket = __commonJS({
  "node_modules/ws/lib/websocket.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events"), https = require("https"), http2 = require("http"), net = require("net"), querystring = require("querystring"), tls = require("tls"), { randomBytes: randomBytes2, createHash } = require("crypto"), { Duplex, Readable } = require("stream"), { URL: URL2 } = require("url"), PerMessageDeflate2 = require_permessage_deflate(), Receiver2 = require_receiver(), Sender2 = require_sender(), { isBlob } = require_validation(), {
      BINARY_TYPES,
      CLOSE_TIMEOUT,
      EMPTY_BUFFER,
      GUID,
      kForOnEventAttribute,
      kListener,
      kStatusCode,
      kWebSocket,
      NOOP
    } = require_constants(), {
      EventTarget: { addEventListener, removeEventListener }
    } = require_event_target(), { format, parse } = require_extension(), { toBuffer } = require_buffer_util(), kAborted = /* @__PURE__ */ Symbol("kAborted"), protocolVersions = [8, 13], readyStates = ["CONNECTING", "OPEN", "CLOSING", "CLOSED"], subprotocolRegex = /^[!#$%&'*+\-.0-9A-Z^_`|a-z~]+$/, WebSocket2 = class _WebSocket extends EventEmitter {
      /**
       * Create a new `WebSocket`.
       *
       * @param {(String|URL)} address The URL to which to connect
       * @param {(String|String[])} [protocols] The subprotocols
       * @param {Object} [options] Connection options
       */
      constructor(address, protocols, options) {
        super(), this._binaryType = BINARY_TYPES[0], this._closeCode = 1006, this._closeFrameReceived = !1, this._closeFrameSent = !1, this._closeMessage = EMPTY_BUFFER, this._closeTimer = null, this._errorEmitted = !1, this._extensions = {}, this._paused = !1, this._protocol = "", this._readyState = _WebSocket.CONNECTING, this._receiver = null, this._sender = null, this._socket = null, address !== null ? (this._bufferedAmount = 0, this._isServer = !1, this._redirects = 0, protocols === void 0 ? !options || options.protocols === void 0 ? protocols = [] : Array.isArray(options.protocols) ? protocols = options.protocols : protocols = [options.protocols] : Array.isArray(protocols) || (typeof protocols == "object" && protocols !== null ? (options = protocols, options.protocols === void 0 ? protocols = [] : Array.isArray(options.protocols) ? protocols = options.protocols : protocols = [options.protocols]) : protocols = [protocols]), options = initAsClient(this, address, protocols, options)) : this._isServer = !0, options.autoPong && (this._autoPong = !0, this._autoPongComplete = () => autoPongComplete(this), this._pendingAutoPongs = 0, this._maxPendingAutoPongs = options.maxPendingAutoPongs | 0), this._closeTimeout = options.closeTimeout;
      }
      /**
       * For historical reasons, the custom "nodebuffer" type is used by the default
       * instead of "blob".
       *
       * @type {String}
       */
      get binaryType() {
        return this._binaryType;
      }
      set binaryType(type) {
        BINARY_TYPES.includes(type) && (this._binaryType = type, this._receiver && (this._receiver._binaryType = type));
      }
      /**
       * @type {Number}
       */
      get bufferedAmount() {
        return this._socket ? this._socket._writableState.length + this._sender._bufferedBytes : this._bufferedAmount;
      }
      /**
       * @type {String}
       */
      get extensions() {
        return Object.keys(this._extensions).join();
      }
      /**
       * @type {Boolean}
       */
      get isPaused() {
        return this._paused;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onclose() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onerror() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onopen() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onmessage() {
        return null;
      }
      /**
       * @type {String}
       */
      get protocol() {
        return this._protocol;
      }
      /**
       * @type {Number}
       */
      get readyState() {
        return this._readyState;
      }
      /**
       * @type {String}
       */
      get url() {
        return this._url;
      }
      /**
       * Set up the socket and the internal resources.
       *
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Object} options Options object
       * @param {Boolean} [options.allowSynchronousEvents=false] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Number} [options.maxBufferedChunks=0] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=0] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=0] The maximum allowed message size
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @private
       */
      setSocket(socket, head, options) {
        let receiver = new Receiver2({
          allowSynchronousEvents: options.allowSynchronousEvents,
          binaryType: this.binaryType,
          extensions: this._extensions,
          isServer: this._isServer,
          maxBufferedChunks: options.maxBufferedChunks,
          maxFragments: options.maxFragments,
          maxPayload: options.maxPayload,
          skipUTF8Validation: options.skipUTF8Validation
        }), sender = new Sender2(socket, this._extensions, options.generateMask);
        this._receiver = receiver, this._sender = sender, this._socket = socket, receiver[kWebSocket] = this, sender[kWebSocket] = this, socket[kWebSocket] = this, receiver.on("conclude", receiverOnConclude), receiver.on("drain", receiverOnDrain), receiver.on("error", receiverOnError), receiver.on("message", receiverOnMessage), receiver.on("ping", receiverOnPing), receiver.on("pong", receiverOnPong), sender.onerror = senderOnError, socket.setTimeout && socket.setTimeout(0), socket.setNoDelay && socket.setNoDelay(), head.length > 0 && socket.unshift(head), socket.on("close", socketOnClose), socket.on("data", socketOnData), socket.on("end", socketOnEnd), socket.on("error", socketOnError), this._readyState = _WebSocket.OPEN, this.emit("open");
      }
      /**
       * Emit the `'close'` event.
       *
       * @private
       */
      emitClose() {
        if (!this._socket) {
          this._readyState = _WebSocket.CLOSED, this.emit("close", this._closeCode, this._closeMessage);
          return;
        }
        this._extensions[PerMessageDeflate2.extensionName] && this._extensions[PerMessageDeflate2.extensionName].cleanup(), this._receiver.removeAllListeners(), this._readyState = _WebSocket.CLOSED, this.emit("close", this._closeCode, this._closeMessage);
      }
      /**
       * Start a closing handshake.
       *
       *          +----------+   +-----------+   +----------+
       *     - - -|ws.close()|-->|close frame|-->|ws.close()|- - -
       *    |     +----------+   +-----------+   +----------+     |
       *          +----------+   +-----------+         |
       * CLOSING  |ws.close()|<--|close frame|<--+-----+       CLOSING
       *          +----------+   +-----------+   |
       *    |           |                        |   +---+        |
       *                +------------------------+-->|fin| - - - -
       *    |         +---+                      |   +---+
       *     - - - - -|fin|<---------------------+
       *              +---+
       *
       * @param {Number} [code] Status code explaining why the connection is closing
       * @param {(String|Buffer)} [data] The reason why the connection is
       *     closing
       * @public
       */
      close(code, data) {
        if (this.readyState !== _WebSocket.CLOSED) {
          if (this.readyState === _WebSocket.CONNECTING) {
            abortHandshake(this, this._req, "WebSocket was closed before the connection was established");
            return;
          }
          if (this.readyState === _WebSocket.CLOSING) {
            this._closeFrameSent && (this._closeFrameReceived || this._receiver._writableState.errorEmitted) && this._socket.end();
            return;
          }
          this._sender.close(code, data, !this._isServer, (err) => {
            err || (this._closeFrameSent = !0, (this._closeFrameReceived || this._receiver._writableState.errorEmitted) && this._socket.end());
          }), this._readyState = _WebSocket.CLOSING, setCloseTimer(this);
        }
      }
      /**
       * Pause the socket.
       *
       * @public
       */
      pause() {
        this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED || (this._paused = !0, this._socket.pause());
      }
      /**
       * Send a ping.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the ping is sent
       * @public
       */
      ping(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING)
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        if (typeof data == "function" ? (cb = data, data = mask = void 0) : typeof mask == "function" && (cb = mask, mask = void 0), typeof data == "number" && (data = data.toString()), this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        mask === void 0 && (mask = !this._isServer), this._sender.ping(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Send a pong.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the pong is sent
       * @public
       */
      pong(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING)
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        if (typeof data == "function" ? (cb = data, data = mask = void 0) : typeof mask == "function" && (cb = mask, mask = void 0), typeof data == "number" && (data = data.toString()), this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        mask === void 0 && (mask = !this._isServer), this._sender.pong(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Resume the socket.
       *
       * @public
       */
      resume() {
        this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED || (this._paused = !1, this._receiver._writableState.needDrain || this._socket.resume());
      }
      /**
       * Send a data message.
       *
       * @param {*} data The message to send
       * @param {Object} [options] Options object
       * @param {Boolean} [options.binary] Specifies whether `data` is binary or
       *     text
       * @param {Boolean} [options.compress] Specifies whether or not to compress
       *     `data`
       * @param {Boolean} [options.fin=true] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when data is written out
       * @public
       */
      send(data, options, cb) {
        if (this.readyState === _WebSocket.CONNECTING)
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        if (typeof options == "function" && (cb = options, options = {}), typeof data == "number" && (data = data.toString()), this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        let opts = {
          binary: typeof data != "string",
          mask: !this._isServer,
          compress: !0,
          fin: !0,
          ...options
        };
        this._extensions[PerMessageDeflate2.extensionName] || (opts.compress = !1), this._sender.send(data || EMPTY_BUFFER, opts, cb);
      }
      /**
       * Forcibly close the connection.
       *
       * @public
       */
      terminate() {
        if (this.readyState !== _WebSocket.CLOSED) {
          if (this.readyState === _WebSocket.CONNECTING) {
            abortHandshake(this, this._req, "WebSocket was closed before the connection was established");
            return;
          }
          this._socket && (this._readyState = _WebSocket.CLOSING, this._socket.destroy());
        }
      }
    };
    Object.defineProperty(WebSocket2, "CONNECTING", {
      enumerable: !0,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2.prototype, "CONNECTING", {
      enumerable: !0,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2, "OPEN", {
      enumerable: !0,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2.prototype, "OPEN", {
      enumerable: !0,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2, "CLOSING", {
      enumerable: !0,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSING", {
      enumerable: !0,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2, "CLOSED", {
      enumerable: !0,
      value: readyStates.indexOf("CLOSED")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSED", {
      enumerable: !0,
      value: readyStates.indexOf("CLOSED")
    });
    [
      "binaryType",
      "bufferedAmount",
      "extensions",
      "isPaused",
      "protocol",
      "readyState",
      "url"
    ].forEach((property) => {
      Object.defineProperty(WebSocket2.prototype, property, { enumerable: !0 });
    });
    ["open", "error", "close", "message"].forEach((method) => {
      Object.defineProperty(WebSocket2.prototype, `on${method}`, {
        enumerable: !0,
        get() {
          for (let listener of this.listeners(method))
            if (listener[kForOnEventAttribute]) return listener[kListener];
          return null;
        },
        set(handler) {
          for (let listener of this.listeners(method))
            if (listener[kForOnEventAttribute]) {
              this.removeListener(method, listener);
              break;
            }
          typeof handler == "function" && this.addEventListener(method, handler, {
            [kForOnEventAttribute]: !0
          });
        }
      });
    });
    WebSocket2.prototype.addEventListener = addEventListener;
    WebSocket2.prototype.removeEventListener = removeEventListener;
    module2.exports = WebSocket2;
    function initAsClient(websocket, address, protocols, options) {
      let opts = {
        allowSynchronousEvents: !0,
        autoPong: !0,
        closeTimeout: CLOSE_TIMEOUT,
        protocolVersion: protocolVersions[1],
        maxBufferedChunks: 262144,
        maxFragments: 16384,
        maxPayload: 104857600,
        maxPendingAutoPongs: 60,
        skipUTF8Validation: !1,
        perMessageDeflate: !0,
        followRedirects: !1,
        maxRedirects: 10,
        ...options,
        socketPath: void 0,
        hostname: void 0,
        protocol: void 0,
        protocols: void 0,
        timeout: void 0,
        method: "GET",
        host: void 0,
        path: void 0,
        port: void 0
      };
      if (!protocolVersions.includes(opts.protocolVersion))
        throw new RangeError(
          `Unsupported protocol version: ${opts.protocolVersion} (supported versions: ${protocolVersions.join(", ")})`
        );
      let parsedUrl;
      if (address instanceof URL2)
        parsedUrl = address;
      else
        try {
          parsedUrl = new URL2(address);
        } catch {
          throw new SyntaxError(`Invalid URL: ${address}`);
        }
      (parsedUrl.protocol === "http:" || parsedUrl.protocol === "https:") && (parsedUrl = new URL2(parsedUrl), parsedUrl.protocol = parsedUrl.protocol === "http:" ? "ws:" : "wss:"), websocket._url = parsedUrl.href;
      let isSecure2 = parsedUrl.protocol === "wss:", isIpcUrl = parsedUrl.protocol === "ws+unix:", invalidUrlMessage;
      if (parsedUrl.protocol !== "ws:" && !isSecure2 && !isIpcUrl ? invalidUrlMessage = `The URL's protocol must be one of "ws:", "wss:", "http:", "https:", or "ws+unix:"` : isIpcUrl && !parsedUrl.pathname ? invalidUrlMessage = "The URL's pathname is empty" : parsedUrl.hash && (invalidUrlMessage = "The URL contains a fragment identifier"), invalidUrlMessage) {
        let err = new SyntaxError(invalidUrlMessage);
        if (websocket._redirects === 0)
          throw err;
        emitErrorAndClose(websocket, err);
        return;
      }
      let defaultPort = isSecure2 ? 443 : 80, key = randomBytes2(16).toString("base64"), request = isSecure2 ? https.request : http2.request, protocolSet = /* @__PURE__ */ new Set(), perMessageDeflate;
      if (opts.createConnection = opts.createConnection || (isSecure2 ? tlsConnect : netConnect), opts.defaultPort = opts.defaultPort || defaultPort, opts.port = parsedUrl.port || defaultPort, opts.host = parsedUrl.hostname.startsWith("[") ? parsedUrl.hostname.slice(1, -1) : parsedUrl.hostname, opts.headers = {
        ...opts.headers,
        "Sec-WebSocket-Version": opts.protocolVersion,
        "Sec-WebSocket-Key": key,
        Connection: "Upgrade",
        Upgrade: "websocket"
      }, opts.path = parsedUrl.pathname + parsedUrl.search, opts.timeout = opts.handshakeTimeout, opts.perMessageDeflate && (perMessageDeflate = new PerMessageDeflate2({
        ...opts.perMessageDeflate,
        isServer: !1,
        maxPayload: opts.maxPayload
      }), opts.headers["Sec-WebSocket-Extensions"] = format({
        [PerMessageDeflate2.extensionName]: perMessageDeflate.offer()
      })), protocols.length) {
        for (let protocol of protocols) {
          if (typeof protocol != "string" || !subprotocolRegex.test(protocol) || protocolSet.has(protocol))
            throw new SyntaxError(
              "An invalid or duplicated subprotocol was specified"
            );
          protocolSet.add(protocol);
        }
        opts.headers["Sec-WebSocket-Protocol"] = protocols.join(",");
      }
      if (opts.origin && (opts.protocolVersion < 13 ? opts.headers["Sec-WebSocket-Origin"] = opts.origin : opts.headers.Origin = opts.origin), parsedUrl.username || parsedUrl.password) {
        let username = querystring.unescape(parsedUrl.username), password = querystring.unescape(parsedUrl.password);
        opts.auth = `${username}:${password}`;
      }
      if (isIpcUrl) {
        let i = opts.path.indexOf(":");
        i === -1 ? (opts.socketPath = opts.path, opts.path = "") : (opts.socketPath = opts.path.slice(0, i), opts.path = opts.path.slice(i + 1));
      }
      let req;
      if (opts.followRedirects) {
        if (websocket._redirects === 0) {
          websocket._originalIpc = isIpcUrl, websocket._originalSecure = isSecure2, websocket._originalHostOrSocketPath = isIpcUrl ? opts.socketPath : parsedUrl.host;
          let headers = options && options.headers;
          if (options = { ...options, headers: {} }, headers)
            for (let [key2, value] of Object.entries(headers))
              options.headers[key2.toLowerCase()] = value;
        } else if (websocket.listenerCount("redirect") === 0) {
          let isSameHost = isIpcUrl ? websocket._originalIpc ? opts.socketPath === websocket._originalHostOrSocketPath : !1 : websocket._originalIpc ? !1 : parsedUrl.host === websocket._originalHostOrSocketPath;
          (!isSameHost || websocket._originalSecure && !isSecure2) && (delete opts.headers.authorization, delete opts.headers.cookie, isSameHost || delete opts.headers.host, opts.auth = void 0);
        }
        opts.auth && !options.headers.authorization && (options.headers.authorization = "Basic " + Buffer.from(opts.auth).toString("base64")), req = websocket._req = request(opts), websocket._redirects && websocket.emit("redirect", websocket.url, req);
      } else
        req = websocket._req = request(opts);
      return opts.timeout && req.on("timeout", () => {
        abortHandshake(websocket, req, "Opening handshake has timed out");
      }), req.on("error", (err) => {
        req === null || req[kAborted] || (req = websocket._req = null, emitErrorAndClose(websocket, err));
      }), req.on("response", (res) => {
        let location = res.headers.location, statusCode = res.statusCode;
        if (location && opts.followRedirects && statusCode >= 300 && statusCode < 400) {
          if (++websocket._redirects > opts.maxRedirects) {
            abortHandshake(websocket, req, "Maximum redirects exceeded");
            return;
          }
          req.abort();
          let addr;
          try {
            addr = new URL2(location, address);
          } catch {
            let err = new SyntaxError(`Invalid URL: ${location}`);
            emitErrorAndClose(websocket, err);
            return;
          }
          initAsClient(websocket, addr, protocols, options);
        } else websocket.emit("unexpected-response", req, res) || abortHandshake(
          websocket,
          req,
          `Unexpected server response: ${res.statusCode}`
        );
      }), req.on("upgrade", (res, socket, head) => {
        if (websocket.emit("upgrade", res), websocket.readyState !== WebSocket2.CONNECTING) return;
        req = websocket._req = null;
        let upgrade = res.headers.upgrade;
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          abortHandshake(websocket, socket, "Invalid Upgrade header");
          return;
        }
        let digest = createHash("sha1").update(key + GUID).digest("base64");
        if (res.headers["sec-websocket-accept"] !== digest) {
          abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Accept header");
          return;
        }
        let serverProt = res.headers["sec-websocket-protocol"], protError;
        if (serverProt !== void 0 ? protocolSet.size ? protocolSet.has(serverProt) || (protError = "Server sent an invalid subprotocol") : protError = "Server sent a subprotocol but none was requested" : protocolSet.size && (protError = "Server sent no subprotocol"), protError) {
          abortHandshake(websocket, socket, protError);
          return;
        }
        serverProt && (websocket._protocol = serverProt);
        let secWebSocketExtensions = res.headers["sec-websocket-extensions"];
        if (secWebSocketExtensions !== void 0) {
          if (!perMessageDeflate) {
            abortHandshake(websocket, socket, "Server sent a Sec-WebSocket-Extensions header but no extension was requested");
            return;
          }
          let extensions;
          try {
            extensions = parse(secWebSocketExtensions);
          } catch {
            abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Extensions header");
            return;
          }
          let extensionNames = Object.keys(extensions);
          if (extensionNames.length !== 1 || extensionNames[0] !== PerMessageDeflate2.extensionName) {
            abortHandshake(websocket, socket, "Server indicated an extension that was not requested");
            return;
          }
          try {
            perMessageDeflate.accept(extensions[PerMessageDeflate2.extensionName]);
          } catch {
            abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Extensions header");
            return;
          }
          websocket._extensions[PerMessageDeflate2.extensionName] = perMessageDeflate;
        }
        websocket.setSocket(socket, head, {
          allowSynchronousEvents: opts.allowSynchronousEvents,
          generateMask: opts.generateMask,
          maxBufferedChunks: opts.maxBufferedChunks,
          maxFragments: opts.maxFragments,
          maxPayload: opts.maxPayload,
          skipUTF8Validation: opts.skipUTF8Validation
        });
      }), opts.finishRequest ? opts.finishRequest(req, websocket) : req.end(), opts;
    }
    function emitErrorAndClose(websocket, err) {
      websocket._readyState = WebSocket2.CLOSING, websocket._errorEmitted = !0, websocket.emit("error", err), websocket.emitClose();
    }
    function netConnect(options) {
      return options.path = options.socketPath, net.connect(options);
    }
    function tlsConnect(options) {
      return options.path = void 0, !options.servername && options.servername !== "" && (options.servername = net.isIP(options.host) ? "" : options.host), tls.connect(options);
    }
    function abortHandshake(websocket, stream, message) {
      websocket._readyState = WebSocket2.CLOSING;
      let err = new Error(message);
      Error.captureStackTrace(err, abortHandshake), stream.setHeader ? (stream[kAborted] = !0, stream.abort(), stream.socket && !stream.socket.destroyed && stream.socket.destroy(), process.nextTick(emitErrorAndClose, websocket, err)) : (stream.destroy(err), stream.once("error", websocket.emit.bind(websocket, "error")), stream.once("close", websocket.emitClose.bind(websocket)));
    }
    function sendAfterClose(websocket, data, cb) {
      if (data) {
        let length = isBlob(data) ? data.size : toBuffer(data).length;
        websocket._socket ? websocket._sender._bufferedBytes += length : websocket._bufferedAmount += length;
      }
      if (cb) {
        let err = new Error(
          `WebSocket is not open: readyState ${websocket.readyState} (${readyStates[websocket.readyState]})`
        );
        process.nextTick(cb, err);
      }
    }
    function receiverOnConclude(code, reason) {
      let websocket = this[kWebSocket];
      websocket._closeFrameReceived = !0, websocket._closeCode = code, reason.length === 0 ? websocket._closeMessage = EMPTY_BUFFER : (websocket._closeMessage = Buffer.allocUnsafeSlow(reason.length), websocket._closeMessage.set(reason)), websocket._socket[kWebSocket] !== void 0 && (websocket._socket.removeListener("data", socketOnData), process.nextTick(resume, websocket._socket), code === 1005 ? websocket.close() : websocket.close(code, reason));
    }
    function receiverOnDrain() {
      let websocket = this[kWebSocket];
      websocket.isPaused || websocket._socket.resume();
    }
    function receiverOnError(err) {
      let websocket = this[kWebSocket];
      websocket._socket[kWebSocket] !== void 0 && (websocket._socket.removeListener("data", socketOnData), process.nextTick(resume, websocket._socket), websocket.close(err[kStatusCode])), websocket._errorEmitted || (websocket._errorEmitted = !0, websocket.emit("error", err));
    }
    function receiverOnFinish() {
      this[kWebSocket].emitClose();
    }
    function receiverOnMessage(data, isBinary) {
      this[kWebSocket].emit("message", data, isBinary);
    }
    function receiverOnPing(data) {
      let websocket = this[kWebSocket];
      websocket._autoPong && websocket.readyState === WebSocket2.OPEN && (websocket._maxPendingAutoPongs > 0 && websocket._pendingAutoPongs >= websocket._maxPendingAutoPongs ? websocket.terminate() : (websocket._pendingAutoPongs++, websocket.pong(data, !this._isServer, websocket._autoPongComplete))), websocket.emit("ping", data);
    }
    function autoPongComplete(websocket) {
      websocket._pendingAutoPongs--;
    }
    function receiverOnPong(data) {
      this[kWebSocket].emit("pong", data);
    }
    function resume(stream) {
      stream.resume();
    }
    function senderOnError(err) {
      let websocket = this[kWebSocket];
      websocket.readyState !== WebSocket2.CLOSED && (websocket.readyState === WebSocket2.OPEN && (websocket._readyState = WebSocket2.CLOSING, setCloseTimer(websocket)), this._socket.end(), websocket._errorEmitted || (websocket._errorEmitted = !0, websocket.emit("error", err)));
    }
    function setCloseTimer(websocket) {
      websocket._closeTimer = setTimeout(
        websocket._socket.destroy.bind(websocket._socket),
        websocket._closeTimeout
      );
    }
    function socketOnClose() {
      let websocket = this[kWebSocket];
      if (this.removeListener("close", socketOnClose), this.removeListener("data", socketOnData), this.removeListener("end", socketOnEnd), websocket._readyState = WebSocket2.CLOSING, !this._readableState.endEmitted && !websocket._closeFrameReceived && !websocket._receiver._writableState.errorEmitted && this._readableState.length !== 0) {
        let chunk = this.read(this._readableState.length);
        websocket._receiver.write(chunk);
      }
      websocket._receiver.end(), this[kWebSocket] = void 0, clearTimeout(websocket._closeTimer), websocket._receiver._writableState.finished || websocket._receiver._writableState.errorEmitted ? websocket.emitClose() : (websocket._receiver.on("error", receiverOnFinish), websocket._receiver.on("finish", receiverOnFinish));
    }
    function socketOnData(chunk) {
      this[kWebSocket]._receiver.write(chunk) || this.pause();
    }
    function socketOnEnd() {
      let websocket = this[kWebSocket];
      websocket._readyState = WebSocket2.CLOSING, websocket._receiver.end(), this.end();
    }
    function socketOnError() {
      let websocket = this[kWebSocket];
      this.removeListener("error", socketOnError), this.on("error", NOOP), websocket && (websocket._readyState = WebSocket2.CLOSING, this.destroy());
    }
  }
});

// node_modules/ws/lib/stream.js
var require_stream = __commonJS({
  "node_modules/ws/lib/stream.js"(exports2, module2) {
    "use strict";
    var WebSocket2 = require_websocket(), { Duplex } = require("stream");
    function emitClose(stream) {
      stream.emit("close");
    }
    function duplexOnEnd() {
      !this.destroyed && this._writableState.finished && this.destroy();
    }
    function duplexOnError(err) {
      this.removeListener("error", duplexOnError), this.destroy(), this.listenerCount("error") === 0 && this.emit("error", err);
    }
    function createWebSocketStream2(ws, options) {
      let terminateOnDestroy = !0, duplex = new Duplex({
        ...options,
        autoDestroy: !1,
        emitClose: !1,
        objectMode: !1,
        writableObjectMode: !1
      });
      return ws.on("message", function(msg, isBinary) {
        let data = !isBinary && duplex._readableState.objectMode ? msg.toString() : msg;
        duplex.push(data) || ws.pause();
      }), ws.once("error", function(err) {
        duplex.destroyed || (terminateOnDestroy = !1, duplex.destroy(err));
      }), ws.once("close", function() {
        duplex.destroyed || duplex.push(null);
      }), duplex._destroy = function(err, callback) {
        if (ws.readyState === ws.CLOSED) {
          callback(err), process.nextTick(emitClose, duplex);
          return;
        }
        let called = !1;
        ws.once("error", function(err2) {
          called = !0, callback(err2);
        }), ws.once("close", function() {
          called || callback(err), process.nextTick(emitClose, duplex);
        }), terminateOnDestroy && ws.terminate();
      }, duplex._final = function(callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function() {
            duplex._final(callback);
          });
          return;
        }
        ws._socket !== null && (ws._socket._writableState.finished ? (callback(), duplex._readableState.endEmitted && duplex.destroy()) : (ws._socket.once("finish", function() {
          callback();
        }), ws.close()));
      }, duplex._read = function() {
        ws.isPaused && ws.resume();
      }, duplex._write = function(chunk, encoding, callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function() {
            duplex._write(chunk, encoding, callback);
          });
          return;
        }
        ws.send(chunk, callback);
      }, duplex.on("end", duplexOnEnd), duplex.on("error", duplexOnError), duplex;
    }
    module2.exports = createWebSocketStream2;
  }
});

// node_modules/ws/lib/subprotocol.js
var require_subprotocol = __commonJS({
  "node_modules/ws/lib/subprotocol.js"(exports2, module2) {
    "use strict";
    var { tokenChars } = require_validation();
    function parse(header) {
      let protocols = /* @__PURE__ */ new Set(), start = -1, end = -1, i = 0;
      for (i; i < header.length; i++) {
        let code = header.charCodeAt(i);
        if (end === -1 && tokenChars[code] === 1)
          start === -1 && (start = i);
        else if (i !== 0 && (code === 32 || code === 9))
          end === -1 && start !== -1 && (end = i);
        else if (code === 44) {
          if (start === -1)
            throw new SyntaxError(`Unexpected character at index ${i}`);
          end === -1 && (end = i);
          let protocol2 = header.slice(start, end);
          if (protocols.has(protocol2))
            throw new SyntaxError(`The "${protocol2}" subprotocol is duplicated`);
          protocols.add(protocol2), start = end = -1;
        } else
          throw new SyntaxError(`Unexpected character at index ${i}`);
      }
      if (start === -1 || end !== -1)
        throw new SyntaxError("Unexpected end of input");
      let protocol = header.slice(start, i);
      if (protocols.has(protocol))
        throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
      return protocols.add(protocol), protocols;
    }
    module2.exports = { parse };
  }
});

// node_modules/ws/lib/websocket-server.js
var require_websocket_server = __commonJS({
  "node_modules/ws/lib/websocket-server.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events"), http2 = require("http"), { Duplex } = require("stream"), { createHash } = require("crypto"), extension2 = require_extension(), PerMessageDeflate2 = require_permessage_deflate(), subprotocol2 = require_subprotocol(), WebSocket2 = require_websocket(), { CLOSE_TIMEOUT, GUID, kWebSocket } = require_constants(), keyRegex = /^[+/0-9A-Za-z]{22}==$/, RUNNING = 0, CLOSING = 1, CLOSED = 2, WebSocketServer2 = class extends EventEmitter {
      /**
       * Create a `WebSocketServer` instance.
       *
       * @param {Object} options Configuration options
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Boolean} [options.autoPong=true] Specifies whether or not to
       *     automatically send a pong in response to a ping
       * @param {Number} [options.backlog=511] The maximum length of the queue of
       *     pending connections
       * @param {Boolean} [options.clientTracking=true] Specifies whether or not to
       *     track clients
       * @param {Number} [options.closeTimeout=30000] Duration in milliseconds to
       *     wait for the closing handshake to finish after `websocket.close()` is
       *     called
       * @param {Function} [options.handleProtocols] A hook to handle protocols
       * @param {String} [options.host] The hostname where to bind the server
       * @param {Number} [options.maxBufferedChunks=262144] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=16384] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=104857600] The maximum allowed message
       *     size
       * @param {Number} [options.maxPendingAutoPongs=60] The maximum number of
       *     pending automatic pongs
       * @param {Boolean} [options.noServer=false] Enable no server mode
       * @param {String} [options.path] Accept only connections matching this path
       * @param {(Boolean|Object)} [options.perMessageDeflate=false] Enable/disable
       *     permessage-deflate
       * @param {Number} [options.port] The port where to bind the server
       * @param {(http.Server|https.Server)} [options.server] A pre-created HTTP/S
       *     server to use
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @param {Function} [options.verifyClient] A hook to reject connections
       * @param {Function} [options.WebSocket=WebSocket] Specifies the `WebSocket`
       *     class to use. It must be the `WebSocket` class or class that extends it
       * @param {Function} [callback] A listener for the `listening` event
       */
      constructor(options, callback) {
        if (super(), options = {
          allowSynchronousEvents: !0,
          autoPong: !0,
          maxBufferedChunks: 256 * 1024,
          maxFragments: 16 * 1024,
          maxPayload: 100 * 1024 * 1024,
          maxPendingAutoPongs: 60,
          skipUTF8Validation: !1,
          perMessageDeflate: !1,
          handleProtocols: null,
          clientTracking: !0,
          closeTimeout: CLOSE_TIMEOUT,
          verifyClient: null,
          noServer: !1,
          backlog: null,
          // use default (511 as implemented in net.js)
          server: null,
          host: null,
          path: null,
          port: null,
          WebSocket: WebSocket2,
          ...options
        }, options.port == null && !options.server && !options.noServer || options.port != null && (options.server || options.noServer) || options.server && options.noServer)
          throw new TypeError(
            'One and only one of the "port", "server", or "noServer" options must be specified'
          );
        if (options.port != null ? (this._server = http2.createServer((req, res) => {
          let body = http2.STATUS_CODES[426];
          res.writeHead(426, {
            "Content-Length": body.length,
            "Content-Type": "text/plain"
          }), res.end(body);
        }), this._server.listen(
          options.port,
          options.host,
          options.backlog,
          callback
        )) : options.server && (this._server = options.server), this._server) {
          let emitConnection = this.emit.bind(this, "connection");
          this._removeListeners = addListeners(this._server, {
            listening: this.emit.bind(this, "listening"),
            error: this.emit.bind(this, "error"),
            upgrade: (req, socket, head) => {
              this.handleUpgrade(req, socket, head, emitConnection);
            }
          });
        }
        options.perMessageDeflate === !0 && (options.perMessageDeflate = {}), options.clientTracking && (this.clients = /* @__PURE__ */ new Set(), this._shouldEmitClose = !1), this.options = options, this._state = RUNNING;
      }
      /**
       * Returns the bound address, the address family name, and port of the server
       * as reported by the operating system if listening on an IP socket.
       * If the server is listening on a pipe or UNIX domain socket, the name is
       * returned as a string.
       *
       * @return {(Object|String|null)} The address of the server
       * @public
       */
      address() {
        if (this.options.noServer)
          throw new Error('The server is operating in "noServer" mode');
        return this._server ? this._server.address() : null;
      }
      /**
       * Stop the server from accepting new connections and emit the `'close'` event
       * when all existing connections are closed.
       *
       * @param {Function} [cb] A one-time listener for the `'close'` event
       * @public
       */
      close(cb) {
        if (this._state === CLOSED) {
          cb && this.once("close", () => {
            cb(new Error("The server is not running"));
          }), process.nextTick(emitClose, this);
          return;
        }
        if (cb && this.once("close", cb), this._state !== CLOSING)
          if (this._state = CLOSING, this.options.noServer || this.options.server)
            this._server && (this._removeListeners(), this._removeListeners = this._server = null), this.clients ? this.clients.size ? this._shouldEmitClose = !0 : process.nextTick(emitClose, this) : process.nextTick(emitClose, this);
          else {
            let server2 = this._server;
            this._removeListeners(), this._removeListeners = this._server = null, server2.close(() => {
              emitClose(this);
            });
          }
      }
      /**
       * See if a given request should be handled by this server instance.
       *
       * @param {http.IncomingMessage} req Request object to inspect
       * @return {Boolean} `true` if the request is valid, else `false`
       * @public
       */
      shouldHandle(req) {
        if (this.options.path) {
          let index = req.url.indexOf("?");
          if ((index !== -1 ? req.url.slice(0, index) : req.url) !== this.options.path) return !1;
        }
        return !0;
      }
      /**
       * Handle a HTTP Upgrade request.
       *
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @public
       */
      handleUpgrade(req, socket, head, cb) {
        socket.on("error", socketOnError);
        let key = req.headers["sec-websocket-key"], upgrade = req.headers.upgrade, version = +req.headers["sec-websocket-version"];
        if (req.method !== "GET") {
          abortHandshakeOrEmitwsClientError(this, req, socket, 405, "Invalid HTTP method");
          return;
        }
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, "Invalid Upgrade header");
          return;
        }
        if (key === void 0 || !keyRegex.test(key)) {
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, "Missing or invalid Sec-WebSocket-Key header");
          return;
        }
        if (version !== 13 && version !== 8) {
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, "Missing or invalid Sec-WebSocket-Version header", {
            "Sec-WebSocket-Version": "13, 8"
          });
          return;
        }
        if (!this.shouldHandle(req)) {
          abortHandshake(socket, 400);
          return;
        }
        let secWebSocketProtocol = req.headers["sec-websocket-protocol"], protocols = /* @__PURE__ */ new Set();
        if (secWebSocketProtocol !== void 0)
          try {
            protocols = subprotocol2.parse(secWebSocketProtocol);
          } catch {
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, "Invalid Sec-WebSocket-Protocol header");
            return;
          }
        let secWebSocketExtensions = req.headers["sec-websocket-extensions"], extensions = {};
        if (this.options.perMessageDeflate && secWebSocketExtensions !== void 0) {
          let perMessageDeflate = new PerMessageDeflate2({
            ...this.options.perMessageDeflate,
            isServer: !0,
            maxPayload: this.options.maxPayload
          });
          try {
            let offers = extension2.parse(secWebSocketExtensions);
            offers[PerMessageDeflate2.extensionName] && (perMessageDeflate.accept(offers[PerMessageDeflate2.extensionName]), extensions[PerMessageDeflate2.extensionName] = perMessageDeflate);
          } catch {
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, "Invalid or unacceptable Sec-WebSocket-Extensions header");
            return;
          }
        }
        if (this.options.verifyClient) {
          let info = {
            origin: req.headers[`${version === 8 ? "sec-websocket-origin" : "origin"}`],
            secure: !!(req.socket.authorized || req.socket.encrypted),
            req
          };
          if (this.options.verifyClient.length === 2) {
            this.options.verifyClient(info, (verified, code, message, headers) => {
              if (!verified)
                return abortHandshake(socket, code || 401, message, headers);
              this.completeUpgrade(
                extensions,
                key,
                protocols,
                req,
                socket,
                head,
                cb
              );
            });
            return;
          }
          if (!this.options.verifyClient(info)) return abortHandshake(socket, 401);
        }
        this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
      }
      /**
       * Upgrade the connection to WebSocket.
       *
       * @param {Object} extensions The accepted extensions
       * @param {String} key The value of the `Sec-WebSocket-Key` header
       * @param {Set} protocols The subprotocols
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @throws {Error} If called more than once with the same socket
       * @private
       */
      completeUpgrade(extensions, key, protocols, req, socket, head, cb) {
        if (!socket.readable || !socket.writable) return socket.destroy();
        if (socket[kWebSocket])
          throw new Error(
            "server.handleUpgrade() was called more than once with the same socket, possibly due to a misconfiguration"
          );
        if (this._state > RUNNING) return abortHandshake(socket, 503);
        let headers = [
          "HTTP/1.1 101 Switching Protocols",
          "Upgrade: websocket",
          "Connection: Upgrade",
          `Sec-WebSocket-Accept: ${createHash("sha1").update(key + GUID).digest("base64")}`
        ], ws = new this.options.WebSocket(null, void 0, this.options);
        if (protocols.size) {
          let protocol = this.options.handleProtocols ? this.options.handleProtocols(protocols, req) : protocols.values().next().value;
          protocol && (headers.push(`Sec-WebSocket-Protocol: ${protocol}`), ws._protocol = protocol);
        }
        if (extensions[PerMessageDeflate2.extensionName]) {
          let params = extensions[PerMessageDeflate2.extensionName].params, value = extension2.format({
            [PerMessageDeflate2.extensionName]: [params]
          });
          headers.push(`Sec-WebSocket-Extensions: ${value}`), ws._extensions = extensions;
        }
        this.emit("headers", headers, req), socket.write(headers.concat(`\r
`).join(`\r
`)), socket.removeListener("error", socketOnError), ws.setSocket(socket, head, {
          allowSynchronousEvents: this.options.allowSynchronousEvents,
          maxBufferedChunks: this.options.maxBufferedChunks,
          maxFragments: this.options.maxFragments,
          maxPayload: this.options.maxPayload,
          skipUTF8Validation: this.options.skipUTF8Validation
        }), this.clients && (this.clients.add(ws), ws.on("close", () => {
          this.clients.delete(ws), this._shouldEmitClose && !this.clients.size && process.nextTick(emitClose, this);
        })), cb(ws, req);
      }
    };
    module2.exports = WebSocketServer2;
    function addListeners(server2, map) {
      for (let event of Object.keys(map)) server2.on(event, map[event]);
      return function() {
        for (let event of Object.keys(map))
          server2.removeListener(event, map[event]);
      };
    }
    function emitClose(server2) {
      server2._state = CLOSED, server2.emit("close");
    }
    function socketOnError() {
      this.destroy();
    }
    function abortHandshake(socket, code, message, headers) {
      message = message || http2.STATUS_CODES[code], headers = {
        Connection: "close",
        "Content-Type": "text/html",
        "Content-Length": Buffer.byteLength(message),
        ...headers
      }, socket.once("finish", socket.destroy), socket.end(
        `HTTP/1.1 ${code} ${http2.STATUS_CODES[code]}\r
` + Object.keys(headers).map((h) => `${h}: ${headers[h]}`).join(`\r
`) + `\r
\r
` + message
      );
    }
    function abortHandshakeOrEmitwsClientError(server2, req, socket, code, message, headers) {
      if (server2.listenerCount("wsClientError")) {
        let err = new Error(message);
        Error.captureStackTrace(err, abortHandshakeOrEmitwsClientError), server2.emit("wsClientError", err, socket, req);
      } else
        abortHandshake(socket, code, message, headers);
    }
  }
});

// node_modules/postgres-array/index.js
var require_postgres_array = __commonJS({
  "node_modules/postgres-array/index.js"(exports2) {
    "use strict";
    exports2.parse = function(source, transform) {
      return new ArrayParser(source, transform).parse();
    };
    var ArrayParser = class _ArrayParser {
      constructor(source, transform) {
        this.source = source, this.transform = transform || identity, this.position = 0, this.entries = [], this.recorded = [], this.dimension = 0;
      }
      isEof() {
        return this.position >= this.source.length;
      }
      nextCharacter() {
        var character = this.source[this.position++];
        return character === "\\" ? {
          value: this.source[this.position++],
          escaped: !0
        } : {
          value: character,
          escaped: !1
        };
      }
      record(character) {
        this.recorded.push(character);
      }
      newEntry(includeEmpty) {
        var entry;
        (this.recorded.length > 0 || includeEmpty) && (entry = this.recorded.join(""), entry === "NULL" && !includeEmpty && (entry = null), entry !== null && (entry = this.transform(entry)), this.entries.push(entry), this.recorded = []);
      }
      consumeDimensions() {
        if (this.source[0] === "[")
          for (; !this.isEof(); ) {
            var char = this.nextCharacter();
            if (char.value === "=") break;
          }
      }
      parse(nested) {
        var character, parser, quote;
        for (this.consumeDimensions(); !this.isEof(); )
          if (character = this.nextCharacter(), character.value === "{" && !quote)
            this.dimension++, this.dimension > 1 && (parser = new _ArrayParser(this.source.substr(this.position - 1), this.transform), this.entries.push(parser.parse(!0)), this.position += parser.position - 2);
          else if (character.value === "}" && !quote) {
            if (this.dimension--, !this.dimension && (this.newEntry(), nested))
              return this.entries;
          } else character.value === '"' && !character.escaped ? (quote && this.newEntry(!0), quote = !quote) : character.value === "," && !quote ? this.newEntry() : this.record(character.value);
        if (this.dimension !== 0)
          throw new Error("array dimension not balanced");
        return this.entries;
      }
    };
    function identity(value) {
      return value;
    }
  }
});

// node_modules/pg-types/lib/arrayParser.js
var require_arrayParser = __commonJS({
  "node_modules/pg-types/lib/arrayParser.js"(exports2, module2) {
    var array = require_postgres_array();
    module2.exports = {
      create: function(source, transform) {
        return {
          parse: function() {
            return array.parse(source, transform);
          }
        };
      }
    };
  }
});

// node_modules/postgres-date/index.js
var require_postgres_date = __commonJS({
  "node_modules/postgres-date/index.js"(exports2, module2) {
    "use strict";
    var DATE_TIME = /(\d{1,})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(\.\d{1,})?.*?( BC)?$/, DATE = /^(\d{1,})-(\d{2})-(\d{2})( BC)?$/, TIME_ZONE = /([Z+-])(\d{2})?:?(\d{2})?:?(\d{2})?/, INFINITY = /^-?infinity$/;
    module2.exports = function(isoDate) {
      if (INFINITY.test(isoDate))
        return Number(isoDate.replace("i", "I"));
      var matches = DATE_TIME.exec(isoDate);
      if (!matches)
        return getDate(isoDate) || null;
      var isBC = !!matches[8], year = parseInt(matches[1], 10);
      isBC && (year = bcYearToNegativeYear(year));
      var month = parseInt(matches[2], 10) - 1, day = matches[3], hour = parseInt(matches[4], 10), minute = parseInt(matches[5], 10), second = parseInt(matches[6], 10), ms = matches[7];
      ms = ms ? 1e3 * parseFloat(ms) : 0;
      var date, offset = timeZoneOffset(isoDate);
      return offset != null ? (date = new Date(Date.UTC(year, month, day, hour, minute, second, ms)), is0To99(year) && date.setUTCFullYear(year), offset !== 0 && date.setTime(date.getTime() - offset)) : (date = new Date(year, month, day, hour, minute, second, ms), is0To99(year) && date.setFullYear(year)), date;
    };
    function getDate(isoDate) {
      var matches = DATE.exec(isoDate);
      if (matches) {
        var year = parseInt(matches[1], 10), isBC = !!matches[4];
        isBC && (year = bcYearToNegativeYear(year));
        var month = parseInt(matches[2], 10) - 1, day = matches[3], date = new Date(year, month, day);
        return is0To99(year) && date.setFullYear(year), date;
      }
    }
    function timeZoneOffset(isoDate) {
      if (isoDate.endsWith("+00"))
        return 0;
      var zone = TIME_ZONE.exec(isoDate.split(" ")[1]);
      if (zone) {
        var type = zone[1];
        if (type === "Z")
          return 0;
        var sign = type === "-" ? -1 : 1, offset = parseInt(zone[2], 10) * 3600 + parseInt(zone[3] || 0, 10) * 60 + parseInt(zone[4] || 0, 10);
        return offset * sign * 1e3;
      }
    }
    function bcYearToNegativeYear(year) {
      return -(year - 1);
    }
    function is0To99(num) {
      return num >= 0 && num < 100;
    }
  }
});

// node_modules/xtend/mutable.js
var require_mutable = __commonJS({
  "node_modules/xtend/mutable.js"(exports2, module2) {
    module2.exports = extend;
    var hasOwnProperty = Object.prototype.hasOwnProperty;
    function extend(target) {
      for (var i = 1; i < arguments.length; i++) {
        var source = arguments[i];
        for (var key in source)
          hasOwnProperty.call(source, key) && (target[key] = source[key]);
      }
      return target;
    }
  }
});

// node_modules/postgres-interval/index.js
var require_postgres_interval = __commonJS({
  "node_modules/postgres-interval/index.js"(exports2, module2) {
    "use strict";
    var extend = require_mutable();
    module2.exports = PostgresInterval;
    function PostgresInterval(raw) {
      if (!(this instanceof PostgresInterval))
        return new PostgresInterval(raw);
      extend(this, parse(raw));
    }
    var properties = ["seconds", "minutes", "hours", "days", "months", "years"];
    PostgresInterval.prototype.toPostgres = function() {
      var filtered = properties.filter(this.hasOwnProperty, this);
      return this.milliseconds && filtered.indexOf("seconds") < 0 && filtered.push("seconds"), filtered.length === 0 ? "0" : filtered.map(function(property) {
        var value = this[property] || 0;
        return property === "seconds" && this.milliseconds && (value = (value + this.milliseconds / 1e3).toFixed(6).replace(/\.?0+$/, "")), value + " " + property;
      }, this).join(" ");
    };
    var propertiesISOEquivalent = {
      years: "Y",
      months: "M",
      days: "D",
      hours: "H",
      minutes: "M",
      seconds: "S"
    }, dateProperties = ["years", "months", "days"], timeProperties = ["hours", "minutes", "seconds"];
    PostgresInterval.prototype.toISOString = PostgresInterval.prototype.toISO = function() {
      var datePart = dateProperties.map(buildProperty, this).join(""), timePart = timeProperties.map(buildProperty, this).join("");
      return "P" + datePart + "T" + timePart;
      function buildProperty(property) {
        var value = this[property] || 0;
        return property === "seconds" && this.milliseconds && (value = (value + this.milliseconds / 1e3).toFixed(6).replace(/0+$/, "")), value + propertiesISOEquivalent[property];
      }
    };
    var NUMBER = "([+-]?\\d+)", YEAR = NUMBER + "\\s+years?", MONTH = NUMBER + "\\s+mons?", DAY = NUMBER + "\\s+days?", TIME = "([+-])?([\\d]*):(\\d\\d):(\\d\\d)\\.?(\\d{1,6})?", INTERVAL = new RegExp([YEAR, MONTH, DAY, TIME].map(function(regexString) {
      return "(" + regexString + ")?";
    }).join("\\s*")), positions = {
      years: 2,
      months: 4,
      days: 6,
      hours: 9,
      minutes: 10,
      seconds: 11,
      milliseconds: 12
    }, negatives = ["hours", "minutes", "seconds", "milliseconds"];
    function parseMilliseconds(fraction) {
      var microseconds = fraction + "000000".slice(fraction.length);
      return parseInt(microseconds, 10) / 1e3;
    }
    function parse(interval) {
      if (!interval) return {};
      var matches = INTERVAL.exec(interval), isNegative = matches[8] === "-";
      return Object.keys(positions).reduce(function(parsed, property) {
        var position = positions[property], value = matches[position];
        return !value || (value = property === "milliseconds" ? parseMilliseconds(value) : parseInt(value, 10), !value) || (isNegative && ~negatives.indexOf(property) && (value *= -1), parsed[property] = value), parsed;
      }, {});
    }
  }
});

// node_modules/postgres-bytea/index.js
var require_postgres_bytea = __commonJS({
  "node_modules/postgres-bytea/index.js"(exports2, module2) {
    "use strict";
    var bufferFrom = Buffer.from || Buffer;
    module2.exports = function(input) {
      if (/^\\x/.test(input))
        return bufferFrom(input.substr(2), "hex");
      for (var output = "", i = 0; i < input.length; )
        if (input[i] !== "\\")
          output += input[i], ++i;
        else if (/[0-7]{3}/.test(input.substr(i + 1, 3)))
          output += String.fromCharCode(parseInt(input.substr(i + 1, 3), 8)), i += 4;
        else {
          for (var backslashes = 1; i + backslashes < input.length && input[i + backslashes] === "\\"; )
            backslashes++;
          for (var k = 0; k < Math.floor(backslashes / 2); ++k)
            output += "\\";
          i += Math.floor(backslashes / 2) * 2;
        }
      return bufferFrom(output, "binary");
    };
  }
});

// node_modules/pg-types/lib/textParsers.js
var require_textParsers = __commonJS({
  "node_modules/pg-types/lib/textParsers.js"(exports2, module2) {
    var array = require_postgres_array(), arrayParser = require_arrayParser(), parseDate = require_postgres_date(), parseInterval = require_postgres_interval(), parseByteA = require_postgres_bytea();
    function allowNull(fn) {
      return function(value) {
        return value === null ? value : fn(value);
      };
    }
    function parseBool(value) {
      return value === null ? value : value === "TRUE" || value === "t" || value === "true" || value === "y" || value === "yes" || value === "on" || value === "1";
    }
    function parseBoolArray(value) {
      return value ? array.parse(value, parseBool) : null;
    }
    function parseBaseTenInt(string) {
      return parseInt(string, 10);
    }
    function parseIntegerArray(value) {
      return value ? array.parse(value, allowNull(parseBaseTenInt)) : null;
    }
    function parseBigIntegerArray(value) {
      return value ? array.parse(value, allowNull(function(entry) {
        return parseBigInteger(entry).trim();
      })) : null;
    }
    var parsePointArray = function(value) {
      if (!value)
        return null;
      var p = arrayParser.create(value, function(entry) {
        return entry !== null && (entry = parsePoint(entry)), entry;
      });
      return p.parse();
    }, parseFloatArray = function(value) {
      if (!value)
        return null;
      var p = arrayParser.create(value, function(entry) {
        return entry !== null && (entry = parseFloat(entry)), entry;
      });
      return p.parse();
    }, parseStringArray = function(value) {
      if (!value)
        return null;
      var p = arrayParser.create(value);
      return p.parse();
    }, parseDateArray = function(value) {
      if (!value)
        return null;
      var p = arrayParser.create(value, function(entry) {
        return entry !== null && (entry = parseDate(entry)), entry;
      });
      return p.parse();
    }, parseIntervalArray = function(value) {
      if (!value)
        return null;
      var p = arrayParser.create(value, function(entry) {
        return entry !== null && (entry = parseInterval(entry)), entry;
      });
      return p.parse();
    }, parseByteAArray = function(value) {
      return value ? array.parse(value, allowNull(parseByteA)) : null;
    }, parseInteger = function(value) {
      return parseInt(value, 10);
    }, parseBigInteger = function(value) {
      var valStr = String(value);
      return /^\d+$/.test(valStr) ? valStr : value;
    }, parseJsonArray = function(value) {
      return value ? array.parse(value, allowNull(JSON.parse)) : null;
    }, parsePoint = function(value) {
      return value[0] !== "(" ? null : (value = value.substring(1, value.length - 1).split(","), {
        x: parseFloat(value[0]),
        y: parseFloat(value[1])
      });
    }, parseCircle = function(value) {
      if (value[0] !== "<" && value[1] !== "(")
        return null;
      for (var point = "(", radius = "", pointParsed = !1, i = 2; i < value.length - 1; i++) {
        if (pointParsed || (point += value[i]), value[i] === ")") {
          pointParsed = !0;
          continue;
        } else if (!pointParsed)
          continue;
        value[i] !== "," && (radius += value[i]);
      }
      var result = parsePoint(point);
      return result.radius = parseFloat(radius), result;
    }, init = function(register2) {
      register2(20, parseBigInteger), register2(21, parseInteger), register2(23, parseInteger), register2(26, parseInteger), register2(700, parseFloat), register2(701, parseFloat), register2(16, parseBool), register2(1082, parseDate), register2(1114, parseDate), register2(1184, parseDate), register2(600, parsePoint), register2(651, parseStringArray), register2(718, parseCircle), register2(1e3, parseBoolArray), register2(1001, parseByteAArray), register2(1005, parseIntegerArray), register2(1007, parseIntegerArray), register2(1028, parseIntegerArray), register2(1016, parseBigIntegerArray), register2(1017, parsePointArray), register2(1021, parseFloatArray), register2(1022, parseFloatArray), register2(1231, parseFloatArray), register2(1014, parseStringArray), register2(1015, parseStringArray), register2(1008, parseStringArray), register2(1009, parseStringArray), register2(1040, parseStringArray), register2(1041, parseStringArray), register2(1115, parseDateArray), register2(1182, parseDateArray), register2(1185, parseDateArray), register2(1186, parseInterval), register2(1187, parseIntervalArray), register2(17, parseByteA), register2(114, JSON.parse.bind(JSON)), register2(3802, JSON.parse.bind(JSON)), register2(199, parseJsonArray), register2(3807, parseJsonArray), register2(3907, parseStringArray), register2(2951, parseStringArray), register2(791, parseStringArray), register2(1183, parseStringArray), register2(1270, parseStringArray);
    };
    module2.exports = {
      init
    };
  }
});

// node_modules/pg-int8/index.js
var require_pg_int8 = __commonJS({
  "node_modules/pg-int8/index.js"(exports2, module2) {
    "use strict";
    var BASE = 1e6;
    function readInt8(buffer) {
      var high = buffer.readInt32BE(0), low = buffer.readUInt32BE(4), sign = "";
      high < 0 && (high = ~high + (low === 0), low = ~low + 1 >>> 0, sign = "-");
      var result = "", carry, t, digits, pad, l, i;
      {
        if (carry = high % BASE, high = high / BASE >>> 0, t = 4294967296 * carry + low, low = t / BASE >>> 0, digits = "" + (t - BASE * low), low === 0 && high === 0)
          return sign + digits + result;
        for (pad = "", l = 6 - digits.length, i = 0; i < l; i++)
          pad += "0";
        result = pad + digits + result;
      }
      {
        if (carry = high % BASE, high = high / BASE >>> 0, t = 4294967296 * carry + low, low = t / BASE >>> 0, digits = "" + (t - BASE * low), low === 0 && high === 0)
          return sign + digits + result;
        for (pad = "", l = 6 - digits.length, i = 0; i < l; i++)
          pad += "0";
        result = pad + digits + result;
      }
      {
        if (carry = high % BASE, high = high / BASE >>> 0, t = 4294967296 * carry + low, low = t / BASE >>> 0, digits = "" + (t - BASE * low), low === 0 && high === 0)
          return sign + digits + result;
        for (pad = "", l = 6 - digits.length, i = 0; i < l; i++)
          pad += "0";
        result = pad + digits + result;
      }
      return carry = high % BASE, t = 4294967296 * carry + low, digits = "" + t % BASE, sign + digits + result;
    }
    module2.exports = readInt8;
  }
});

// node_modules/pg-types/lib/binaryParsers.js
var require_binaryParsers = __commonJS({
  "node_modules/pg-types/lib/binaryParsers.js"(exports2, module2) {
    var parseInt64 = require_pg_int8(), parseBits = function(data, bits, offset, invert, callback) {
      offset = offset || 0, invert = invert || !1, callback = callback || function(lastValue, newValue, bits2) {
        return lastValue * Math.pow(2, bits2) + newValue;
      };
      var offsetBytes = offset >> 3, inv = function(value) {
        return invert ? ~value & 255 : value;
      }, mask = 255, firstBits = 8 - offset % 8;
      bits < firstBits && (mask = 255 << 8 - bits & 255, firstBits = bits), offset && (mask = mask >> offset % 8);
      var result = 0;
      offset % 8 + bits >= 8 && (result = callback(0, inv(data[offsetBytes]) & mask, firstBits));
      for (var bytes = bits + offset >> 3, i = offsetBytes + 1; i < bytes; i++)
        result = callback(result, inv(data[i]), 8);
      var lastBits = (bits + offset) % 8;
      return lastBits > 0 && (result = callback(result, inv(data[bytes]) >> 8 - lastBits, lastBits)), result;
    }, parseFloatFromBits = function(data, precisionBits, exponentBits) {
      var bias = Math.pow(2, exponentBits - 1) - 1, sign = parseBits(data, 1), exponent = parseBits(data, exponentBits, 1);
      if (exponent === 0)
        return 0;
      var precisionBitsCounter = 1, parsePrecisionBits = function(lastValue, newValue, bits) {
        lastValue === 0 && (lastValue = 1);
        for (var i = 1; i <= bits; i++)
          precisionBitsCounter /= 2, (newValue & 1 << bits - i) > 0 && (lastValue += precisionBitsCounter);
        return lastValue;
      }, mantissa = parseBits(data, precisionBits, exponentBits + 1, !1, parsePrecisionBits);
      return exponent == Math.pow(2, exponentBits + 1) - 1 ? mantissa === 0 ? sign === 0 ? 1 / 0 : -1 / 0 : NaN : (sign === 0 ? 1 : -1) * Math.pow(2, exponent - bias) * mantissa;
    }, parseInt16 = function(value) {
      return parseBits(value, 1) == 1 ? -1 * (parseBits(value, 15, 1, !0) + 1) : parseBits(value, 15, 1);
    }, parseInt32 = function(value) {
      return parseBits(value, 1) == 1 ? -1 * (parseBits(value, 31, 1, !0) + 1) : parseBits(value, 31, 1);
    }, parseFloat32 = function(value) {
      return parseFloatFromBits(value, 23, 8);
    }, parseFloat64 = function(value) {
      return parseFloatFromBits(value, 52, 11);
    }, parseNumeric = function(value) {
      var sign = parseBits(value, 16, 32);
      if (sign == 49152)
        return NaN;
      for (var weight = Math.pow(1e4, parseBits(value, 16, 16)), result = 0, digits = [], ndigits = parseBits(value, 16), i = 0; i < ndigits; i++)
        result += parseBits(value, 16, 64 + 16 * i) * weight, weight /= 1e4;
      var scale = Math.pow(10, parseBits(value, 16, 48));
      return (sign === 0 ? 1 : -1) * Math.round(result * scale) / scale;
    }, parseDate = function(isUTC, value) {
      var sign = parseBits(value, 1), rawValue = parseBits(value, 63, 1), result = new Date((sign === 0 ? 1 : -1) * rawValue / 1e3 + 9466848e5);
      return isUTC || result.setTime(result.getTime() + result.getTimezoneOffset() * 6e4), result.usec = rawValue % 1e3, result.getMicroSeconds = function() {
        return this.usec;
      }, result.setMicroSeconds = function(value2) {
        this.usec = value2;
      }, result.getUTCMicroSeconds = function() {
        return this.usec;
      }, result;
    }, parseArray = function(value) {
      for (var dim = parseBits(value, 32), flags = parseBits(value, 32, 32), elementType = parseBits(value, 32, 64), offset = 96, dims = [], i = 0; i < dim; i++)
        dims[i] = parseBits(value, 32, offset), offset += 32, offset += 32;
      var parseElement = function(elementType2) {
        var length = parseBits(value, 32, offset);
        if (offset += 32, length == 4294967295)
          return null;
        var result;
        if (elementType2 == 23 || elementType2 == 20)
          return result = parseBits(value, length * 8, offset), offset += length * 8, result;
        if (elementType2 == 25)
          return result = value.toString(this.encoding, offset >> 3, (offset += length << 3) >> 3), result;
        console.log("ERROR: ElementType not implemented: " + elementType2);
      }, parse = function(dimension, elementType2) {
        var array = [], i2;
        if (dimension.length > 1) {
          var count = dimension.shift();
          for (i2 = 0; i2 < count; i2++)
            array[i2] = parse(dimension, elementType2);
          dimension.unshift(count);
        } else
          for (i2 = 0; i2 < dimension[0]; i2++)
            array[i2] = parseElement(elementType2);
        return array;
      };
      return parse(dims, elementType);
    }, parseText = function(value) {
      return value.toString("utf8");
    }, parseBool = function(value) {
      return value === null ? null : parseBits(value, 8) > 0;
    }, init = function(register2) {
      register2(20, parseInt64), register2(21, parseInt16), register2(23, parseInt32), register2(26, parseInt32), register2(1700, parseNumeric), register2(700, parseFloat32), register2(701, parseFloat64), register2(16, parseBool), register2(1114, parseDate.bind(null, !1)), register2(1184, parseDate.bind(null, !0)), register2(1e3, parseArray), register2(1007, parseArray), register2(1016, parseArray), register2(1008, parseArray), register2(1009, parseArray), register2(25, parseText);
    };
    module2.exports = {
      init
    };
  }
});

// node_modules/pg-types/lib/builtins.js
var require_builtins = __commonJS({
  "node_modules/pg-types/lib/builtins.js"(exports2, module2) {
    module2.exports = {
      BOOL: 16,
      BYTEA: 17,
      CHAR: 18,
      INT8: 20,
      INT2: 21,
      INT4: 23,
      REGPROC: 24,
      TEXT: 25,
      OID: 26,
      TID: 27,
      XID: 28,
      CID: 29,
      JSON: 114,
      XML: 142,
      PG_NODE_TREE: 194,
      SMGR: 210,
      PATH: 602,
      POLYGON: 604,
      CIDR: 650,
      FLOAT4: 700,
      FLOAT8: 701,
      ABSTIME: 702,
      RELTIME: 703,
      TINTERVAL: 704,
      CIRCLE: 718,
      MACADDR8: 774,
      MONEY: 790,
      MACADDR: 829,
      INET: 869,
      ACLITEM: 1033,
      BPCHAR: 1042,
      VARCHAR: 1043,
      DATE: 1082,
      TIME: 1083,
      TIMESTAMP: 1114,
      TIMESTAMPTZ: 1184,
      INTERVAL: 1186,
      TIMETZ: 1266,
      BIT: 1560,
      VARBIT: 1562,
      NUMERIC: 1700,
      REFCURSOR: 1790,
      REGPROCEDURE: 2202,
      REGOPER: 2203,
      REGOPERATOR: 2204,
      REGCLASS: 2205,
      REGTYPE: 2206,
      UUID: 2950,
      TXID_SNAPSHOT: 2970,
      PG_LSN: 3220,
      PG_NDISTINCT: 3361,
      PG_DEPENDENCIES: 3402,
      TSVECTOR: 3614,
      TSQUERY: 3615,
      GTSVECTOR: 3642,
      REGCONFIG: 3734,
      REGDICTIONARY: 3769,
      JSONB: 3802,
      REGNAMESPACE: 4089,
      REGROLE: 4096
    };
  }
});

// node_modules/pg-types/index.js
var require_pg_types = __commonJS({
  "node_modules/pg-types/index.js"(exports2) {
    var textParsers = require_textParsers(), binaryParsers = require_binaryParsers(), arrayParser = require_arrayParser(), builtinTypes = require_builtins();
    exports2.getTypeParser = getTypeParser;
    exports2.setTypeParser = setTypeParser;
    exports2.arrayParser = arrayParser;
    exports2.builtins = builtinTypes;
    var typeParsers = {
      text: {},
      binary: {}
    };
    function noParse(val) {
      return String(val);
    }
    function getTypeParser(oid, format) {
      return format = format || "text", typeParsers[format] && typeParsers[format][oid] || noParse;
    }
    function setTypeParser(oid, format, parseFn) {
      typeof format == "function" && (parseFn = format, format = "text"), typeParsers[format][oid] = parseFn;
    }
    textParsers.init(function(oid, converter) {
      typeParsers.text[oid] = converter;
    });
    binaryParsers.init(function(oid, converter) {
      typeParsers.binary[oid] = converter;
    });
  }
});

// node_modules/pg/lib/defaults.js
var require_defaults = __commonJS({
  "node_modules/pg/lib/defaults.js"(exports2, module2) {
    "use strict";
    var user;
    try {
      user = process.platform === "win32" ? process.env.USERNAME : process.env.USER;
    } catch {
    }
    module2.exports = {
      // database host. defaults to localhost
      host: "localhost",
      // database user's name
      user,
      // name of database to connect
      database: void 0,
      // database user's password
      password: null,
      // a Postgres connection string to be used instead of setting individual connection items
      // NOTE:  Setting this value will cause it to override any other value (such as database or user) defined
      // in the defaults object.
      connectionString: void 0,
      // database port
      port: 5432,
      // number of rows to return at a time from a prepared statement's
      // portal. 0 will return all rows at once
      rows: 0,
      // binary result mode
      binary: !1,
      // Connection pool options - see https://github.com/brianc/node-pg-pool
      // number of connections to use in connection pool
      // 0 will disable connection pooling
      max: 10,
      // max milliseconds a client can go unused before it is removed
      // from the pool and destroyed
      idleTimeoutMillis: 3e4,
      client_encoding: "",
      ssl: !1,
      // SSL negotiation style: 'postgres' (traditional SSLRequest) or 'direct'
      sslnegotiation: void 0,
      application_name: void 0,
      fallback_application_name: void 0,
      options: void 0,
      parseInputDatesAsUTC: !1,
      // max milliseconds any query using this connection will execute for before timing out in error.
      // false=unlimited
      statement_timeout: !1,
      // Abort any statement that waits longer than the specified duration in milliseconds while attempting to acquire a lock.
      // false=unlimited
      lock_timeout: !1,
      // Terminate any session with an open transaction that has been idle for longer than the specified duration in milliseconds
      // false=unlimited
      idle_in_transaction_session_timeout: !1,
      // max milliseconds to wait for query to complete (client side)
      query_timeout: !1,
      connect_timeout: 0,
      keepalives: 1,
      keepalives_idle: 0
    };
    var pgTypes = require_pg_types(), parseBigInteger = pgTypes.getTypeParser(20, "text"), parseBigIntegerArray = pgTypes.getTypeParser(1016, "text");
    module2.exports.__defineSetter__("parseInt8", function(val) {
      pgTypes.setTypeParser(20, "text", val ? pgTypes.getTypeParser(23, "text") : parseBigInteger), pgTypes.setTypeParser(1016, "text", val ? pgTypes.getTypeParser(1007, "text") : parseBigIntegerArray);
    });
  }
});

// node_modules/pg/lib/utils.js
var require_utils = __commonJS({
  "node_modules/pg/lib/utils.js"(exports2, module2) {
    "use strict";
    var defaults2 = require_defaults(), nodeUtils = require("util"), { isDate } = require("util/types"), invalidDateDeprecationNotice = nodeUtils.deprecate(
      () => {
      },
      "Sending an invalid date to Postgres is deprecated and will throw an error in the next major version of pg. Ensure any Date object passed as a query parameter is valid.",
      "PG_INVALID_DATE"
    );
    function escapeElement(elementRepresentation) {
      return '"' + elementRepresentation.replace(/\\/g, "\\\\").replace(/"/g, '\\"') + '"';
    }
    function arrayString(val) {
      let result = "{";
      for (let i = 0; i < val.length; i++) {
        i > 0 && (result += ",");
        let item = val[i];
        item == null ? result += "NULL" : Array.isArray(item) ? result += arrayString(item) : ArrayBuffer.isView(item) ? (item instanceof Buffer || (item = Buffer.from(item.buffer, item.byteOffset, item.byteLength)), result += "\\\\x" + item.toString("hex")) : result += escapeElement(prepareValue(item));
      }
      return result += "}", result;
    }
    var prepareValue = function(val, seen) {
      return val == null ? null : typeof val == "object" ? val instanceof Buffer ? val : ArrayBuffer.isView(val) ? Buffer.from(val.buffer, val.byteOffset, val.byteLength) : isDate(val) ? (isNaN(val.getTime()) && invalidDateDeprecationNotice(), defaults2.parseInputDatesAsUTC ? dateToStringUTC(val) : dateToString(val)) : Array.isArray(val) ? arrayString(val) : prepareObject(val, seen) : val.toString();
    };
    function prepareObject(val, seen) {
      if (val && typeof val.toPostgres == "function") {
        if (seen = seen || [], seen.indexOf(val) !== -1)
          throw new Error('circular reference detected while preparing "' + val + '" for query');
        return seen.push(val), prepareValue(val.toPostgres(prepareValue), seen);
      }
      return JSON.stringify(val);
    }
    function dateToString(date) {
      let offset = -date.getTimezoneOffset(), year = date.getFullYear(), isBCYear = year < 1;
      isBCYear && (year = Math.abs(year) + 1);
      let ret = String(year).padStart(4, "0") + "-" + String(date.getMonth() + 1).padStart(2, "0") + "-" + String(date.getDate()).padStart(2, "0") + "T" + String(date.getHours()).padStart(2, "0") + ":" + String(date.getMinutes()).padStart(2, "0") + ":" + String(date.getSeconds()).padStart(2, "0") + "." + String(date.getMilliseconds()).padStart(3, "0");
      return offset < 0 ? (ret += "-", offset *= -1) : ret += "+", ret += String(Math.floor(offset / 60)).padStart(2, "0") + ":" + String(offset % 60).padStart(2, "0"), isBCYear && (ret += " BC"), ret;
    }
    function dateToStringUTC(date) {
      let year = date.getUTCFullYear(), isBCYear = year < 1;
      isBCYear && (year = Math.abs(year) + 1);
      let ret = String(year).padStart(4, "0") + "-" + String(date.getUTCMonth() + 1).padStart(2, "0") + "-" + String(date.getUTCDate()).padStart(2, "0") + "T" + String(date.getUTCHours()).padStart(2, "0") + ":" + String(date.getUTCMinutes()).padStart(2, "0") + ":" + String(date.getUTCSeconds()).padStart(2, "0") + "." + String(date.getUTCMilliseconds()).padStart(3, "0");
      return ret += "+00:00", isBCYear && (ret += " BC"), ret;
    }
    function normalizeQueryConfig(config, values, callback) {
      return config = typeof config == "string" ? { text: config } : cloneQueryConfig(config), values && (typeof values == "function" ? config.callback = values : config.values = values), callback && (config.callback = callback), config;
    }
    function cloneQueryConfig(config) {
      return config == null ? config : Object.defineProperties(Object.create(Object.getPrototypeOf(config)), Object.getOwnPropertyDescriptors(config));
    }
    var escapeIdentifier2 = function(str2) {
      return '"' + str2.replace(/"/g, '""') + '"';
    }, escapeLiteral2 = function(str2) {
      let hasBackslash = !1, escaped = "'";
      if (str2 == null || typeof str2 != "string")
        return "''";
      for (let i = 0; i < str2.length; i++) {
        let c = str2[i];
        c === "'" ? escaped += c + c : c === "\\" ? (escaped += c + c, hasBackslash = !0) : escaped += c;
      }
      return escaped += "'", hasBackslash === !0 && (escaped = " E" + escaped), escaped;
    };
    module2.exports = {
      prepareValue: function(value) {
        return prepareValue(value);
      },
      normalizeQueryConfig,
      escapeIdentifier: escapeIdentifier2,
      escapeLiteral: escapeLiteral2
    };
  }
});

// node_modules/pg/lib/crypto/utils.js
var require_utils2 = __commonJS({
  "node_modules/pg/lib/crypto/utils.js"(exports2, module2) {
    var nodeCrypto2 = require("crypto");
    module2.exports = {
      postgresMd5PasswordHash,
      randomBytes: randomBytes2,
      deriveKey,
      sha256: sha2562,
      hashByName,
      hmacSha256,
      md5
    };
    var webCrypto = nodeCrypto2.webcrypto || globalThis.crypto, subtleCrypto = webCrypto.subtle, textEncoder = new TextEncoder();
    function randomBytes2(length) {
      return webCrypto.getRandomValues(Buffer.alloc(length));
    }
    async function md5(string) {
      try {
        return nodeCrypto2.createHash("md5").update(string, "utf-8").digest("hex");
      } catch {
        let data = typeof string == "string" ? textEncoder.encode(string) : string, hash2 = await subtleCrypto.digest("MD5", data);
        return Array.from(new Uint8Array(hash2)).map((b) => b.toString(16).padStart(2, "0")).join("");
      }
    }
    async function postgresMd5PasswordHash(user, password, salt) {
      let inner = await md5(password + user);
      return "md5" + await md5(Buffer.concat([Buffer.from(inner), salt]));
    }
    async function sha2562(text) {
      return await subtleCrypto.digest("SHA-256", text);
    }
    async function hashByName(hashName, text) {
      return await subtleCrypto.digest(hashName, text);
    }
    async function hmacSha256(keyBuffer, msg) {
      let key = await subtleCrypto.importKey("raw", keyBuffer, { name: "HMAC", hash: "SHA-256" }, !1, ["sign"]);
      return await subtleCrypto.sign("HMAC", key, textEncoder.encode(msg));
    }
    async function deriveKey(password, salt, iterations) {
      let key = await subtleCrypto.importKey("raw", textEncoder.encode(password), "PBKDF2", !1, ["deriveBits"]), params = { name: "PBKDF2", hash: "SHA-256", salt, iterations };
      return await subtleCrypto.deriveBits(params, key, 256, ["deriveBits"]);
    }
  }
});

// node_modules/pg/lib/crypto/cert-signatures.js
var require_cert_signatures = __commonJS({
  "node_modules/pg/lib/crypto/cert-signatures.js"(exports2, module2) {
    function x509Error(msg, cert) {
      return new Error("SASL channel binding: " + msg + " when parsing public certificate " + cert.toString("base64"));
    }
    function readASN1Length(data, index) {
      let length = data[index++];
      if (length < 128) return { length, index };
      let lengthBytes = length & 127;
      if (lengthBytes > 4) throw x509Error("bad length", data);
      length = 0;
      for (let i = 0; i < lengthBytes; i++)
        length = length << 8 | data[index++];
      return { length, index };
    }
    function readASN1OID(data, index) {
      if (data[index++] !== 6) throw x509Error("non-OID data", data);
      let { length: OIDLength, index: indexAfterOIDLength } = readASN1Length(data, index);
      index = indexAfterOIDLength;
      let lastIndex = index + OIDLength, byte1 = data[index++], oid = (byte1 / 40 >> 0) + "." + byte1 % 40;
      for (; index < lastIndex; ) {
        let value = 0;
        for (; index < lastIndex; ) {
          let nextByte = data[index++];
          if (value = value << 7 | nextByte & 127, nextByte < 128) break;
        }
        oid += "." + value;
      }
      return { oid, index };
    }
    function expectASN1Seq(data, index) {
      if (data[index++] !== 48) throw x509Error("non-sequence data", data);
      return readASN1Length(data, index);
    }
    function signatureAlgorithmHashFromCertificate(data, index) {
      index === void 0 && (index = 0), index = expectASN1Seq(data, index).index;
      let { length: certInfoLength, index: indexAfterCertInfoLength } = expectASN1Seq(data, index);
      index = indexAfterCertInfoLength + certInfoLength, index = expectASN1Seq(data, index).index;
      let { oid, index: indexAfterOID } = readASN1OID(data, index);
      switch (oid) {
        // RSA
        case "1.2.840.113549.1.1.4":
          return "MD5";
        case "1.2.840.113549.1.1.5":
          return "SHA-1";
        case "1.2.840.113549.1.1.11":
          return "SHA-256";
        case "1.2.840.113549.1.1.12":
          return "SHA-384";
        case "1.2.840.113549.1.1.13":
          return "SHA-512";
        case "1.2.840.113549.1.1.14":
          return "SHA-224";
        case "1.2.840.113549.1.1.15":
          return "SHA512-224";
        case "1.2.840.113549.1.1.16":
          return "SHA512-256";
        // ECDSA
        case "1.2.840.10045.4.1":
          return "SHA-1";
        case "1.2.840.10045.4.3.1":
          return "SHA-224";
        case "1.2.840.10045.4.3.2":
          return "SHA-256";
        case "1.2.840.10045.4.3.3":
          return "SHA-384";
        case "1.2.840.10045.4.3.4":
          return "SHA-512";
        // RSASSA-PSS: hash is indicated separately
        case "1.2.840.113549.1.1.10": {
          if (index = indexAfterOID, index = expectASN1Seq(data, index).index, data[index++] !== 160) throw x509Error("non-tag data", data);
          index = readASN1Length(data, index).index, index = expectASN1Seq(data, index).index;
          let { oid: hashOID } = readASN1OID(data, index);
          switch (hashOID) {
            // standalone hash OIDs
            case "1.2.840.113549.2.5":
              return "MD5";
            case "1.3.14.3.2.26":
              return "SHA-1";
            case "2.16.840.1.101.3.4.2.1":
              return "SHA-256";
            case "2.16.840.1.101.3.4.2.2":
              return "SHA-384";
            case "2.16.840.1.101.3.4.2.3":
              return "SHA-512";
          }
          throw x509Error("unknown hash OID " + hashOID, data);
        }
        // Ed25519 -- see https://github.com/openssl/openssl/issues/15477
        case "1.3.101.110":
        case "1.3.101.112":
          return "SHA-512";
        // Ed448 -- still not in pg 17.2 (if supported, digest would be SHAKE256 x 64 bytes)
        case "1.3.101.111":
        case "1.3.101.113":
          throw x509Error("Ed448 certificate channel binding is not currently supported by Postgres");
      }
      throw x509Error("unknown OID " + oid, data);
    }
    module2.exports = { signatureAlgorithmHashFromCertificate };
  }
});

// node_modules/pg/lib/crypto/sasl.js
var require_sasl = __commonJS({
  "node_modules/pg/lib/crypto/sasl.js"(exports2, module2) {
    "use strict";
    var crypto4 = require_utils2(), { signatureAlgorithmHashFromCertificate } = require_cert_signatures();
    function saslprep(password) {
      let nonAsciiSpace = /[\u00A0\u1680\u2000-\u200B\u202F\u205F\u3000]/g, mappedToNothing = /[\u00AD\u034F\u1806\u180B\u180C\u180D\u200C\u200D\u2060\uFE00-\uFE0F\uFEFF]/g;
      return password.replace(nonAsciiSpace, " ").replace(mappedToNothing, "").normalize("NFKC");
    }
    var DEFAULT_MAX_SCRAM_ITERATIONS = 1e5;
    function startSession(mechanisms, stream, scramMaxIterations = DEFAULT_MAX_SCRAM_ITERATIONS) {
      let candidates = ["SCRAM-SHA-256"];
      stream && candidates.unshift("SCRAM-SHA-256-PLUS");
      let mechanism = candidates.find((candidate) => mechanisms.includes(candidate));
      if (!mechanism)
        throw new Error("SASL: Only mechanism(s) " + candidates.join(" and ") + " are supported");
      if (mechanism === "SCRAM-SHA-256-PLUS" && typeof stream.getPeerCertificate != "function")
        throw new Error("SASL: Mechanism SCRAM-SHA-256-PLUS requires a certificate");
      let clientNonce = crypto4.randomBytes(18).toString("base64");
      return {
        mechanism,
        clientNonce,
        response: (mechanism === "SCRAM-SHA-256-PLUS" ? "p=tls-server-end-point" : stream ? "y" : "n") + ",,n=*,r=" + clientNonce,
        message: "SASLInitialResponse",
        scramMaxIterations
      };
    }
    async function continueSession(session, password, serverData, stream) {
      if (session.message !== "SASLInitialResponse")
        throw new Error("SASL: Last message was not SASLInitialResponse");
      if (typeof password != "string")
        throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: client password must be a string");
      if (password === "")
        throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: client password must be a non-empty string");
      if (typeof serverData != "string")
        throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: serverData must be a string");
      let sv = parseServerFirstMessage(serverData);
      if (sv.nonce.startsWith(session.clientNonce)) {
        if (sv.nonce.length === session.clientNonce.length)
          throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: server nonce is too short");
      } else throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: server nonce does not start with client nonce");
      let scramMaxIterations = typeof session.scramMaxIterations == "number" ? session.scramMaxIterations : DEFAULT_MAX_SCRAM_ITERATIONS;
      if (scramMaxIterations !== 0 && sv.iteration > scramMaxIterations)
        throw new Error(
          "SASL: SCRAM-SERVER-FIRST-MESSAGE: iteration count " + sv.iteration + " exceeds scramMaxIterations of " + scramMaxIterations
        );
      let clientFirstMessageBare = "n=*,r=" + session.clientNonce, serverFirstMessage = "r=" + sv.nonce + ",s=" + sv.salt + ",i=" + sv.iteration, channelBinding = stream ? "eSws" : "biws";
      if (session.mechanism === "SCRAM-SHA-256-PLUS") {
        let peerCert = stream.getPeerCertificate().raw, hashName = signatureAlgorithmHashFromCertificate(peerCert);
        (hashName === "MD5" || hashName === "SHA-1") && (hashName = "SHA-256");
        let certHash = await crypto4.hashByName(hashName, peerCert);
        channelBinding = Buffer.concat([Buffer.from("p=tls-server-end-point,,"), Buffer.from(certHash)]).toString("base64");
      }
      let clientFinalMessageWithoutProof = "c=" + channelBinding + ",r=" + sv.nonce, authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof, saltBytes = Buffer.from(sv.salt, "base64"), saltedPassword = await crypto4.deriveKey(saslprep(password), saltBytes, sv.iteration), clientKey = await crypto4.hmacSha256(saltedPassword, "Client Key"), storedKey = await crypto4.sha256(clientKey), clientSignature = await crypto4.hmacSha256(storedKey, authMessage), clientProof = xorBuffers(Buffer.from(clientKey), Buffer.from(clientSignature)).toString("base64"), serverKey = await crypto4.hmacSha256(saltedPassword, "Server Key"), serverSignatureBytes = await crypto4.hmacSha256(serverKey, authMessage);
      session.message = "SASLResponse", session.serverSignature = Buffer.from(serverSignatureBytes).toString("base64"), session.response = clientFinalMessageWithoutProof + ",p=" + clientProof;
    }
    function finalizeSession(session, serverData) {
      if (session.message !== "SASLResponse")
        throw new Error("SASL: Last message was not SASLResponse");
      if (typeof serverData != "string")
        throw new Error("SASL: SCRAM-SERVER-FINAL-MESSAGE: serverData must be a string");
      let { serverSignature } = parseServerFinalMessage(serverData);
      if (serverSignature !== session.serverSignature)
        throw new Error("SASL: SCRAM-SERVER-FINAL-MESSAGE: server signature does not match");
    }
    function isPrintableChars(text) {
      if (typeof text != "string")
        throw new TypeError("SASL: text must be a string");
      return text.split("").map((_, i) => text.charCodeAt(i)).every((c) => c >= 33 && c <= 43 || c >= 45 && c <= 126);
    }
    function isBase64(text) {
      return /^(?:[a-zA-Z0-9+/]{4})*(?:[a-zA-Z0-9+/]{2}==|[a-zA-Z0-9+/]{3}=)?$/.test(text);
    }
    function parseAttributePairs(text) {
      if (typeof text != "string")
        throw new TypeError("SASL: attribute pairs text must be a string");
      return new Map(
        text.split(",").map((attrValue) => {
          if (!/^.=/.test(attrValue))
            throw new Error("SASL: Invalid attribute pair entry");
          let name = attrValue[0], value = attrValue.substring(2);
          return [name, value];
        })
      );
    }
    function parseServerFirstMessage(data) {
      let attrPairs = parseAttributePairs(data), nonce = attrPairs.get("r");
      if (nonce) {
        if (!isPrintableChars(nonce))
          throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: nonce must only contain printable characters");
      } else throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: nonce missing");
      let salt = attrPairs.get("s");
      if (salt) {
        if (!isBase64(salt))
          throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: salt must be base64");
      } else throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: salt missing");
      let iterationText = attrPairs.get("i");
      if (iterationText) {
        if (!/^[1-9][0-9]*$/.test(iterationText))
          throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: invalid iteration count");
      } else throw new Error("SASL: SCRAM-SERVER-FIRST-MESSAGE: iteration missing");
      let iteration = parseInt(iterationText, 10);
      return {
        nonce,
        salt,
        iteration
      };
    }
    function parseServerFinalMessage(serverData) {
      let attrPairs = parseAttributePairs(serverData), error = attrPairs.get("e"), serverSignature = attrPairs.get("v");
      if (error)
        throw new Error(`SASL: SCRAM-SERVER-FINAL-MESSAGE: server returned error: "${error}"`);
      if (serverSignature) {
        if (!isBase64(serverSignature))
          throw new Error("SASL: SCRAM-SERVER-FINAL-MESSAGE: server signature must be base64");
      } else throw new Error("SASL: SCRAM-SERVER-FINAL-MESSAGE: server signature is missing");
      return {
        serverSignature
      };
    }
    function xorBuffers(a, b) {
      if (!Buffer.isBuffer(a))
        throw new TypeError("first argument must be a Buffer");
      if (!Buffer.isBuffer(b))
        throw new TypeError("second argument must be a Buffer");
      if (a.length !== b.length)
        throw new Error("Buffer lengths must match");
      if (a.length === 0)
        throw new Error("Buffers cannot be empty");
      return Buffer.from(a.map((_, i) => a[i] ^ b[i]));
    }
    module2.exports = {
      startSession,
      continueSession,
      finalizeSession,
      DEFAULT_MAX_SCRAM_ITERATIONS
    };
  }
});

// node_modules/pg/lib/type-overrides.js
var require_type_overrides = __commonJS({
  "node_modules/pg/lib/type-overrides.js"(exports2, module2) {
    "use strict";
    var types3 = require_pg_types();
    function TypeOverrides2(userTypes) {
      this._types = userTypes || types3, this.text = {}, this.binary = {};
    }
    TypeOverrides2.prototype.getOverrides = function(format) {
      switch (format) {
        case "text":
          return this.text;
        case "binary":
          return this.binary;
        default:
          return {};
      }
    };
    TypeOverrides2.prototype.setTypeParser = function(oid, format, parseFn) {
      typeof format == "function" && (parseFn = format, format = "text"), this.getOverrides(format)[oid] = parseFn;
    };
    TypeOverrides2.prototype.getTypeParser = function(oid, format) {
      return format = format || "text", this.getOverrides(format)[oid] || this._types.getTypeParser(oid, format);
    };
    module2.exports = TypeOverrides2;
  }
});

// node_modules/pg-connection-string/index.js
var require_pg_connection_string = __commonJS({
  "node_modules/pg-connection-string/index.js"(exports2, module2) {
    "use strict";
    function parse(str2, options = {}) {
      if (str2.charAt(0) === "/") {
        let config2 = str2.split(" ");
        return { host: config2[0], database: config2[1] };
      }
      let config = /* @__PURE__ */ Object.create(null), result, dummyHost = !1;
      / |%[^a-f0-9]|%[a-f0-9][^a-f0-9]/i.test(str2) && (str2 = encodeURI(str2).replace(/%25(\d\d)/g, "%$1"));
      try {
        try {
          result = new URL(str2, "postgres://base");
        } catch {
          result = new URL(str2.replace("@/", "@___DUMMY___/"), "postgres://base"), dummyHost = !0;
        }
      } catch (err) {
        throw err.input && (err.input = "*****REDACTED*****"), err;
      }
      for (let entry of result.searchParams.entries())
        config[entry[0]] = entry[1];
      if (config.user = config.user || decodeURIComponent(result.username), config.password = config.password || decodeURIComponent(result.password), result.protocol == "socket:")
        return config.host = decodeURI(result.pathname), config.database = result.searchParams.get("db"), config.client_encoding = result.searchParams.get("encoding"), config;
      let hostname = (dummyHost ? "" : result.hostname).replace(/^\[(.+)\]$/, "$1");
      config.host ? hostname && /^%2f/i.test(hostname) && (result.pathname = hostname + result.pathname) : config.host = decodeURIComponent(hostname), config.port || (config.port = result.port);
      let pathname = result.pathname.slice(1) || null;
      config.database = pathname ? decodeURI(pathname) : null, (config.ssl === "true" || config.ssl === "1") && (config.ssl = !0), config.ssl === "0" && (config.ssl = !1), (config.sslcert || config.sslkey || config.sslrootcert || config.sslmode) && (config.ssl = {}), config.sslnegotiation === "direct" && config.ssl === void 0 && (config.ssl = !0);
      let fs2 = config.sslcert || config.sslkey || config.sslrootcert ? require("fs") : null;
      if (config.sslcert && (config.ssl.cert = fs2.readFileSync(config.sslcert).toString()), config.sslkey && (config.ssl.key = fs2.readFileSync(config.sslkey).toString()), config.sslrootcert && (config.ssl.ca = fs2.readFileSync(config.sslrootcert).toString()), options.useLibpqCompat && config.uselibpqcompat)
        throw new Error("Both useLibpqCompat and uselibpqcompat are set. Please use only one of them.");
      if (config.uselibpqcompat === "true" || options.useLibpqCompat)
        switch (config.sslmode) {
          case "disable": {
            config.ssl = !1;
            break;
          }
          case "prefer": {
            config.ssl.rejectUnauthorized = !1;
            break;
          }
          case "require": {
            config.sslrootcert ? config.ssl.checkServerIdentity = function() {
            } : config.ssl.rejectUnauthorized = !1;
            break;
          }
          case "verify-ca": {
            if (!config.ssl.ca)
              throw new Error(
                "SECURITY WARNING: Using sslmode=verify-ca requires specifying a CA with sslrootcert. If a public CA is used, verify-ca allows connections to a server that somebody else may have registered with the CA, making you vulnerable to Man-in-the-Middle attacks. Either specify a custom CA certificate with sslrootcert parameter or use sslmode=verify-full for proper security."
              );
            config.ssl.checkServerIdentity = function() {
            };
            break;
          }
          case "verify-full":
            break;
        }
      else
        switch (config.sslmode) {
          case "disable": {
            config.ssl = !1;
            break;
          }
          case "prefer":
          case "require":
          case "verify-ca":
          case "verify-full": {
            config.sslmode !== "verify-full" && deprecatedSslModeWarning(config.sslmode);
            break;
          }
          case "no-verify": {
            config.ssl.rejectUnauthorized = !1;
            break;
          }
        }
      return config;
    }
    function toConnectionOptions(sslConfig) {
      return Object.entries(sslConfig).reduce((c, [key, value]) => (value != null && (c[key] = value), c), /* @__PURE__ */ Object.create(null));
    }
    function toClientConfig(config) {
      return Object.entries(config).reduce((c, [key, value]) => {
        if (key === "ssl") {
          let sslConfig = value;
          typeof sslConfig == "boolean" && (c[key] = sslConfig), typeof sslConfig == "object" && (c[key] = toConnectionOptions(sslConfig));
        } else if (value != null)
          if (key === "port") {
            if (value !== "") {
              let v = parseInt(value, 10);
              if (isNaN(v))
                throw new Error(`Invalid ${key}: ${value}`);
              c[key] = v;
            }
          } else
            c[key] = value;
        return c;
      }, /* @__PURE__ */ Object.create(null));
    }
    function parseIntoClientConfig(str2) {
      return toClientConfig(parse(str2));
    }
    function deprecatedSslModeWarning(sslmode) {
      !deprecatedSslModeWarning.warned && typeof process < "u" && process.emitWarning && (deprecatedSslModeWarning.warned = !0, process.emitWarning(`SECURITY WARNING: The SSL modes 'prefer', 'require', and 'verify-ca' are treated as aliases for 'verify-full'.
In the next major version (pg-connection-string v3.0.0 and pg v9.0.0), these modes will adopt standard libpq semantics, which have weaker security guarantees.

To prepare for this change:
- If you want the current behavior, explicitly use 'sslmode=verify-full'
- If you want libpq compatibility now, use 'uselibpqcompat=true&sslmode=${sslmode}'

See https://www.postgresql.org/docs/current/libpq-ssl.html for libpq SSL mode definitions.`));
    }
    module2.exports = parse;
    parse.parse = parse;
    parse.toClientConfig = toClientConfig;
    parse.parseIntoClientConfig = parseIntoClientConfig;
  }
});

// node_modules/pg/lib/connection-parameters.js
var require_connection_parameters = __commonJS({
  "node_modules/pg/lib/connection-parameters.js"(exports2, module2) {
    "use strict";
    var dns = require("dns"), defaults2 = require_defaults(), parse = require_pg_connection_string().parse, val = function(key, config, envVar) {
      return config[key] ? config[key] : (envVar === void 0 ? envVar = process.env["PG" + key.toUpperCase()] : envVar === !1 || (envVar = process.env[envVar]), envVar || defaults2[key]);
    }, readSSLConfigFromEnvironment = function() {
      switch (process.env.PGSSLMODE) {
        case "disable":
          return !1;
        case "prefer":
        case "require":
        case "verify-ca":
        case "verify-full":
          return !0;
        case "no-verify":
          return { rejectUnauthorized: !1 };
      }
      return defaults2.ssl;
    }, quoteParamValue = function(value) {
      return "'" + ("" + value).replace(/\\/g, "\\\\").replace(/'/g, "\\'") + "'";
    }, add = function(params, config, paramName) {
      let value = config[paramName];
      value != null && params.push(paramName + "=" + quoteParamValue(value));
    }, ConnectionParameters = class {
      constructor(config) {
        if (config = typeof config == "string" ? parse(config) : config || {}, config.connectionString && (config = Object.assign({}, config, parse(config.connectionString))), this.user = val("user", config), this.database = val("database", config), this.database === void 0 && (this.database = this.user), this.port = parseInt(val("port", config), 10), this.host = val("host", config), Object.defineProperty(this, "password", {
          configurable: !0,
          enumerable: !1,
          writable: !0,
          value: val("password", config)
        }), this.binary = val("binary", config), this.options = val("options", config), this.ssl = typeof config.ssl > "u" ? readSSLConfigFromEnvironment() : config.ssl, typeof this.ssl == "string" && this.ssl === "true" && (this.ssl = !0), this.ssl === "no-verify" && (this.ssl = { rejectUnauthorized: !1 }), this.ssl && this.ssl.key && Object.defineProperty(this.ssl, "key", {
          enumerable: !1
        }), this.sslnegotiation = val("sslnegotiation", config, "PGSSLNEGOTIATION"), this.sslnegotiation !== void 0 && this.sslnegotiation !== "postgres" && this.sslnegotiation !== "direct")
          throw new Error(
            `Invalid sslnegotiation value: "${this.sslnegotiation}". Valid values are "postgres" and "direct".`
          );
        if (this.sslnegotiation === "direct" && !this.ssl)
          throw new Error("sslnegotiation=direct requires SSL to be enabled");
        this.client_encoding = val("client_encoding", config), this.replication = val("replication", config), this.isDomainSocket = !(this.host || "").indexOf("/"), this.application_name = val("application_name", config, "PGAPPNAME"), this.fallback_application_name = val("fallback_application_name", config, !1), this.statement_timeout = val("statement_timeout", config, !1), this.lock_timeout = val("lock_timeout", config, !1), this.idle_in_transaction_session_timeout = val("idle_in_transaction_session_timeout", config, !1), this.query_timeout = val("query_timeout", config, !1), config.connectionTimeoutMillis === void 0 ? this.connect_timeout = process.env.PGCONNECT_TIMEOUT || 0 : this.connect_timeout = Math.floor(config.connectionTimeoutMillis / 1e3), config.keepAlive === !1 ? this.keepalives = 0 : config.keepAlive === !0 && (this.keepalives = 1), typeof config.keepAliveInitialDelayMillis == "number" && (this.keepalives_idle = Math.floor(config.keepAliveInitialDelayMillis / 1e3));
      }
      getLibpqConnectionString(cb) {
        let params = [];
        add(params, this, "user"), add(params, this, "password"), add(params, this, "port"), add(params, this, "application_name"), add(params, this, "fallback_application_name"), add(params, this, "connect_timeout"), add(params, this, "options");
        let ssl = typeof this.ssl == "object" ? this.ssl : this.ssl ? { sslmode: this.ssl } : {};
        if (add(params, ssl, "sslmode"), add(params, ssl, "sslca"), add(params, ssl, "sslkey"), add(params, ssl, "sslcert"), add(params, ssl, "sslrootcert"), add(params, this, "sslnegotiation"), this.database && params.push("dbname=" + quoteParamValue(this.database)), this.replication && params.push("replication=" + quoteParamValue(this.replication)), this.host && params.push("host=" + quoteParamValue(this.host)), this.isDomainSocket)
          return cb(null, params.join(" "));
        this.client_encoding && params.push("client_encoding=" + quoteParamValue(this.client_encoding)), dns.lookup(this.host, function(err, address) {
          return err ? cb(err, null) : (params.push("hostaddr=" + quoteParamValue(address)), cb(null, params.join(" ")));
        });
      }
    };
    module2.exports = ConnectionParameters;
  }
});

// node_modules/pg/lib/result.js
var require_result = __commonJS({
  "node_modules/pg/lib/result.js"(exports2, module2) {
    "use strict";
    var types3 = require_pg_types(), matchRegexp = /^([A-Za-z]+)(?: (\d+))?(?: (\d+))?/, Result2 = class {
      constructor(rowMode, types4) {
        this.command = null, this.rowCount = null, this.oid = null, this.rows = [], this.fields = [], this._parsers = void 0, this._types = types4, this.RowCtor = null, this.rowAsArray = rowMode === "array", this.rowAsArray && (this.parseRow = this._parseRowAsArray), this._prebuiltEmptyResultObject = null;
      }
      // adds a command complete message
      addCommandComplete(msg) {
        let match;
        msg.text ? match = matchRegexp.exec(msg.text) : match = matchRegexp.exec(msg.command), match && (this.command = match[1], match[3] ? (this.oid = parseInt(match[2], 10), this.rowCount = parseInt(match[3], 10)) : match[2] && (this.rowCount = parseInt(match[2], 10)));
      }
      _parseRowAsArray(rowData) {
        let row = new Array(rowData.length);
        for (let i = 0, len = rowData.length; i < len; i++) {
          let rawValue = rowData[i];
          rawValue !== null ? row[i] = this._parsers[i](rawValue) : row[i] = null;
        }
        return row;
      }
      parseRow(rowData) {
        let row = { ...this._prebuiltEmptyResultObject };
        for (let i = 0, len = rowData.length; i < len; i++) {
          let rawValue = rowData[i], field = this.fields[i].name;
          if (rawValue !== null) {
            let v = this.fields[i].format === "binary" ? Buffer.from(rawValue) : rawValue;
            row[field] = this._parsers[i](v);
          } else
            row[field] = null;
        }
        return row;
      }
      addRow(row) {
        this.rows.push(row);
      }
      addFields(fieldDescriptions) {
        this.fields = fieldDescriptions, this.fields.length && (this._parsers = new Array(fieldDescriptions.length));
        let row = /* @__PURE__ */ Object.create(null);
        for (let i = 0; i < fieldDescriptions.length; i++) {
          let desc = fieldDescriptions[i];
          row[desc.name] = null, this._types ? this._parsers[i] = this._types.getTypeParser(desc.dataTypeID, desc.format || "text") : this._parsers[i] = types3.getTypeParser(desc.dataTypeID, desc.format || "text");
        }
        this._prebuiltEmptyResultObject = { ...row };
      }
    };
    module2.exports = Result2;
  }
});

// node_modules/pg/lib/query.js
var require_query = __commonJS({
  "node_modules/pg/lib/query.js"(exports2, module2) {
    "use strict";
    var { EventEmitter } = require("events"), Result2 = require_result(), utils = require_utils(), Query2 = class extends EventEmitter {
      constructor(config, values, callback) {
        super(), config = utils.normalizeQueryConfig(config, values, callback), this.text = config.text, this.values = config.values, this.rows = config.rows, this.types = config.types, this.name = config.name, this.queryMode = config.queryMode, this.binary = config.binary, this.portal = config.portal || "", this.callback = config.callback, this._rowMode = config.rowMode, process.domain && config.callback && (this.callback = process.domain.bind(config.callback)), this._result = new Result2(this._rowMode, this.types), this._results = this._result, this._canceledDueToError = !1;
      }
      requiresPreparation() {
        return this.queryMode === "extended" || this.name || this.rows ? !0 : !this.text || !this.values ? !1 : this.values.length > 0;
      }
      _checkForMultirow() {
        this._result.command && (Array.isArray(this._results) || (this._results = [this._result]), this._result = new Result2(this._rowMode, this._result._types), this._results.push(this._result));
      }
      // associates row metadata from the supplied
      // message with this query object
      // metadata used when parsing row results
      handleRowDescription(msg) {
        this._checkForMultirow(), this._result.addFields(msg.fields), this._accumulateRows = this.callback || !this.listeners("row").length;
      }
      handleDataRow(msg) {
        let row;
        if (!this._canceledDueToError) {
          try {
            row = this._result.parseRow(msg.fields);
          } catch (err) {
            this._canceledDueToError = err;
            return;
          }
          this.emit("row", row, this._result), this._accumulateRows && this._result.addRow(row);
        }
      }
      handleCommandComplete(msg, connection) {
        this._checkForMultirow(), this._result.addCommandComplete(msg), this.rows && connection.sync();
      }
      // if a named prepared statement is created with empty query text
      // the backend will send an emptyQuery message but *not* a command complete message
      // since we pipeline sync immediately after execute we don't need to do anything here
      // unless we have rows specified, in which case we did not pipeline the initial sync call
      handleEmptyQuery(connection) {
        this.rows && connection.sync();
      }
      handleError(err, connection) {
        if (this._canceledDueToError && (err = this._canceledDueToError, this._canceledDueToError = !1), this.callback)
          return this.callback(err);
        this.emit("error", err);
      }
      handleReadyForQuery(con) {
        if (this._canceledDueToError)
          return this.handleError(this._canceledDueToError, con);
        if (this.callback)
          try {
            this.callback(null, this._results);
          } catch (err) {
            process.nextTick(() => {
              throw err;
            });
          }
        this.emit("end", this._results);
      }
      submit(connection) {
        if (typeof this.text != "string" && typeof this.name != "string")
          return new Error("A query must have either text or a name. Supplying neither is unsupported.");
        let previous = connection.parsedStatements[this.name] || connection.submittedNamedStatements[this.name];
        if (this.text && previous && this.text !== previous)
          return new Error(`Prepared statements must be unique - '${this.name}' was used for a different statement`);
        if (this.values && !Array.isArray(this.values))
          return new Error("Query values must be an array");
        if (this.requiresPreparation()) {
          connection.stream.cork && connection.stream.cork();
          try {
            this.prepare(connection);
          } finally {
            connection.stream.uncork && connection.stream.uncork();
          }
        } else
          connection.query(this.text);
        return null;
      }
      hasBeenParsed(connection) {
        return this.name && (connection.parsedStatements[this.name] !== void 0 || connection.submittedNamedStatements[this.name] !== void 0);
      }
      handlePortalSuspended(connection) {
        this._getRows(connection, this.rows);
      }
      _getRows(connection, rows) {
        connection.execute({
          portal: this.portal,
          rows
        }), rows ? connection.flush() : connection.sync();
      }
      // http://developer.postgresql.org/pgdocs/postgres/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
      prepare(connection) {
        this.hasBeenParsed(connection) || (connection.parse({
          text: this.text,
          name: this.name,
          types: this.types
        }), this.name && (connection.submittedNamedStatements[this.name] = this.text));
        try {
          connection.bind({
            portal: this.portal,
            statement: this.name,
            values: this.values,
            binary: this.binary,
            valueMapper: utils.prepareValue
          });
        } catch (err) {
          connection.close({ type: "S", name: this.name }), connection.sync(), this.handleError(err, connection);
          return;
        }
        connection.describe({
          type: "P",
          name: this.portal || ""
        }), this._getRows(connection, this.rows);
      }
      handleCopyInResponse(connection) {
        connection.sendCopyFail("No source stream defined");
      }
      handleCopyData(msg, connection) {
      }
    };
    module2.exports = Query2;
  }
});

// node_modules/pg-protocol/dist/messages.js
var require_messages = __commonJS({
  "node_modules/pg-protocol/dist/messages.js"(exports2) {
    "use strict";
    Object.defineProperty(exports2, "__esModule", { value: !0 });
    exports2.NoticeMessage = exports2.DataRowMessage = exports2.CommandCompleteMessage = exports2.ReadyForQueryMessage = exports2.NotificationResponseMessage = exports2.BackendKeyDataMessage = exports2.AuthenticationMD5Password = exports2.ParameterStatusMessage = exports2.ParameterDescriptionMessage = exports2.RowDescriptionMessage = exports2.Field = exports2.CopyResponse = exports2.CopyDataMessage = exports2.DatabaseError = exports2.copyDone = exports2.emptyQuery = exports2.replicationStart = exports2.portalSuspended = exports2.noData = exports2.closeComplete = exports2.bindComplete = exports2.parseComplete = void 0;
    exports2.parseComplete = {
      name: "parseComplete",
      length: 5
    };
    exports2.bindComplete = {
      name: "bindComplete",
      length: 5
    };
    exports2.closeComplete = {
      name: "closeComplete",
      length: 5
    };
    exports2.noData = {
      name: "noData",
      length: 5
    };
    exports2.portalSuspended = {
      name: "portalSuspended",
      length: 5
    };
    exports2.replicationStart = {
      name: "replicationStart",
      length: 4
    };
    exports2.emptyQuery = {
      name: "emptyQuery",
      length: 4
    };
    exports2.copyDone = {
      name: "copyDone",
      length: 4
    };
    var DatabaseError2 = class extends Error {
      constructor(message, length, name) {
        super(message), this.length = length, this.name = name;
      }
    };
    exports2.DatabaseError = DatabaseError2;
    var CopyDataMessage = class {
      constructor(length, chunk) {
        this.length = length, this.chunk = chunk, this.name = "copyData";
      }
    };
    exports2.CopyDataMessage = CopyDataMessage;
    var CopyResponse = class {
      constructor(length, name, binary, columnCount) {
        this.length = length, this.name = name, this.binary = binary, this.columnTypes = new Array(columnCount);
      }
    };
    exports2.CopyResponse = CopyResponse;
    var Field = class {
      constructor(name, tableID, columnID, dataTypeID, dataTypeSize, dataTypeModifier, format) {
        this.name = name, this.tableID = tableID, this.columnID = columnID, this.dataTypeID = dataTypeID, this.dataTypeSize = dataTypeSize, this.dataTypeModifier = dataTypeModifier, this.format = format;
      }
    };
    exports2.Field = Field;
    var RowDescriptionMessage = class {
      constructor(length, fieldCount) {
        this.length = length, this.fieldCount = fieldCount, this.name = "rowDescription", this.fields = new Array(this.fieldCount);
      }
    };
    exports2.RowDescriptionMessage = RowDescriptionMessage;
    var ParameterDescriptionMessage = class {
      constructor(length, parameterCount) {
        this.length = length, this.parameterCount = parameterCount, this.name = "parameterDescription", this.dataTypeIDs = new Array(this.parameterCount);
      }
    };
    exports2.ParameterDescriptionMessage = ParameterDescriptionMessage;
    var ParameterStatusMessage = class {
      constructor(length, parameterName, parameterValue) {
        this.length = length, this.parameterName = parameterName, this.parameterValue = parameterValue, this.name = "parameterStatus";
      }
    };
    exports2.ParameterStatusMessage = ParameterStatusMessage;
    var AuthenticationMD5Password = class {
      constructor(length, salt) {
        this.length = length, this.salt = salt, this.name = "authenticationMD5Password";
      }
    };
    exports2.AuthenticationMD5Password = AuthenticationMD5Password;
    var BackendKeyDataMessage = class {
      constructor(length, processID, secretKey) {
        this.length = length, this.processID = processID, this.secretKey = secretKey, this.name = "backendKeyData";
      }
    };
    exports2.BackendKeyDataMessage = BackendKeyDataMessage;
    var NotificationResponseMessage = class {
      constructor(length, processId, channel, payload) {
        this.length = length, this.processId = processId, this.channel = channel, this.payload = payload, this.name = "notification";
      }
    };
    exports2.NotificationResponseMessage = NotificationResponseMessage;
    var ReadyForQueryMessage = class {
      constructor(length, status) {
        this.length = length, this.status = status, this.name = "readyForQuery";
      }
    };
    exports2.ReadyForQueryMessage = ReadyForQueryMessage;
    var CommandCompleteMessage = class {
      constructor(length, text) {
        this.length = length, this.text = text, this.name = "commandComplete";
      }
    };
    exports2.CommandCompleteMessage = CommandCompleteMessage;
    var DataRowMessage = class {
      constructor(length, fields) {
        this.length = length, this.fields = fields, this.name = "dataRow", this.fieldCount = fields.length;
      }
    };
    exports2.DataRowMessage = DataRowMessage;
    var NoticeMessage = class {
      constructor(length, message) {
        this.length = length, this.message = message, this.name = "notice";
      }
    };
    exports2.NoticeMessage = NoticeMessage;
  }
});

// node_modules/pg-protocol/dist/buffer-writer.js
var require_buffer_writer = __commonJS({
  "node_modules/pg-protocol/dist/buffer-writer.js"(exports2) {
    "use strict";
    Object.defineProperty(exports2, "__esModule", { value: !0 });
    exports2.Writer = void 0;
    var Writer = class {
      constructor(size = 256) {
        this.size = size, this.offset = 5, this.headerPosition = 0, this.buffer = Buffer.allocUnsafe(size);
      }
      ensure(size) {
        if (this.buffer.length - this.offset < size) {
          let oldBuffer = this.buffer, newSize = oldBuffer.length + (oldBuffer.length >> 1) + size;
          this.buffer = Buffer.allocUnsafe(newSize), oldBuffer.copy(this.buffer, 0, 0, this.offset);
        }
      }
      addInt32(num) {
        return this.ensure(4), this.buffer[this.offset++] = num >>> 24 & 255, this.buffer[this.offset++] = num >>> 16 & 255, this.buffer[this.offset++] = num >>> 8 & 255, this.buffer[this.offset++] = num >>> 0 & 255, this;
      }
      addInt16(num) {
        return this.ensure(2), this.buffer[this.offset++] = num >>> 8 & 255, this.buffer[this.offset++] = num >>> 0 & 255, this;
      }
      addCString(string) {
        if (!string)
          this.ensure(1);
        else {
          let len = Buffer.byteLength(string);
          this.ensure(len + 1), this.buffer.write(string, this.offset, "utf-8"), this.offset += len;
        }
        return this.buffer[this.offset++] = 0, this;
      }
      addString(string = "") {
        let len = Buffer.byteLength(string);
        return this.ensure(len), this.buffer.write(string, this.offset), this.offset += len, this;
      }
      // Write an Int32 byte-length prefix immediately followed by the string's UTF-8
      // bytes. Postgres' Bind wire format prefixes every parameter with its length,
      // and doing it in one method computes Buffer.byteLength ONCE — the previous
      // `addInt32(Buffer.byteLength(s)).addString(s)` pairing scanned the string
      // three times (byteLength for the prefix, byteLength again inside addString,
      // then the encode), which is costly for large text parameters.
      addInt32PrefixedString(string) {
        let len = Buffer.byteLength(string);
        this.ensure(4 + len);
        let buffer = this.buffer, offset = this.offset;
        return buffer[offset++] = len >>> 24 & 255, buffer[offset++] = len >>> 16 & 255, buffer[offset++] = len >>> 8 & 255, buffer[offset++] = len >>> 0 & 255, buffer.write(string, offset, "utf-8"), this.offset = offset + len, this;
      }
      add(otherBuffer) {
        return this.ensure(otherBuffer.length), otherBuffer.copy(this.buffer, this.offset), this.offset += otherBuffer.length, this;
      }
      /**
       * Appends an uninitialized block of {@link size} bytes to the buffer and returns its offset.
       */
      reserveUnsafe(size) {
        let offset = this.offset;
        return this.ensure(size), this.offset += size, offset;
      }
      join(code) {
        if (code) {
          this.buffer[this.headerPosition] = code;
          let length = this.offset - (this.headerPosition + 1);
          this.buffer.writeInt32BE(length, this.headerPosition + 1);
        }
        return this.buffer.slice(code ? 0 : 5, this.offset);
      }
      flush(code) {
        let result = this.join(code);
        return this.offset = 5, this.headerPosition = 0, this.buffer = Buffer.allocUnsafe(this.size), result;
      }
      clear() {
        this.offset = 5, this.headerPosition = 0;
      }
    };
    exports2.Writer = Writer;
  }
});

// node_modules/pg-protocol/dist/serializer.js
var require_serializer = __commonJS({
  "node_modules/pg-protocol/dist/serializer.js"(exports2) {
    "use strict";
    Object.defineProperty(exports2, "__esModule", { value: !0 });
    exports2.serialize = void 0;
    var buffer_writer_1 = require_buffer_writer(), writer = new buffer_writer_1.Writer(), startup = (opts) => {
      writer.addInt16(3).addInt16(0);
      for (let key of Object.keys(opts))
        writer.addCString(key).addCString(opts[key]);
      writer.addCString("client_encoding").addCString("UTF8");
      let bodyBuffer = writer.addCString("").flush(), length = bodyBuffer.length + 4;
      return new buffer_writer_1.Writer().addInt32(length).add(bodyBuffer).flush();
    }, requestSsl = () => {
      let response = Buffer.allocUnsafe(8);
      return response.writeInt32BE(8, 0), response.writeInt32BE(80877103, 4), response;
    }, password = (password2) => writer.addCString(password2).flush(
      112
      /* code.startup */
    ), sendSASLInitialResponseMessage = function(mechanism, initialResponse) {
      return writer.addCString(mechanism).addInt32PrefixedString(initialResponse), writer.flush(
        112
        /* code.startup */
      );
    }, sendSCRAMClientFinalMessage = function(additionalData) {
      return writer.addString(additionalData).flush(
        112
        /* code.startup */
      );
    }, query = (text) => writer.addCString(text).flush(
      81
      /* code.query */
    ), emptyArray = [], parse = (query2) => {
      let name = query2.name || "";
      name.length > 63 && (console.error("Warning! Postgres only supports 63 characters for query names."), console.error("You supplied %s (%s)", name, name.length), console.error("This can cause conflicts and silent errors executing queries"));
      let types3 = query2.types || emptyArray, len = types3.length, buffer = writer.addCString(name).addCString(query2.text).addInt16(len);
      for (let i = 0; i < len; i++)
        buffer.addInt32(types3[i]);
      return writer.flush(
        80
        /* code.parse */
      );
    }, writeValues = function(values, valueMapper, formatsOffset) {
      let len = values.length;
      for (let i = 0; i < len; i++) {
        let mappedVal = valueMapper ? valueMapper(values[i], i) : values[i], formatByte = 0;
        mappedVal == null ? writer.addInt32(-1) : mappedVal instanceof Buffer ? (formatByte = 1, writer.addInt32(mappedVal.length), writer.add(mappedVal)) : writer.addInt32PrefixedString(mappedVal);
        let buf = writer.buffer;
        buf[formatsOffset++] = 0, buf[formatsOffset++] = formatByte;
      }
    }, bind = (config = {}) => {
      let portal = config.portal || "", statement = config.statement || "", binary = config.binary || !1, values = config.values || emptyArray, len = values.length;
      writer.addCString(portal).addCString(statement), writer.addInt16(len);
      let formatsOffset = writer.reserveUnsafe(len * 2);
      writer.addInt16(len);
      try {
        writeValues(values, config.valueMapper, formatsOffset);
      } catch (err) {
        throw writer.clear(), err;
      }
      return writer.addInt16(1), writer.addInt16(
        binary ? 1 : 0
        /* ParamType.STRING */
      ), writer.flush(
        66
        /* code.bind */
      );
    }, emptyExecute = Buffer.from([69, 0, 0, 0, 9, 0, 0, 0, 0, 0]), execute = (config) => {
      if (!config || !config.portal && !config.rows)
        return emptyExecute;
      let portal = config.portal || "", rows = config.rows || 0, portalLength = Buffer.byteLength(portal), len = 4 + portalLength + 1 + 4, buff = Buffer.allocUnsafe(1 + len);
      return buff[0] = 69, buff.writeInt32BE(len, 1), buff.write(portal, 5, "utf-8"), buff[portalLength + 5] = 0, buff.writeUInt32BE(rows, buff.length - 4), buff;
    }, cancel = (processID, secretKey) => {
      let buffer = Buffer.allocUnsafe(16);
      return buffer.writeInt32BE(16, 0), buffer.writeInt16BE(1234, 4), buffer.writeInt16BE(5678, 6), buffer.writeInt32BE(processID, 8), buffer.writeInt32BE(secretKey, 12), buffer;
    }, cstringMessage = (code, string) => {
      let len = 4 + Buffer.byteLength(string) + 1, buffer = Buffer.allocUnsafe(1 + len);
      return buffer[0] = code, buffer.writeInt32BE(len, 1), buffer.write(string, 5, "utf-8"), buffer[len] = 0, buffer;
    }, emptyDescribePortal = writer.addCString("P").flush(
      68
      /* code.describe */
    ), emptyDescribeStatement = writer.addCString("S").flush(
      68
      /* code.describe */
    ), describe = (msg) => msg.name ? cstringMessage(68, `${msg.type}${msg.name || ""}`) : msg.type === "P" ? emptyDescribePortal : emptyDescribeStatement, close = (msg) => {
      let text = `${msg.type}${msg.name || ""}`;
      return cstringMessage(67, text);
    }, copyData = (chunk) => writer.add(chunk).flush(
      100
      /* code.copyFromChunk */
    ), copyFail = (message) => cstringMessage(102, message), codeOnlyBuffer = (code) => Buffer.from([code, 0, 0, 0, 4]), flushBuffer = codeOnlyBuffer(
      72
      /* code.flush */
    ), syncBuffer = codeOnlyBuffer(
      83
      /* code.sync */
    ), endBuffer = codeOnlyBuffer(
      88
      /* code.end */
    ), copyDoneBuffer = codeOnlyBuffer(
      99
      /* code.copyDone */
    ), serialize = {
      startup,
      password,
      requestSsl,
      sendSASLInitialResponseMessage,
      sendSCRAMClientFinalMessage,
      query,
      parse,
      bind,
      execute,
      describe,
      close,
      flush: () => flushBuffer,
      sync: () => syncBuffer,
      end: () => endBuffer,
      copyData,
      copyDone: () => copyDoneBuffer,
      copyFail,
      cancel
    };
    exports2.serialize = serialize;
  }
});

// node_modules/pg-protocol/dist/buffer-reader.js
var require_buffer_reader = __commonJS({
  "node_modules/pg-protocol/dist/buffer-reader.js"(exports2) {
    "use strict";
    Object.defineProperty(exports2, "__esModule", { value: !0 });
    exports2.BufferReader = void 0;
    var BufferReader = class {
      constructor(offset = 0) {
        this.offset = offset, this.buffer = Buffer.allocUnsafe(0), this.encoding = "utf-8";
      }
      setBuffer(offset, buffer) {
        this.offset = offset, this.buffer = buffer;
      }
      int16() {
        let result = this.buffer.readInt16BE(this.offset);
        return this.offset += 2, result;
      }
      byte() {
        let result = this.buffer[this.offset];
        return this.offset++, result;
      }
      int32() {
        let result = this.buffer.readInt32BE(this.offset);
        return this.offset += 4, result;
      }
      uint32() {
        let result = this.buffer.readUInt32BE(this.offset);
        return this.offset += 4, result;
      }
      string(length) {
        let result = this.buffer.toString(this.encoding, this.offset, this.offset + length);
        return this.offset += length, result;
      }
      cstring() {
        let start = this.offset, end = start;
        for (; this.buffer[end++]; )
          ;
        return this.offset = end, this.buffer.toString(this.encoding, start, end - 1);
      }
      bytes(length) {
        let result = this.buffer.slice(this.offset, this.offset + length);
        return this.offset += length, result;
      }
    };
    exports2.BufferReader = BufferReader;
  }
});

// node_modules/pg-protocol/dist/parser.js
var require_parser = __commonJS({
  "node_modules/pg-protocol/dist/parser.js"(exports2) {
    "use strict";
    Object.defineProperty(exports2, "__esModule", { value: !0 });
    exports2.Parser = void 0;
    var messages_1 = require_messages(), buffer_reader_1 = require_buffer_reader(), CODE_LENGTH = 1, LEN_LENGTH = 4, HEADER_LENGTH = CODE_LENGTH + LEN_LENGTH, LATEINIT_LENGTH = -1, emptyBuffer = Buffer.allocUnsafe(0), Parser = class {
      constructor(opts) {
        if (this.buffer = emptyBuffer, this.bufferLength = 0, this.bufferOffset = 0, this.reader = new buffer_reader_1.BufferReader(), opts?.mode === "binary")
          throw new Error("Binary mode not supported yet");
        this.mode = opts?.mode || "text";
      }
      parse(buffer, callback) {
        this.mergeBuffer(buffer);
        let bufferFullLength = this.bufferOffset + this.bufferLength, offset = this.bufferOffset;
        for (; offset + HEADER_LENGTH <= bufferFullLength; ) {
          let code = this.buffer[offset], length = this.buffer.readUInt32BE(offset + CODE_LENGTH), fullMessageLength = CODE_LENGTH + length;
          if (fullMessageLength + offset <= bufferFullLength) {
            let message = this.handlePacket(offset + HEADER_LENGTH, code, length, this.buffer);
            callback(message), offset += fullMessageLength;
          } else
            break;
        }
        offset === bufferFullLength ? (this.buffer = emptyBuffer, this.bufferLength = 0, this.bufferOffset = 0) : (this.bufferLength = bufferFullLength - offset, this.bufferOffset = offset);
      }
      mergeBuffer(buffer) {
        if (this.bufferLength > 0) {
          let newLength = this.bufferLength + buffer.byteLength;
          if (newLength + this.bufferOffset > this.buffer.byteLength) {
            let newBuffer;
            if (newLength <= this.buffer.byteLength && this.bufferOffset >= this.bufferLength)
              newBuffer = this.buffer;
            else {
              let newBufferLength = this.buffer.byteLength * 2;
              for (; newLength >= newBufferLength; )
                newBufferLength *= 2;
              newBuffer = Buffer.allocUnsafe(newBufferLength);
            }
            this.buffer.copy(newBuffer, 0, this.bufferOffset, this.bufferOffset + this.bufferLength), this.buffer = newBuffer, this.bufferOffset = 0;
          }
          buffer.copy(this.buffer, this.bufferOffset + this.bufferLength), this.bufferLength = newLength;
        } else
          this.buffer = buffer, this.bufferOffset = 0, this.bufferLength = buffer.byteLength;
      }
      handlePacket(offset, code, length, bytes) {
        let { reader } = this;
        reader.setBuffer(offset, bytes);
        let message;
        switch (code) {
          case 50:
            message = messages_1.bindComplete;
            break;
          case 49:
            message = messages_1.parseComplete;
            break;
          case 51:
            message = messages_1.closeComplete;
            break;
          case 110:
            message = messages_1.noData;
            break;
          case 115:
            message = messages_1.portalSuspended;
            break;
          case 99:
            message = messages_1.copyDone;
            break;
          case 87:
            message = messages_1.replicationStart;
            break;
          case 73:
            message = messages_1.emptyQuery;
            break;
          case 68:
            message = parseDataRowMessage(reader);
            break;
          case 67:
            message = parseCommandCompleteMessage(reader);
            break;
          case 90:
            message = parseReadyForQueryMessage(reader);
            break;
          case 65:
            message = parseNotificationMessage(reader);
            break;
          case 82:
            message = parseAuthenticationResponse(reader, length);
            break;
          case 83:
            message = parseParameterStatusMessage(reader);
            break;
          case 75:
            message = parseBackendKeyData(reader);
            break;
          case 69:
            message = parseErrorMessage(reader, "error");
            break;
          case 78:
            message = parseErrorMessage(reader, "notice");
            break;
          case 84:
            message = parseRowDescriptionMessage(reader);
            break;
          case 116:
            message = parseParameterDescriptionMessage(reader);
            break;
          case 71:
            message = parseCopyInMessage(reader);
            break;
          case 72:
            message = parseCopyOutMessage(reader);
            break;
          case 100:
            message = parseCopyData(reader, length);
            break;
          default:
            return new messages_1.DatabaseError("received invalid response: " + code.toString(16), length, "error");
        }
        return reader.setBuffer(0, emptyBuffer), message.length = length, message;
      }
    };
    exports2.Parser = Parser;
    var parseReadyForQueryMessage = (reader) => {
      let status = reader.string(1);
      return new messages_1.ReadyForQueryMessage(LATEINIT_LENGTH, status);
    }, parseCommandCompleteMessage = (reader) => {
      let text = reader.cstring();
      return new messages_1.CommandCompleteMessage(LATEINIT_LENGTH, text);
    }, parseCopyData = (reader, length) => {
      let chunk = reader.bytes(length - 4);
      return new messages_1.CopyDataMessage(LATEINIT_LENGTH, chunk);
    }, parseCopyInMessage = (reader) => parseCopyMessage(reader, "copyInResponse"), parseCopyOutMessage = (reader) => parseCopyMessage(reader, "copyOutResponse"), parseCopyMessage = (reader, messageName) => {
      let isBinary = reader.byte() !== 0, columnCount = reader.int16(), message = new messages_1.CopyResponse(LATEINIT_LENGTH, messageName, isBinary, columnCount);
      for (let i = 0; i < columnCount; i++)
        message.columnTypes[i] = reader.int16();
      return message;
    }, parseNotificationMessage = (reader) => {
      let processId = reader.int32(), channel = reader.cstring(), payload = reader.cstring();
      return new messages_1.NotificationResponseMessage(LATEINIT_LENGTH, processId, channel, payload);
    }, parseRowDescriptionMessage = (reader) => {
      let fieldCount = reader.int16(), message = new messages_1.RowDescriptionMessage(LATEINIT_LENGTH, fieldCount);
      for (let i = 0; i < fieldCount; i++)
        message.fields[i] = parseField(reader);
      return message;
    }, parseField = (reader) => {
      let name = reader.cstring(), tableID = reader.uint32(), columnID = reader.int16(), dataTypeID = reader.uint32(), dataTypeSize = reader.int16(), dataTypeModifier = reader.int32(), mode = reader.int16() === 0 ? "text" : "binary";
      return new messages_1.Field(name, tableID, columnID, dataTypeID, dataTypeSize, dataTypeModifier, mode);
    }, parseParameterDescriptionMessage = (reader) => {
      let parameterCount = reader.int16(), message = new messages_1.ParameterDescriptionMessage(LATEINIT_LENGTH, parameterCount);
      for (let i = 0; i < parameterCount; i++)
        message.dataTypeIDs[i] = reader.uint32();
      return message;
    }, parseDataRowMessage = (reader) => {
      let fieldCount = reader.int16(), fields = new Array(fieldCount);
      for (let i = 0; i < fieldCount; i++) {
        let len = reader.int32();
        fields[i] = len === -1 ? null : reader.string(len);
      }
      return new messages_1.DataRowMessage(LATEINIT_LENGTH, fields);
    }, parseParameterStatusMessage = (reader) => {
      let name = reader.cstring(), value = reader.cstring();
      return new messages_1.ParameterStatusMessage(LATEINIT_LENGTH, name, value);
    }, parseBackendKeyData = (reader) => {
      let processID = reader.int32(), secretKey = reader.int32();
      return new messages_1.BackendKeyDataMessage(LATEINIT_LENGTH, processID, secretKey);
    }, parseAuthenticationResponse = (reader, length) => {
      let code = reader.int32(), message = {
        name: "authenticationOk",
        length
      };
      switch (code) {
        case 0:
          break;
        case 3:
          message.length === 8 && (message.name = "authenticationCleartextPassword");
          break;
        case 5:
          if (message.length === 12) {
            message.name = "authenticationMD5Password";
            let salt = reader.bytes(4);
            return new messages_1.AuthenticationMD5Password(LATEINIT_LENGTH, salt);
          }
          break;
        case 10:
          {
            message.name = "authenticationSASL", message.mechanisms = [];
            let mechanism;
            do
              mechanism = reader.cstring(), mechanism && message.mechanisms.push(mechanism);
            while (mechanism);
          }
          break;
        case 11:
          message.name = "authenticationSASLContinue", message.data = reader.string(length - 8);
          break;
        case 12:
          message.name = "authenticationSASLFinal", message.data = reader.string(length - 8);
          break;
        default:
          throw new Error("Unknown authenticationOk message type " + code);
      }
      return message;
    }, parseErrorMessage = (reader, name) => {
      let fields = {}, fieldType = reader.string(1);
      for (; fieldType !== "\0"; )
        fields[fieldType] = reader.cstring(), fieldType = reader.string(1);
      let messageValue = fields.M, message = name === "notice" ? new messages_1.NoticeMessage(LATEINIT_LENGTH, messageValue) : new messages_1.DatabaseError(messageValue, LATEINIT_LENGTH, name);
      return message.severity = fields.S, message.code = fields.C, message.detail = fields.D, message.hint = fields.H, message.position = fields.P, message.internalPosition = fields.p, message.internalQuery = fields.q, message.where = fields.W, message.schema = fields.s, message.table = fields.t, message.column = fields.c, message.dataType = fields.d, message.constraint = fields.n, message.file = fields.F, message.line = fields.L, message.routine = fields.R, message;
    };
  }
});

// node_modules/pg-protocol/dist/index.js
var require_dist = __commonJS({
  "node_modules/pg-protocol/dist/index.js"(exports2) {
    "use strict";
    Object.defineProperty(exports2, "__esModule", { value: !0 });
    exports2.DatabaseError = exports2.serialize = void 0;
    exports2.parse = parse;
    var messages_1 = require_messages();
    Object.defineProperty(exports2, "DatabaseError", { enumerable: !0, get: function() {
      return messages_1.DatabaseError;
    } });
    var serializer_1 = require_serializer();
    Object.defineProperty(exports2, "serialize", { enumerable: !0, get: function() {
      return serializer_1.serialize;
    } });
    var parser_1 = require_parser();
    function parse(stream, callback) {
      let parser = new parser_1.Parser();
      return stream.on("data", (buffer) => parser.parse(buffer, callback)), new Promise((resolve) => stream.on("end", () => resolve()));
    }
  }
});

// node_modules/pg-cloudflare/dist/empty.js
var require_empty = __commonJS({
  "node_modules/pg-cloudflare/dist/empty.js"(exports2) {
    "use strict";
    Object.defineProperty(exports2, "__esModule", { value: !0 });
    exports2.default = {};
  }
});

// node_modules/pg/lib/stream.js
var require_stream2 = __commonJS({
  "node_modules/pg/lib/stream.js"(exports2, module2) {
    var { getStream, getSecureStream } = getStreamFuncs();
    module2.exports = {
      /**
       * Get a socket stream compatible with the current runtime environment.
       * @returns {Duplex}
       */
      getStream,
      /**
       * Get a TLS secured socket, compatible with the current environment,
       * using the socket and other settings given in `options`.
       * @returns {Duplex}
       */
      getSecureStream
    };
    function getNodejsStreamFuncs() {
      function getStream2(ssl) {
        let net = require("net");
        return new net.Socket();
      }
      function getSecureStream2(options) {
        return require("tls").connect(options);
      }
      return {
        getStream: getStream2,
        getSecureStream: getSecureStream2
      };
    }
    function getCloudflareStreamFuncs() {
      function getStream2(ssl) {
        let { CloudflareSocket } = require_empty();
        return new CloudflareSocket(ssl);
      }
      function getSecureStream2(options) {
        return options.socket.startTls(options), options.socket;
      }
      return {
        getStream: getStream2,
        getSecureStream: getSecureStream2
      };
    }
    function isCloudflareRuntime() {
      if (typeof navigator == "object" && navigator !== null && typeof navigator.userAgent == "string")
        return navigator.userAgent === "Cloudflare-Workers";
      if (typeof Response == "function") {
        let resp = new Response(null, { cf: { thing: !0 } });
        if (typeof resp.cf == "object" && resp.cf !== null && resp.cf.thing)
          return !0;
      }
      return !1;
    }
    function getStreamFuncs() {
      return isCloudflareRuntime() ? getCloudflareStreamFuncs() : getNodejsStreamFuncs();
    }
  }
});

// node_modules/pg/lib/connection.js
var require_connection = __commonJS({
  "node_modules/pg/lib/connection.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events").EventEmitter, { parse, serialize } = require_dist(), stream = require_stream2(), { getStream } = stream, flushBuffer = serialize.flush(), syncBuffer = serialize.sync(), endBuffer = serialize.end(), Connection2 = class extends EventEmitter {
      constructor(config) {
        super(), config = config || {}, this.stream = config.stream || getStream(config.ssl), typeof this.stream == "function" && (this.stream = this.stream(config)), this._keepAlive = config.keepAlive, this._keepAliveInitialDelayMillis = config.keepAliveInitialDelayMillis, this.parsedStatements = {}, this.submittedNamedStatements = {}, this.ssl = config.ssl || !1, this.sslNegotiation = config.sslNegotiation || "postgres", this._ending = !1, this._emitMessage = !1;
        let self = this;
        this.on("newListener", function(eventName) {
          eventName === "message" && (self._emitMessage = !0);
        });
      }
      connect(port, host) {
        let self = this;
        this._connecting = !0, this.stream.setNoDelay(!0), this.stream.connect(port, host), this.stream.once("connect", function() {
          self._keepAlive && self.stream.setKeepAlive(!0, self._keepAliveInitialDelayMillis), self.emit("connect");
        });
        let reportStreamError = function(error) {
          self._ending && (error.code === "ECONNRESET" || error.code === "EPIPE") || self.emit("error", error);
        };
        if (this.stream.on("error", reportStreamError), this.stream.on("close", function() {
          self.emit("end");
        }), !this.ssl)
          return this.attachListeners(this.stream);
        if (this.sslNegotiation === "direct")
          return this.stream.once("connect", function() {
            self.upgradeToSSL(host, reportStreamError);
          });
        this.stream.once("data", function(buffer) {
          switch (buffer.toString("utf8")) {
            case "S":
              break;
            case "N":
              return self.stream.end(), self.emit("error", new Error("The server does not support SSL connections"));
            default:
              return self.stream.end(), self.emit("error", new Error("There was an error establishing an SSL connection"));
          }
          self.upgradeToSSL(host, reportStreamError);
        });
      }
      upgradeToSSL(host, reportStreamError) {
        let self = this, options = {
          socket: self.stream,
          // tls.connect checks the server identity against `servername`, falling
          // back to `host` and then to 'localhost'. `servername` must stay unset
          // for IP addresses (see below), so `host` is needed to keep certificate
          // validation working when connecting to an IP address.
          host
        };
        self.ssl !== !0 && (Object.assign(options, self.ssl), "key" in self.ssl && (options.key = self.ssl.key)), self.sslNegotiation === "direct" && (options.ALPNProtocols = ["postgresql"]);
        let net = require("net");
        net.isIP && net.isIP(host) === 0 && (options.servername = host);
        try {
          self.stream = stream.getSecureStream(options);
        } catch (err) {
          return self.emit("error", err);
        }
        self.attachListeners(self.stream), self.stream.on("error", reportStreamError), self.emit("sslconnect");
      }
      attachListeners(stream2) {
        parse(stream2, (msg) => {
          let eventName = msg.name === "error" ? "errorMessage" : msg.name;
          this._emitMessage && this.emit("message", msg), this.emit(eventName, msg);
        });
      }
      requestSsl() {
        this.stream.write(serialize.requestSsl());
      }
      startup(config) {
        this.stream.write(serialize.startup(config));
      }
      cancel(processID, secretKey) {
        this._send(serialize.cancel(processID, secretKey));
      }
      password(password) {
        this._send(serialize.password(password));
      }
      sendSASLInitialResponseMessage(mechanism, initialResponse) {
        this._send(serialize.sendSASLInitialResponseMessage(mechanism, initialResponse));
      }
      sendSCRAMClientFinalMessage(additionalData) {
        this._send(serialize.sendSCRAMClientFinalMessage(additionalData));
      }
      _send(buffer) {
        return this.stream.writable ? this.stream.write(buffer) : !1;
      }
      query(text) {
        this._send(serialize.query(text));
      }
      // send parse message
      parse(query) {
        this._send(serialize.parse(query));
      }
      // send bind message
      bind(config) {
        this._send(serialize.bind(config));
      }
      // send execute message
      execute(config) {
        this._send(serialize.execute(config));
      }
      flush() {
        this.stream.writable && this.stream.write(flushBuffer);
      }
      sync() {
        this._send(syncBuffer);
      }
      ref() {
        this.stream.ref();
      }
      unref() {
        this.stream.unref();
      }
      end() {
        if (this._ending = !0, !this._connecting || !this.stream.writable) {
          this.stream.end();
          return;
        }
        return this.stream.write(endBuffer, () => {
          this.stream.end();
        });
      }
      close(msg) {
        this._send(serialize.close(msg));
      }
      describe(msg) {
        this._send(serialize.describe(msg));
      }
      sendCopyFromChunk(chunk) {
        this._send(serialize.copyData(chunk));
      }
      endCopyFrom() {
        this._send(serialize.copyDone());
      }
      sendCopyFail(msg) {
        this._send(serialize.copyFail(msg));
      }
    };
    module2.exports = Connection2;
  }
});

// node_modules/split2/index.js
var require_split2 = __commonJS({
  "node_modules/split2/index.js"(exports2, module2) {
    "use strict";
    var { Transform } = require("stream"), { StringDecoder } = require("string_decoder"), kLast = /* @__PURE__ */ Symbol("last"), kDecoder = /* @__PURE__ */ Symbol("decoder");
    function transform(chunk, enc, cb) {
      let list;
      if (this.overflow) {
        if (list = this[kDecoder].write(chunk).split(this.matcher), list.length === 1) return cb();
        list.shift(), this.overflow = !1;
      } else
        this[kLast] += this[kDecoder].write(chunk), list = this[kLast].split(this.matcher);
      this[kLast] = list.pop();
      for (let i = 0; i < list.length; i++)
        try {
          push(this, this.mapper(list[i]));
        } catch (error) {
          return cb(error);
        }
      if (this.overflow = this[kLast].length > this.maxLength, this.overflow && !this.skipOverflow) {
        cb(new Error("maximum buffer reached"));
        return;
      }
      cb();
    }
    function flush(cb) {
      if (this[kLast] += this[kDecoder].end(), this[kLast])
        try {
          push(this, this.mapper(this[kLast]));
        } catch (error) {
          return cb(error);
        }
      cb();
    }
    function push(self, val) {
      val !== void 0 && self.push(val);
    }
    function noop(incoming) {
      return incoming;
    }
    function split(matcher, mapper, options) {
      switch (matcher = matcher || /\r?\n/, mapper = mapper || noop, options = options || {}, arguments.length) {
        case 1:
          typeof matcher == "function" ? (mapper = matcher, matcher = /\r?\n/) : typeof matcher == "object" && !(matcher instanceof RegExp) && !matcher[Symbol.split] && (options = matcher, matcher = /\r?\n/);
          break;
        case 2:
          typeof matcher == "function" ? (options = mapper, mapper = matcher, matcher = /\r?\n/) : typeof mapper == "object" && (options = mapper, mapper = noop);
      }
      options = Object.assign({}, options), options.autoDestroy = !0, options.transform = transform, options.flush = flush, options.readableObjectMode = !0;
      let stream = new Transform(options);
      return stream[kLast] = "", stream[kDecoder] = new StringDecoder("utf8"), stream.matcher = matcher, stream.mapper = mapper, stream.maxLength = options.maxLength, stream.skipOverflow = options.skipOverflow || !1, stream.overflow = !1, stream._destroy = function(err, cb) {
        this._writableState.errorEmitted = !1, cb(err);
      }, stream;
    }
    module2.exports = split;
  }
});

// node_modules/pgpass/lib/helper.js
var require_helper = __commonJS({
  "node_modules/pgpass/lib/helper.js"(exports2, module2) {
    "use strict";
    var path2 = require("path"), Stream = require("stream").Stream, split = require_split2(), util = require("util"), defaultPort = 5432, isWin = process.platform === "win32", warnStream = process.stderr, S_IRWXG = 56, S_IRWXO = 7, S_IFMT = 61440, S_IFREG = 32768;
    function isRegFile(mode) {
      return (mode & S_IFMT) == S_IFREG;
    }
    var fieldNames = ["host", "port", "database", "user", "password"], nrOfFields = fieldNames.length, passKey = fieldNames[nrOfFields - 1];
    function warn() {
      var isWritable = warnStream instanceof Stream && warnStream.writable === !0;
      if (isWritable) {
        var args = Array.prototype.slice.call(arguments).concat(`
`);
        warnStream.write(util.format.apply(util, args));
      }
    }
    Object.defineProperty(module2.exports, "isWin", {
      get: function() {
        return isWin;
      },
      set: function(val) {
        isWin = val;
      }
    });
    module2.exports.warnTo = function(stream) {
      var old = warnStream;
      return warnStream = stream, old;
    };
    module2.exports.getFileName = function(rawEnv) {
      var env = rawEnv || process.env, file = env.PGPASSFILE || (isWin ? path2.join(env.APPDATA || "./", "postgresql", "pgpass.conf") : path2.join(env.HOME || "./", ".pgpass"));
      return file;
    };
    module2.exports.usePgPass = function(stats, fname) {
      return Object.prototype.hasOwnProperty.call(process.env, "PGPASSWORD") ? !1 : isWin ? !0 : (fname = fname || "<unkn>", isRegFile(stats.mode) ? stats.mode & (S_IRWXG | S_IRWXO) ? (warn('WARNING: password file "%s" has group or world access; permissions should be u=rw (0600) or less', fname), !1) : !0 : (warn('WARNING: password file "%s" is not a plain file', fname), !1));
    };
    var matcher = module2.exports.match = function(connInfo, entry) {
      return fieldNames.slice(0, -1).reduce(function(prev, field, idx) {
        return idx == 1 && Number(connInfo[field] || defaultPort) === Number(entry[field]) ? prev && !0 : prev && (entry[field] === "*" || entry[field] === connInfo[field]);
      }, !0);
    };
    module2.exports.getPassword = function(connInfo, stream, cb) {
      var pass, lineStream = stream.pipe(split());
      function onLine(line) {
        var entry = parseLine(line);
        entry && isValidEntry(entry) && matcher(connInfo, entry) && (pass = entry[passKey], lineStream.end());
      }
      var onEnd = function() {
        stream.destroy(), cb(pass);
      }, onErr = function(err) {
        stream.destroy(), warn("WARNING: error on reading file: %s", err), cb(void 0);
      };
      stream.on("error", onErr), lineStream.on("data", onLine).on("end", onEnd).on("error", onErr);
    };
    var parseLine = module2.exports.parseLine = function(line) {
      if (line.length < 11 || line.match(/^\s+#/))
        return null;
      for (var curChar = "", prevChar = "", fieldIdx = 0, startIdx = 0, endIdx = 0, obj = {}, isLastField = !1, addToObj = function(idx, i0, i1) {
        var field = line.substring(i0, i1);
        Object.hasOwnProperty.call(process.env, "PGPASS_NO_DEESCAPE") || (field = field.replace(/\\([:\\])/g, "$1")), obj[fieldNames[idx]] = field;
      }, i = 0; i < line.length - 1; i += 1) {
        if (curChar = line.charAt(i + 1), prevChar = line.charAt(i), isLastField = fieldIdx == nrOfFields - 1, isLastField) {
          addToObj(fieldIdx, startIdx);
          break;
        }
        i >= 0 && curChar == ":" && prevChar !== "\\" && (addToObj(fieldIdx, startIdx, i + 1), startIdx = i + 2, fieldIdx += 1);
      }
      return obj = Object.keys(obj).length === nrOfFields ? obj : null, obj;
    }, isValidEntry = module2.exports.isValidEntry = function(entry) {
      for (var rules = {
        // host
        0: function(x) {
          return x.length > 0;
        },
        // port
        1: function(x) {
          return x === "*" ? !0 : (x = Number(x), isFinite(x) && x > 0 && x < 9007199254740992 && Math.floor(x) === x);
        },
        // database
        2: function(x) {
          return x.length > 0;
        },
        // username
        3: function(x) {
          return x.length > 0;
        },
        // password
        4: function(x) {
          return x.length > 0;
        }
      }, idx = 0; idx < fieldNames.length; idx += 1) {
        var rule = rules[idx], value = entry[fieldNames[idx]] || "", res = rule(value);
        if (!res)
          return !1;
      }
      return !0;
    };
  }
});

// node_modules/pgpass/lib/index.js
var require_lib = __commonJS({
  "node_modules/pgpass/lib/index.js"(exports2, module2) {
    "use strict";
    var path2 = require("path"), fs2 = require("fs"), helper = require_helper();
    module2.exports = function(connInfo, cb) {
      var file = helper.getFileName();
      fs2.stat(file, function(err, stat) {
        if (err || !helper.usePgPass(stat, file))
          return cb(void 0);
        var st = fs2.createReadStream(file);
        helper.getPassword(connInfo, st, cb);
      });
    };
    module2.exports.warnTo = helper.warnTo;
  }
});

// node_modules/pg/lib/client.js
var require_client = __commonJS({
  "node_modules/pg/lib/client.js"(exports2, module2) {
    var EventEmitter = require("events").EventEmitter, utils = require_utils(), nodeUtils = require("util"), sasl = require_sasl(), TypeOverrides2 = require_type_overrides(), ConnectionParameters = require_connection_parameters(), Query2 = require_query(), defaults2 = require_defaults(), Connection2 = require_connection(), crypto4 = require_utils2(), activeQueryDeprecationNotice = nodeUtils.deprecate(
      () => {
      },
      "Client.activeQuery is deprecated and will be removed in pg@9.0"
    ), queryQueueDeprecationNotice = nodeUtils.deprecate(
      () => {
      },
      "Client.queryQueue is deprecated and will be removed in pg@9.0."
    ), pgPassDeprecationNotice = nodeUtils.deprecate(
      () => {
      },
      "pgpass support is deprecated and will be removed in pg@9.0. You can provide an async function as the password property to the Client/Pool constructor that returns a password instead. Within this function you can call the pgpass module in your own code."
    ), byoPromiseDeprecationNotice = nodeUtils.deprecate(
      () => {
      },
      "Passing a custom Promise implementation to the Client/Pool constructor is deprecated and will be removed in pg@9.0."
    ), queryQueueLengthDeprecationNotice = nodeUtils.deprecate(
      () => {
      },
      "Calling client.query() when the client is already executing a query is deprecated and will be removed in pg@9.0. Use async/await or an external async flow control mechanism instead."
    );
    function coerceNumberOrDefault(value, defaultValue) {
      if (typeof value == "number")
        return Number.isFinite(value) ? value : defaultValue;
      if (typeof value == "string" && value.trim() !== "") {
        let n = Number(value);
        return Number.isFinite(n) ? n : defaultValue;
      }
      return defaultValue;
    }
    var Client2 = class extends EventEmitter {
      constructor(config) {
        super(), this.connectionParameters = new ConnectionParameters(config), this.user = this.connectionParameters.user, this.database = this.connectionParameters.database, this.port = this.connectionParameters.port, this.host = this.connectionParameters.host, Object.defineProperty(this, "password", {
          configurable: !0,
          enumerable: !1,
          writable: !0,
          value: this.connectionParameters.password
        }), this.replication = this.connectionParameters.replication;
        let c = config || {};
        c.Promise && byoPromiseDeprecationNotice(), this._Promise = c.Promise || global.Promise, this._types = new TypeOverrides2(c.types), this._ending = !1, this._ended = !1, this._connecting = !1, this._connected = !1, this._connectionError = !1, this._queryable = !0, this._activeQuery = null, this._txStatus = null, this.enableChannelBinding = !!c.enableChannelBinding, this.scramMaxIterations = coerceNumberOrDefault(c.scramMaxIterations, sasl.DEFAULT_MAX_SCRAM_ITERATIONS), this.connection = c.connection || new Connection2({
          stream: c.stream,
          ssl: this.connectionParameters.ssl,
          sslNegotiation: this.connectionParameters.sslnegotiation,
          keepAlive: c.keepAlive || !1,
          keepAliveInitialDelayMillis: c.keepAliveInitialDelayMillis || 0,
          encoding: this.connectionParameters.client_encoding || "utf8"
        }), this._queryQueue = [], this._sentQueryQueue = [], this.pipeline = !!c.pipeline, this.binary = c.binary || defaults2.binary, this.processID = null, this.secretKey = null, this.ssl = this.connectionParameters.ssl || !1, this.sslNegotiation = this.connectionParameters.sslnegotiation || "postgres", this.ssl && this.ssl.key && Object.defineProperty(this.ssl, "key", {
          enumerable: !1
        }), this._connectionTimeoutMillis = c.connectionTimeoutMillis || 0;
      }
      get activeQuery() {
        return activeQueryDeprecationNotice(), this._activeQuery;
      }
      set activeQuery(val) {
        activeQueryDeprecationNotice(), this._activeQuery = val;
      }
      _getActiveQuery() {
        return this._activeQuery;
      }
      _errorAllQueries(err) {
        let enqueueError = (query) => {
          process.nextTick(() => {
            query.handleError(err, this.connection);
          });
        }, activeQuery = this._getActiveQuery();
        activeQuery && (enqueueError(activeQuery), this._activeQuery = null), this._sentQueryQueue.forEach(enqueueError), this._sentQueryQueue.length = 0, this._queryQueue.forEach(enqueueError), this._queryQueue.length = 0;
      }
      _connect(callback) {
        let self = this, con = this.connection;
        if (this._connectionCallback = callback, this._connecting || this._connected) {
          let err = new Error("Client has already been connected. You cannot reuse a client.");
          process.nextTick(() => {
            callback(err);
          });
          return;
        }
        this._connecting = !0, this._connectionTimeoutMillis > 0 && (this.connectionTimeoutHandle = setTimeout(() => {
          con._ending = !0, con.stream.destroy(new Error("timeout expired"));
        }, this._connectionTimeoutMillis), this.connectionTimeoutHandle.unref && this.connectionTimeoutHandle.unref()), this.host && this.host.indexOf("/") === 0 ? con.connect(this.host + "/.s.PGSQL." + this.port) : con.connect(this.port, this.host), con.on("connect", function() {
          self.ssl ? self.sslNegotiation !== "direct" && con.requestSsl() : con.startup(self.getStartupConf());
        }), con.on("sslconnect", function() {
          con.startup(self.getStartupConf());
        }), this._attachListeners(con), con.once("end", () => {
          let error = this._ending ? new Error("Connection terminated") : new Error("Connection terminated unexpectedly");
          clearTimeout(this.connectionTimeoutHandle), this._errorAllQueries(error), this._ended = !0, this._ending || (this._connecting && !this._connectionError ? this._connectionCallback ? this._connectionCallback(error) : this._handleErrorEvent(error) : this._connectionError || this._handleErrorEvent(error)), process.nextTick(() => {
            this.emit("end");
          });
        });
      }
      connect(callback) {
        if (callback) {
          this._connect(callback);
          return;
        }
        return new this._Promise((resolve, reject) => {
          this._connect((error) => {
            error ? reject(error) : resolve(this);
          });
        });
      }
      _attachListeners(con) {
        con.on("authenticationCleartextPassword", this._handleAuthCleartextPassword.bind(this)), con.on("authenticationMD5Password", this._handleAuthMD5Password.bind(this)), con.on("authenticationSASL", this._handleAuthSASL.bind(this)), con.on("authenticationSASLContinue", this._handleAuthSASLContinue.bind(this)), con.on("authenticationSASLFinal", this._handleAuthSASLFinal.bind(this)), con.on("backendKeyData", this._handleBackendKeyData.bind(this)), con.on("error", this._handleErrorEvent.bind(this)), con.on("errorMessage", this._handleErrorMessage.bind(this)), con.on("readyForQuery", this._handleReadyForQuery.bind(this)), con.on("notice", this._handleNotice.bind(this)), con.on("rowDescription", this._handleRowDescription.bind(this)), con.on("dataRow", this._handleDataRow.bind(this)), con.on("portalSuspended", this._handlePortalSuspended.bind(this)), con.on("emptyQuery", this._handleEmptyQuery.bind(this)), con.on("commandComplete", this._handleCommandComplete.bind(this)), con.on("parseComplete", this._handleParseComplete.bind(this)), con.on("copyInResponse", this._handleCopyInResponse.bind(this)), con.on("copyData", this._handleCopyData.bind(this)), con.on("notification", this._handleNotification.bind(this));
      }
      _getPassword(cb) {
        let con = this.connection;
        if (typeof this.password == "function")
          this._Promise.resolve().then(() => this.password(this.connectionParameters)).then((pass) => {
            if (pass !== void 0) {
              if (typeof pass != "string") {
                con.emit("error", new TypeError("Password must be a string"));
                return;
              }
              this.connectionParameters.password = this.password = pass;
            } else
              this.connectionParameters.password = this.password = null;
            cb();
          }).catch((err) => {
            con.emit("error", err);
          });
        else if (this.password !== null)
          cb();
        else
          try {
            require_lib()(this.connectionParameters, (pass) => {
              pass !== void 0 && (pgPassDeprecationNotice(), this.connectionParameters.password = this.password = pass), cb();
            });
          } catch (e) {
            this.emit("error", e);
          }
      }
      _handleAuthCleartextPassword(msg) {
        this._getPassword(() => {
          this.connection.password(this.password);
        });
      }
      _handleAuthMD5Password(msg) {
        this._getPassword(async () => {
          try {
            let hashedPassword = await crypto4.postgresMd5PasswordHash(this.user, this.password, msg.salt);
            this.connection.password(hashedPassword);
          } catch (e) {
            this.emit("error", e);
          }
        });
      }
      _handleAuthSASL(msg) {
        this._getPassword(() => {
          try {
            this.saslSession = sasl.startSession(
              msg.mechanisms,
              this.enableChannelBinding && this.connection.stream,
              this.scramMaxIterations
            ), this.connection.sendSASLInitialResponseMessage(this.saslSession.mechanism, this.saslSession.response);
          } catch (err) {
            this.connection.emit("error", err);
          }
        });
      }
      async _handleAuthSASLContinue(msg) {
        try {
          await sasl.continueSession(
            this.saslSession,
            this.password,
            msg.data,
            this.enableChannelBinding && this.connection.stream
          ), this.connection.sendSCRAMClientFinalMessage(this.saslSession.response);
        } catch (err) {
          this.connection.emit("error", err);
        }
      }
      _handleAuthSASLFinal(msg) {
        try {
          sasl.finalizeSession(this.saslSession, msg.data), this.saslSession = null;
        } catch (err) {
          this.connection.emit("error", err);
        }
      }
      _handleBackendKeyData(msg) {
        this.processID = msg.processID, this.secretKey = msg.secretKey;
      }
      _handleReadyForQuery(msg) {
        this._connecting && (this._connecting = !1, this._connected = !0, clearTimeout(this.connectionTimeoutHandle), this._connectionCallback && (this._connectionCallback(null, this), this._connectionCallback = null), this.emit("connect"));
        let activeQuery = this._getActiveQuery();
        this._activeQuery = null, this._txStatus = msg?.status ?? null, this.readyForQuery = !0, activeQuery && activeQuery.handleReadyForQuery(this.connection), this._pulseQueryQueue();
      }
      // if we receive an error event or error message
      // during the connection process we handle it here
      _handleErrorWhileConnecting(err) {
        if (!this._connectionError) {
          if (this._connectionError = !0, clearTimeout(this.connectionTimeoutHandle), this._connectionCallback)
            return this._connectionCallback(err);
          this.emit("error", err);
        }
      }
      // if we're connected and we receive an error event from the connection
      // this means the socket is dead - do a hard abort of all queries and emit
      // the socket error on the client as well
      _handleErrorEvent(err) {
        if (this._connecting)
          return this._handleErrorWhileConnecting(err);
        this._queryable = !1, this._errorAllQueries(err), this.emit("error", err);
      }
      // handle error messages from the postgres backend
      _handleErrorMessage(msg) {
        if (this._connecting)
          return this._handleErrorWhileConnecting(msg);
        let activeQuery = this._getActiveQuery();
        if (!activeQuery) {
          this._handleErrorEvent(msg);
          return;
        }
        this._activeQuery = null, activeQuery.name && delete this.connection.submittedNamedStatements[activeQuery.name], activeQuery.handleError(msg, this.connection);
      }
      _handleRowDescription(msg) {
        let activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          let error = new Error("Received unexpected rowDescription message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.handleRowDescription(msg);
      }
      _handleDataRow(msg) {
        let activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          let error = new Error("Received unexpected dataRow message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.handleDataRow(msg);
      }
      _handlePortalSuspended(msg) {
        let activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          let error = new Error("Received unexpected portalSuspended message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.handlePortalSuspended(this.connection);
      }
      _handleEmptyQuery(msg) {
        let activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          let error = new Error("Received unexpected emptyQuery message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.handleEmptyQuery(this.connection);
      }
      _handleCommandComplete(msg) {
        let activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          let error = new Error("Received unexpected commandComplete message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.handleCommandComplete(msg, this.connection);
      }
      _handleParseComplete() {
        let activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          let error = new Error("Received unexpected parseComplete message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.name && (this.connection.parsedStatements[activeQuery.name] = activeQuery.text, delete this.connection.submittedNamedStatements[activeQuery.name]);
      }
      _handleCopyInResponse(msg) {
        let activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          let error = new Error("Received unexpected copyInResponse message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.handleCopyInResponse(this.connection);
      }
      _handleCopyData(msg) {
        let activeQuery = this._getActiveQuery();
        if (activeQuery == null) {
          let error = new Error("Received unexpected copyData message from backend.");
          this._handleErrorEvent(error);
          return;
        }
        activeQuery.handleCopyData(msg, this.connection);
      }
      _handleNotification(msg) {
        this.emit("notification", msg);
      }
      _handleNotice(msg) {
        this.emit("notice", msg);
      }
      getStartupConf() {
        let params = this.connectionParameters, data = {
          user: params.user,
          database: params.database
        }, appName = params.application_name || params.fallback_application_name;
        return appName && (data.application_name = appName), params.replication && (data.replication = "" + params.replication), params.statement_timeout && (data.statement_timeout = String(parseInt(params.statement_timeout, 10))), params.lock_timeout && (data.lock_timeout = String(parseInt(params.lock_timeout, 10))), params.idle_in_transaction_session_timeout && (data.idle_in_transaction_session_timeout = String(parseInt(params.idle_in_transaction_session_timeout, 10))), params.options && (data.options = params.options), data;
      }
      cancel(client, query) {
        if (client.activeQuery === query) {
          let con = this.connection;
          this.host && this.host.indexOf("/") === 0 ? con.connect(this.host + "/.s.PGSQL." + this.port) : con.connect(this.port, this.host), con.on("connect", function() {
            con.cancel(client.processID, client.secretKey);
          });
        } else client._queryQueue.indexOf(query) !== -1 ? client._queryQueue.splice(client._queryQueue.indexOf(query), 1) : client._sentQueryQueue.indexOf(query) !== -1 && (query.callback = () => {
        });
      }
      setTypeParser(oid, format, parseFn) {
        return this._types.setTypeParser(oid, format, parseFn);
      }
      getTypeParser(oid, format) {
        return this._types.getTypeParser(oid, format);
      }
      // escapeIdentifier and escapeLiteral moved to utility functions & exported
      // on PG
      // re-exported here for backwards compatibility
      escapeIdentifier(str2) {
        return utils.escapeIdentifier(str2);
      }
      escapeLiteral(str2) {
        return utils.escapeLiteral(str2);
      }
      _pulseQueryQueue() {
        if (this.pipeline) {
          this._pulsePipelinedQueryQueue();
          return;
        }
        if (this.readyForQuery === !0) {
          this._activeQuery = this._queryQueue.shift();
          let activeQuery = this._getActiveQuery();
          if (activeQuery) {
            this.readyForQuery = !1, this.hasExecuted = !0;
            let queryError = activeQuery.submit(this.connection);
            queryError && process.nextTick(() => {
              activeQuery.handleError(queryError, this.connection), this.readyForQuery = !0, this._pulseQueryQueue();
            });
          } else this.hasExecuted && (this._activeQuery = null, this.emit("drain"));
        }
      }
      _pulsePipelinedQueryQueue() {
        if (!(!this._connected || !this._queryable)) {
          for (; this._queryQueue.length > 0; ) {
            let query = this._queryQueue.shift();
            this.hasExecuted = !0;
            let queryError = query.submit(this.connection);
            if (queryError) {
              process.nextTick(() => {
                query.handleError(queryError, this.connection);
              });
              continue;
            }
            this._sentQueryQueue.push(query);
          }
          this.readyForQuery && !this._activeQuery && this._sentQueryQueue.length > 0 && (this._activeQuery = this._sentQueryQueue.shift(), this.readyForQuery = !1), !this._activeQuery && this._sentQueryQueue.length === 0 && this._queryQueue.length === 0 && this.hasExecuted && this.emit("drain");
        }
      }
      query(config, values, callback) {
        let query, result;
        if (config == null)
          throw new TypeError("Client was passed a null or undefined query");
        if (typeof config.submit == "function")
          result = query = config, query.callback || (typeof values == "function" ? query.callback = values : callback && (query.callback = callback));
        else if (query = new Query2(config, values, callback), !query.callback)
          result = new this._Promise((resolve, reject) => {
            query.callback = (err, res) => err ? reject(err) : resolve(res);
          }).catch((err) => {
            throw Error.captureStackTrace(err), err;
          });
        else if (typeof query.callback != "function")
          throw new TypeError("callback is not a function");
        let readTimeout = config.query_timeout || this.connectionParameters.query_timeout;
        if (readTimeout) {
          let queryCallback = query.callback || (() => {
          }), readTimeoutTimer = setTimeout(() => {
            let error = new Error("Query read timeout");
            process.nextTick(() => {
              query.handleError(error, this.connection);
            }), queryCallback(error), query.callback = () => {
            };
            let index = this._queryQueue.indexOf(query);
            if (index > -1)
              this._queryQueue.splice(index, 1);
            else if (this.pipeline) {
              this.connection.stream.destroy();
              return;
            }
            this._pulseQueryQueue();
          }, readTimeout);
          query.callback = (err, res) => {
            clearTimeout(readTimeoutTimer), queryCallback(err, res);
          };
        }
        if (this.binary && !query.binary && (query.binary = !0), query._result && !query._result._types && (query._result._types = this._types), this.pipeline) {
          let portalQuery = typeof config.submit == "function" && !(query instanceof Query2) ? "Custom query classes such as pg-cursor and pg-query-stream are" : query.rows ? "The `rows` option is" : null;
          if (portalQuery)
            return process.nextTick(() => {
              query.handleError(new Error(`${portalQuery} not supported in pipeline mode`), this.connection);
            }), result;
        }
        return this._queryable ? this._ending ? (process.nextTick(() => {
          query.handleError(new Error("Client was closed and is not queryable"), this.connection);
        }), result) : (this._queryQueue.length > 0 && !this.pipeline && queryQueueLengthDeprecationNotice(), this._queryQueue.push(query), this._pulseQueryQueue(), result) : (process.nextTick(() => {
          query.handleError(new Error("Client has encountered a connection error and is not queryable"), this.connection);
        }), result);
      }
      ref() {
        this.connection.ref();
      }
      unref() {
        this.connection.unref();
      }
      getTransactionStatus() {
        return this._txStatus;
      }
      end(cb) {
        if (this._ending = !0, !this.connection._connecting || this._ended)
          if (cb) {
            cb();
            return;
          } else
            return this._Promise.resolve();
        if (this._queryable ? this.pipeline && (this._getActiveQuery() || this._sentQueryQueue.length > 0 || this._queryQueue.length > 0) ? this.once("drain", () => this.connection.end()) : this._getActiveQuery() ? this.connection.stream.destroy() : this.connection.end() : this.connection.stream.destroy(), cb)
          this.connection.once("end", cb);
        else
          return new this._Promise((resolve) => {
            this.connection.once("end", resolve);
          });
      }
      get queryQueue() {
        return queryQueueDeprecationNotice(), this._queryQueue;
      }
    };
    Client2.Query = Query2;
    module2.exports = Client2;
  }
});

// node_modules/pg-pool/index.js
var require_pg_pool = __commonJS({
  "node_modules/pg-pool/index.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events").EventEmitter, NOOP = function() {
    }, removeWhere = (list, predicate) => {
      let i = list.findIndex(predicate);
      return i === -1 ? void 0 : list.splice(i, 1)[0];
    }, IdleItem = class {
      constructor(client, idleListener, timeoutId) {
        this.client = client, this.idleListener = idleListener, this.timeoutId = timeoutId;
      }
    }, PendingItem = class {
      constructor(callback) {
        this.callback = callback;
      }
    };
    function throwOnDoubleRelease() {
      throw new Error("Release called on client which has already been released to the pool.");
    }
    function promisify(Promise2, callback) {
      if (callback)
        return { callback, result: void 0 };
      let rej, res, cb = function(err, client) {
        err ? rej(err) : res(client);
      }, result = new Promise2(function(resolve, reject) {
        res = resolve, rej = reject;
      }).catch((err) => {
        throw Error.captureStackTrace(err), err;
      });
      return { callback: cb, result };
    }
    function makeIdleListener(pool2, client) {
      return function idleListener(err) {
        err.client = client, client.removeListener("error", idleListener), client.on("error", () => {
          pool2.log("additional client error after disconnection due to error", err);
        }), pool2._remove(client), pool2.emit("error", err, client);
      };
    }
    var Pool3 = class extends EventEmitter {
      constructor(options, Client2) {
        super(), this.options = Object.assign({}, options), options != null && "password" in options && Object.defineProperty(this.options, "password", {
          configurable: !0,
          enumerable: !1,
          writable: !0,
          value: options.password
        }), options != null && options.ssl && options.ssl.key && Object.defineProperty(this.options.ssl, "key", {
          enumerable: !1
        }), this.options.max = this.options.max || this.options.poolSize || 10, this.options.min = this.options.min || 0, this.options.maxUses = this.options.maxUses || 1 / 0, this.options.allowExitOnIdle = this.options.allowExitOnIdle || !1, this.options.maxLifetimeSeconds = this.options.maxLifetimeSeconds || 0, this.log = this.options.log || function() {
        }, this.Client = this.options.Client || Client2 || require_lib2().Client, this.Promise = this.options.Promise || global.Promise, typeof this.options.idleTimeoutMillis > "u" && (this.options.idleTimeoutMillis = 1e4), this._clients = [], this._idle = [], this._expired = /* @__PURE__ */ new WeakSet(), this._pendingQueue = [], this._endCallback = void 0, this.ending = !1, this.ended = !1;
      }
      _promiseTry(f) {
        let Promise2 = this.Promise;
        return typeof Promise2.try == "function" ? Promise2.try(f) : new Promise2((resolve) => resolve(f()));
      }
      _isFull() {
        return this._clients.length >= this.options.max;
      }
      _isAboveMin() {
        return this._clients.length > this.options.min;
      }
      _pulseQueue() {
        if (this.log("pulse queue"), this.ended) {
          this.log("pulse queue ended");
          return;
        }
        if (this.ending) {
          this.log("pulse queue on ending"), this._idle.length && this._idle.slice().map((item) => {
            this._remove(item.client);
          }), this._clients.length || (this.ended = !0, this._endCallback());
          return;
        }
        if (!this._pendingQueue.length) {
          this.log("no queued requests");
          return;
        }
        if (!this._idle.length && this._isFull())
          return;
        let pendingItem = this._pendingQueue.shift();
        if (this._idle.length) {
          let idleItem = this._idle.pop();
          clearTimeout(idleItem.timeoutId);
          let client = idleItem.client;
          client.ref && client.ref();
          let idleListener = idleItem.idleListener;
          return this._acquireClient(client, pendingItem, idleListener, !1);
        }
        if (!this._isFull())
          return this.newClient(pendingItem);
        throw new Error("unexpected condition");
      }
      _remove(client, callback) {
        let removed = removeWhere(this._idle, (item) => item.client === client);
        removed !== void 0 && clearTimeout(removed.timeoutId), this._clients = this._clients.filter((c) => c !== client);
        let context = this;
        client.end(() => {
          context.emit("remove", client), typeof callback == "function" && callback();
        });
      }
      connect(cb) {
        if (this.ending) {
          let err = new Error("Cannot use a pool after calling end on the pool");
          return cb ? cb(err) : this.Promise.reject(err);
        }
        let response = promisify(this.Promise, cb), result = response.result;
        if (this._isFull() || this._idle.length) {
          if (this._idle.length && process.nextTick(() => this._pulseQueue()), !this.options.connectionTimeoutMillis)
            return this._pendingQueue.push(new PendingItem(response.callback)), result;
          let queueCallback = (err, res, done) => {
            clearTimeout(tid), response.callback(err, res, done);
          }, pendingItem = new PendingItem(queueCallback), tid = setTimeout(() => {
            removeWhere(this._pendingQueue, (i) => i.callback === queueCallback), pendingItem.timedOut = !0, response.callback(new Error("timeout exceeded when trying to connect"));
          }, this.options.connectionTimeoutMillis);
          return tid.unref && tid.unref(), this._pendingQueue.push(pendingItem), result;
        }
        return this.newClient(new PendingItem(response.callback)), result;
      }
      newClient(pendingItem) {
        let client = new this.Client(this.options);
        this._clients.push(client);
        let idleListener = makeIdleListener(this, client);
        this.log("checking client timeout");
        let tid, timeoutHit = !1;
        this.options.connectionTimeoutMillis && (tid = setTimeout(() => {
          client.connection ? (this.log("ending client due to timeout"), timeoutHit = !0, client.connection.stream.destroy()) : client.isConnected() || (this.log("ending client due to timeout"), timeoutHit = !0, client.end());
        }, this.options.connectionTimeoutMillis)), this.log("connecting new client"), client.connect((err) => {
          if (tid && clearTimeout(tid), client.on("error", idleListener), err)
            this.log("client failed to connect", err), this._clients = this._clients.filter((c) => c !== client), timeoutHit && (err = new Error("Connection terminated due to connection timeout", { cause: err })), this._pulseQueue(), pendingItem.timedOut || pendingItem.callback(err, void 0, NOOP);
          else {
            if (this.log("new client connected"), this.options.onConnect) {
              this._promiseTry(() => this.options.onConnect(client)).then(
                () => {
                  this._afterConnect(client, pendingItem, idleListener);
                },
                (hookErr) => {
                  this._clients = this._clients.filter((c) => c !== client), client.end(() => {
                    this._pulseQueue(), pendingItem.timedOut || pendingItem.callback(hookErr, void 0, NOOP);
                  });
                }
              );
              return;
            }
            return this._afterConnect(client, pendingItem, idleListener);
          }
        });
      }
      _afterConnect(client, pendingItem, idleListener) {
        if (this.options.maxLifetimeSeconds !== 0) {
          let maxLifetimeTimeout = setTimeout(() => {
            this.log("ending client due to expired lifetime"), this._expired.add(client), this._idle.findIndex((idleItem) => idleItem.client === client) !== -1 && this._acquireClient(
              client,
              new PendingItem((err, client2, clientRelease) => clientRelease()),
              idleListener,
              !1
            );
          }, this.options.maxLifetimeSeconds * 1e3);
          maxLifetimeTimeout.unref(), client.once("end", () => clearTimeout(maxLifetimeTimeout));
        }
        return this._acquireClient(client, pendingItem, idleListener, !0);
      }
      // acquire a client for a pending work item
      _acquireClient(client, pendingItem, idleListener, isNew) {
        isNew && this.emit("connect", client), this.emit("acquire", client), client.release = this._releaseOnce(client, idleListener), client.removeListener("error", idleListener), pendingItem.timedOut ? isNew && this.options.verify ? this.options.verify(client, client.release) : client.release() : isNew && this.options.verify ? this.options.verify(client, (err) => {
          if (err)
            return client.release(err), pendingItem.callback(err, void 0, NOOP);
          pendingItem.callback(void 0, client, client.release);
        }) : pendingItem.callback(void 0, client, client.release);
      }
      // returns a function that wraps _release and throws if called more than once
      _releaseOnce(client, idleListener) {
        let released = !1;
        return (err) => {
          released && throwOnDoubleRelease(), released = !0, this._release(client, idleListener, err);
        };
      }
      // release a client back to the poll, include an error
      // to remove it from the pool
      _release(client, idleListener, err) {
        if (client.on("error", idleListener), client._poolUseCount = (client._poolUseCount || 0) + 1, this.emit("release", err, client), err || this.ending || !client._queryable || client._ending || client._poolUseCount >= this.options.maxUses)
          return client._poolUseCount >= this.options.maxUses && this.log("remove expended client"), this._remove(client, this._pulseQueue.bind(this));
        if (this._expired.has(client))
          return this.log("remove expired client"), this._expired.delete(client), this._remove(client, this._pulseQueue.bind(this));
        let tid;
        this.options.idleTimeoutMillis && this._isAboveMin() && (tid = setTimeout(() => {
          this._isAboveMin() && (this.log("remove idle client"), this._remove(client, this._pulseQueue.bind(this)));
        }, this.options.idleTimeoutMillis), this.options.allowExitOnIdle && tid.unref()), this.options.allowExitOnIdle && client.unref(), this._idle.push(new IdleItem(client, idleListener, tid)), this._pulseQueue();
      }
      query(text, values, cb) {
        if (typeof text == "function") {
          let response2 = promisify(this.Promise, text);
          return setImmediate(function() {
            return response2.callback(new Error("Passing a function as the first parameter to pool.query is not supported"));
          }), response2.result;
        }
        typeof values == "function" && (cb = values, values = void 0);
        let response = promisify(this.Promise, cb);
        return cb = response.callback, this.connect((err, client) => {
          if (err)
            return cb(err);
          let clientReleased = !1, onError = (err2) => {
            clientReleased || (clientReleased = !0, client.release(err2), cb(err2));
          };
          client.once("error", onError), this.log("dispatching query");
          try {
            client.query(text, values, (err2, res) => {
              if (this.log("query dispatched"), client.removeListener("error", onError), !clientReleased)
                return clientReleased = !0, client.release(err2), err2 ? cb(err2) : cb(void 0, res);
            });
          } catch (err2) {
            return client.release(err2), cb(err2);
          }
        }), response.result;
      }
      end(cb) {
        if (this.log("ending"), this.ending) {
          let err = new Error("Called end on pool more than once");
          return cb ? cb(err) : this.Promise.reject(err);
        }
        this.ending = !0;
        let promised = promisify(this.Promise, cb);
        return this._endCallback = promised.callback, this._pulseQueue(), promised.result;
      }
      get waitingCount() {
        return this._pendingQueue.length;
      }
      get idleCount() {
        return this._idle.length;
      }
      get expiredCount() {
        return this._clients.reduce((acc, client) => acc + (this._expired.has(client) ? 1 : 0), 0);
      }
      get totalCount() {
        return this._clients.length;
      }
    };
    module2.exports = Pool3;
  }
});

// node_modules/pg/lib/native/query.js
var require_query2 = __commonJS({
  "node_modules/pg/lib/native/query.js"(exports2, module2) {
    "use strict";
    var EventEmitter = require("events").EventEmitter, util = require("util"), utils = require_utils(), NativeQuery = module2.exports = function(config, values, callback) {
      EventEmitter.call(this), config = utils.normalizeQueryConfig(config, values, callback), this.text = config.text, this.values = config.values, this.name = config.name, this.queryMode = config.queryMode, this.callback = config.callback, this.state = "new", this._arrayMode = config.rowMode === "array", this._emitRowEvents = !1, this.on(
        "newListener",
        function(event) {
          event === "row" && (this._emitRowEvents = !0);
        }.bind(this)
      );
    };
    util.inherits(NativeQuery, EventEmitter);
    var errorFieldMap = {
      sqlState: "code",
      statementPosition: "position",
      messagePrimary: "message",
      messageDetail: "detail",
      messageHint: "hint",
      context: "where",
      schemaName: "schema",
      tableName: "table",
      columnName: "column",
      dataTypeName: "dataType",
      constraintName: "constraint",
      sourceFile: "file",
      sourceLine: "line",
      sourceFunction: "routine"
    };
    NativeQuery.prototype.handleError = function(err) {
      let fields = this.native && this.native.pq.resultErrorFields();
      if (fields)
        for (let key in fields) {
          let normalizedFieldName = errorFieldMap[key] || key;
          err[normalizedFieldName] = fields[key];
        }
      this.callback ? this.callback(err) : this.emit("error", err), this.state = "error";
    };
    NativeQuery.prototype.then = function(onSuccess, onFailure) {
      return this._getPromise().then(onSuccess, onFailure);
    };
    NativeQuery.prototype.catch = function(callback) {
      return this._getPromise().catch(callback);
    };
    NativeQuery.prototype._getPromise = function() {
      return this._promise ? this._promise : (this._promise = new Promise(
        function(resolve, reject) {
          this._once("end", resolve), this._once("error", reject);
        }.bind(this)
      ), this._promise);
    };
    NativeQuery.prototype.submit = function(client) {
      this.state = "running";
      let self = this;
      this.native = client.native, client.native.arrayMode = this._arrayMode;
      let after = function(err, rows, results) {
        if (client.native.arrayMode = !1, setImmediate(function() {
          self.emit("_done");
        }), err)
          return self.handleError(err);
        self._emitRowEvents && (results.length > 1 ? rows.forEach((rowOfRows, i) => {
          rowOfRows.forEach((row) => {
            self.emit("row", row, results[i]);
          });
        }) : rows.forEach(function(row) {
          self.emit("row", row, results);
        })), self.state = "end", self.emit("end", results), self.callback && self.callback(null, results);
      };
      if (process.domain && (after = process.domain.bind(after)), this.name) {
        this.name.length > 63 && (console.error("Warning! Postgres only supports 63 characters for query names."), console.error("You supplied %s (%s)", this.name, this.name.length), console.error("This can cause conflicts and silent errors executing queries"));
        let values = (this.values || []).map(utils.prepareValue);
        if (client.namedQueries[this.name] !== void 0) {
          if (this.text && client.namedQueries[this.name] !== this.text) {
            let err = new Error(`Prepared statements must be unique - '${this.name}' was used for a different statement`);
            return after(err);
          }
          return client.native.execute(this.name, values, after);
        }
        return client.native.prepare(this.name, this.text, values.length, function(err) {
          return err ? after(err) : (client.namedQueries[self.name] = self.text, self.native.execute(self.name, values, after));
        });
      } else if (this.values) {
        if (!Array.isArray(this.values)) {
          let err = new Error("Query values must be an array");
          return after(err);
        }
        let vals = this.values.map(utils.prepareValue);
        client.native.query(this.text, vals, after);
      } else this.queryMode === "extended" ? client.native.query(this.text, [], after) : client.native.query(this.text, after);
    };
  }
});

// node_modules/pg/lib/native/client.js
var require_client2 = __commonJS({
  "node_modules/pg/lib/native/client.js"(exports2, module2) {
    var nodeUtils = require("util"), Native;
    try {
      Native = require("pg-native");
    } catch (e) {
      throw e;
    }
    var TypeOverrides2 = require_type_overrides(), EventEmitter = require("events").EventEmitter, util = require("util"), ConnectionParameters = require_connection_parameters(), NativeQuery = require_query2(), queryQueueLengthDeprecationNotice = nodeUtils.deprecate(
      () => {
      },
      "Calling client.query() when the client is already executing a query is deprecated and will be removed in pg@9.0. Use async/await or an external async flow control mechanism instead."
    ), Client2 = module2.exports = function(config) {
      EventEmitter.call(this), config = config || {}, this._Promise = config.Promise || global.Promise, this._types = new TypeOverrides2(config.types), this.native = new Native({
        types: this._types
      }), this._queryQueue = [], this._ending = !1, this._connecting = !1, this._connected = !1, this._queryable = !0, this.pipeline = !!config.pipeline, this._pipelineInFlight = !1;
      let cp = this.connectionParameters = new ConnectionParameters(config);
      config.nativeConnectionString && (cp.nativeConnectionString = config.nativeConnectionString), this.user = cp.user, Object.defineProperty(this, "password", {
        configurable: !0,
        enumerable: !1,
        writable: !0,
        value: cp.password
      }), this.database = cp.database, this.host = cp.host, this.port = cp.port, this.namedQueries = {};
    };
    Client2.Query = NativeQuery;
    util.inherits(Client2, EventEmitter);
    Client2.prototype._errorAllQueries = function(err) {
      let enqueueError = (query) => {
        process.nextTick(() => {
          query.native = this.native, query.handleError(err);
        });
      };
      this._hasActiveQuery() && (enqueueError(this._activeQuery), this._activeQuery = null), this._queryQueue.forEach(enqueueError), this._queryQueue.length = 0;
    };
    Client2.prototype._connect = function(cb) {
      let self = this;
      if (this._connecting) {
        process.nextTick(() => cb(new Error("Client has already been connected. You cannot reuse a client.")));
        return;
      }
      this._connecting = !0, this.connectionParameters.getLibpqConnectionString(function(err, conString) {
        if (self.connectionParameters.nativeConnectionString && (conString = self.connectionParameters.nativeConnectionString), err) return cb(err);
        self.native.connect(conString, function(err2) {
          if (err2)
            return self.native.end(), cb(err2);
          self._connected = !0, self.native.on("error", function(err3) {
            self._queryable = !1, self._errorAllQueries(err3), self.emit("error", err3);
          }), self.native.on("notification", function(msg) {
            self.emit("notification", {
              channel: msg.relname,
              payload: msg.extra
            });
          }), self.emit("connect"), self._pulseQueryQueue(!0), cb(null, this);
        });
      });
    };
    Client2.prototype.connect = function(callback) {
      if (callback) {
        this._connect(callback);
        return;
      }
      return new this._Promise((resolve, reject) => {
        this._connect((error) => {
          error ? reject(error) : resolve(this);
        });
      });
    };
    Client2.prototype.query = function(config, values, callback) {
      let query, result, readTimeout, readTimeoutTimer, queryCallback;
      if (config == null)
        throw new TypeError("Client was passed a null or undefined query");
      if (typeof config.submit == "function")
        readTimeout = config.query_timeout || this.connectionParameters.query_timeout, result = query = config, typeof values == "function" && (config.callback = values);
      else if (readTimeout = config.query_timeout || this.connectionParameters.query_timeout, query = new NativeQuery(config, values, callback), !query.callback) {
        let resolveOut, rejectOut;
        result = new this._Promise((resolve, reject) => {
          resolveOut = resolve, rejectOut = reject;
        }).catch((err) => {
          throw Error.captureStackTrace(err), err;
        }), query.callback = (err, res) => err ? rejectOut(err) : resolveOut(res);
      }
      return readTimeout && (queryCallback = query.callback || (() => {
      }), readTimeoutTimer = setTimeout(() => {
        let error = new Error("Query read timeout");
        process.nextTick(() => {
          query.handleError(error, this.connection);
        }), queryCallback(error), query.callback = () => {
        };
        let index = this._queryQueue.indexOf(query);
        index > -1 && this._queryQueue.splice(index, 1), this._pulseQueryQueue();
      }, readTimeout), query.callback = (err, res) => {
        clearTimeout(readTimeoutTimer), queryCallback(err, res);
      }), this._queryable ? this._ending ? (query.native = this.native, process.nextTick(() => {
        query.handleError(new Error("Client was closed and is not queryable"));
      }), result) : (this._queryQueue.length > 0 && !this.pipeline && queryQueueLengthDeprecationNotice(), this._queryQueue.push(query), this._pulseQueryQueue(), result) : (query.native = this.native, process.nextTick(() => {
        query.handleError(new Error("Client has encountered a connection error and is not queryable"));
      }), result);
    };
    Client2.prototype.end = function(cb) {
      let self = this;
      this._ending = !0, this._connecting && !this._connected && this.once("connect", () => {
        this.end(() => {
        });
      });
      let result;
      cb || (result = new this._Promise(function(resolve, reject) {
        cb = (err) => err ? reject(err) : resolve();
      }));
      let doEnd = function() {
        self.native.end(function() {
          self._connected = !1, self._errorAllQueries(new Error("Connection terminated")), process.nextTick(() => {
            self.emit("end"), cb && cb();
          });
        });
      };
      return this.pipeline && (this._pipelineInFlight || this._queryQueue.length > 0) ? this.once("drain", doEnd) : doEnd(), result;
    };
    Client2.prototype._hasActiveQuery = function() {
      return this._activeQuery && this._activeQuery.state !== "error" && this._activeQuery.state !== "end";
    };
    Client2.prototype._pulseQueryQueue = function(initialConnection) {
      if (!this._connected)
        return;
      if (this.pipeline && !initialConnection)
        return this._pulsePipelinedQueryQueue();
      if (this._hasActiveQuery())
        return;
      let query = this._queryQueue.shift();
      if (!query) {
        initialConnection || this.emit("drain");
        return;
      }
      this._activeQuery = query, query.submit(this);
      let self = this;
      query.once("_done", function() {
        self._pulseQueryQueue();
      });
    };
    Client2.prototype._pulsePipelinedQueryQueue = function() {
      if (!this._connected || this._pipelineInFlight)
        return;
      if (this._queryQueue.length === 0) {
        this.hasExecuted && this.emit("drain");
        return;
      }
      this._pipelineInFlight = !0;
      let self = this, queries = [], nativeQueries = [], utils = require_utils();
      for (; this._queryQueue.length > 0; ) {
        let query = this._queryQueue.shift();
        this.hasExecuted = !0, nativeQueries.push(query);
        let values = query.values ? query.values.map(utils.prepareValue) : null, pipelineEntry = { text: query.text, name: query.name, arrayMode: query._arrayMode };
        values && (pipelineEntry.values = values), query.name && this.namedQueries[query.name] && (pipelineEntry._alreadyPrepared = !0), queries.push(pipelineEntry);
      }
      this.native.pipeline(queries, function(err, results) {
        if (self._pipelineInFlight = !1, err) {
          self._connected = !1, self._queryable = !1;
          for (let i = 0; i < nativeQueries.length; i++) {
            let q2 = nativeQueries[i];
            q2.native = self.native, q2.handleError(err);
          }
          self._errorAllQueries(err), self.emit("error", err), self.emit("end");
          return;
        }
        for (let i = 0; i < nativeQueries.length; i++) {
          let q2 = nativeQueries[i], r = results[i];
          q2.native = self.native, r.err ? q2.handleError(r.err) : (q2.name && (self.namedQueries[q2.name] = q2.text), q2.state = "end", q2.emit("end", r.result), q2.callback && q2.callback(null, r.result)), setImmediate(function() {
            q2.emit("_done");
          });
        }
        self._pulsePipelinedQueryQueue();
      });
    };
    Client2.prototype.cancel = function(query) {
      this._activeQuery === query ? this.native.cancel(function() {
      }) : this._queryQueue.indexOf(query) !== -1 && this._queryQueue.splice(this._queryQueue.indexOf(query), 1);
    };
    Client2.prototype.ref = function() {
    };
    Client2.prototype.unref = function() {
    };
    Client2.prototype.setTypeParser = function(oid, format, parseFn) {
      return this._types.setTypeParser(oid, format, parseFn);
    };
    Client2.prototype.getTypeParser = function(oid, format) {
      return this._types.getTypeParser(oid, format);
    };
    Client2.prototype.isConnected = function() {
      return this._connected;
    };
    Client2.prototype.getTransactionStatus = function() {
      return this.native.getTransactionStatus();
    };
  }
});

// node_modules/pg/lib/native/index.js
var require_native = __commonJS({
  "node_modules/pg/lib/native/index.js"(exports2, module2) {
    "use strict";
    module2.exports = require_client2();
  }
});

// node_modules/pg/lib/index.js
var require_lib2 = __commonJS({
  "node_modules/pg/lib/index.js"(exports2, module2) {
    "use strict";
    var Client2 = require_client(), defaults2 = require_defaults(), Connection2 = require_connection(), Result2 = require_result(), utils = require_utils(), Pool3 = require_pg_pool(), TypeOverrides2 = require_type_overrides(), { DatabaseError: DatabaseError2 } = require_dist(), { escapeIdentifier: escapeIdentifier2, escapeLiteral: escapeLiteral2 } = require_utils(), poolFactory = (Client3) => class extends Pool3 {
      constructor(options) {
        super(options, Client3);
      }
    }, PG = function(clientConstructor2) {
      this.defaults = defaults2, this.Client = clientConstructor2, this.Query = this.Client.Query, this.Pool = poolFactory(this.Client), this._pools = [], this.Connection = Connection2, this.types = require_pg_types(), this.DatabaseError = DatabaseError2, this.TypeOverrides = TypeOverrides2, this.escapeIdentifier = escapeIdentifier2, this.escapeLiteral = escapeLiteral2, this.Result = Result2, this.utils = utils;
    }, clientConstructor = Client2, forceNative = !1;
    try {
      forceNative = !!process.env.NODE_PG_FORCE_NATIVE;
    } catch {
    }
    forceNative && (clientConstructor = require_native());
    module2.exports = new PG(clientConstructor);
    Object.defineProperty(module2.exports, "native", {
      configurable: !0,
      enumerable: !1,
      get() {
        let native = null;
        try {
          native = new PG(require_native());
        } catch (err) {
          if (err.code !== "MODULE_NOT_FOUND")
            throw err;
        }
        return Object.defineProperty(module2.exports, "native", {
          value: native
        }), native;
      }
    });
  }
});

// node_modules/pg/esm/index.mjs
var import_lib, Client, Pool, Connection, types, Query, DatabaseError, escapeIdentifier, escapeLiteral, Result, TypeOverrides, defaults, esm_default, init_esm = __esm({
  "node_modules/pg/esm/index.mjs"() {
    import_lib = __toESM(require_lib2(), 1), Client = import_lib.default.Client, Pool = import_lib.default.Pool, Connection = import_lib.default.Connection, types = import_lib.default.types, Query = import_lib.default.Query, DatabaseError = import_lib.default.DatabaseError, escapeIdentifier = import_lib.default.escapeIdentifier, escapeLiteral = import_lib.default.escapeLiteral, Result = import_lib.default.Result, TypeOverrides = import_lib.default.TypeOverrides, defaults = import_lib.default.defaults, esm_default = import_lib.default;
  }
});

// server/db.ts
var db_exports = {};
__export(db_exports, {
  migrate: () => migrate,
  pool: () => pool,
  q: () => q,
  q1: () => q1,
  withTx: () => withTx
});
async function q(text, params = []) {
  return (await pool.query(text, params)).rows;
}
async function q1(text, params = []) {
  return (await pool.query(text, params)).rows[0];
}
async function withTx(fn) {
  for (let attempt = 0; ; attempt++) {
    let c = await pool.connect();
    try {
      await c.query("BEGIN");
      let out = await fn(c);
      return await c.query("COMMIT"), out;
    } catch (e) {
      try {
        await c.query("ROLLBACK");
      } catch {
      }
      if ((e.code === "40001" || e.code === "40P01") && attempt < 3) continue;
      throw e;
    } finally {
      c.release();
    }
  }
}
async function migrate() {
  await pool.query(SCHEMA);
}
var Pool2, types2, url, needsSSL, pool, SCHEMA, init_db = __esm({
  "server/db.ts"() {
    "use strict";
    init_esm();
    ({ Pool: Pool2, types: types2 } = esm_default);
    types2.setTypeParser(20, (v) => v === null ? null : Number(v));
    types2.setTypeParser(1700, (v) => v === null ? null : Number(v));
    url = process.env.DATABASE_URL;
    url || console.error("DATABASE_URL is not set. Add your PostgreSQL connection string as an environment variable.");
    needsSSL = !!url && !/localhost|127\.0\.0\.1|\/tmp|host=\//.test(url) && !/sslmode=disable/.test(url), pool = new Pool2({
      connectionString: url,
      ssl: needsSSL ? { rejectUnauthorized: !1 } : void 0,
      max: Number(process.env.DB_POOL_MAX || 8),
      idleTimeoutMillis: 3e4,
      connectionTimeoutMillis: 1e4
    });
    pool.on("error", (e) => console.error("db pool error", e.message));
    SCHEMA = `
CREATE EXTENSION IF NOT EXISTS citext;

CREATE TABLE IF NOT EXISTS users (
  id            BIGSERIAL PRIMARY KEY,
  username      CITEXT NOT NULL UNIQUE,
  email         CITEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  role          TEXT NOT NULL DEFAULT 'player' CHECK (role IN ('player','admin')),
  status        TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','suspended')),
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at TIMESTAMPTZ,
  last_seen_at  TIMESTAMPTZ,
  last_daily_bonus_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS profiles (
  user_id      BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  display_name TEXT,
  avatar_key   TEXT NOT NULL DEFAULT 'crown',
  avatar_image TEXT,
  prefs        JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS wallets (
  user_id    BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  balance    BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
  id          BIGSERIAL PRIMARY KEY,
  txn_ref     TEXT GENERATED ALWAYS AS ('TXN-' || lpad(id::text, 9, '0')) STORED,
  user_id     BIGINT NOT NULL REFERENCES users(id),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  game        TEXT,
  type        TEXT NOT NULL CHECK (type IN ('welcome_bonus','game_wager','game_win','game_loss','refund','promotional')),
  opening     BIGINT NOT NULL CHECK (opening >= 0),
  wagered     BIGINT NOT NULL DEFAULT 0 CHECK (wagered >= 0),
  won         BIGINT NOT NULL DEFAULT 0 CHECK (won >= 0),
  net         BIGINT NOT NULL,
  closing     BIGINT NOT NULL CHECK (closing >= 0),
  status      TEXT NOT NULL DEFAULT 'completed',
  round_id    BIGINT,
  detail      TEXT,
  idem_key    TEXT,
  CHECK (net = won - wagered OR type IN ('welcome_bonus','promotional','refund')),
  CHECK (closing = opening + net)
);
CREATE UNIQUE INDEX IF NOT EXISTS transactions_idem ON transactions(user_id, idem_key) WHERE idem_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS transactions_user_time ON transactions(user_id, id DESC);
CREATE INDEX IF NOT EXISTS transactions_time ON transactions(created_at DESC);

CREATE OR REPLACE FUNCTION r21_ledger_immutable() RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'Ledger entries are permanent and cannot be changed or deleted';
END; $$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS transactions_no_update ON transactions;
CREATE TRIGGER transactions_no_update BEFORE UPDATE OR DELETE ON transactions
  FOR EACH ROW EXECUTE FUNCTION r21_ledger_immutable();

CREATE TABLE IF NOT EXISTS game_rounds (
  id          BIGSERIAL PRIMARY KEY,
  user_id     BIGINT NOT NULL REFERENCES users(id),
  game        TEXT NOT NULL,
  status      TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','settled','refunded')),
  bet         BIGINT NOT NULL DEFAULT 0,
  payout      BIGINT NOT NULL DEFAULT 0,
  state       JSONB,
  result      JSONB,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  settled_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS rounds_user ON game_rounds(user_id, id DESC);
CREATE INDEX IF NOT EXISTS rounds_open ON game_rounds(status) WHERE status = 'open';
CREATE INDEX IF NOT EXISTS rounds_game_time ON game_rounds(game, created_at DESC);

CREATE TABLE IF NOT EXISTS crash_rounds (
  id          BIGSERIAL PRIMARY KEY,
  seed_hash   TEXT NOT NULL,
  seed        TEXT,
  crash_point NUMERIC(12,2),
  started_at  TIMESTAMPTZ,
  ended_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS sessions (
  token_hash  TEXT PRIMARY KEY,
  user_id     BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at  TIMESTAMPTZ NOT NULL,
  user_agent  TEXT,
  ip          TEXT
);
CREATE INDEX IF NOT EXISTS sessions_user ON sessions(user_id);

CREATE TABLE IF NOT EXISTS password_resets (
  token_hash  TEXT PRIMARY KEY,
  user_id     BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  expires_at  TIMESTAMPTZ NOT NULL,
  used_at     TIMESTAMPTZ,
  created_by  TEXT NOT NULL DEFAULT 'user'
);

CREATE TABLE IF NOT EXISTS settings (
  key   TEXT PRIMARY KEY,
  value JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
  id         BIGSERIAL PRIMARY KEY,
  admin_id   BIGINT REFERENCES users(id),
  action     TEXT NOT NULL,
  target_id  BIGINT,
  detail     JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS contact_messages (
  id         BIGSERIAL PRIMARY KEY,
  user_id    BIGINT REFERENCES users(id),
  name       TEXT NOT NULL,
  email      TEXT NOT NULL,
  topic      TEXT NOT NULL,
  message    TEXT NOT NULL,
  handled    BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
`;
  }
});

// server/index.ts
var index_exports = {};
__export(index_exports, {
  fmt: () => fmt,
  pool: () => pool
});
module.exports = __toCommonJS(index_exports);
var import_node_http = __toESM(require("node:http")), import_node_fs = __toESM(require("node:fs")), import_node_path = __toESM(require("node:path")), import_node_zlib = __toESM(require("node:zlib"));

// node_modules/ws/wrapper.mjs
var import_stream = __toESM(require_stream(), 1), import_extension = __toESM(require_extension(), 1), import_permessage_deflate = __toESM(require_permessage_deflate(), 1), import_receiver = __toESM(require_receiver(), 1), import_sender = __toESM(require_sender(), 1), import_subprotocol = __toESM(require_subprotocol(), 1), import_websocket = __toESM(require_websocket(), 1), import_websocket_server = __toESM(require_websocket_server(), 1);

// server/index.ts
init_db();

// node_modules/bcryptjs/index.js
var import_crypto = __toESM(require("crypto"), 1), randomFallback = null;
function randomBytes(len) {
  try {
    return crypto.getRandomValues(new Uint8Array(len));
  } catch {
  }
  try {
    return import_crypto.default.randomBytes(len);
  } catch {
  }
  if (!randomFallback)
    throw Error(
      "Neither WebCryptoAPI nor a crypto module is available. Use bcrypt.setRandomFallback to set an alternative"
    );
  return randomFallback(len);
}
function setRandomFallback(random) {
  randomFallback = random;
}
function genSaltSync(rounds, seed_length) {
  if (rounds = rounds || GENSALT_DEFAULT_LOG2_ROUNDS, typeof rounds != "number")
    throw Error(
      "Illegal arguments: " + typeof rounds + ", " + typeof seed_length
    );
  rounds < 4 ? rounds = 4 : rounds > 31 && (rounds = 31);
  var salt = [];
  return salt.push("$2b$"), rounds < 10 && salt.push("0"), salt.push(rounds.toString()), salt.push("$"), salt.push(base64_encode(randomBytes(BCRYPT_SALT_LEN), BCRYPT_SALT_LEN)), salt.join("");
}
function genSalt(rounds, seed_length, callback) {
  if (typeof seed_length == "function" && (callback = seed_length, seed_length = void 0), typeof rounds == "function" && (callback = rounds, rounds = void 0), typeof rounds > "u") rounds = GENSALT_DEFAULT_LOG2_ROUNDS;
  else if (typeof rounds != "number")
    throw Error("illegal arguments: " + typeof rounds);
  function _async(callback2) {
    nextTick(function() {
      try {
        callback2(null, genSaltSync(rounds));
      } catch (err) {
        callback2(err);
      }
    });
  }
  if (callback) {
    if (typeof callback != "function")
      throw Error("Illegal callback: " + typeof callback);
    _async(callback);
  } else
    return new Promise(function(resolve, reject) {
      _async(function(err, res) {
        if (err) {
          reject(err);
          return;
        }
        resolve(res);
      });
    });
}
function hashSync(password, salt) {
  if (typeof salt > "u" && (salt = GENSALT_DEFAULT_LOG2_ROUNDS), typeof salt == "number" && (salt = genSaltSync(salt)), typeof password != "string" || typeof salt != "string")
    throw Error("Illegal arguments: " + typeof password + ", " + typeof salt);
  return _hash(password, salt);
}
function hash(password, salt, callback, progressCallback) {
  function _async(callback2) {
    typeof password == "string" && typeof salt == "number" ? genSalt(salt, function(err, salt2) {
      _hash(password, salt2, callback2, progressCallback);
    }) : typeof password == "string" && typeof salt == "string" ? _hash(password, salt, callback2, progressCallback) : nextTick(
      callback2.bind(
        this,
        Error("Illegal arguments: " + typeof password + ", " + typeof salt)
      )
    );
  }
  if (callback) {
    if (typeof callback != "function")
      throw Error("Illegal callback: " + typeof callback);
    _async(callback);
  } else
    return new Promise(function(resolve, reject) {
      _async(function(err, res) {
        if (err) {
          reject(err);
          return;
        }
        resolve(res);
      });
    });
}
function safeStringCompare(known, unknown) {
  for (var diff = known.length ^ unknown.length, i = 0; i < known.length; ++i)
    diff |= known.charCodeAt(i) ^ unknown.charCodeAt(i);
  return diff === 0;
}
function compareSync(password, hash2) {
  if (typeof password != "string" || typeof hash2 != "string")
    throw Error("Illegal arguments: " + typeof password + ", " + typeof hash2);
  return hash2.length !== 60 ? !1 : safeStringCompare(
    hashSync(password, hash2.substring(0, hash2.length - 31)),
    hash2
  );
}
function compare(password, hashValue, callback, progressCallback) {
  function _async(callback2) {
    if (typeof password != "string" || typeof hashValue != "string") {
      nextTick(
        callback2.bind(
          this,
          Error(
            "Illegal arguments: " + typeof password + ", " + typeof hashValue
          )
        )
      );
      return;
    }
    if (hashValue.length !== 60) {
      nextTick(callback2.bind(this, null, !1));
      return;
    }
    hash(
      password,
      hashValue.substring(0, 29),
      function(err, comp) {
        err ? callback2(err) : callback2(null, safeStringCompare(comp, hashValue));
      },
      progressCallback
    );
  }
  if (callback) {
    if (typeof callback != "function")
      throw Error("Illegal callback: " + typeof callback);
    _async(callback);
  } else
    return new Promise(function(resolve, reject) {
      _async(function(err, res) {
        if (err) {
          reject(err);
          return;
        }
        resolve(res);
      });
    });
}
function getRounds(hash2) {
  if (typeof hash2 != "string")
    throw Error("Illegal arguments: " + typeof hash2);
  return parseInt(hash2.split("$")[2], 10);
}
function getSalt(hash2) {
  if (typeof hash2 != "string")
    throw Error("Illegal arguments: " + typeof hash2);
  if (hash2.length !== 60)
    throw Error("Illegal hash length: " + hash2.length + " != 60");
  return hash2.substring(0, 29);
}
function truncates(password) {
  if (typeof password != "string")
    throw Error("Illegal arguments: " + typeof password);
  return utf8Length(password) > 72;
}
var nextTick = typeof setImmediate == "function" ? setImmediate : typeof scheduler == "object" && typeof scheduler.postTask == "function" ? scheduler.postTask.bind(scheduler) : setTimeout;
function utf8Length(string) {
  for (var len = 0, c = 0, i = 0; i < string.length; ++i)
    c = string.charCodeAt(i), c < 128 ? len += 1 : c < 2048 ? len += 2 : (c & 64512) === 55296 && (string.charCodeAt(i + 1) & 64512) === 56320 ? (++i, len += 4) : len += 3;
  return len;
}
function utf8Array(string) {
  for (var offset = 0, c1, c2, buffer = new Array(utf8Length(string)), i = 0, k = string.length; i < k; ++i)
    c1 = string.charCodeAt(i), c1 < 128 ? buffer[offset++] = c1 : c1 < 2048 ? (buffer[offset++] = c1 >> 6 | 192, buffer[offset++] = c1 & 63 | 128) : (c1 & 64512) === 55296 && ((c2 = string.charCodeAt(i + 1)) & 64512) === 56320 ? (c1 = 65536 + ((c1 & 1023) << 10) + (c2 & 1023), ++i, buffer[offset++] = c1 >> 18 | 240, buffer[offset++] = c1 >> 12 & 63 | 128, buffer[offset++] = c1 >> 6 & 63 | 128, buffer[offset++] = c1 & 63 | 128) : (buffer[offset++] = c1 >> 12 | 224, buffer[offset++] = c1 >> 6 & 63 | 128, buffer[offset++] = c1 & 63 | 128);
  return buffer;
}
var BASE64_CODE = "./ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".split(""), BASE64_INDEX = [
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  0,
  1,
  54,
  55,
  56,
  57,
  58,
  59,
  60,
  61,
  62,
  63,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  13,
  14,
  15,
  16,
  17,
  18,
  19,
  20,
  21,
  22,
  23,
  24,
  25,
  26,
  27,
  -1,
  -1,
  -1,
  -1,
  -1,
  -1,
  28,
  29,
  30,
  31,
  32,
  33,
  34,
  35,
  36,
  37,
  38,
  39,
  40,
  41,
  42,
  43,
  44,
  45,
  46,
  47,
  48,
  49,
  50,
  51,
  52,
  53,
  -1,
  -1,
  -1,
  -1,
  -1
];
function base64_encode(b, len) {
  var off = 0, rs = [], c1, c2;
  if (len <= 0 || len > b.length) throw Error("Illegal len: " + len);
  for (; off < len; ) {
    if (c1 = b[off++] & 255, rs.push(BASE64_CODE[c1 >> 2 & 63]), c1 = (c1 & 3) << 4, off >= len) {
      rs.push(BASE64_CODE[c1 & 63]);
      break;
    }
    if (c2 = b[off++] & 255, c1 |= c2 >> 4 & 15, rs.push(BASE64_CODE[c1 & 63]), c1 = (c2 & 15) << 2, off >= len) {
      rs.push(BASE64_CODE[c1 & 63]);
      break;
    }
    c2 = b[off++] & 255, c1 |= c2 >> 6 & 3, rs.push(BASE64_CODE[c1 & 63]), rs.push(BASE64_CODE[c2 & 63]);
  }
  return rs.join("");
}
function base64_decode(s, len) {
  var off = 0, slen = s.length, olen = 0, rs = [], c1, c2, c3, c4, o, code;
  if (len <= 0) throw Error("Illegal len: " + len);
  for (; off < slen - 1 && olen < len && (code = s.charCodeAt(off++), c1 = code < BASE64_INDEX.length ? BASE64_INDEX[code] : -1, code = s.charCodeAt(off++), c2 = code < BASE64_INDEX.length ? BASE64_INDEX[code] : -1, !(c1 == -1 || c2 == -1 || (o = c1 << 2 >>> 0, o |= (c2 & 48) >> 4, rs.push(String.fromCharCode(o)), ++olen >= len || off >= slen) || (code = s.charCodeAt(off++), c3 = code < BASE64_INDEX.length ? BASE64_INDEX[code] : -1, c3 == -1) || (o = (c2 & 15) << 4 >>> 0, o |= (c3 & 60) >> 2, rs.push(String.fromCharCode(o)), ++olen >= len || off >= slen))); )
    code = s.charCodeAt(off++), c4 = code < BASE64_INDEX.length ? BASE64_INDEX[code] : -1, o = (c3 & 3) << 6 >>> 0, o |= c4, rs.push(String.fromCharCode(o)), ++olen;
  var res = [];
  for (off = 0; off < olen; off++) res.push(rs[off].charCodeAt(0));
  return res;
}
var BCRYPT_SALT_LEN = 16, GENSALT_DEFAULT_LOG2_ROUNDS = 10, BLOWFISH_NUM_ROUNDS = 16, MAX_EXECUTION_TIME = 100, P_ORIG = [
  608135816,
  2242054355,
  320440878,
  57701188,
  2752067618,
  698298832,
  137296536,
  3964562569,
  1160258022,
  953160567,
  3193202383,
  887688300,
  3232508343,
  3380367581,
  1065670069,
  3041331479,
  2450970073,
  2306472731
], S_ORIG = [
  3509652390,
  2564797868,
  805139163,
  3491422135,
  3101798381,
  1780907670,
  3128725573,
  4046225305,
  614570311,
  3012652279,
  134345442,
  2240740374,
  1667834072,
  1901547113,
  2757295779,
  4103290238,
  227898511,
  1921955416,
  1904987480,
  2182433518,
  2069144605,
  3260701109,
  2620446009,
  720527379,
  3318853667,
  677414384,
  3393288472,
  3101374703,
  2390351024,
  1614419982,
  1822297739,
  2954791486,
  3608508353,
  3174124327,
  2024746970,
  1432378464,
  3864339955,
  2857741204,
  1464375394,
  1676153920,
  1439316330,
  715854006,
  3033291828,
  289532110,
  2706671279,
  2087905683,
  3018724369,
  1668267050,
  732546397,
  1947742710,
  3462151702,
  2609353502,
  2950085171,
  1814351708,
  2050118529,
  680887927,
  999245976,
  1800124847,
  3300911131,
  1713906067,
  1641548236,
  4213287313,
  1216130144,
  1575780402,
  4018429277,
  3917837745,
  3693486850,
  3949271944,
  596196993,
  3549867205,
  258830323,
  2213823033,
  772490370,
  2760122372,
  1774776394,
  2652871518,
  566650946,
  4142492826,
  1728879713,
  2882767088,
  1783734482,
  3629395816,
  2517608232,
  2874225571,
  1861159788,
  326777828,
  3124490320,
  2130389656,
  2716951837,
  967770486,
  1724537150,
  2185432712,
  2364442137,
  1164943284,
  2105845187,
  998989502,
  3765401048,
  2244026483,
  1075463327,
  1455516326,
  1322494562,
  910128902,
  469688178,
  1117454909,
  936433444,
  3490320968,
  3675253459,
  1240580251,
  122909385,
  2157517691,
  634681816,
  4142456567,
  3825094682,
  3061402683,
  2540495037,
  79693498,
  3249098678,
  1084186820,
  1583128258,
  426386531,
  1761308591,
  1047286709,
  322548459,
  995290223,
  1845252383,
  2603652396,
  3431023940,
  2942221577,
  3202600964,
  3727903485,
  1712269319,
  422464435,
  3234572375,
  1170764815,
  3523960633,
  3117677531,
  1434042557,
  442511882,
  3600875718,
  1076654713,
  1738483198,
  4213154764,
  2393238008,
  3677496056,
  1014306527,
  4251020053,
  793779912,
  2902807211,
  842905082,
  4246964064,
  1395751752,
  1040244610,
  2656851899,
  3396308128,
  445077038,
  3742853595,
  3577915638,
  679411651,
  2892444358,
  2354009459,
  1767581616,
  3150600392,
  3791627101,
  3102740896,
  284835224,
  4246832056,
  1258075500,
  768725851,
  2589189241,
  3069724005,
  3532540348,
  1274779536,
  3789419226,
  2764799539,
  1660621633,
  3471099624,
  4011903706,
  913787905,
  3497959166,
  737222580,
  2514213453,
  2928710040,
  3937242737,
  1804850592,
  3499020752,
  2949064160,
  2386320175,
  2390070455,
  2415321851,
  4061277028,
  2290661394,
  2416832540,
  1336762016,
  1754252060,
  3520065937,
  3014181293,
  791618072,
  3188594551,
  3933548030,
  2332172193,
  3852520463,
  3043980520,
  413987798,
  3465142937,
  3030929376,
  4245938359,
  2093235073,
  3534596313,
  375366246,
  2157278981,
  2479649556,
  555357303,
  3870105701,
  2008414854,
  3344188149,
  4221384143,
  3956125452,
  2067696032,
  3594591187,
  2921233993,
  2428461,
  544322398,
  577241275,
  1471733935,
  610547355,
  4027169054,
  1432588573,
  1507829418,
  2025931657,
  3646575487,
  545086370,
  48609733,
  2200306550,
  1653985193,
  298326376,
  1316178497,
  3007786442,
  2064951626,
  458293330,
  2589141269,
  3591329599,
  3164325604,
  727753846,
  2179363840,
  146436021,
  1461446943,
  4069977195,
  705550613,
  3059967265,
  3887724982,
  4281599278,
  3313849956,
  1404054877,
  2845806497,
  146425753,
  1854211946,
  1266315497,
  3048417604,
  3681880366,
  3289982499,
  290971e4,
  1235738493,
  2632868024,
  2414719590,
  3970600049,
  1771706367,
  1449415276,
  3266420449,
  422970021,
  1963543593,
  2690192192,
  3826793022,
  1062508698,
  1531092325,
  1804592342,
  2583117782,
  2714934279,
  4024971509,
  1294809318,
  4028980673,
  1289560198,
  2221992742,
  1669523910,
  35572830,
  157838143,
  1052438473,
  1016535060,
  1802137761,
  1753167236,
  1386275462,
  3080475397,
  2857371447,
  1040679964,
  2145300060,
  2390574316,
  1461121720,
  2956646967,
  4031777805,
  4028374788,
  33600511,
  2920084762,
  1018524850,
  629373528,
  3691585981,
  3515945977,
  2091462646,
  2486323059,
  586499841,
  988145025,
  935516892,
  3367335476,
  2599673255,
  2839830854,
  265290510,
  3972581182,
  2759138881,
  3795373465,
  1005194799,
  847297441,
  406762289,
  1314163512,
  1332590856,
  1866599683,
  4127851711,
  750260880,
  613907577,
  1450815602,
  3165620655,
  3734664991,
  3650291728,
  3012275730,
  3704569646,
  1427272223,
  778793252,
  1343938022,
  2676280711,
  2052605720,
  1946737175,
  3164576444,
  3914038668,
  3967478842,
  3682934266,
  1661551462,
  3294938066,
  4011595847,
  840292616,
  3712170807,
  616741398,
  312560963,
  711312465,
  1351876610,
  322626781,
  1910503582,
  271666773,
  2175563734,
  1594956187,
  70604529,
  3617834859,
  1007753275,
  1495573769,
  4069517037,
  2549218298,
  2663038764,
  504708206,
  2263041392,
  3941167025,
  2249088522,
  1514023603,
  1998579484,
  1312622330,
  694541497,
  2582060303,
  2151582166,
  1382467621,
  776784248,
  2618340202,
  3323268794,
  2497899128,
  2784771155,
  503983604,
  4076293799,
  907881277,
  423175695,
  432175456,
  1378068232,
  4145222326,
  3954048622,
  3938656102,
  3820766613,
  2793130115,
  2977904593,
  26017576,
  3274890735,
  3194772133,
  1700274565,
  1756076034,
  4006520079,
  3677328699,
  720338349,
  1533947780,
  354530856,
  688349552,
  3973924725,
  1637815568,
  332179504,
  3949051286,
  53804574,
  2852348879,
  3044236432,
  1282449977,
  3583942155,
  3416972820,
  4006381244,
  1617046695,
  2628476075,
  3002303598,
  1686838959,
  431878346,
  2686675385,
  1700445008,
  1080580658,
  1009431731,
  832498133,
  3223435511,
  2605976345,
  2271191193,
  2516031870,
  1648197032,
  4164389018,
  2548247927,
  300782431,
  375919233,
  238389289,
  3353747414,
  2531188641,
  2019080857,
  1475708069,
  455242339,
  2609103871,
  448939670,
  3451063019,
  1395535956,
  2413381860,
  1841049896,
  1491858159,
  885456874,
  4264095073,
  4001119347,
  1565136089,
  3898914787,
  1108368660,
  540939232,
  1173283510,
  2745871338,
  3681308437,
  4207628240,
  3343053890,
  4016749493,
  1699691293,
  1103962373,
  3625875870,
  2256883143,
  3830138730,
  1031889488,
  3479347698,
  1535977030,
  4236805024,
  3251091107,
  2132092099,
  1774941330,
  1199868427,
  1452454533,
  157007616,
  2904115357,
  342012276,
  595725824,
  1480756522,
  206960106,
  497939518,
  591360097,
  863170706,
  2375253569,
  3596610801,
  1814182875,
  2094937945,
  3421402208,
  1082520231,
  3463918190,
  2785509508,
  435703966,
  3908032597,
  1641649973,
  2842273706,
  3305899714,
  1510255612,
  2148256476,
  2655287854,
  3276092548,
  4258621189,
  236887753,
  3681803219,
  274041037,
  1734335097,
  3815195456,
  3317970021,
  1899903192,
  1026095262,
  4050517792,
  356393447,
  2410691914,
  3873677099,
  3682840055,
  3913112168,
  2491498743,
  4132185628,
  2489919796,
  1091903735,
  1979897079,
  3170134830,
  3567386728,
  3557303409,
  857797738,
  1136121015,
  1342202287,
  507115054,
  2535736646,
  337727348,
  3213592640,
  1301675037,
  2528481711,
  1895095763,
  1721773893,
  3216771564,
  62756741,
  2142006736,
  835421444,
  2531993523,
  1442658625,
  3659876326,
  2882144922,
  676362277,
  1392781812,
  170690266,
  3921047035,
  1759253602,
  3611846912,
  1745797284,
  664899054,
  1329594018,
  3901205900,
  3045908486,
  2062866102,
  2865634940,
  3543621612,
  3464012697,
  1080764994,
  553557557,
  3656615353,
  3996768171,
  991055499,
  499776247,
  1265440854,
  648242737,
  3940784050,
  980351604,
  3713745714,
  1749149687,
  3396870395,
  4211799374,
  3640570775,
  1161844396,
  3125318951,
  1431517754,
  545492359,
  4268468663,
  3499529547,
  1437099964,
  2702547544,
  3433638243,
  2581715763,
  2787789398,
  1060185593,
  1593081372,
  2418618748,
  4260947970,
  69676912,
  2159744348,
  86519011,
  2512459080,
  3838209314,
  1220612927,
  3339683548,
  133810670,
  1090789135,
  1078426020,
  1569222167,
  845107691,
  3583754449,
  4072456591,
  1091646820,
  628848692,
  1613405280,
  3757631651,
  526609435,
  236106946,
  48312990,
  2942717905,
  3402727701,
  1797494240,
  859738849,
  992217954,
  4005476642,
  2243076622,
  3870952857,
  3732016268,
  765654824,
  3490871365,
  2511836413,
  1685915746,
  3888969200,
  1414112111,
  2273134842,
  3281911079,
  4080962846,
  172450625,
  2569994100,
  980381355,
  4109958455,
  2819808352,
  2716589560,
  2568741196,
  3681446669,
  3329971472,
  1835478071,
  660984891,
  3704678404,
  4045999559,
  3422617507,
  3040415634,
  1762651403,
  1719377915,
  3470491036,
  2693910283,
  3642056355,
  3138596744,
  1364962596,
  2073328063,
  1983633131,
  926494387,
  3423689081,
  2150032023,
  4096667949,
  1749200295,
  3328846651,
  309677260,
  2016342300,
  1779581495,
  3079819751,
  111262694,
  1274766160,
  443224088,
  298511866,
  1025883608,
  3806446537,
  1145181785,
  168956806,
  3641502830,
  3584813610,
  1689216846,
  3666258015,
  3200248200,
  1692713982,
  2646376535,
  4042768518,
  1618508792,
  1610833997,
  3523052358,
  4130873264,
  2001055236,
  3610705100,
  2202168115,
  4028541809,
  2961195399,
  1006657119,
  2006996926,
  3186142756,
  1430667929,
  3210227297,
  1314452623,
  4074634658,
  4101304120,
  2273951170,
  1399257539,
  3367210612,
  3027628629,
  1190975929,
  2062231137,
  2333990788,
  2221543033,
  2438960610,
  1181637006,
  548689776,
  2362791313,
  3372408396,
  3104550113,
  3145860560,
  296247880,
  1970579870,
  3078560182,
  3769228297,
  1714227617,
  3291629107,
  3898220290,
  166772364,
  1251581989,
  493813264,
  448347421,
  195405023,
  2709975567,
  677966185,
  3703036547,
  1463355134,
  2715995803,
  1338867538,
  1343315457,
  2802222074,
  2684532164,
  233230375,
  2599980071,
  2000651841,
  3277868038,
  1638401717,
  4028070440,
  3237316320,
  6314154,
  819756386,
  300326615,
  590932579,
  1405279636,
  3267499572,
  3150704214,
  2428286686,
  3959192993,
  3461946742,
  1862657033,
  1266418056,
  963775037,
  2089974820,
  2263052895,
  1917689273,
  448879540,
  3550394620,
  3981727096,
  150775221,
  3627908307,
  1303187396,
  508620638,
  2975983352,
  2726630617,
  1817252668,
  1876281319,
  1457606340,
  908771278,
  3720792119,
  3617206836,
  2455994898,
  1729034894,
  1080033504,
  976866871,
  3556439503,
  2881648439,
  1522871579,
  1555064734,
  1336096578,
  3548522304,
  2579274686,
  3574697629,
  3205460757,
  3593280638,
  3338716283,
  3079412587,
  564236357,
  2993598910,
  1781952180,
  1464380207,
  3163844217,
  3332601554,
  1699332808,
  1393555694,
  1183702653,
  3581086237,
  1288719814,
  691649499,
  2847557200,
  2895455976,
  3193889540,
  2717570544,
  1781354906,
  1676643554,
  2592534050,
  3230253752,
  1126444790,
  2770207658,
  2633158820,
  2210423226,
  2615765581,
  2414155088,
  3127139286,
  673620729,
  2805611233,
  1269405062,
  4015350505,
  3341807571,
  4149409754,
  1057255273,
  2012875353,
  2162469141,
  2276492801,
  2601117357,
  993977747,
  3918593370,
  2654263191,
  753973209,
  36408145,
  2530585658,
  25011837,
  3520020182,
  2088578344,
  530523599,
  2918365339,
  1524020338,
  1518925132,
  3760827505,
  3759777254,
  1202760957,
  3985898139,
  3906192525,
  674977740,
  4174734889,
  2031300136,
  2019492241,
  3983892565,
  4153806404,
  3822280332,
  352677332,
  2297720250,
  60907813,
  90501309,
  3286998549,
  1016092578,
  2535922412,
  2839152426,
  457141659,
  509813237,
  4120667899,
  652014361,
  1966332200,
  2975202805,
  55981186,
  2327461051,
  676427537,
  3255491064,
  2882294119,
  3433927263,
  1307055953,
  942726286,
  933058658,
  2468411793,
  3933900994,
  4215176142,
  1361170020,
  2001714738,
  2830558078,
  3274259782,
  1222529897,
  1679025792,
  2729314320,
  3714953764,
  1770335741,
  151462246,
  3013232138,
  1682292957,
  1483529935,
  471910574,
  1539241949,
  458788160,
  3436315007,
  1807016891,
  3718408830,
  978976581,
  1043663428,
  3165965781,
  1927990952,
  4200891579,
  2372276910,
  3208408903,
  3533431907,
  1412390302,
  2931980059,
  4132332400,
  1947078029,
  3881505623,
  4168226417,
  2941484381,
  1077988104,
  1320477388,
  886195818,
  18198404,
  3786409e3,
  2509781533,
  112762804,
  3463356488,
  1866414978,
  891333506,
  18488651,
  661792760,
  1628790961,
  3885187036,
  3141171499,
  876946877,
  2693282273,
  1372485963,
  791857591,
  2686433993,
  3759982718,
  3167212022,
  3472953795,
  2716379847,
  445679433,
  3561995674,
  3504004811,
  3574258232,
  54117162,
  3331405415,
  2381918588,
  3769707343,
  4154350007,
  1140177722,
  4074052095,
  668550556,
  3214352940,
  367459370,
  261225585,
  2610173221,
  4209349473,
  3468074219,
  3265815641,
  314222801,
  3066103646,
  3808782860,
  282218597,
  3406013506,
  3773591054,
  379116347,
  1285071038,
  846784868,
  2669647154,
  3771962079,
  3550491691,
  2305946142,
  453669953,
  1268987020,
  3317592352,
  3279303384,
  3744833421,
  2610507566,
  3859509063,
  266596637,
  3847019092,
  517658769,
  3462560207,
  3443424879,
  370717030,
  4247526661,
  2224018117,
  4143653529,
  4112773975,
  2788324899,
  2477274417,
  1456262402,
  2901442914,
  1517677493,
  1846949527,
  2295493580,
  3734397586,
  2176403920,
  1280348187,
  1908823572,
  3871786941,
  846861322,
  1172426758,
  3287448474,
  3383383037,
  1655181056,
  3139813346,
  901632758,
  1897031941,
  2986607138,
  3066810236,
  3447102507,
  1393639104,
  373351379,
  950779232,
  625454576,
  3124240540,
  4148612726,
  2007998917,
  544563296,
  2244738638,
  2330496472,
  2058025392,
  1291430526,
  424198748,
  50039436,
  29584100,
  3605783033,
  2429876329,
  2791104160,
  1057563949,
  3255363231,
  3075367218,
  3463963227,
  1469046755,
  985887462
], C_ORIG = [
  1332899944,
  1700884034,
  1701343084,
  1684370003,
  1668446532,
  1869963892
];
function _encipher(lr, off, P, S) {
  var n, l = lr[off], r = lr[off + 1];
  return l ^= P[0], n = S[l >>> 24], n += S[256 | l >> 16 & 255], n ^= S[512 | l >> 8 & 255], n += S[768 | l & 255], r ^= n ^ P[1], n = S[r >>> 24], n += S[256 | r >> 16 & 255], n ^= S[512 | r >> 8 & 255], n += S[768 | r & 255], l ^= n ^ P[2], n = S[l >>> 24], n += S[256 | l >> 16 & 255], n ^= S[512 | l >> 8 & 255], n += S[768 | l & 255], r ^= n ^ P[3], n = S[r >>> 24], n += S[256 | r >> 16 & 255], n ^= S[512 | r >> 8 & 255], n += S[768 | r & 255], l ^= n ^ P[4], n = S[l >>> 24], n += S[256 | l >> 16 & 255], n ^= S[512 | l >> 8 & 255], n += S[768 | l & 255], r ^= n ^ P[5], n = S[r >>> 24], n += S[256 | r >> 16 & 255], n ^= S[512 | r >> 8 & 255], n += S[768 | r & 255], l ^= n ^ P[6], n = S[l >>> 24], n += S[256 | l >> 16 & 255], n ^= S[512 | l >> 8 & 255], n += S[768 | l & 255], r ^= n ^ P[7], n = S[r >>> 24], n += S[256 | r >> 16 & 255], n ^= S[512 | r >> 8 & 255], n += S[768 | r & 255], l ^= n ^ P[8], n = S[l >>> 24], n += S[256 | l >> 16 & 255], n ^= S[512 | l >> 8 & 255], n += S[768 | l & 255], r ^= n ^ P[9], n = S[r >>> 24], n += S[256 | r >> 16 & 255], n ^= S[512 | r >> 8 & 255], n += S[768 | r & 255], l ^= n ^ P[10], n = S[l >>> 24], n += S[256 | l >> 16 & 255], n ^= S[512 | l >> 8 & 255], n += S[768 | l & 255], r ^= n ^ P[11], n = S[r >>> 24], n += S[256 | r >> 16 & 255], n ^= S[512 | r >> 8 & 255], n += S[768 | r & 255], l ^= n ^ P[12], n = S[l >>> 24], n += S[256 | l >> 16 & 255], n ^= S[512 | l >> 8 & 255], n += S[768 | l & 255], r ^= n ^ P[13], n = S[r >>> 24], n += S[256 | r >> 16 & 255], n ^= S[512 | r >> 8 & 255], n += S[768 | r & 255], l ^= n ^ P[14], n = S[l >>> 24], n += S[256 | l >> 16 & 255], n ^= S[512 | l >> 8 & 255], n += S[768 | l & 255], r ^= n ^ P[15], n = S[r >>> 24], n += S[256 | r >> 16 & 255], n ^= S[512 | r >> 8 & 255], n += S[768 | r & 255], l ^= n ^ P[16], lr[off] = r ^ P[BLOWFISH_NUM_ROUNDS + 1], lr[off + 1] = l, lr;
}
function _streamtoword(data, offp) {
  for (var i = 0, word = 0; i < 4; ++i)
    word = word << 8 | data[offp] & 255, offp = (offp + 1) % data.length;
  return { key: word, offp };
}
function _key(key, P, S) {
  for (var offset = 0, lr = [0, 0], plen = P.length, slen = S.length, sw, i = 0; i < plen; i++)
    sw = _streamtoword(key, offset), offset = sw.offp, P[i] = P[i] ^ sw.key;
  for (i = 0; i < plen; i += 2)
    lr = _encipher(lr, 0, P, S), P[i] = lr[0], P[i + 1] = lr[1];
  for (i = 0; i < slen; i += 2)
    lr = _encipher(lr, 0, P, S), S[i] = lr[0], S[i + 1] = lr[1];
}
function _ekskey(data, key, P, S) {
  for (var offp = 0, lr = [0, 0], plen = P.length, slen = S.length, sw, i = 0; i < plen; i++)
    sw = _streamtoword(key, offp), offp = sw.offp, P[i] = P[i] ^ sw.key;
  for (offp = 0, i = 0; i < plen; i += 2)
    sw = _streamtoword(data, offp), offp = sw.offp, lr[0] ^= sw.key, sw = _streamtoword(data, offp), offp = sw.offp, lr[1] ^= sw.key, lr = _encipher(lr, 0, P, S), P[i] = lr[0], P[i + 1] = lr[1];
  for (i = 0; i < slen; i += 2)
    sw = _streamtoword(data, offp), offp = sw.offp, lr[0] ^= sw.key, sw = _streamtoword(data, offp), offp = sw.offp, lr[1] ^= sw.key, lr = _encipher(lr, 0, P, S), S[i] = lr[0], S[i + 1] = lr[1];
}
function _crypt(b, salt, rounds, callback, progressCallback) {
  var cdata = C_ORIG.slice(), clen = cdata.length, err;
  if (rounds < 4 || rounds > 31)
    if (err = Error("Illegal number of rounds (4-31): " + rounds), callback) {
      nextTick(callback.bind(this, err));
      return;
    } else throw err;
  if (salt.length !== BCRYPT_SALT_LEN)
    if (err = Error(
      "Illegal salt length: " + salt.length + " != " + BCRYPT_SALT_LEN
    ), callback) {
      nextTick(callback.bind(this, err));
      return;
    } else throw err;
  rounds = 1 << rounds >>> 0;
  var P, S, i = 0, j;
  typeof Int32Array == "function" ? (P = new Int32Array(P_ORIG), S = new Int32Array(S_ORIG)) : (P = P_ORIG.slice(), S = S_ORIG.slice()), _ekskey(salt, b, P, S);
  function next() {
    if (progressCallback && progressCallback(i / rounds), i < rounds)
      for (var start = Date.now(); i < rounds && (i = i + 1, _key(b, P, S), _key(salt, P, S), !(Date.now() - start > MAX_EXECUTION_TIME)); )
        ;
    else {
      for (i = 0; i < 64; i++)
        for (j = 0; j < clen >> 1; j++) _encipher(cdata, j << 1, P, S);
      var ret = [];
      for (i = 0; i < clen; i++)
        ret.push((cdata[i] >> 24 & 255) >>> 0), ret.push((cdata[i] >> 16 & 255) >>> 0), ret.push((cdata[i] >> 8 & 255) >>> 0), ret.push((cdata[i] & 255) >>> 0);
      if (callback) {
        callback(null, ret);
        return;
      } else return ret;
    }
    callback && nextTick(next);
  }
  if (typeof callback < "u")
    next();
  else
    for (var res; ; ) if (typeof (res = next()) < "u") return res || [];
}
function _hash(password, salt, callback, progressCallback) {
  var err;
  if (typeof password != "string" || typeof salt != "string")
    if (err = Error("Invalid string / salt: Not a string"), callback) {
      nextTick(callback.bind(this, err));
      return;
    } else throw err;
  var minor, offset;
  if (salt.charAt(0) !== "$" || salt.charAt(1) !== "2")
    if (err = Error("Invalid salt version: " + salt.substring(0, 2)), callback) {
      nextTick(callback.bind(this, err));
      return;
    } else throw err;
  if (salt.charAt(2) === "$") minor = "\0", offset = 3;
  else {
    if (minor = salt.charAt(2), minor !== "a" && minor !== "b" && minor !== "y" || salt.charAt(3) !== "$")
      if (err = Error("Invalid salt revision: " + salt.substring(2, 4)), callback) {
        nextTick(callback.bind(this, err));
        return;
      } else throw err;
    offset = 4;
  }
  if (salt.charAt(offset + 2) > "$")
    if (err = Error("Missing salt rounds"), callback) {
      nextTick(callback.bind(this, err));
      return;
    } else throw err;
  var r1 = parseInt(salt.substring(offset, offset + 1), 10) * 10, r2 = parseInt(salt.substring(offset + 1, offset + 2), 10), rounds = r1 + r2, real_salt = salt.substring(offset + 3, offset + 25);
  password += minor >= "a" ? "\0" : "";
  var passwordb = utf8Array(password), saltb = base64_decode(real_salt, BCRYPT_SALT_LEN);
  function finish2(bytes) {
    var res = [];
    return res.push("$2"), minor >= "a" && res.push(minor), res.push("$"), rounds < 10 && res.push("0"), res.push(rounds.toString()), res.push("$"), res.push(base64_encode(saltb, saltb.length)), res.push(base64_encode(bytes, C_ORIG.length * 4 - 1)), res.join("");
  }
  if (typeof callback > "u")
    return finish2(_crypt(passwordb, saltb, rounds));
  _crypt(
    passwordb,
    saltb,
    rounds,
    function(err2, bytes) {
      err2 ? callback(err2, null) : callback(null, finish2(bytes));
    },
    progressCallback
  );
}
function encodeBase64(bytes, length) {
  return base64_encode(bytes, length);
}
function decodeBase64(string, length) {
  return base64_decode(string, length);
}
var bcryptjs_default = {
  setRandomFallback,
  genSaltSync,
  genSalt,
  hashSync,
  hash,
  compareSync,
  compare,
  getRounds,
  getSalt,
  truncates,
  encodeBase64,
  decodeBase64
};

// server/auth.ts
init_db();

// server/ledger.ts
init_db();

// server/util.ts
var import_node_crypto = __toESM(require("node:crypto")), randInt = (n) => import_node_crypto.default.randomInt(n);
function shuffle(a) {
  for (let i = a.length - 1; i > 0; i--) {
    let j = import_node_crypto.default.randomInt(i + 1);
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}
var RANKS = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"], SUITS = ["S", "H", "D", "C"];
function newDecks(n) {
  let d = [];
  for (let k = 0; k < n; k++) for (let s of SUITS) for (let r of RANKS) d.push(r + s);
  return shuffle(d);
}
var rankOf = (c) => c.slice(0, -1), token = (bytes = 32) => import_node_crypto.default.randomBytes(bytes).toString("base64url"), sha256 = (s) => import_node_crypto.default.createHash("sha256").update(s).digest("hex"), HttpError = class extends Error {
  constructor(status, message, code) {
    super(message);
    this.status = status;
    this.code = code;
  }
}, bad = (msg, code) => new HttpError(400, msg, code);
function send(res, status, body, headers = {}) {
  let data = typeof body == "string" || Buffer.isBuffer(body) ? body : JSON.stringify(body);
  res.writeHead(status, {
    "content-type": typeof body == "string" || Buffer.isBuffer(body) ? "text/plain; charset=utf-8" : "application/json; charset=utf-8",
    "cache-control": "no-store",
    ...headers
  }), res.end(data);
}
async function readJson(req, limit = 6e5) {
  return new Promise((resolve, reject) => {
    let size = 0, chunks = [];
    req.on("data", (c) => {
      size += c.length, size > limit ? (reject(new HttpError(413, "Request too large")), req.destroy()) : chunks.push(c);
    }), req.on("end", () => {
      if (!chunks.length) return resolve({});
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")));
      } catch {
        reject(bad("Invalid JSON"));
      }
    }), req.on("error", reject);
  });
}
function parseCookies(h) {
  let out = {};
  if (!h) return out;
  for (let part of h.split(";")) {
    let i = part.indexOf("=");
    if (i < 0) continue;
    let k = part.slice(0, i).trim(), v = part.slice(i + 1).trim();
    try {
      out[k] = decodeURIComponent(v);
    } catch {
      out[k] = v;
    }
  }
  return out;
}
function clientIp(req) {
  return String(req.headers["x-forwarded-for"] || "").split(",")[0].trim() || req.socket.remoteAddress || "unknown";
}
function int(v, name, min, max) {
  let n = Number(v);
  if (!Number.isFinite(n) || !Number.isInteger(n)) throw bad(`${name} must be a whole number`);
  if (n < min || n > max) throw bad(`${name} must be between ${min.toLocaleString("en-IN")} and ${max.toLocaleString("en-IN")}`);
  return n;
}
function str(v, name, min, max) {
  if (typeof v != "string") throw bad(`${name} is required`);
  let s = v.trim();
  if (s.length < min || s.length > max) throw bad(`${name} must be ${min}\u2013${max} characters`);
  return s;
}
var buckets = /* @__PURE__ */ new Map();
function rateLimit(key, max, windowMs) {
  let now = Date.now(), b = buckets.get(key);
  if (!b || now > b.reset) {
    buckets.set(key, { n: 1, reset: now + windowMs });
    return;
  }
  if (b.n++, b.n > max) throw new HttpError(429, "Too many attempts. Please wait a moment and try again.");
}
setInterval(() => {
  let now = Date.now();
  for (let [k, b] of buckets) now > b.reset && buckets.delete(k);
}, 6e4).unref();
var fmt = (n) => Math.round(n).toLocaleString("en-IN");

// server/ledger.ts
async function post(c, a) {
  let wagered = Math.max(0, Math.floor(a.wagered || 0)), won = Math.max(0, Math.floor(a.won || 0)), credit = Math.max(0, Math.floor(a.credit || 0));
  if (a.idemKey && (await c.query("SELECT * FROM transactions WHERE user_id=$1 AND idem_key=$2", [a.userId, a.idemKey])).rows[0])
    throw new HttpError(409, "This action was already processed", "DUPLICATE");
  let w = await c.query("SELECT balance FROM wallets WHERE user_id=$1 FOR UPDATE", [a.userId]);
  if (!w.rows[0]) throw new HttpError(404, "Wallet not found");
  let opening = Number(w.rows[0].balance), net = a.type === "welcome_bonus" || a.type === "promotional" || a.type === "refund" ? credit : won - wagered, closing = opening + net;
  if (closing < 0) throw new HttpError(400, "Not enough credits for this bet", "INSUFFICIENT");
  let r = await c.query(
    `INSERT INTO transactions (user_id, game, type, opening, wagered, won, net, closing, status, round_id, detail, idem_key)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING *`,
    [
      a.userId,
      a.game || null,
      a.type,
      opening,
      wagered,
      won,
      net,
      closing,
      a.type === "refund" ? "refunded" : "completed",
      a.roundId || null,
      a.detail || null,
      a.idemKey || null
    ]
  );
  return await c.query("UPDATE wallets SET balance=$2, updated_at=now() WHERE user_id=$1", [a.userId, closing]), r.rows[0];
}
function where(userId, f) {
  let cond = [], p = [];
  return userId != null && (p.push(userId), cond.push(`t.user_id = $${p.length}`)), f.game && (p.push(f.game), cond.push(`t.game = $${p.length}`)), f.type && (p.push(f.type), cond.push(`t.type = $${p.length}`)), f.from && (p.push(f.from), cond.push(`t.created_at >= ($${p.length}::date AT TIME ZONE 'Asia/Kolkata')`)), f.to && (p.push(f.to), cond.push(`t.created_at < (($${p.length}::date + 1) AT TIME ZONE 'Asia/Kolkata')`)), f.search && (p.push("%" + f.search.replace(/[%_\\]/g, (m) => "\\" + m) + "%"), cond.push(`(t.txn_ref ILIKE $${p.length} OR coalesce(t.detail,'') ILIKE $${p.length} OR coalesce(t.game,'') ILIKE $${p.length} OR t.type ILIKE $${p.length})`)), { sql: cond.length ? "WHERE " + cond.join(" AND ") : "", params: p };
}
async function listLedger(userId, f) {
  let { sql, params } = where(userId, f), limit = Math.min(Math.max(Number(f.limit) || 50, 1), 5e3), offset = Math.max(Number(f.offset) || 0, 0), order = f.sort === "asc" ? "ASC" : "DESC", rows = await q(
    `SELECT t.*, u.username FROM transactions t JOIN users u ON u.id=t.user_id ${sql} ORDER BY t.id ${order} LIMIT ${limit} OFFSET ${offset}`,
    params
  ), tot = await q1(
    `SELECT count(*)::bigint AS count,
            coalesce(sum(t.wagered),0)::bigint AS spent,
            coalesce(sum(t.won),0)::bigint + coalesce(sum(CASE WHEN t.type IN ('welcome_bonus','promotional','refund') THEN t.net ELSE 0 END),0)::bigint AS earned,
            coalesce(sum(t.net),0)::bigint AS net
       FROM transactions t ${sql}`,
    params
  );
  return { rows, total: Number(tot.count), totals: { earned: Number(tot.earned), spent: Number(tot.spent), net: Number(tot.net) } };
}
async function ledgerSummary(userId, period, f) {
  let { sql, params } = where(userId, f);
  return await q(
    `SELECT to_char(date_trunc('${period}', t.created_at AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD') AS period,
            count(*)::bigint AS count,
            sum(t.wagered)::bigint AS spent,
            (sum(t.won) + sum(CASE WHEN t.type IN ('welcome_bonus','promotional','refund') THEN t.net ELSE 0 END))::bigint AS earned,
            sum(t.net)::bigint AS net,
            (array_agg(t.opening ORDER BY t.id ASC))[1] AS opening,
            (array_agg(t.closing ORDER BY t.id DESC))[1] AS closing
       FROM transactions t ${sql}
      GROUP BY 1 ORDER BY 1 DESC LIMIT 120`,
    params
  );
}
async function verifyLedger(userId) {
  let params = [], filter = "";
  userId && (params.push(userId), filter = "WHERE user_id = $1");
  let breaks = await q(
    `SELECT user_id, id, opening, prev_closing FROM (
        SELECT user_id, id, opening, lag(closing) OVER (PARTITION BY user_id ORDER BY id) AS prev_closing
          FROM transactions ${filter}) s
      WHERE prev_closing IS NOT NULL AND prev_closing <> opening LIMIT 20`,
    params
  ), mismatch = await q(
    `SELECT w.user_id, w.balance, t.closing FROM wallets w
       LEFT JOIN LATERAL (SELECT closing FROM transactions WHERE user_id=w.user_id ORDER BY id DESC LIMIT 1) t ON true
      ${userId ? "WHERE w.user_id=$1" : ""}
      ${userId ? "AND" : "WHERE"} coalesce(t.closing,0) <> w.balance LIMIT 20`,
    params
  );
  return { ok: !breaks.length && !mismatch.length, chainBreaks: breaks, walletMismatches: mismatch };
}

// server/auth.ts
var WELCOME_BONUS = Number(process.env.WELCOME_BONUS || 1e4), DAILY_BONUS = Number(process.env.DAILY_BONUS || 1e3), COOKIE = "r21s", SHORT = 12 * 36e5, LONG = 720 * 36e5, DUMMY = bcryptjs_default.hashSync("timing-equaliser", 11), USERNAME_RE = /^[A-Za-z0-9_.]{3,20}$/, EMAIL_RE = /^[^\s@]{1,64}@[^\s@]{1,190}\.[^\s@]{2,24}$/;
function validatePassword(p) {
  if (typeof p != "string" || p.length < 8) throw bad("Password must be at least 8 characters");
  if (p.length > 128) throw bad("Password is too long");
  if (!/[A-Za-z]/.test(p) || !/[0-9]/.test(p)) throw bad("Password must include letters and numbers");
}
function cookieHeader(value, maxAgeMs, secure) {
  let parts = [`${COOKIE}=${value}`, "Path=/", "HttpOnly", "SameSite=Lax"];
  return secure && parts.push("Secure"), maxAgeMs !== null && parts.push(`Max-Age=${Math.floor(maxAgeMs / 1e3)}`), parts.join("; ");
}
var isSecure = (req) => String(req.headers["x-forwarded-proto"] || "").startsWith("https");
async function createSession(userId, remember, req) {
  let t = token(32), ttl = remember ? LONG : SHORT;
  return await q(
    "INSERT INTO sessions (token_hash, user_id, expires_at, user_agent, ip) VALUES ($1,$2,now() + ($3 || ' milliseconds')::interval,$4,$5)",
    [sha256(t), userId, String(ttl), String(req.headers["user-agent"] || "").slice(0, 200), clientIp(req)]
  ), cookieHeader(t, remember ? ttl : null, isSecure(req));
}
async function userFromRequest(req) {
  let t = parseCookies(req.headers.cookie)[COOKIE];
  if (!t || t.length > 100) return null;
  let u = await q1(
    `SELECT u.id, u.username, u.email, u.role, u.status, u.created_at, u.last_daily_bonus_at
       FROM sessions s JOIN users u ON u.id = s.user_id
      WHERE s.token_hash = $1 AND s.expires_at > now()`,
    [sha256(t)]
  );
  return !u || u.status !== "active" ? null : (q("UPDATE users SET last_seen_at = now() WHERE id = $1 AND (last_seen_at IS NULL OR last_seen_at < now() - interval '1 minute')", [u.id]).catch(() => {
  }), u);
}
async function register(req, body) {
  rateLimit("reg:" + clientIp(req), 8, 36e5);
  let username = str(body.username, "Username", 3, 20);
  if (!USERNAME_RE.test(username)) throw bad("Username can use letters, numbers, dot and underscore (3\u201320 characters)");
  let email = str(body.email, "Email", 5, 254).toLowerCase();
  if (!EMAIL_RE.test(email)) throw bad("Please enter a valid email address");
  if (validatePassword(body.password), body.confirm !== void 0 && body.confirm !== body.password) throw bad("Passwords do not match");
  let hash2 = await bcryptjs_default.hash(body.password, 11), adminEmail = String(process.env.ADMIN_EMAIL || "").trim().toLowerCase(), user = await withTx(async (c) => {
    let exists = await c.query("SELECT username, email FROM users WHERE username = $1 OR email = $2", [username, email]);
    if (exists.rows.some((r) => r.username.toLowerCase() === username.toLowerCase())) throw bad("That username is taken");
    if (exists.rows.length) throw bad("An account with this email already exists");
    let role = adminEmail && email === adminEmail ? "admin" : "player", u = (await c.query("INSERT INTO users (username, email, password_hash, role, last_login_at) VALUES ($1,$2,$3,$4,now()) RETURNING id, username, email, role", [username, email, hash2, role])).rows[0];
    return await c.query("INSERT INTO profiles (user_id, display_name) VALUES ($1,$2)", [u.id, username]), await c.query("INSERT INTO wallets (user_id, balance) VALUES ($1, 0)", [u.id]), await post(c, { userId: u.id, type: "welcome_bonus", credit: WELCOME_BONUS, detail: "Welcome to ROYALE21CASINO", idemKey: "welcome" }), u;
  }), cookie = await createSession(user.id, !!body.remember, req);
  return { user, cookie };
}
async function login(req, body) {
  let ip = clientIp(req), ident = str(body.identifier ?? body.username ?? body.email, "Username or email", 3, 254);
  rateLimit("login:" + ip, 20, 10 * 6e4), rateLimit("login-id:" + ident.toLowerCase(), 8, 10 * 6e4);
  let u = await q1("SELECT id, username, email, role, status, password_hash FROM users WHERE username = $1 OR email = $1", [ident]), ok = u ? await bcryptjs_default.compare(String(body.password || ""), u.password_hash) : (await bcryptjs_default.compare("x", DUMMY), !1);
  if (!u || !ok) throw new HttpError(401, "Incorrect username/email or password");
  if (u.status !== "active") throw new HttpError(403, "This account has been suspended. Contact support.");
  await q("UPDATE users SET last_login_at = now() WHERE id = $1", [u.id]);
  let cookie = await createSession(u.id, !!body.remember, req);
  return { user: { id: u.id, username: u.username, email: u.email, role: u.role }, cookie };
}
async function logout(req) {
  let t = parseCookies(req.headers.cookie)[COOKIE];
  return t && await q("DELETE FROM sessions WHERE token_hash = $1", [sha256(t)]), cookieHeader("", 0, isSecure(req));
}
async function logoutAll(userId, req) {
  return await q("DELETE FROM sessions WHERE user_id = $1", [userId]), cookieHeader("", 0, isSecure(req));
}
async function changePassword(userId, body) {
  let u = await q1("SELECT password_hash FROM users WHERE id = $1", [userId]);
  if (!u || !await bcryptjs_default.compare(String(body.current || ""), u.password_hash)) throw bad("Current password is incorrect");
  if (validatePassword(body.password), body.confirm !== void 0 && body.confirm !== body.password) throw bad("Passwords do not match");
  await q("UPDATE users SET password_hash = $2 WHERE id = $1", [userId, await bcryptjs_default.hash(body.password, 11)]);
}
async function createResetToken(userId, by) {
  let t = token(32);
  return await q("INSERT INTO password_resets (token_hash, user_id, expires_at, created_by) VALUES ($1,$2,now() + interval '1 hour',$3)", [sha256(t), userId, by]), t;
}
async function requestReset(req, body, origin2) {
  rateLimit("reset:" + clientIp(req), 5, 36e5);
  let email = str(body.email, "Email", 5, 254).toLowerCase(), u = await q1("SELECT id, username, email FROM users WHERE email = $1 AND status = 'active'", [email]), mailReady = !!(process.env.RESEND_API_KEY && process.env.MAIL_FROM);
  if (u && mailReady) {
    let t = await createResetToken(u.id, "user"), link = `${origin2}/reset-password?token=${t}`;
    await sendMail(u.email, "Reset your ROYALE21CASINO password", resetEmail(u.username, link)).catch((e) => console.error("mail failed", e.message));
  }
  return { mailReady };
}
async function completeReset(body) {
  let t = str(body.token, "Reset link", 10, 100);
  if (validatePassword(body.password), body.confirm !== void 0 && body.confirm !== body.password) throw bad("Passwords do not match");
  await withTx(async (c) => {
    let r = (await c.query("SELECT user_id FROM password_resets WHERE token_hash = $1 AND used_at IS NULL AND expires_at > now() FOR UPDATE", [sha256(t)])).rows[0];
    if (!r) throw bad("This reset link has expired or was already used. Please request a new one.");
    await c.query("UPDATE password_resets SET used_at = now() WHERE token_hash = $1", [sha256(t)]), await c.query("UPDATE users SET password_hash = $2 WHERE id = $1", [r.user_id, await bcryptjs_default.hash(body.password, 11)]), await c.query("DELETE FROM sessions WHERE user_id = $1", [r.user_id]);
  });
}
async function sendMail(to, subject, html) {
  let r = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: { Authorization: `Bearer ${process.env.RESEND_API_KEY}`, "Content-Type": "application/json" },
    body: JSON.stringify({ from: process.env.MAIL_FROM, to: [to], subject, html })
  });
  if (!r.ok) throw new Error("Resend " + r.status + " " + (await r.text()).slice(0, 200));
}
function resetEmail(name, link) {
  return `<div style="background:#080808;padding:40px 20px;font-family:Georgia,serif;color:#F5E6BE">
  <div style="max-width:480px;margin:0 auto;border:1px solid #6b5520;border-radius:14px;padding:32px;background:#151515">
    <div style="font-size:22px;letter-spacing:6px;color:#D4AF37;text-align:center">ROYALE21CASINO</div>
    <p style="font-family:Arial,sans-serif;font-size:15px;line-height:1.6;color:#e9e1cf;margin-top:24px">Hello ${name.replace(/[<>&]/g, "")},</p>
    <p style="font-family:Arial,sans-serif;font-size:15px;line-height:1.6;color:#e9e1cf">We received a request to reset your password. This link is valid for one hour.</p>
    <p style="text-align:center;margin:28px 0"><a href="${link}" style="background:#D4AF37;color:#1a1204;text-decoration:none;padding:13px 26px;border-radius:10px;font-family:Arial,sans-serif;font-weight:bold">Reset password</a></p>
    <p style="font-family:Arial,sans-serif;font-size:13px;color:#9a917e">If you didn't ask for this, you can ignore this email.</p>
  </div></div>`;
}
async function claimDaily(userId) {
  return withTx(async (c) => {
    let u = (await c.query("SELECT last_daily_bonus_at FROM users WHERE id = $1 FOR UPDATE", [userId])).rows[0], next = (u.last_daily_bonus_at ? new Date(u.last_daily_bonus_at).getTime() : 0) + 24 * 36e5;
    if (Date.now() < next) throw new HttpError(400, "Your daily bonus is not ready yet", "NOT_READY");
    return await c.query("UPDATE users SET last_daily_bonus_at = now() WHERE id = $1", [userId]), post(c, { userId, type: "promotional", credit: DAILY_BONUS, detail: "Daily bonus" });
  });
}

// server/settings.ts
init_db();
var GAMES = ["slots", "baccarat", "roulette", "crash", "blackjack", "poker"], GAME_NAMES = {
  slots: "Slots",
  baccarat: "Baccarat",
  roulette: "Roulette",
  crash: "Crash",
  blackjack: "Blackjack",
  poker: "Poker"
}, DEFAULTS = {
  slots: { enabled: !0, minBet: 10, maxBet: 5e4 },
  baccarat: { enabled: !0, minBet: 10, maxBet: 1e5 },
  roulette: { enabled: !0, minBet: 10, maxBet: 1e5 },
  crash: { enabled: !0, minBet: 10, maxBet: 5e4 },
  blackjack: { enabled: !0, minBet: 10, maxBet: 5e4 },
  poker: { enabled: !0, minBet: 500, maxBet: 2e5 }
  // table buy-in range
}, cache = null;
async function gameSettings() {
  if (cache) return cache;
  let v = (await q1("SELECT value FROM settings WHERE key = $1", ["games"]))?.value || {}, out = {};
  for (let g of GAMES) out[g] = { ...DEFAULTS[g], ...v[g] || {} };
  return cache = out, out;
}
async function saveGameSettings(next) {
  let cur = await gameSettings();
  for (let g of GAMES) {
    let n = next[g];
    if (!n) continue;
    let minBet = Math.max(1, Math.floor(Number(n.minBet ?? cur[g].minBet))), maxBet = Math.max(minBet, Math.floor(Number(n.maxBet ?? cur[g].maxBet)));
    cur[g] = { enabled: n.enabled ?? cur[g].enabled, minBet, maxBet };
  }
  return await q("INSERT INTO settings (key, value) VALUES ($1,$2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", ["games", JSON.stringify(cur)]), cache = cur, cur;
}
async function checkBet(game, bet) {
  let s = (await gameSettings())[game];
  if (!s.enabled) throw new HttpError(403, `${GAME_NAMES[game]} is temporarily closed`, "GAME_CLOSED");
  if (!Number.isInteger(bet) || bet < s.minBet) throw new HttpError(400, `Minimum bet is ${s.minBet.toLocaleString("en-IN")}`);
  if (bet > s.maxBet) throw new HttpError(400, `Maximum bet is ${s.maxBet.toLocaleString("en-IN")}`);
}

// server/games/instant.ts
init_db();
async function instantRound(userId, game, bet, idem, play) {
  if (await checkBet(game, bet), typeof idem != "string" || idem.length < 8 || idem.length > 64) throw bad("Missing request id");
  return withTx(async (c) => {
    let round = (await c.query("INSERT INTO game_rounds (user_id, game, bet) VALUES ($1,$2,$3) RETURNING id", [userId, game, bet])).rows[0], wager = await post(c, { userId, type: "game_wager", game, wagered: bet, roundId: round.id, idemKey: idem, detail: `${GAME_NAMES[game]} bet` }), { payout, result, detail } = play(), settle2 = await post(c, {
      userId,
      type: payout > 0 ? "game_win" : "game_loss",
      game,
      won: payout,
      roundId: round.id,
      idemKey: idem + ":settle",
      detail
    });
    return await c.query("UPDATE game_rounds SET status='settled', payout=$2, result=$3, settled_at=now() WHERE id=$1", [round.id, payout, JSON.stringify(result)]), { roundId: round.id, bet, payout, net: payout - bet, result, balance: settle2.closing, txns: [wager.txn_ref, settle2.txn_ref] };
  });
}
var SLOT_SYMBOLS = ["wild", "crown", "diamond", "gold", "ace", "champagne", "ring", "chip"], SLOT_PAYS = {
  wild: [50, 250, 1e3],
  crown: [25, 100, 500],
  diamond: [20, 75, 300],
  gold: [15, 50, 200],
  ace: [10, 30, 120],
  champagne: [8, 20, 80],
  ring: [5, 15, 60],
  chip: [4, 10, 40]
}, SLOT_WEIGHTS = { wild: 2, crown: 2, diamond: 3, gold: 3, ace: 4, champagne: 4, ring: 6, chip: 6 }, PAYLINES = [
  [1, 1, 1, 1, 1],
  [0, 0, 0, 0, 0],
  [2, 2, 2, 2, 2],
  [0, 1, 2, 1, 0],
  [2, 1, 0, 1, 2],
  [0, 0, 1, 2, 2],
  [2, 2, 1, 0, 0],
  [1, 0, 0, 0, 1],
  [1, 2, 2, 2, 1],
  [0, 1, 1, 1, 0]
], STRIP = (() => {
  let s = [];
  for (let k of SLOT_SYMBOLS) for (let i = 0; i < SLOT_WEIGHTS[k]; i++) s.push(k);
  return s;
})();
function slotSpin(totalBet, rnd = randInt) {
  let grid = [];
  for (let r = 0; r < 5; r++) {
    let stop = rnd(STRIP.length);
    grid.push([0, 1, 2].map((k) => STRIP[(stop + k) % STRIP.length]));
  }
  let lineBet = totalBet / 10, wins = [];
  PAYLINES.forEach((pl, li) => {
    let syms = pl.map((row, reel) => grid[reel][row]), base = null, count = 0;
    for (let s of syms) {
      if (s === "wild") {
        count++;
        continue;
      }
      if (base === null) {
        base = s, count++;
        continue;
      }
      if (s === base) {
        count++;
        continue;
      }
      break;
    }
    let wildRun = 0;
    for (let s of syms)
      if (s === "wild") wildRun++;
      else break;
    let best2 = 0, bestSym = base || "wild", bestCount = count;
    if (count >= 3 && (best2 = SLOT_PAYS[base || "wild"][count - 3] * lineBet), wildRun >= 3) {
      let w = SLOT_PAYS.wild[wildRun - 3] * lineBet;
      w > best2 && (best2 = w, bestSym = "wild", bestCount = wildRun);
    }
    best2 > 0 && wins.push({ line: li, symbol: bestSym, count: bestCount, pay: Math.floor(best2) });
  });
  let payout = wins.reduce((a, w) => a + w.pay, 0);
  return { grid, wins, payout };
}
async function playSlots(userId, body) {
  let bet = int(body.bet, "Bet", 10, 1e7);
  if (bet % 10 !== 0) throw bad("Slots bets must be a multiple of 10 (10 paylines)");
  return instantRound(userId, "slots", bet, body.idem, () => {
    let s = slotSpin(bet), top = s.wins.sort((a, b) => b.pay - a.pay)[0];
    return {
      payout: s.payout,
      result: s,
      detail: s.payout > 0 ? `Slots \xB7 ${s.wins.length} winning line${s.wins.length > 1 ? "s" : ""}${top ? ` \xB7 ${top.count}\xD7 ${top.symbol}` : ""}` : "Slots \xB7 no win"
    };
  });
}
var RED = /* @__PURE__ */ new Set([1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36]);
function rouletteWins(b, n) {
  let k = b.kind;
  if (k === "straight") return b.value === n ? 36 : 0;
  if (n === 0) return 0;
  switch (k) {
    case "red":
      return RED.has(n) ? 2 : 0;
    case "black":
      return RED.has(n) ? 0 : 2;
    case "odd":
      return n % 2 === 1 ? 2 : 0;
    case "even":
      return n % 2 === 0 ? 2 : 0;
    case "low":
      return n <= 18 ? 2 : 0;
    case "high":
      return n >= 19 ? 2 : 0;
    case "dozen":
      return Math.ceil(n / 12) === b.value ? 3 : 0;
    case "column":
      return (n - 1) % 3 + 1 === b.value ? 3 : 0;
  }
  return 0;
}
function validateRouletteBets(raw) {
  if (!Array.isArray(raw) || !raw.length) throw bad("Place at least one bet");
  if (raw.length > 60) throw bad("Too many separate bets");
  let kinds = ["straight", "red", "black", "odd", "even", "low", "high", "dozen", "column"];
  return raw.map((b) => {
    if (!b || !kinds.includes(b.kind)) throw bad("Unknown bet type");
    let amount = int(b.amount, "Chip amount", 1, 1e7);
    return b.kind === "straight" ? { kind: "straight", value: int(b.value, "Number", 0, 36), amount } : b.kind === "dozen" || b.kind === "column" ? { kind: b.kind, value: int(b.value, b.kind, 1, 3), amount } : { kind: b.kind, amount };
  });
}
function rouletteSpin(bets2, n = randInt(37)) {
  let lines = bets2.map((b) => ({ ...b, pay: b.amount * rouletteWins(b, n) }));
  return { number: n, color: n === 0 ? "green" : RED.has(n) ? "red" : "black", lines, payout: lines.reduce((a, l) => a + l.pay, 0) };
}
async function playRoulette(userId, body) {
  let bets2 = validateRouletteBets(body.bets), total2 = bets2.reduce((a, b) => a + b.amount, 0);
  return instantRound(userId, "roulette", total2, body.idem, () => {
    let r = rouletteSpin(bets2);
    return { payout: r.payout, result: r, detail: `Roulette \xB7 ${r.number} ${r.color} \xB7 ${bets2.length} bet${bets2.length > 1 ? "s" : ""}` };
  });
}
var bv = (c) => {
  let r = rankOf(c);
  return r === "A" ? 1 : ["10", "J", "Q", "K"].includes(r) ? 0 : Number(r);
}, bTotal = (h) => h.reduce((a, c) => a + bv(c), 0) % 10;
function baccaratDeal(deck = newDecks(8)) {
  let draw = () => deck.pop(), P = [draw()], B = [draw()];
  P.push(draw()), B.push(draw());
  let pt = bTotal(P), bt = bTotal(B);
  if (pt < 8 && bt < 8) {
    let p3 = null;
    pt <= 5 && (p3 = draw(), P.push(p3));
    let bNow = bTotal(B), bankerDraws = !1;
    if (p3 === null) bankerDraws = bNow <= 5;
    else {
      let v = bv(p3);
      bNow <= 2 ? bankerDraws = !0 : bNow === 3 ? bankerDraws = v !== 8 : bNow === 4 ? bankerDraws = v >= 2 && v <= 7 : bNow === 5 ? bankerDraws = v >= 4 && v <= 7 : bNow === 6 && (bankerDraws = v === 6 || v === 7);
    }
    bankerDraws && B.push(draw());
  }
  let p = bTotal(P), b = bTotal(B);
  return { player: P, banker: B, playerTotal: p, bankerTotal: b, winner: p > b ? "player" : b > p ? "banker" : "tie", natural: P.length === 2 && B.length === 2 && (p >= 8 || b >= 8) };
}
function baccaratPayout(bets2, winner) {
  let pay = 0;
  return winner === "player" && (pay += bets2.player * 2), winner === "banker" && (pay += bets2.banker + Math.floor(bets2.banker * 0.95)), winner === "tie" && (pay += bets2.tie * 9 + bets2.player + bets2.banker), pay;
}
async function playBaccarat(userId, body) {
  let bets2 = { player: int(body.bets?.player ?? 0, "Player bet", 0, 1e9), banker: int(body.bets?.banker ?? 0, "Banker bet", 0, 1e9), tie: int(body.bets?.tie ?? 0, "Tie bet", 0, 1e9) }, total2 = bets2.player + bets2.banker + bets2.tie;
  if (!total2) throw bad("Place a bet on Player, Banker or Tie");
  return instantRound(userId, "baccarat", total2, body.idem, () => {
    let d = baccaratDeal(), payout = baccaratPayout(bets2, d.winner), w = d.winner === "tie" ? "Tie" : d.winner === "player" ? "Player wins" : "Banker wins";
    return { payout, result: { ...d, bets: bets2 }, detail: `Baccarat \xB7 ${w} ${d.playerTotal}\u2013${d.bankerTotal}` };
  });
}

// server/games/blackjack.ts
init_db();
var cval = (r) => r === "A" ? 11 : ["J", "Q", "K", "10"].includes(r) ? 10 : Number(r);
function total(cards) {
  let t = 0, a = 0;
  for (let c of cards) {
    let r = rankOf(c);
    t += cval(r), r === "A" && a++;
  }
  for (; t > 21 && a; )
    t -= 10, a--;
  return t;
}
function soft(cards) {
  let t = 0, a = 0;
  for (let c of cards) {
    let r = rankOf(c);
    t += cval(r), r === "A" && a++;
  }
  for (; t > 21 && a; )
    t -= 10, a--;
  return a > 0 && t <= 21;
}
var isBJ = (cards) => cards.length === 2 && total(cards) === 21;
function publicView(roundId2, s, extra = {}) {
  let hide = s.phase === "player";
  return {
    roundId: roundId2,
    phase: s.phase,
    active: s.active,
    hands: s.hands.map((h) => ({ cards: h.cards, bet: h.bet, done: h.done, doubled: !!h.doubled, split: !!h.split, total: total(h.cards), soft: soft(h.cards), result: h.result, payout: h.payout })),
    dealer: hide ? [s.dealer[0], "XX"] : s.dealer,
    dealerTotal: total(hide ? [s.dealer[0]] : s.dealer),
    ...extra
  };
}
function nextActive(s) {
  let i = s.hands.findIndex((h) => !h.done);
  return i >= 0 ? (s.active = i, !1) : !0;
}
function settle(s) {
  if (s.hands.some((h) => total(h.cards) <= 21 && !(isBJ(h.cards) && !h.split))) for (; total(s.dealer) < 17; ) s.dealer.push(s.deck.pop());
  let d = total(s.dealer), dBJ = isBJ(s.dealer), pay = 0;
  for (let h of s.hands) {
    let v = total(h.cards), bj = isBJ(h.cards) && !h.split, p = 0, r;
    v > 21 ? r = "bust" : bj && dBJ ? (r = "push", p = h.bet) : bj ? (r = "blackjack", p = h.bet + Math.floor(h.bet * 1.5)) : dBJ ? r = "lose" : d > 21 || v > d ? (r = "win", p = h.bet * 2) : v === d ? (r = "push", p = h.bet) : r = "lose", h.result = r, h.payout = p, h.done = !0, pay += p;
  }
  return s.phase = "done", pay;
}
async function finish(c, userId, roundId2, s, idem) {
  let payout = settle(s), totalBet = s.hands.reduce((a, h) => a + h.bet, 0), results = s.hands.map((h) => h.result).join(" / "), tx = await post(c, { userId, type: payout > 0 ? "game_win" : "game_loss", game: "blackjack", won: payout, roundId: roundId2, idemKey: idem + ":settle", detail: `Blackjack \xB7 ${results} \xB7 dealer ${total(s.dealer) > 21 ? "bust" : total(s.dealer)}` });
  return await c.query(
    "UPDATE game_rounds SET status='settled', bet=$2, payout=$3, state=NULL, result=$4, settled_at=now() WHERE id=$1",
    [roundId2, totalBet, payout, JSON.stringify(publicView(roundId2, s))]
  ), { payout, balance: tx.closing, net: payout - totalBet };
}
async function bjStart(userId, body) {
  let bet = int(body.bet, "Bet", 1, 1e9);
  await checkBet("blackjack", bet);
  let idem = String(body.idem || "");
  if (idem.length < 8) throw bad("Missing request id");
  return withTx(async (c) => {
    if ((await c.query("SELECT id FROM game_rounds WHERE user_id=$1 AND game='blackjack' AND status='open' LIMIT 1", [userId])).rows[0]) throw new HttpError(409, "Finish your current hand first", "OPEN_ROUND");
    let round = (await c.query("INSERT INTO game_rounds (user_id, game, bet) VALUES ($1,'blackjack',$2) RETURNING id", [userId, bet])).rows[0], w = await post(c, { userId, type: "game_wager", game: "blackjack", wagered: bet, roundId: round.id, idemKey: idem, detail: "Blackjack bet" }), deck = newDecks(6), s = { deck, hands: [{ cards: [], bet, done: !1 }], dealer: [], active: 0, phase: "player" };
    s.hands[0].cards.push(deck.pop()), s.dealer.push(deck.pop()), s.hands[0].cards.push(deck.pop()), s.dealer.push(deck.pop());
    let balance = w.closing, extra = {};
    if (cval(rankOf(s.dealer[0])) >= 10 && isBJ(s.dealer) || isBJ(s.hands[0].cards)) {
      let f = await finish(c, userId, round.id, s, idem);
      balance = f.balance, extra = { payout: f.payout, net: f.net };
    } else
      await c.query("UPDATE game_rounds SET state=$2 WHERE id=$1", [round.id, JSON.stringify(s)]);
    return { ...publicView(round.id, s, extra), balance };
  });
}
async function bjAction(userId, body) {
  let roundId2 = int(body.roundId, "Round", 1, Number.MAX_SAFE_INTEGER), action = String(body.action);
  if (!["hit", "stand", "double", "split"].includes(action)) throw bad("Unknown action");
  let idem = String(body.idem || "");
  if (idem.length < 8) throw bad("Missing request id");
  return withTx(async (c) => {
    let row = (await c.query("SELECT * FROM game_rounds WHERE id=$1 AND user_id=$2 AND game='blackjack' FOR UPDATE", [roundId2, userId])).rows[0];
    if (!row || row.status !== "open" || !row.state) throw new HttpError(409, "This hand is already finished", "ROUND_CLOSED");
    let s = row.state, h = s.hands[s.active], balance;
    if (action === "hit") {
      if (h.splitAces) throw bad("Split aces receive one card only");
      h.cards.push(s.deck.pop()), total(h.cards) >= 21 && (h.done = !0);
    } else if (action === "stand")
      h.done = !0;
    else if (action === "double") {
      if (h.cards.length !== 2 || h.splitAces) throw bad("You can only double on your first two cards");
      balance = (await post(c, { userId, type: "game_wager", game: "blackjack", wagered: h.bet, roundId: roundId2, idemKey: idem, detail: "Blackjack double down" })).closing, h.bet *= 2, h.doubled = !0, h.cards.push(s.deck.pop()), h.done = !0;
    } else if (action === "split") {
      if (h.cards.length !== 2 || cval(rankOf(h.cards[0])) !== cval(rankOf(h.cards[1]))) throw bad("You can only split two cards of the same value");
      if (s.hands.length >= 4) throw bad("Maximum of four hands");
      balance = (await post(c, { userId, type: "game_wager", game: "blackjack", wagered: h.bet, roundId: roundId2, idemKey: idem, detail: "Blackjack split" })).closing;
      let aces = rankOf(h.cards[0]) === "A", nh = { cards: [h.cards[1]], bet: h.bet, done: !1, split: !0, splitAces: aces };
      h.cards = [h.cards[0], s.deck.pop()], h.split = !0, h.splitAces = aces, nh.cards.push(s.deck.pop()), s.hands.splice(s.active + 1, 0, nh), aces && (h.done = !0, nh.done = !0), total(h.cards) === 21 && (h.done = !0), total(nh.cards) === 21 && (nh.done = !0);
    }
    if (nextActive(s)) {
      let f = await finish(c, userId, roundId2, s, `${roundId2}:${row.bet}:${s.hands.length}`);
      return { ...publicView(roundId2, s, { payout: f.payout, net: f.net }), balance: f.balance };
    }
    return await c.query("UPDATE game_rounds SET state=$2, bet=$3 WHERE id=$1", [roundId2, JSON.stringify(s), s.hands.reduce((a, x) => a + x.bet, 0)]), balance === void 0 && (balance = Number((await c.query("SELECT balance FROM wallets WHERE user_id=$1", [userId])).rows[0].balance)), { ...publicView(roundId2, s), balance };
  });
}
async function bjCurrent(userId) {
  let { q1: q12 } = await Promise.resolve().then(() => (init_db(), db_exports)), row = await q12("SELECT id, state FROM game_rounds WHERE user_id=$1 AND game='blackjack' AND status='open' ORDER BY id DESC LIMIT 1", [userId]);
  return row && row.state ? publicView(row.id, row.state) : null;
}

// server/games/crash.ts
var import_node_crypto2 = __toESM(require("node:crypto"));
init_db();
var K = 6e-5, WAIT_MS = 7e3, CRASHED_MS = 3500, MAX_MULT = 1e3, multAt = (ms) => Math.floor(Math.exp(K * Math.max(0, ms)) * 100) / 100, msFor = (m) => Math.log(m) / K;
function crashPointFromSeed(seed2) {
  let h = import_node_crypto2.default.createHmac("sha256", seed2).update("royale21casino").digest("hex"), n = parseInt(h.slice(0, 13), 16);
  if (n % 101 === 0) return 1;
  let e = 2 ** 52, p = Math.floor((100 * e - n) / (e - n)) / 100;
  return Math.min(Math.max(1, p), MAX_MULT);
}
var clients = /* @__PURE__ */ new Set(), phase = "wait", roundId = 0, seed = "", seedHash = "", crashAt = 1, phaseStart = Date.now(), runStart = 0, bets = /* @__PURE__ */ new Map(), history = [], timer = null, busy = Promise.resolve(), serial = (fn) => busy = busy.then(fn).catch((e) => console.error("crash", e.message));
function publicBets() {
  return [...bets.values()].map((b) => ({ name: b.name, amount: b.amount, cashedAt: b.cashedAt ?? null, win: b.win ?? null })).sort((a, b) => b.amount - a.amount).slice(0, 60);
}
function snapshot(forUser) {
  let mine = forUser != null ? bets.get(forUser) : void 0;
  return {
    t: "state",
    phase,
    roundId,
    seedHash,
    now: Date.now(),
    waitEndsAt: phase === "wait" ? phaseStart + WAIT_MS : null,
    runStart: phase !== "wait" ? runStart : null,
    crashPoint: phase === "crashed" ? crashAt : null,
    history: history.slice(0, 24).map((h) => ({ id: h.id, point: h.point })),
    bets: publicBets(),
    my: mine ? { amount: mine.amount, auto: mine.auto, cashedAt: mine.cashedAt ?? null, win: mine.win ?? null } : null
  };
}
function broadcast(msg) {
  let s = JSON.stringify(msg);
  for (let c of clients) c.ws.readyState === 1 && c.ws.send(s);
}
function sendState() {
  for (let c of clients) c.ws.readyState === 1 && c.ws.send(JSON.stringify(snapshot(c.userId)));
}
function sendTo(userId, msg) {
  let s = JSON.stringify(msg);
  for (let c of clients) c.userId === userId && c.ws.readyState === 1 && c.ws.send(s);
}
async function newRound() {
  seed = import_node_crypto2.default.randomBytes(32).toString("hex"), seedHash = import_node_crypto2.default.createHash("sha256").update(seed).digest("hex"), crashAt = crashPointFromSeed(seed), roundId = (await q("INSERT INTO crash_rounds (seed_hash) VALUES ($1) RETURNING id", [seedHash]))[0].id, bets.clear(), phase = "wait", phaseStart = Date.now(), sendState(), schedule(WAIT_MS, startRun);
}
function schedule(ms, fn) {
  timer && clearTimeout(timer), timer = setTimeout(fn, ms);
}
function startRun() {
  serial(async () => {
    phase = "run", runStart = Date.now(), phaseStart = runStart, await q("UPDATE crash_rounds SET started_at=now() WHERE id=$1", [roundId]), sendState(), timer && clearTimeout(timer), timer = setInterval(() => serial(tick), 40);
  });
}
async function tick() {
  if (phase !== "run") return;
  let el = Date.now() - runStart, crashMs = msFor(crashAt);
  for (let b of bets.values())
    b.settled || !b.auto || b.auto <= crashAt && el >= msFor(b.auto) && await cashOut(b, b.auto);
  (el >= crashMs || crashAt <= 1) && await crash();
}
async function cashOut(b, at) {
  if (b.settled) return;
  b.settled = !0, b.cashedAt = at, b.win = Math.floor(b.amount * at);
  let tx = await withTx(async (c) => {
    let t = await post(c, { userId: b.userId, type: "game_win", game: "crash", won: b.win, roundId: b.roundRowId, idemKey: `crash:${roundId}:settle`, detail: `Crash \xB7 cashed out at ${at.toFixed(2)}\xD7` });
    return await c.query("UPDATE game_rounds SET status='settled', payout=$2, result=$3, settled_at=now() WHERE id=$1", [b.roundRowId, b.win, JSON.stringify({ crashRound: roundId, cashedAt: at, crashPoint: null })]), t;
  });
  broadcast({ t: "cashout", name: b.name, at, win: b.win }), sendTo(b.userId, { t: "you", cashedAt: at, win: b.win, balance: tx.closing });
}
async function crash() {
  timer && (clearInterval(timer), timer = null), phase = "crashed", phaseStart = Date.now();
  for (let b of bets.values()) {
    if (b.settled) continue;
    b.settled = !0, b.win = 0;
    let tx = await withTx(async (c) => {
      let t = await post(c, { userId: b.userId, type: "game_loss", game: "crash", won: 0, roundId: b.roundRowId, idemKey: `crash:${roundId}:settle`, detail: `Crash \xB7 crashed at ${crashAt.toFixed(2)}\xD7` });
      return await c.query("UPDATE game_rounds SET status='settled', payout=0, result=$2, settled_at=now() WHERE id=$1", [b.roundRowId, JSON.stringify({ crashRound: roundId, cashedAt: null, crashPoint: crashAt })]), t;
    });
    sendTo(b.userId, { t: "you", cashedAt: null, win: 0, balance: tx.closing });
  }
  await q("UPDATE crash_rounds SET seed=$2, crash_point=$3, ended_at=now() WHERE id=$1", [roundId, seed, crashAt]), history.unshift({ id: roundId, point: crashAt, seed }), history.length > 50 && history.pop(), broadcast({ t: "crash", roundId, point: crashAt, seed }), sendState(), schedule(CRASHED_MS, () => serial(newRound));
}
async function placeBet(cl, m) {
  if (!cl.userId) throw new HttpError(401, "Sign in to play");
  if (phase !== "wait") throw new HttpError(400, "Betting is closed for this round");
  if (bets.has(cl.userId)) throw new HttpError(400, "You already have a bet in this round");
  let amount = Math.floor(Number(m.amount));
  await checkBet("crash", amount);
  let auto = m.auto ? Math.floor(Number(m.auto) * 100) / 100 : null;
  if (auto !== null && (!(auto >= 1.01) || auto > MAX_MULT)) throw new HttpError(400, "Auto cash-out must be between 1.01\xD7 and 1000\xD7");
  let userId = cl.userId, rid = roundId, out = await withTx(async (c) => {
    let row = (await c.query("INSERT INTO game_rounds (user_id, game, bet, state) VALUES ($1,'crash',$2,$3) RETURNING id", [userId, amount, JSON.stringify({ crashRound: rid })])).rows[0], t = await post(c, { userId, type: "game_wager", game: "crash", wagered: amount, roundId: row.id, idemKey: `crash:${rid}`, detail: `Crash bet${auto ? ` \xB7 auto ${auto.toFixed(2)}\xD7` : ""}` });
    return { rowId: row.id, balance: t.closing };
  });
  if (phase !== "wait" || roundId !== rid)
    throw await withTx(async (c) => {
      await post(c, { userId, type: "refund", game: "crash", credit: amount, roundId: out.rowId, idemKey: `crash:${rid}:refund`, detail: "Crash bet returned (round had already started)" }), await c.query("UPDATE game_rounds SET status='refunded', settled_at=now() WHERE id=$1", [out.rowId]);
    }), new HttpError(400, "Betting closed just before your bet. Your credits were returned.");
  bets.set(userId, { userId, name: cl.name || "Player", amount, auto, roundRowId: out.rowId }), broadcast({ t: "bet", name: cl.name, amount }), sendTo(userId, { t: "you", placed: !0, amount, auto, balance: out.balance }), sendState();
}
async function manualCashOut(cl) {
  if (!cl.userId) return;
  let b = bets.get(cl.userId);
  if (!b || b.settled) throw new HttpError(400, "No active bet to cash out");
  if (phase !== "run") throw new HttpError(400, "The round is not running");
  let at = multAt(Date.now() - runStart);
  if (at >= crashAt) {
    await crash();
    return;
  }
  await cashOut(b, at);
}
function crashConnect(ws, user) {
  let cl = { ws, userId: user?.id ?? null, name: user?.username ?? null };
  clients.add(cl), ws.send(JSON.stringify(snapshot(cl.userId))), ws.on("message", (raw) => {
    let m;
    try {
      m = JSON.parse(String(raw));
    } catch {
      return;
    }
    if (m.t === "ping") {
      ws.send(JSON.stringify({ t: "pong", now: Date.now() }));
      return;
    }
    let reply = (e) => ws.readyState === 1 && ws.send(JSON.stringify({ t: "error", message: e.message || "Something went wrong" }));
    m.t === "bet" ? serial(() => placeBet(cl, m).catch(reply)) : m.t === "cashout" && serial(() => manualCashOut(cl).catch(reply));
  }), ws.on("close", () => clients.delete(cl));
}
async function startCrashEngine() {
  let s = await gameSettings(), recent = await q("SELECT id, crash_point, seed FROM crash_rounds WHERE crash_point IS NOT NULL ORDER BY id DESC LIMIT 24");
  for (let r of recent) history.push({ id: r.id, point: Number(r.crash_point), seed: r.seed });
  serial(newRound);
}
var crashStatus = () => ({ phase, roundId, players: clients.size, bets: bets.size });

// server/tables.ts
init_db();

// server/games/pokereval.ts
var V = { 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 9, 10: 10, J: 11, Q: 12, K: 13, A: 14 }, cardVal = (c) => V[c.slice(0, -1)], cardSuit = (c) => c.slice(-1), CATEGORY = ["High Card", "One Pair", "Two Pair", "Three of a Kind", "Straight", "Flush", "Full House", "Four of a Kind", "Straight Flush", "Royal Flush"];
function score5(cards) {
  let vals = cards.map(cardVal).sort((a, b) => b - a), suits = cards.map(cardSuit), flush = suits.every((s2) => s2 === suits[0]), uniq = [...new Set(vals)], straightHigh = 0;
  uniq.length === 5 && (vals[0] - vals[4] === 4 ? straightHigh = vals[0] : vals[0] === 14 && vals[1] === 5 && vals[4] === 2 && (straightHigh = 5));
  let counts = /* @__PURE__ */ new Map();
  for (let v of vals) counts.set(v, (counts.get(v) || 0) + 1);
  let groups = [...counts.entries()].sort((a, b) => b[1] - a[1] || b[0] - a[0]), cat, tb;
  straightHigh && flush ? (cat = straightHigh === 14 ? 9 : 8, tb = [straightHigh]) : groups[0][1] === 4 ? (cat = 7, tb = [groups[0][0], groups[1][0]]) : groups[0][1] === 3 && groups[1][1] === 2 ? (cat = 6, tb = [groups[0][0], groups[1][0]]) : flush ? (cat = 5, tb = vals) : straightHigh ? (cat = 4, tb = [straightHigh]) : groups[0][1] === 3 ? (cat = 3, tb = [groups[0][0], ...groups.slice(1).map((g) => g[0])]) : groups[0][1] === 2 && groups[1][1] === 2 ? (cat = 2, tb = [groups[0][0], groups[1][0], groups[2][0]]) : groups[0][1] === 2 ? (cat = 1, tb = [groups[0][0], ...groups.slice(1).map((g) => g[0])]) : (cat = 0, tb = vals);
  let s = cat;
  for (let i = 0; i < 5; i++) s = s * 15 + (tb[i] || 0);
  return s;
}
var categoryOf = (score) => Math.floor(score / 15 ** 5), COMBOS = [];
for (let a = 0; a < 7; a++) for (let b = a + 1; b < 7; b++) for (let c = b + 1; c < 7; c++) for (let d = c + 1; d < 7; d++) for (let e = d + 1; e < 7; e++) COMBOS.push([a, b, c, d, e]);
function best(cards) {
  if (cards.length === 5) {
    let s = score5(cards);
    return { score: s, hand: cards, name: CATEGORY[categoryOf(s)] };
  }
  let bestS = -1, bestH = [], idx = cards.length === 7 ? COMBOS : combos(cards.length);
  for (let cmb of idx) {
    let h = cmb.map((i) => cards[i]), s = score5(h);
    s > bestS && (bestS = s, bestH = h);
  }
  return { score: bestS, hand: bestH, name: CATEGORY[categoryOf(bestS)] };
}
function combos(n) {
  let out = [], rec = (start, cur) => {
    if (cur.length === 5) {
      out.push(cur.slice());
      return;
    }
    for (let i = start; i < n; i++)
      cur.push(i), rec(i + 1, cur), cur.pop();
  };
  return rec(0, []), out;
}
function equity(hole, board, opponents, sims = 220) {
  let used = /* @__PURE__ */ new Set([...hole, ...board]), deck = [];
  for (let s of "SHDC") for (let r of ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"]) {
    let c = r + s;
    used.has(c) || deck.push(c);
  }
  let wins = 0;
  for (let t = 0; t < sims; t++) {
    let need = 5 - board.length + opponents * 2;
    for (let i = 0; i < need; i++) {
      let j = i + Math.floor(Math.random() * (deck.length - i));
      [deck[i], deck[j]] = [deck[j], deck[i]];
    }
    let k = 0, b = board.slice();
    for (; b.length < 5; ) b.push(deck[k++]);
    let mine = best([...hole, ...b]).score, beat = !0, tie = 0;
    for (let o = 0; o < opponents; o++) {
      let os = best([deck[k++], deck[k++], ...b]).score;
      if (os > mine) {
        beat = !1;
        break;
      }
      os === mine && tie++;
    }
    beat && (wins += tie ? 1 / (tie + 1) : 1);
  }
  return wins / sims;
}

// server/games/poker.ts
var PokerEngine = class {
  constructor(room) {
    this.room = room;
    this.phase = "waiting";
    this.hand = 0;
    this.button = -1;
    this.board = [];
    this.deck = [];
    this.toAct = null;
    this.deadline = null;
    this.currentBet = 0;
    this.minRaise = 0;
    this.lastAggressor = null;
    this.st = this.fresh();
    this.results = [];
    this.revealed = /* @__PURE__ */ new Set();
    this.timers = [];
  }
  fresh() {
    return { hole: {}, folded: /* @__PURE__ */ new Set(), allIn: /* @__PURE__ */ new Set(), acted: /* @__PURE__ */ new Set(), street: 0, committed: {}, total: {} };
  }
  get sb() {
    return this.room.settings.smallBlind;
  }
  get bb() {
    return this.room.settings.bigBlind;
  }
  clear() {
    this.timers.forEach(clearTimeout), this.timers = [];
  }
  later(ms, fn) {
    let h = this.room.handId;
    this.timers.push(setTimeout(() => {
      this.room.handId === h && !this.room.closed && fn();
    }, ms));
  }
  inHand() {
    return this.room.seats.filter((s) => s && this.st.hole[s.pos] && !this.st.folded.has(s.pos));
  }
  eligible() {
    return this.room.seats.filter((s) => s && s.stack > 0 && !s.sittingOut && (s.isBot || s.connected));
  }
  busy() {
    return this.phase !== "waiting" && this.phase !== "showdown";
  }
  /* -------- hand flow -------- */
  maybeStart() {
    if (!(this.room.closed || this.room.paused || this.busy() || this.room.phase !== "playing")) {
      if (this.eligible().length < 2) {
        this.phase = "waiting", this.room.msg = this.room.seats.filter(Boolean).length < 2 ? "Waiting for players" : "Waiting for players with chips", this.room.bc();
        return;
      }
      this.startHand();
    }
  }
  order(fromPos, players) {
    let ps = players.slice().sort((a, b) => a.pos - b.pos), after = ps.filter((p) => p.pos > fromPos), before = ps.filter((p) => p.pos <= fromPos);
    return [...after, ...before];
  }
  startHand() {
    this.clear();
    let players = this.eligible();
    this.room.handId++, this.hand++, this.st = this.fresh(), this.board = [], this.results = [], this.revealed.clear(), this.deck = newDecks(1);
    let ord = this.order(this.button, players);
    this.button = ord[0].pos;
    let seq = this.order(this.button, players), headsUp = players.length === 2, sbSeat = headsUp ? players.find((p) => p.pos === this.button) : seq[0], bbSeat = headsUp ? players.find((p) => p.pos !== this.button) : seq[1];
    for (let p of players)
      this.st.hole[p.pos] = [], this.st.committed[p.pos] = 0, this.st.total[p.pos] = 0;
    for (let k = 0; k < 2; k++) for (let p of this.order(this.button, players)) this.st.hole[p.pos].push(this.deck.pop());
    this.phase = "preflop", this.currentBet = 0, this.minRaise = this.bb, this.post(sbSeat, Math.min(this.sb, sbSeat.stack)), this.post(bbSeat, Math.min(this.bb, bbSeat.stack)), this.currentBet = Math.max(this.st.committed[sbSeat.pos], this.st.committed[bbSeat.pos]), this.lastAggressor = bbSeat.pos, this.room.addLog(`-- Hand #${this.hand} -- Button ${this.room.seatName(this.button)}. Blinds ${this.sb}/${this.bb}. Players: ${players.map((p) => `${p.name} (${p.stack + this.st.total[p.pos]})`).join(", ")}.`), this.room.msg = "", this.room.sub = "", this.setTurn(this.nextActor(bbSeat.pos));
  }
  post(s, amt) {
    amt = Math.min(amt, s.stack), s.stack -= amt, this.st.committed[s.pos] += amt, this.st.total[s.pos] += amt, s.stack === 0 && this.st.allIn.add(s.pos);
  }
  canAct(pos) {
    return !!this.st.hole[pos] && !this.st.folded.has(pos) && !this.st.allIn.has(pos);
  }
  nextActor(fromPos) {
    let seats = this.room.seats.filter((s) => s && this.canAct(s.pos));
    if (!seats.length) return null;
    let o = this.order(fromPos, seats);
    for (let s of o) {
      let owes = this.st.committed[s.pos] < this.currentBet;
      if (!this.st.acted.has(s.pos) || owes) return s.pos;
    }
    return null;
  }
  setTurn(pos) {
    let live = this.inHand();
    if (live.length <= 1) {
      this.finishHand();
      return;
    }
    if (pos === null) {
      this.nextStreet();
      return;
    }
    if (live.filter((s2) => this.canAct(s2.pos)).length === 1 && this.st.committed[pos] >= this.currentBet && this.st.acted.has(pos)) {
      this.nextStreet();
      return;
    }
    this.toAct = pos;
    let t = this.room.settings.timer;
    this.deadline = t > 0 ? Date.now() + t * 1e3 : null;
    let s = this.room.seatAt(pos);
    this.room.msg = s.name, this.room.sub = "to act", this.room.bc(), s.isBot && this.later(700 + Math.random() * 1300, () => this.botAct(s));
  }
  tick() {
    if (this.toAct !== null && this.deadline && Date.now() > this.deadline && !this.room.paused) {
      let s = this.room.seatAt(this.toAct);
      s && (this.room.sys(`${s.name} ran out of time`), this.act(s, { type: this.st.committed[s.pos] >= this.currentBet ? "check" : "fold" }));
    }
  }
  legal(s) {
    let owe = this.currentBet - this.st.committed[s.pos], maxTo = this.st.committed[s.pos] + s.stack, minTo = Math.min(maxTo, this.currentBet === 0 ? this.bb : this.currentBet + this.minRaise);
    return { canCheck: owe <= 0, callAmt: Math.min(Math.max(owe, 0), s.stack), minTo, maxTo, canRaise: maxTo > this.currentBet && s.stack > Math.max(owe, 0) };
  }
  act(s, a) {
    if (this.toAct !== s.pos || !this.busy()) return;
    let L = this.legal(s), name = s.name;
    switch (a.type) {
      case "fold":
        this.st.folded.add(s.pos), this.room.addLog(`${name} folds`);
        break;
      case "check":
        if (!L.canCheck) return;
        this.room.addLog(`${name} checks`);
        break;
      case "call":
        if (L.canCheck) {
          this.room.addLog(`${name} checks`);
          break;
        }
        this.post(s, L.callAmt), this.room.addLog(`${name} calls ${L.callAmt}${s.stack === 0 ? " and is all-in" : ""}`);
        break;
      case "raise":
      case "bet":
      case "allin": {
        if (!L.canRaise && a.type !== "allin") return;
        let to = a.type === "allin" ? L.maxTo : Math.floor(Number(a.to));
        if (!Number.isFinite(to)) return;
        if (to = Math.min(Math.max(to, L.minTo), L.maxTo), to <= this.currentBet) {
          let amt = to - this.st.committed[s.pos];
          if (amt <= 0) return;
          this.post(s, amt), this.room.addLog(`${name} calls ${amt} and is all-in`);
          break;
        }
        let raiseBy = to - this.currentBet, full = raiseBy >= this.minRaise;
        this.post(s, to - this.st.committed[s.pos]);
        let verb = this.currentBet === 0 ? "bets" : "raises to";
        this.room.addLog(`${name} ${verb} ${to}${s.stack === 0 ? " and is all-in" : ""}`), full && (this.minRaise = raiseBy, this.st.acted.clear()), this.currentBet = to, this.lastAggressor = s.pos;
        break;
      }
      default:
        return;
    }
    this.st.acted.add(s.pos), this.toAct = null, this.setTurn(this.nextActor(s.pos));
  }
  nextStreet() {
    this.toAct = null, this.deadline = null;
    for (let p of Object.keys(this.st.committed)) this.st.committed[+p] = 0;
    this.st.acted.clear(), this.currentBet = 0, this.minRaise = this.bb;
    let live = this.inHand();
    if (live.length <= 1) {
      this.finishHand();
      return;
    }
    if (this.phase === "river") {
      this.showdown();
      return;
    }
    if (this.deck.pop(), this.phase === "preflop" ? (this.board.push(this.deck.pop(), this.deck.pop(), this.deck.pop()), this.phase = "flop") : this.phase === "flop" ? (this.board.push(this.deck.pop()), this.phase = "turn") : this.phase === "turn" && (this.board.push(this.deck.pop()), this.phase = "river"), this.room.addLog(`${this.phase[0].toUpperCase() + this.phase.slice(1)}: ${this.board.map(pc).join(" ")}`), live.filter((s) => this.canAct(s.pos)).length <= 1) {
      this.toAct = null, this.room.bc(), this.later(1100, () => this.nextStreet());
      return;
    }
    this.room.bc(), this.later(450, () => this.setTurn(this.nextActor(this.button)));
  }
  pots() {
    let contrib = Object.entries(this.st.total).map(([p, v]) => ({ pos: +p, v })).filter((x) => x.v > 0), levels = [...new Set(contrib.map((c) => c.v))].sort((a, b) => a - b), pots = [], prev = 0;
    for (let lv of levels) {
      let amount = contrib.reduce((a, c) => a + Math.max(0, Math.min(c.v, lv) - prev), 0), eligible = contrib.filter((c) => c.v >= lv && !this.st.folded.has(c.pos)).map((c) => c.pos);
      amount > 0 && (eligible.length ? pots.push({ amount, eligible }) : pots.length && (pots[pots.length - 1].amount += amount)), prev = lv;
    }
    return pots;
  }
  potTotal() {
    return Object.values(this.st.total).reduce((a, b) => a + b, 0);
  }
  showdown() {
    this.phase = "showdown", this.toAct = null, this.deadline = null;
    let live = this.inHand(), scores = /* @__PURE__ */ new Map();
    for (let s of live) {
      let b = best([...this.st.hole[s.pos], ...this.board]);
      scores.set(s.pos, b), this.revealed.add(s.pos);
    }
    let wins = /* @__PURE__ */ new Map();
    for (let pot of this.pots()) {
      let el = pot.eligible.filter((p) => scores.has(p)), top = Math.max(...el.map((p) => scores.get(p).score)), winners = this.order(this.button, el.filter((p) => scores.get(p).score === top).map((p) => this.room.seatAt(p))).map((s) => s.pos), share = Math.floor(pot.amount / winners.length), odd = pot.amount - share * winners.length;
      for (let w of winners)
        wins.set(w, (wins.get(w) || 0) + share + (odd > 0 ? 1 : 0)), odd > 0 && odd--;
    }
    for (let s of live) this.room.addLog(`${s.name} shows ${this.st.hole[s.pos].map(pc).join(" ")} (${scores.get(s.pos).name})`);
    this.payout(wins, scores);
  }
  finishHand() {
    let w = this.inHand()[0], wins = /* @__PURE__ */ new Map([[w.pos, this.potTotal()]]);
    this.phase = "showdown", this.payout(wins, null);
  }
  payout(wins, scores) {
    this.toAct = null, this.deadline = null, this.results = [];
    for (let [pos, amt] of wins) {
      let s = this.room.seatAt(pos);
      if (!s) continue;
      s.stack += amt;
      let sc = scores?.get(pos);
      this.results.push({ pos, win: amt, hand: sc?.name, cards: sc?.hand }), this.room.addLog(`${s.name} wins ${amt}${sc ? ` with ${sc.name}` : ""}`);
    }
    let top = this.results.sort((a, b) => b.win - a.win)[0];
    if (top) {
      let s = this.room.seatAt(top.pos);
      this.room.msg = `${s?.name} wins ${top.win.toLocaleString("en-IN")}`, this.room.sub = top.hand || "Everyone else folded";
    }
    this.room.handOver(), this.room.bc(), this.later(scores ? 5200 : 3200, () => {
      this.phase = "waiting", this.maybeStart();
    });
  }
  /* -------- seats leaving mid-hand -------- */
  seatLeaving(s) {
    this.st.hole[s.pos] && !this.st.folded.has(s.pos) && this.busy() && (this.st.folded.add(s.pos), this.toAct === s.pos ? (this.toAct = null, this.setTurn(this.nextActor(s.pos))) : this.inHand().length <= 1 && this.finishHand());
  }
  /** Chips this seat currently has committed in the pot (returned with stack on a forced close). */
  committedOf(pos) {
    return this.st.total[pos] || 0;
  }
  stop() {
    this.clear();
  }
  /* -------- computer players -------- */
  botAct(s) {
    if (this.toAct !== s.pos) return;
    let L = this.legal(s), opp = this.inHand().length - 1, hole = this.st.hole[s.pos], style = s.botStyle || "balanced", loose = style === "loose" ? 0.08 : style === "tight" ? -0.06 : 0, aggr = style === "aggressive" ? 0.15 : style === "tight" ? -0.05 : 0, eq;
    this.board.length === 0 ? eq = preflopStrength(hole, opp) : eq = equity(hole, this.board, Math.max(1, opp), 180);
    let pot = this.potTotal(), toCall = L.callAmt, potOdds = toCall > 0 ? toCall / (pot + toCall) : 0, r = Math.random(), strong = eq + loose > 0.62 - aggr, decent = eq + loose > Math.max(potOdds + 0.04, 0.33), action;
    L.canCheck ? strong && L.canRaise && r < 0.75 + aggr ? action = { type: "raise", to: this.sizing(s, eq, pot) } : eq > 0.45 && L.canRaise && r < 0.25 + aggr ? action = { type: "raise", to: this.sizing(s, eq * 0.7, pot) } : r < 0.06 + aggr / 2 && L.canRaise ? action = { type: "raise", to: this.sizing(s, 0.4, pot) } : action = { type: "check" } : strong && L.canRaise && r < 0.55 + aggr ? action = { type: "raise", to: this.sizing(s, eq, pot) } : decent || toCall <= this.bb && eq > 0.25 ? action = { type: "call" } : r < 0.04 ? action = { type: "call" } : action = { type: "fold" }, this.act(s, action);
  }
  sizing(s, eq, pot) {
    let L = this.legal(s), frac = eq > 0.85 ? 1 + Math.random() : eq > 0.7 ? 0.66 + Math.random() * 0.5 : 0.5 + Math.random() * 0.25, to = this.currentBet + Math.max(this.minRaise, Math.round(pot * frac / this.bb) * this.bb);
    return eq > 0.9 && Math.random() < 0.3 && (to = L.maxTo), Math.min(Math.max(to, L.minTo), L.maxTo);
  }
  view(viewerPos) {
    let showAll = this.phase === "showdown";
    return {
      kind: "poker",
      phase: this.phase,
      hand: this.hand,
      button: this.button,
      board: this.board,
      pot: this.potTotal(),
      pots: this.busy() || this.phase === "showdown" ? this.pots().map((p) => p.amount) : [],
      currentBet: this.currentBet,
      minRaise: this.minRaise,
      toAct: this.toAct,
      remaining: this.deadline ? Math.max(0, this.deadline - Date.now()) : null,
      timerTotal: this.room.settings.timer * 1e3,
      players: Object.keys(this.st.hole).map((p) => {
        let pos = +p, show = pos === viewerPos || showAll && this.revealed.has(pos);
        return { pos, cards: show ? this.st.hole[pos] : this.st.hole[pos].map(() => "XX"), folded: this.st.folded.has(pos), allIn: this.st.allIn.has(pos), street: this.st.committed[pos] || 0, total: this.st.total[pos] || 0 };
      }),
      results: this.phase === "showdown" ? this.results : [],
      legal: viewerPos !== null && this.toAct === viewerPos ? this.legal(this.room.seatAt(viewerPos)) : null,
      blinds: [this.sb, this.bb]
    };
  }
}, pc = (c) => c.slice(0, -1) + { S: "\u2660", H: "\u2665", D: "\u2666", C: "\u2663" }[c.slice(-1)];
function preflopStrength(h, opp) {
  let a = cardVal(h[0]), b = cardVal(h[1]), hi = Math.max(a, b), lo = Math.min(a, b), pair = a === b, suited = cardSuit(h[0]) === cardSuit(h[1]), gap = hi - lo, s = (hi + lo) / 28 * 0.55;
  return pair && (s = 0.5 + lo / 14 * 0.4), suited && (s += 0.05), gap === 1 ? s += 0.04 : gap === 2 ? s += 0.02 : gap > 4 && (s -= 0.05), hi === 14 && (s += 0.06), Math.max(0.05, Math.min(0.95, s - (opp - 1) * 0.04));
}

// server/games/bjtable.ts
var BlackjackTableEngine = class {
  constructor(room) {
    this.room = room;
    this.phase = "betting";
    this.shoe = newDecks(6);
    this.dealer = [];
    this.hidden = !0;
    this.bets = /* @__PURE__ */ new Map();
    // pos -> bet placed this round
    this.hands = /* @__PURE__ */ new Map();
    // pos -> hands
    this.turn = null;
    this.deadline = null;
    this.round = 0;
    this.timers = [];
  }
  clear() {
    this.timers.forEach(clearTimeout), this.timers = [];
  }
  later(ms, fn) {
    let r = this.round;
    this.timers.push(setTimeout(() => {
      this.round === r && !this.room.closed && fn();
    }, ms));
  }
  draw() {
    return this.shoe.length < 20 && (this.shoe = newDecks(6), this.room.sys("Dealer shuffles a fresh shoe")), this.shoe.pop();
  }
  get minBet() {
    return this.room.settings.minBet;
  }
  get maxBet() {
    return this.room.settings.maxBet;
  }
  busy() {
    return this.phase === "dealing" || this.phase === "playing" || this.phase === "dealer";
  }
  maybeStart() {
    (this.phase === "results" || this.phase === "betting") && (this.toBetting(), this.room.bc());
  }
  toBetting() {
    this.clear(), this.phase = "betting", this.turn = null, this.deadline = null, this.dealer = [], this.hidden = !0, this.bets.clear(), this.hands.clear(), this.room.msg = "Place your bets", this.room.sub = `Min ${this.minBet.toLocaleString("en-IN")} \xB7 Max ${this.maxBet.toLocaleString("en-IN")}`;
  }
  players() {
    return this.room.seats.filter((s) => s && !s.isBot);
  }
  checkAutoDeal() {
    if (this.phase !== "betting" || this.room.paused) return;
    let elig = this.players().filter((s) => !s.sittingOut && s.connected && s.stack >= this.minBet);
    elig.length && elig.every((s) => this.bets.has(s.pos)) && this.deal();
  }
  act(s, a) {
    let t = a.type;
    if (t === "bet" && this.phase === "betting" && !this.bets.has(s.pos)) {
      let amt = Math.floor(Number(a.amount));
      if (!(amt >= this.minBet) || amt > this.maxBet || amt > s.stack) return;
      this.bets.set(s.pos, amt), this.room.bc(), this.checkAutoDeal();
      return;
    }
    if (t === "unbet" && this.phase === "betting") {
      this.bets.delete(s.pos), this.room.bc();
      return;
    }
    if (t === "deal" && s.userId === this.room.hostId && this.phase === "betting") {
      this.deal();
      return;
    }
    if (t === "next" && s.userId === this.room.hostId && this.phase === "results") {
      this.toBetting(), this.room.bc();
      return;
    }
    if (t === "skip" && s.userId === this.room.hostId && this.phase === "playing" && this.turn) {
      this.standCurrent();
      return;
    }
    if (["hit", "stand", "double", "split"].includes(t) && this.phase === "playing" && this.turn && this.turn.pos === s.pos) {
      let hs = this.hands.get(s.pos), h = hs[this.turn.h];
      if (!h || h.done) return;
      if (t === "hit") {
        if (h.splitAces) return;
        h.cards.push(this.draw()), total(h.cards) >= 21 && (h.done = !0), this.room.addLog(`${s.name} hits \u2192 ${total(h.cards)}`);
      } else if (t === "stand")
        h.done = !0, this.room.addLog(`${s.name} stands on ${total(h.cards)}`);
      else if (t === "double") {
        if (h.cards.length !== 2 || s.stack < h.bet || h.splitAces) return;
        s.stack -= h.bet, h.bet *= 2, h.doubled = !0, h.cards.push(this.draw()), h.done = !0, this.room.addLog(`${s.name} doubles \u2192 ${total(h.cards)}`);
      } else if (t === "split") {
        if (h.cards.length !== 2 || cval(rankOf(h.cards[0])) !== cval(rankOf(h.cards[1])) || s.stack < h.bet || hs.length >= 4) return;
        s.stack -= h.bet;
        let aces = rankOf(h.cards[0]) === "A", nh = { cards: [h.cards[1]], bet: h.bet, done: !1, split: !0, splitAces: aces };
        h.cards = [h.cards[0], this.draw()], h.split = !0, h.splitAces = aces, nh.cards.push(this.draw()), hs.splice(this.turn.h + 1, 0, nh), aces && (h.done = !0, nh.done = !0), total(h.cards) === 21 && (h.done = !0), total(nh.cards) === 21 && (nh.done = !0), this.room.addLog(`${s.name} splits`);
      }
      this.advance();
    }
  }
  async deal() {
    if (this.phase !== "betting") return;
    let inR = this.players().filter((s) => this.bets.has(s.pos) && this.bets.get(s.pos) <= s.stack && !s.sittingOut);
    if (!inR.length) return;
    this.clear(), this.round++, this.room.handId++;
    for (let s of inR) {
      let b = this.bets.get(s.pos);
      s.stack -= b, this.hands.set(s.pos, [{ cards: [], bet: b, done: !1 }]);
    }
    for (let p of [...this.bets.keys()]) this.hands.has(p) || this.bets.delete(p);
    this.dealer = [], this.hidden = !0, this.phase = "dealing", this.turn = null, this.room.msg = "", this.room.sub = "", this.room.addLog(`-- Round #${this.round} -- Bets: ${inR.map((s) => `${s.name} ${this.bets.get(s.pos)}`).join(", ")}`), this.room.bc();
    let order = inR.slice().sort((a, b) => a.pos - b.pos), step = 0, seq = [];
    for (let k = 0; k < 2; k++) {
      for (let s of order) seq.push(() => this.hands.get(s.pos)?.[0].cards.push(this.draw()));
      seq.push(() => this.dealer.push(this.draw()));
    }
    let run = () => {
      if (step < seq.length) {
        seq[step++](), this.room.bc(), this.later(210, run);
        return;
      }
      if (cval(rankOf(this.dealer[0])) >= 10 && isBJ(this.dealer)) {
        this.hidden = !1, this.room.msg = "Dealer has Blackjack", this.room.bc(), this.later(700, () => this.settle());
        return;
      }
      for (let s of order) {
        let h = this.hands.get(s.pos)[0];
        isBJ(h.cards) && (h.done = !0);
      }
      this.phase = "playing", this.advance();
    };
    this.later(200, run);
  }
  advance() {
    let order = [...this.hands.keys()].sort((a, b) => a - b);
    for (let pos of order) {
      let hs = this.hands.get(pos), hi = hs.findIndex((h) => !h.done);
      if (hi >= 0) {
        let same = this.turn && this.turn.pos === pos && this.turn.h === hi;
        if (this.turn = { pos, h: hi }, !same) {
          let t = this.room.settings.timer;
          this.deadline = t > 0 && !this.room.paused ? Date.now() + t * 1e3 : null;
        }
        let s = this.room.seatAt(pos);
        this.room.msg = (s?.name || "") + (hs.length > 1 ? ` \xB7 hand ${hi + 1}` : ""), this.room.sub = "to act", this.room.bc();
        return;
      }
    }
    this.turn = null, this.deadline = null, this.dealerPlay();
  }
  standCurrent() {
    if (!this.turn) return;
    let s = this.room.seatAt(this.turn.pos), h = this.hands.get(this.turn.pos)?.[this.turn.h];
    h && (h.done = !0, this.room.sys(`${s?.name || "Player"} timed out and stands`)), this.advance();
  }
  dealerPlay() {
    this.phase = "dealer", this.room.msg = "", this.room.sub = "", this.room.bc(), this.later(450, () => {
      this.hidden = !1, this.room.bc();
      let live = [...this.hands.values()].some((hs) => hs.some((h) => total(h.cards) <= 21 && !(isBJ(h.cards) && !h.split))), step = () => {
        live && total(this.dealer) < 17 ? (this.dealer.push(this.draw()), this.room.bc(), this.later(700, step)) : this.later(500, () => this.settle());
      };
      this.later(400, step);
    });
  }
  settle() {
    let d = total(this.dealer), dBJ = isBJ(this.dealer);
    this.hidden = !1;
    let lines = [];
    for (let [pos, hs] of this.hands) {
      let s = this.room.seatAt(pos);
      for (let h of hs) {
        let v = total(h.cards), bj = isBJ(h.cards) && !h.split, p = 0, r;
        v > 21 ? r = "bust" : bj && dBJ ? (r = "push", p = h.bet) : bj ? (r = "bj", p = h.bet + Math.floor(h.bet * 1.5)) : dBJ ? r = "lose" : d > 21 || v > d ? (r = "win", p = h.bet * 2) : v === d ? (r = "push", p = h.bet) : r = "lose", h.result = r, h.payout = p, h.done = !0, s && (s.stack += p), lines.push(`${s?.name || "Player"}: ${v} ${r}${p - h.bet ? ` ${p - h.bet > 0 ? "+" : ""}${p - h.bet}` : ""}`);
      }
    }
    this.room.addLog(`Dealer ${this.dealer.join(" ")} (${dBJ ? "Blackjack" : d > 21 ? "bust" : d}). ${lines.join("; ")}`), this.phase = "results", this.turn = null, this.deadline = null, this.room.msg = dBJ ? "Dealer Blackjack" : d > 21 ? "Dealer busts!" : "Dealer stands on " + d, this.room.sub = this.room.settings.autoNext ? "Next round shortly\u2026" : "Waiting for the host", this.room.handOver(), this.room.bc(), this.room.settings.autoNext && this.later(5200, () => {
      this.room.paused || (this.toBetting(), this.room.bc());
    });
  }
  tick() {
    this.phase === "playing" && this.deadline && Date.now() > this.deadline && !this.room.paused && this.standCurrent();
  }
  onSeatChange() {
    this.checkAutoDeal();
  }
  seatLeaving(s) {
    this.hands.get(s.pos) && this.busy() && (this.hands.delete(s.pos), this.turn && this.turn.pos === s.pos && (this.turn = null, this.phase === "playing" && this.advance())), this.bets.delete(s.pos), this.checkAutoDeal();
  }
  committedOf(pos) {
    return this.busy() ? (this.hands.get(pos) || []).reduce((a, h) => a + h.bet, 0) : 0;
  }
  stop() {
    this.clear();
  }
  paused(p) {
    p || (this.checkAutoDeal(), this.phase === "results" && this.room.settings.autoNext && this.toBetting());
  }
  view(viewerPos) {
    let dealer = this.hidden && this.dealer.length > 1 ? [this.dealer[0], "XX", ...this.dealer.slice(2)] : this.dealer;
    return {
      kind: "blackjack",
      phase: this.phase,
      round: this.round,
      dealer,
      dealerTotal: this.hidden ? this.dealer[0] ? total([this.dealer[0]]) : 0 : total(this.dealer),
      hidden: this.hidden,
      bets: Object.fromEntries(this.bets),
      hands: Object.fromEntries([...this.hands].map(([p, hs]) => [p, hs.map((h) => ({ ...h, total: total(h.cards) }))])),
      turn: this.turn,
      remaining: this.deadline ? Math.max(0, this.deadline - Date.now()) : null,
      timerTotal: this.room.settings.timer * 1e3,
      shoeLeft: this.shoe.length,
      minBet: this.minBet,
      maxBet: this.maxBet,
      you: viewerPos
    };
  }
};

// server/tables.ts
var THEMES = ["vegas", "montecarlo", "macau", "marinabay"], REACTIONS = ["\u{1F525}", "\u{1F602}", "\u{1F44F}", "\u{1F631}", "\u{1F60E}", "\u{1F911}", "\u{1F64F}", "\u{1F480}", "\u{1F389}", "\u{1F624}", "\u{1F340}", "\u2764\uFE0F"], BOTS = [
  ["Victoria", "tight"],
  ["Alessandro", "aggressive"],
  ["Mei Lin", "balanced"],
  ["Rafael", "loose"],
  ["Sienna", "balanced"],
  ["Hiroshi", "tight"],
  ["Isabella", "aggressive"],
  ["Dmitri", "balanced"]
], rooms = /* @__PURE__ */ new Map(), CODE_CH = "ABCDEFGHJKMNPQRSTUVWXYZ23456789", newCode = () => {
  for (; ; ) {
    let c = "";
    for (let i = 0; i < 5; i++) c += CODE_CH[Math.floor(Math.random() * CODE_CH.length)];
    if (!rooms.has(c)) return c;
  }
}, GAME_NAME = { poker: "Poker", blackjack: "Blackjack" }, Room = class {
  constructor(o) {
    this.phase = "lobby";
    this.paused = !1;
    this.closed = !1;
    this.handId = 0;
    this.msg = "Waiting for players";
    this.sub = "";
    this.requests = [];
    this.chat = [];
    this.log = [];
    this.ledger = /* @__PURE__ */ new Map();
    this.conns = /* @__PURE__ */ new Map();
    this.createdAt = Date.now();
    this.lastHuman = Date.now();
    this.seq = 0;
    this.bcPending = !1;
    this.queue = Promise.resolve();
    this.code = o.code, this.game = o.game, this.mode = o.mode, this.hostId = o.hostId, this.hostName = o.hostName, this.title = o.title, this.theme = o.theme, this.settings = o.settings, this.seats = Array(o.settings.maxPlayers).fill(null), this.engine = o.game === "poker" ? new PokerEngine(this) : new BlackjackTableEngine(this);
  }
  /* ---------- helpers used by engines ---------- */
  seatAt(pos) {
    return this.seats[pos] || null;
  }
  seatName(pos) {
    return this.seats[pos]?.name || "\u2013";
  }
  seatOf(userId) {
    return this.seats.find((s) => s && s.userId === userId) || null;
  }
  addLog(text) {
    this.log.push({ at: Date.now(), text }), this.log.length > 4e3 && this.log.splice(0, 800);
  }
  sys(text) {
    this.pushChat({ sys: !0, text, at: Date.now() }), this.addLog(text);
  }
  serial(fn) {
    let p = this.queue.then(fn);
    return this.queue = p.then(() => {
    }, () => {
    }), p;
  }
  pushChat(e) {
    this.chat.push(e), this.chat.length > 120 && this.chat.splice(0, 40), this.broadcastRaw({ t: "chat", chat: [e] });
  }
  broadcastRaw(msg) {
    let s = JSON.stringify(msg);
    for (let set of this.conns.values()) for (let ws of set) ws.readyState === 1 && ws.send(s);
  }
  sendTo(userId, msg) {
    let s = JSON.stringify(msg);
    for (let ws of this.conns.get(userId) || []) ws.readyState === 1 && ws.send(s);
  }
  bc() {
    this.bcPending || (this.bcPending = !0, setImmediate(() => {
      if (this.bcPending = !1, !this.closed) {
        this.seq++;
        for (let [uid] of this.conns) this.sendTo(uid, { t: "state", s: this.view(uid) });
      }
    }));
  }
  view(uid) {
    let me = this.seatOf(uid), isHost = uid === this.hostId;
    return {
      code: this.code,
      game: this.game,
      mode: this.mode,
      title: this.title,
      theme: this.theme,
      settings: this.settings,
      phase: this.phase,
      paused: this.paused,
      msg: this.msg,
      sub: this.sub,
      seq: this.seq,
      hostName: this.hostName,
      seats: this.seats.map((s) => s && { pos: s.pos, name: s.name, stack: s.stack, isBot: s.isBot, connected: s.isBot || s.connected, sittingOut: s.sittingOut, host: s.userId === this.hostId, you: s.userId === uid, pendingTopup: s.pendingTopup }),
      you: me ? me.pos : null,
      isHost,
      hostOnline: this.conns.has(this.hostId),
      requests: isHost ? this.requests.map((r) => ({ userId: r.userId, name: r.name, amount: r.amount, kind: r.kind })) : [],
      pending: !me && this.requests.some((r) => r.userId === uid && r.kind === "join"),
      myTopup: this.requests.some((r) => r.userId === uid && r.kind === "topup"),
      engine: this.engine.view(me ? me.pos : null)
    };
  }
  /* ---------- wallet movements ---------- */
  async buyIn(userId, name, amount) {
    return withTx(async (c) => {
      let row = (await c.query(
        "INSERT INTO game_rounds (user_id, game, bet, state) VALUES ($1,$2,$3,$4) RETURNING id",
        [userId, this.game, amount, JSON.stringify({ table: this.code, stack: amount })]
      )).rows[0];
      return await post(c, { userId, type: "game_wager", game: this.game, wagered: amount, roundId: row.id, detail: `${GAME_NAME[this.game]} table ${this.code} \xB7 buy-in` }), row.id;
    });
  }
  async addChips(s, amount) {
    await withTx(async (c) => {
      await post(c, { userId: s.userId, type: "game_wager", game: this.game, wagered: amount, roundId: s.roundRowId, detail: `${GAME_NAME[this.game]} table ${this.code} \xB7 top-up` }), await c.query("UPDATE game_rounds SET bet = bet + $2 WHERE id = $1", [s.roundRowId, amount]);
    });
  }
  async cashOut(s, extra = 0, note = "cash-out") {
    let amount = s.stack + extra;
    if (s.stack = 0, s.isBot || !s.userId || !s.roundRowId) return;
    let userId = s.userId, rowId = s.roundRowId, tx = await withTx(async (c) => {
      let r = (await c.query("SELECT bet, status FROM game_rounds WHERE id=$1 FOR UPDATE", [rowId])).rows[0];
      if (!r || r.status !== "open") return null;
      let t = await post(c, {
        userId,
        type: amount >= Number(r.bet) && amount > 0 ? "game_win" : "game_loss",
        game: this.game,
        won: amount,
        roundId: rowId,
        detail: `${GAME_NAME[this.game]} table ${this.code} \xB7 ${note} ${fmt(amount)} (session ${amount - Number(r.bet) >= 0 ? "+" : "\u2212"}${fmt(Math.abs(amount - Number(r.bet)))})`
      });
      return await c.query("UPDATE game_rounds SET status='settled', payout=$2, state=NULL, result=$3, settled_at=now() WHERE id=$1", [rowId, amount, JSON.stringify({ table: this.code, cashout: amount })]), t;
    }), L = this.ledger.get(userId);
    L && (L.cashout += amount, L.entries.push({ at: Date.now(), kind: "Cash-out", amount })), tx && this.sendTo(userId, { t: "balance", balance: tx.closing });
  }
  /** Save every human's stack after each hand so chips are never lost if the server restarts. */
  handOver() {
    for (let s of this.seats)
      s && (s.isBot && this.game === "poker" && s.stack < this.settings.bigBlind * 2 && (s.stack = this.botStack()), s.pendingTopup && !s.isBot && (s.stack += s.pendingTopup, this.sys(`${s.name} added ${fmt(s.pendingTopup)} chips`), s.pendingTopup = 0));
    let humans = this.seats.filter((s) => s && !s.isBot && s.roundRowId);
    for (let s of humans) q("UPDATE game_rounds SET state = jsonb_set(coalesce(state, '{}'::jsonb), '{stack}', to_jsonb($2::bigint)) WHERE id=$1 AND status='open'", [s.roundRowId, s.stack]).catch(() => {
    });
    this.syncLedger();
  }
  syncLedger() {
    for (let s of this.seats) if (s && s.userId) {
      let L = this.ledger.get(s.userId);
      L && (L.name = s.name);
    }
  }
  botStack() {
    return this.settings.bigBlind * 100;
  }
  /* ---------- seats ---------- */
  freePos() {
    let mid = (this.seats.length - 1) / 2;
    return this.seats.map((s, i) => s ? -1 : i).filter((i) => i >= 0).sort((a, b) => Math.abs(a - mid) - Math.abs(b - mid) || a - b)[0];
  }
  async seatPlayer(userId, name, amount) {
    if (this.seatOf(userId)) return;
    let pos = this.freePos();
    if (pos === void 0) throw new HttpError(400, "The table is full");
    let rowId = await this.buyIn(userId, name, amount), s = { pos, userId, isBot: !1, name, stack: amount, connected: this.conns.has(userId), sittingOut: !1, roundRowId: rowId, buyin: amount, pendingTopup: 0 };
    this.seats[pos] = s;
    let L = this.ledger.get(userId) || { name, buyin: 0, cashout: 0, entries: [] };
    L.buyin += amount, L.entries.push({ at: Date.now(), kind: "Buy-in", amount }), this.ledger.set(userId, L), userId !== this.hostId ? this.sys(`${name} sat down with ${fmt(amount)}`) : this.addLog(`${name} (host) sat down with ${fmt(amount)}`), this.engine.onSeatChange?.(), this.phase === "playing" && this.engine.maybeStart();
  }
  addBot(i) {
    let pos = this.freePos();
    if (pos === void 0) return;
    let [name, style] = BOTS[i % BOTS.length];
    this.seats[pos] = { pos, userId: null, isBot: !0, botStyle: style, name, stack: this.botStack(), connected: !0, sittingOut: !1, roundRowId: null, buyin: 0, pendingTopup: 0 };
  }
  async removeSeat(s, why) {
    let extra = why === "close" ? this.engine.committedOf(s.pos) + s.pendingTopup : s.pendingTopup;
    why !== "close" && this.engine.seatLeaving(s), this.seats[s.pos] = null, await this.cashOut(s, extra, why === "kick" ? "removed by host, cash-out" : why === "close" ? "table closed, cash-out" : "cash-out"), why !== "close" && this.sys(`${s.name} ${why === "kick" ? "was removed by the host" : "left the table"}`), why === "kick" && s.userId && this.sendTo(s.userId, { t: "status", status: "kicked" }), this.engine.onSeatChange?.(), this.bc();
  }
  async close(reason = "The host closed the table") {
    if (!this.closed) {
      this.engine.stop();
      for (let s of this.seats) s && await this.removeSeat(s, "close");
      this.closed = !0, this.broadcastRaw({ t: "status", status: "closed", text: reason }), rooms.delete(this.code);
    }
  }
  /* ---------- incoming ---------- */
  async request(userId, name, amount) {
    if (this.seatOf(userId)) return;
    if (this.mode === "house" && userId !== this.hostId) throw new HttpError(403, "This is a private table");
    if (amount < this.settings.minBuy || amount > this.settings.maxBuy) throw bad(`Buy-in must be between ${fmt(this.settings.minBuy)} and ${fmt(this.settings.maxBuy)}`);
    if (!this.seats.some((s) => !s)) throw new HttpError(400, "The table is full");
    let bal = (await q("SELECT balance FROM wallets WHERE user_id=$1", [userId]))[0]?.balance ?? 0;
    if (Number(bal) < amount) throw bad(`You only have ${fmt(Number(bal))} credits`);
    this.requests = this.requests.filter((r2) => r2.userId !== userId);
    let r = { userId, name, amount, kind: "join", at: Date.now() };
    this.requests.push(r), this.addLog(`${name} requested a seat with ${fmt(amount)}`), this.sendTo(this.hostId, { t: "req", r }), this.bc();
  }
  async dispatch(userId, name, a) {
    let s = this.seatOf(userId), isHost = userId === this.hostId, t = a?.type;
    if (t === "chat") {
      let text = String(a.text || "").slice(0, 200).trim();
      if (!text) return;
      this.pushChat({ name: s?.name || name, pos: s?.pos, text, at: Date.now() });
      return;
    }
    if (t === "react") {
      if (!s || !REACTIONS.includes(a.e)) return;
      let now = Date.now();
      if (now - (s.lastReact || 0) < 700) return;
      s.lastReact = now, this.broadcastRaw({ t: "react", pos: s.pos, e: a.e });
      return;
    }
    if (t === "leave") {
      s && await this.removeSeat(s, "leave");
      return;
    }
    if (t === "topup" && s) {
      let amt = int(a.amount, "Top-up", 1, this.settings.maxBuy), bal = Number((await q("SELECT balance FROM wallets WHERE user_id=$1", [userId]))[0]?.balance ?? 0);
      if (bal < amt) throw bad(`You only have ${fmt(bal)} credits`);
      if (isHost || this.mode === "house") {
        await this.applyTopup(s, amt);
        return;
      }
      this.requests = this.requests.filter((r2) => r2.userId !== userId);
      let r = { userId, name: s.name, amount: amt, kind: "topup", at: Date.now() };
      this.requests.push(r), this.sendTo(this.hostId, { t: "req", r }), this.bc();
      return;
    }
    if (t === "sitout" && s) {
      s.sittingOut = !s.sittingOut, this.engine.onSeatChange?.(), s.sittingOut || this.engine.maybeStart(), this.bc();
      return;
    }
    if (isHost) {
      if (t === "approve" || t === "reject") {
        let r = this.requests.find((x) => x.userId === Number(a.userId));
        if (!r) return;
        if (this.requests = this.requests.filter((x) => x !== r), t === "reject") {
          r.kind === "join" ? (this.sendTo(r.userId, { t: "status", status: "rejected" }), this.addLog(`The host declined ${r.name}`)) : this.sys(`Top-up for ${r.name} was declined`), this.bc();
          return;
        }
        try {
          if (r.kind === "join") await this.seatPlayer(r.userId, r.name, r.amount);
          else {
            let ss = this.seatOf(r.userId);
            ss && await this.applyTopup(ss, r.amount);
          }
        } catch (e) {
          this.sendTo(r.userId, { t: "error", message: e.message }), this.sendTo(this.hostId, { t: "error", message: `${r.name}: ${e.message}` });
        }
        this.bc();
        return;
      }
      if (t === "kick") {
        let v = this.seats[Number(a.pos)];
        v && v.userId !== this.hostId && (v.isBot ? (this.engine.seatLeaving(v), this.seats[v.pos] = null, this.bc()) : await this.removeSeat(v, "kick"));
        return;
      }
      if (t === "start" && this.phase === "lobby") {
        this.phase = "playing", this.addLog("The host started the game"), this.engine.maybeStart(), this.bc();
        return;
      }
      if (t === "pause") {
        this.paused = !this.paused, this.engine.paused?.(this.paused), this.paused || this.engine.maybeStart(), this.sys(this.paused ? "Host paused the game" : "Game resumed"), this.bc();
        return;
      }
      if (t === "addbot" && this.game === "poker") {
        this.addBot(this.seats.filter((x) => x?.isBot).length + Math.floor(Math.random() * 3)), this.engine.maybeStart(), this.bc();
        return;
      }
      if (t === "close") {
        await this.close();
        return;
      }
      if (t === "settings") {
        let n = a.s || {}, st = this.settings;
        typeof n.title == "string" && n.title.trim() && (this.title = n.title.replace(/[<>]/g, "").trim().slice(0, 28)), THEMES.includes(n.theme) && (this.theme = n.theme), n.timer !== void 0 && [0, 15, 30, 60].includes(Number(n.timer)) && (st.timer = Number(n.timer)), n.autoNext !== void 0 && (st.autoNext = !!n.autoNext), this.sys("The host updated the table settings"), this.bc();
        return;
      }
    }
    s && this.engine.act(s, a);
  }
  async applyTopup(s, amt) {
    await this.addChips(s, amt);
    let L = this.ledger.get(s.userId);
    L && (L.buyin += amt, L.entries.push({ at: Date.now(), kind: "Top-up", amount: amt })), this.engine.busy() && this.engine.committedOf(s.pos) >= 0 && this.game === "poker" ? (s.pendingTopup += amt, this.sys(`${s.name} will add ${fmt(amt)} chips after this hand`)) : this.engine.busy() && this.game === "blackjack" ? s.pendingTopup += amt : (s.stack += amt, this.sys(`${s.name} added ${fmt(amt)} chips`), this.engine.maybeStart()), this.bc();
  }
  logPayload() {
    return {
      t: "log",
      code: this.code,
      title: this.title,
      game: this.game,
      createdAt: this.createdAt,
      log: this.log.slice(-3e3),
      ledger: [...this.ledger.entries()].map(([uid, l]) => {
        let s = this.seatOf(uid);
        return { name: l.name, buyin: l.buyin, cashout: l.cashout, stack: s ? s.stack + this.engine.committedOf(s.pos) + s.pendingTopup : 0, seated: !!s, entries: l.entries };
      })
    };
  }
};
async function createTable(user, body) {
  let game = body.game === "blackjack" ? "blackjack" : "poker", mode = body.mode === "house" && game === "poker" ? "house" : "friends", gs = (await gameSettings())[game];
  if (!gs.enabled) throw new HttpError(403, `${GAME_NAME[game]} tables are temporarily closed`);
  if ([...rooms.values()].filter((r) => r.hostId === user.id && !r.closed).length >= 3) throw bad("You already host 3 open tables. Close one first.");
  let maxPlayers = int(body.maxPlayers ?? (game === "poker" ? 6 : 7), "Max players", 2, game === "poker" ? 9 : 7), settings;
  if (game === "poker") {
    let bigBlind = int(body.bigBlind ?? 20, "Big blind", 2, 1e5), smallBlind = Math.max(1, Math.floor(bigBlind / 2));
    settings = { maxPlayers, minBuy: Math.max(gs.minBet, bigBlind * 20), maxBuy: Math.min(gs.maxBet, bigBlind * 300), timer: [0, 15, 30, 60].includes(Number(body.timer)) ? Number(body.timer) : 30, autoNext: !0, smallBlind, bigBlind, minBet: 0, maxBet: 0 };
  } else {
    let minBet = int(body.minBet ?? 10, "Min bet", 1, 1e7), maxBet = int(body.maxBet ?? 1e3, "Max bet", minBet, 1e8);
    settings = { maxPlayers, minBuy: int(body.minBuy ?? 100, "Min buy-in", 1, 1e9), maxBuy: int(body.maxBuy ?? 1e5, "Max buy-in", 1, 1e9), timer: [0, 15, 30, 60].includes(Number(body.timer)) ? Number(body.timer) : 30, autoNext: body.autoNext !== !1, smallBlind: 0, bigBlind: 0, minBet, maxBet }, settings.maxBuy < settings.minBuy && (settings.maxBuy = settings.minBuy);
  }
  let buyin = int(body.buyin, "Your buy-in", settings.minBuy, settings.maxBuy), title = (typeof body.title == "string" && body.title.trim() ? body.title : `${user.username}'s ${GAME_NAME[game]}`).replace(/[<>]/g, "").trim().slice(0, 28), theme = THEMES.includes(body.theme) ? body.theme : "montecarlo", room = new Room({ code: newCode(), game, mode, hostId: user.id, hostName: user.username, title, theme, settings });
  rooms.set(room.code, room);
  try {
    await room.seatPlayer(user.id, user.username, buyin);
  } catch (e) {
    throw rooms.delete(room.code), e;
  }
  if (mode === "house") {
    let bots = int(body.bots ?? 4, "Computer players", 1, maxPlayers - 1), start = Math.floor(Math.random() * BOTS.length);
    for (let i = 0; i < bots; i++) room.addBot(start + i);
    room.phase = "playing", setTimeout(() => room.engine.maybeStart(), 600);
  }
  return { code: room.code };
}
var getRoom = (code) => rooms.get(String(code || "").toUpperCase());
function myTables(userId) {
  return [...rooms.values()].filter((r) => r.seatOf(userId) || r.hostId === userId).map((r) => ({ code: r.code, game: r.game, mode: r.mode, title: r.title, theme: r.theme, host: r.hostId === userId, players: r.seats.filter(Boolean).length, max: r.settings.maxPlayers }));
}
function tableStats() {
  let players = 0;
  for (let r of rooms.values()) players += r.seats.filter((s) => s && !s.isBot).length;
  return { tables: rooms.size, players };
}
function tableConnect(ws, user, code) {
  let room = getRoom(code);
  if (!room) {
    ws.send(JSON.stringify({ t: "status", status: "notfound", code })), ws.close();
    return;
  }
  if (room.mode === "house" && user.id !== room.hostId) {
    ws.send(JSON.stringify({ t: "status", status: "private" })), ws.close();
    return;
  }
  let set = room.conns.get(user.id) || /* @__PURE__ */ new Set();
  set.add(ws), room.conns.set(user.id, set), room.lastHuman = Date.now();
  let s = room.seatOf(user.id);
  s && !s.connected && (s.connected = !0, room.sys(`${s.name} is back`), room.engine.onSeatChange?.(), room.engine.maybeStart()), ws.send(JSON.stringify({ t: "chat", full: !0, chat: room.chat.slice(-60) })), ws.send(JSON.stringify({ t: "state", s: room.view(user.id) })), room.bc();
  let bucket = 30, last = Date.now();
  ws.on("message", (raw) => {
    let now = Date.now();
    if (bucket = Math.min(30, bucket + (now - last) / 60), last = now, bucket < 1) return;
    bucket--;
    let m;
    try {
      m = JSON.parse(String(raw));
    } catch {
      return;
    }
    if (room.closed) return;
    let fail = (e) => ws.readyState === 1 && ws.send(JSON.stringify({ t: "error", message: e?.message || "Something went wrong" }));
    if (m.t === "ping") {
      ws.send('{"t":"pong"}');
      return;
    }
    if (m.t === "getlog") {
      ws.send(JSON.stringify(room.logPayload()));
      return;
    }
    if (m.t === "join") {
      room.serial(() => room.request(user.id, user.username, int(m.buyin, "Buy-in", 1, 1e9))).catch(fail);
      return;
    }
    m.t === "act" && room.serial(() => room.dispatch(user.id, user.username, m.a || {})).catch(fail);
  }), ws.on("close", () => {
    let set2 = room.conns.get(user.id);
    if (set2?.delete(ws), set2 && set2.size === 0) {
      room.conns.delete(user.id);
      let s2 = room.seatOf(user.id);
      s2 && (s2.connected = !1, room.sys(`${s2.name} disconnected`), room.engine.onSeatChange?.()), room.requests = room.requests.filter((r) => !(r.userId === user.id && r.kind === "join")), room.lastHuman = Date.now(), room.bc();
    }
  });
}
setInterval(() => {
  let now = Date.now();
  for (let r of rooms.values()) {
    try {
      r.engine.tick();
    } catch (e) {
      console.error("table tick", e.message);
    }
    ![...r.conns.keys()].length && now - r.lastHuman > 20 * 6e4 && r.serial(() => r.close("Table closed after 20 minutes without players")).catch(() => {
    });
  }
}, 300).unref();
async function recoverTables() {
  let open = await q("SELECT id, user_id, game, bet, state FROM game_rounds WHERE status='open' AND state ? 'table'");
  for (let r of open)
    await withTx(async (c) => {
      let stack = Math.max(0, Number(r.state.stack || 0)), t = await post(c, { userId: r.user_id, type: "refund", game: r.game, credit: stack, roundId: r.id, idemKey: `table-recover:${r.id}`, detail: `${GAME_NAME[r.game] || r.game} table ${r.state.table} \xB7 chips returned after server restart` });
      return await c.query("UPDATE game_rounds SET status='refunded', payout=$2, settled_at=now() WHERE id=$1", [r.id, stack]), t;
    });
  let crash2 = await q("SELECT id, user_id, bet FROM game_rounds WHERE status='open' AND game='crash'");
  for (let r of crash2)
    await withTx(async (c) => {
      await post(c, { userId: r.user_id, type: "refund", game: "crash", credit: Number(r.bet), roundId: r.id, idemKey: `crash-recover:${r.id}`, detail: "Crash bet returned after server restart" }), await c.query("UPDATE game_rounds SET status='refunded', payout=$2, settled_at=now() WHERE id=$1", [r.id, r.bet]);
    });
  return { tables: open.length, crash: crash2.length };
}

// server/stats.ts
init_db();
async function dashboard(userId) {
  let [w, agg, byGame, recent, chart, u] = await Promise.all([
    q1("SELECT balance FROM wallets WHERE user_id=$1", [userId]),
    q1(`SELECT count(*)::bigint AS played,
               coalesce(sum(CASE WHEN payout > bet THEN payout - bet ELSE 0 END),0)::bigint AS winnings,
               coalesce(sum(CASE WHEN payout < bet THEN bet - payout ELSE 0 END),0)::bigint AS losses,
               count(*) FILTER (WHERE payout > bet)::bigint AS wins,
               count(*) FILTER (WHERE payout = bet)::bigint AS pushes,
               coalesce(sum(bet),0)::bigint AS wagered, coalesce(max(payout - bet),0)::bigint AS biggest
          FROM game_rounds WHERE user_id=$1 AND status='settled'`, [userId]),
    q(`SELECT game, count(*)::bigint AS played, coalesce(sum(payout - bet),0)::bigint AS net
         FROM game_rounds WHERE user_id=$1 AND status='settled' GROUP BY game ORDER BY played DESC`, [userId]),
    q("SELECT id, game, bet, payout, result, settled_at FROM game_rounds WHERE user_id=$1 AND status='settled' ORDER BY id DESC LIMIT 12", [userId]),
    q(`SELECT to_char(d, 'YYYY-MM-DD') AS day,
              (SELECT closing FROM transactions t WHERE t.user_id=$1 AND t.created_at < (d + interval '1 day') AT TIME ZONE 'Asia/Kolkata' ORDER BY id DESC LIMIT 1) AS balance
         FROM generate_series((now() AT TIME ZONE 'Asia/Kolkata')::date - 29, (now() AT TIME ZONE 'Asia/Kolkata')::date, interval '1 day') d`, [userId]),
    q1("SELECT u.username, u.email, u.role, u.created_at, u.last_daily_bonus_at, p.avatar_key, (p.avatar_image IS NOT NULL) AS has_image, p.display_name FROM users u JOIN profiles p ON p.user_id=u.id WHERE u.id=$1", [userId])
  ]), played = Number(agg.played), decided = played - Number(agg.pushes);
  return {
    user: u,
    balance: Number(w?.balance || 0),
    stats: { played, winnings: Number(agg.winnings), losses: Number(agg.losses), wins: Number(agg.wins), winRate: decided > 0 ? Number(agg.wins) / decided : 0, wagered: Number(agg.wagered), biggest: Number(agg.biggest) },
    byGame: byGame.map((g) => ({ game: g.game, name: GAME_NAMES[g.game] || g.game, played: Number(g.played), net: Number(g.net) })),
    recent: recent.map(roundSummary),
    chart: chart.map((c) => ({ day: c.day, balance: c.balance === null ? null : Number(c.balance) })),
    nextDaily: u?.last_daily_bonus_at ? new Date(new Date(u.last_daily_bonus_at).getTime() + 24 * 36e5).toISOString() : null
  };
}
function roundSummary(r) {
  let outcome = "", res = r.result || {};
  switch (r.game) {
    case "slots":
      outcome = res.wins?.length ? `${res.wins.length} winning line${res.wins.length > 1 ? "s" : ""}` : "No win";
      break;
    case "roulette":
      outcome = `${res.number} ${res.color}`;
      break;
    case "baccarat":
      outcome = res.winner === "tie" ? `Tie ${res.playerTotal}\u2013${res.bankerTotal}` : `${res.winner === "player" ? "Player" : "Banker"} ${res.playerTotal}\u2013${res.bankerTotal}`;
      break;
    case "crash":
      outcome = res.cashedAt ? `Cashed out ${Number(res.cashedAt).toFixed(2)}\xD7` : `Crashed ${res.crashPoint ? Number(res.crashPoint).toFixed(2) + "\xD7" : ""}`;
      break;
    case "blackjack":
      outcome = res.table ? "Table session" : (res.hands || []).map((h) => h.result).join(" / ");
      break;
    case "poker":
      outcome = "Table session";
      break;
  }
  return { id: r.id, game: r.game, name: GAME_NAMES[r.game] || r.game, bet: Number(r.bet), payout: Number(r.payout), net: Number(r.payout) - Number(r.bet), outcome, at: r.settled_at, user: r.username };
}
async function publicActivity() {
  return (await q(`SELECT r.id, r.game, r.bet, r.payout, r.result, r.settled_at, u.username FROM game_rounds r JOIN users u ON u.id=r.user_id
     WHERE r.status='settled' ORDER BY r.id DESC LIMIT 14`)).map((r) => ({ ...roundSummary(r), user: maskName(r.username) }));
}
var maskName = (n) => n.length <= 3 ? n[0] + "**" : n.slice(0, 2) + "\u2022\u2022\u2022" + n.slice(-1);
async function adminOverview() {
  let [users, active, rounds, pop, flow, recentUsers] = await Promise.all([
    q1("SELECT count(*)::bigint AS total, count(*) FILTER (WHERE created_at > now() - interval '7 days')::bigint AS new7, count(*) FILTER (WHERE status='suspended')::bigint AS suspended FROM users"),
    q1("SELECT count(*) FILTER (WHERE last_seen_at > now() - interval '15 minutes')::bigint AS now, count(*) FILTER (WHERE last_seen_at > now() - interval '24 hours')::bigint AS day FROM users"),
    q1("SELECT count(*)::bigint AS total, count(*) FILTER (WHERE created_at > now() - interval '24 hours')::bigint AS day FROM game_rounds WHERE status='settled'"),
    q("SELECT game, count(*)::bigint AS played, coalesce(sum(bet),0)::bigint AS wagered, coalesce(sum(payout),0)::bigint AS paid FROM game_rounds WHERE status='settled' GROUP BY game ORDER BY played DESC"),
    q(`SELECT to_char(date_trunc('day', created_at AT TIME ZONE 'Asia/Kolkata'), 'YYYY-MM-DD') AS day,
              sum(wagered)::bigint AS wagered, sum(won)::bigint AS won,
              sum(CASE WHEN type IN ('welcome_bonus','promotional') THEN net ELSE 0 END)::bigint AS bonuses
         FROM transactions WHERE created_at > now() - interval '14 days' GROUP BY 1 ORDER BY 1`),
    q("SELECT u.id, u.username, u.email, u.created_at, w.balance FROM users u JOIN wallets w ON w.user_id=u.id ORDER BY u.id DESC LIMIT 6")
  ]), circulation = await q1("SELECT coalesce(sum(balance),0)::bigint AS total FROM wallets");
  return {
    users: { total: Number(users.total), new7: Number(users.new7), suspended: Number(users.suspended), activeNow: Number(active.now), active24h: Number(active.day) },
    rounds: { total: Number(rounds.total), day: Number(rounds.day) },
    popularity: GAMES.map((g) => {
      let p = pop.find((x) => x.game === g) || {};
      return { game: g, name: GAME_NAMES[g], played: Number(p.played || 0), wagered: Number(p.wagered || 0), paid: Number(p.paid || 0) };
    }),
    flow: flow.map((f) => ({ day: f.day, wagered: Number(f.wagered), won: Number(f.won), bonuses: Number(f.bonuses) })),
    circulation: Number(circulation.total),
    recentUsers
  };
}
var bootAt = Date.now();
async function health() {
  let t0 = performance.now(), dbOk = !0;
  try {
    await pool.query("SELECT 1");
  } catch {
    dbOk = !1;
  }
  let dbMs = Math.round((performance.now() - t0) * 10) / 10, m = process.memoryUsage();
  return {
    ok: dbOk,
    uptimeSec: Math.round((Date.now() - bootAt) / 1e3),
    dbOk,
    dbMs,
    memoryMB: Math.round(m.rss / 1048576),
    heapMB: Math.round(m.heapUsed / 1048576),
    pool: { total: pool.totalCount, idle: pool.idleCount, waiting: pool.waitingCount },
    crash: crashStatus(),
    tables: tableStats(),
    node: process.version
  };
}
async function publicWins() {
  return (await q(`SELECT r.id, r.game, r.bet, r.payout, r.result, r.settled_at, u.username FROM game_rounds r JOIN users u ON u.id=r.user_id
     WHERE r.status='settled' AND r.payout > r.bet AND r.settled_at > now() - interval '7 days' ORDER BY r.id DESC LIMIT 24`)).map((r) => ({ ...roundSummary(r), user: maskName(r.username) }));
}

// server/index.ts
var PORT = Number(process.env.PORT || 3e3), routes = [];
function route(method, p, h, a) {
  let keys = [], re = new RegExp("^" + p.replace(/:(\w+)/g, (_, k) => (keys.push(k), "([^/]+)")) + "$");
  routes.push({ method, re, keys, h, auth: a });
}
var origin = (req) => process.env.PUBLIC_URL ? process.env.PUBLIC_URL.replace(/\/$/, "") : `${String(req.headers["x-forwarded-proto"] || "http").split(",")[0]}://${String(req.headers.host || "localhost").replace(/[^a-zA-Z0-9.:-]/g, "")}`;
route("GET", "/api/public/config", async () => ({ games: await gameSettings(), slots: { pays: SLOT_PAYS, lines: PAYLINES }, welcomeBonus: WELCOME_BONUS, dailyBonus: DAILY_BONUS, mailReady: !!(process.env.RESEND_API_KEY && process.env.MAIL_FROM) }));
route("GET", "/api/public/activity", async () => ({ activity: await publicActivity() }));
route("GET", "/api/public/wins", async () => ({ wins: await publicWins() }));
route("POST", "/api/auth/register", async ({ req, res, body }) => {
  let r = await register(req, body);
  return res.setHeader("set-cookie", r.cookie), { user: r.user };
});
route("POST", "/api/auth/login", async ({ req, res, body }) => {
  let r = await login(req, body);
  return res.setHeader("set-cookie", r.cookie), { user: r.user };
});
route("POST", "/api/auth/logout", async ({ req, res }) => (res.setHeader("set-cookie", await logout(req)), { ok: !0 }));
route("POST", "/api/auth/logout-all", async ({ req, res, user }) => (res.setHeader("set-cookie", await logoutAll(user.id, req)), { ok: !0 }), "user");
route("POST", "/api/auth/forgot", async ({ req, body }) => requestReset(req, body, origin(req)));
route("POST", "/api/auth/reset", async ({ body }) => (await completeReset(body), { ok: !0 }));
route("GET", "/api/me", async ({ user }) => {
  if (!user) return { user: null };
  let p = await q1("SELECT p.display_name, p.avatar_key, (p.avatar_image IS NOT NULL) AS has_image, p.prefs, w.balance, (SELECT coalesce(sum(wagered),0) FROM transactions t WHERE t.user_id=p.user_id)::bigint AS xp FROM profiles p JOIN wallets w ON w.user_id=p.user_id WHERE p.user_id=$1", [user.id]);
  return { user: {
    ...user,
    displayName: p.display_name,
    avatarKey: p.avatar_key,
    hasImage: p.has_image,
    prefs: p.prefs,
    balance: Number(p.balance),
    xp: Number(p.xp),
    nextDaily: user.last_daily_bonus_at ? new Date(new Date(user.last_daily_bonus_at).getTime() + 24 * 36e5).toISOString() : null
  } };
});
route("POST", "/api/bonus/daily", async ({ user }) => {
  let t = await claimDaily(user.id);
  return { balance: t.closing, txn: t.txn_ref, amount: t.net };
}, "user");
route("POST", "/api/password", async ({ user, body }) => (await changePassword(user.id, body), { ok: !0 }), "user");
var AVATARS = ["crown", "spade", "diamond", "club", "heart", "chip", "ace", "star", "monogram"];
route("POST", "/api/profile", async ({ user, body }) => {
  let sets = [], p = [user.id];
  if (body.displayName !== void 0 && (p.push(str(body.displayName, "Display name", 2, 24).replace(/[<>]/g, "")), sets.push(`display_name=$${p.length}`)), body.avatarKey !== void 0) {
    if (!AVATARS.includes(body.avatarKey)) throw bad("Unknown avatar");
    p.push(body.avatarKey), sets.push(`avatar_key=$${p.length}`);
  }
  if (body.avatarImage !== void 0)
    if (body.avatarImage === null) sets.push("avatar_image=NULL");
    else {
      if (typeof body.avatarImage != "string" || !/^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+/=]+$/.test(body.avatarImage)) throw bad("Please upload a JPG or PNG image");
      if (body.avatarImage.length > 22e4) throw bad("Image is too large");
      p.push(body.avatarImage), sets.push(`avatar_image=$${p.length}`);
    }
  if (body.prefs !== void 0) {
    let pr = { sound: !!body.prefs.sound, reducedMotion: !!body.prefs.reducedMotion, defaultBet: Math.max(10, Math.min(1e7, Math.floor(Number(body.prefs.defaultBet) || 100))) };
    p.push(JSON.stringify(pr)), sets.push(`prefs=$${p.length}`);
  }
  return sets.length ? (await q(`UPDATE profiles SET ${sets.join(", ")}, updated_at=now() WHERE user_id=$1`, p), { ok: !0 }) : { ok: !0 };
}, "user");
route("GET", "/api/avatar/:id", async ({ res, params }) => {
  let r = await q1("SELECT avatar_image FROM profiles WHERE user_id=$1", [Number(params.id) || 0]);
  if (!r?.avatar_image) throw new HttpError(404, "No image");
  let m = /^data:(image\/\w+);base64,(.*)$/.exec(r.avatar_image);
  res.writeHead(200, { "content-type": m[1], "cache-control": "private, max-age=60" }), res.end(Buffer.from(m[2], "base64"));
});
route("GET", "/api/dashboard", async ({ user }) => dashboard(user.id), "user");
var ledgerFilters = (u) => ({
  game: u.searchParams.get("game") || void 0,
  type: u.searchParams.get("type") || void 0,
  from: valDate(u.searchParams.get("from")),
  to: valDate(u.searchParams.get("to")),
  search: (u.searchParams.get("search") || "").slice(0, 60) || void 0,
  sort: u.searchParams.get("sort") === "asc" ? "asc" : "desc",
  limit: Number(u.searchParams.get("limit") || 50),
  offset: Number(u.searchParams.get("offset") || 0)
}), valDate = (s) => s && /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : void 0;
route("GET", "/api/ledger", async ({ user, url: url2 }) => listLedger(user.id, ledgerFilters(url2)), "user");
route("GET", "/api/ledger/summary", async ({ user, url: url2 }) => {
  let period = ["day", "week", "month"].includes(String(url2.searchParams.get("period"))) ? url2.searchParams.get("period") : "day";
  return { period, rows: await ledgerSummary(user.id, period, ledgerFilters(url2)) };
}, "user");
route("GET", "/api/ledger/verify", async ({ user }) => verifyLedger(user.id), "user");
route("GET", "/api/ledger/export.csv", async ({ user, url: url2, res }) => {
  let f = { ...ledgerFilters(url2), limit: 5e3, offset: 0 }, { rows } = await listLedger(user.id, f);
  csv(res, `royale21casino-ledger-${(/* @__PURE__ */ new Date()).toISOString().slice(0, 10)}.csv`, rows);
}, "user");
var TYPE_LABEL = { welcome_bonus: "Welcome bonus", game_wager: "Game wager", game_win: "Game win", game_loss: "Game loss", refund: "Refund", promotional: "Promotional credits" };
function csv(res, name, rows, withUser = !1) {
  let esc = (v) => '"' + String(v ?? "").replace(/"/g, '""') + '"', ist = (d) => new Date(d).toLocaleString("en-GB", { timeZone: "Asia/Kolkata", hour12: !1 }), lines = [["Transaction ID", ...withUser ? ["Player"] : [], "Date", "Time (IST)", "Game", "Type", "Details", "Opening balance", "Wagered", "Won", "Net", "Closing balance", "Status"].join(",")];
  for (let r of rows) {
    let [d, t] = ist(r.created_at).split(", ");
    lines.push([esc(r.txn_ref), ...withUser ? [esc(r.username)] : [], esc(d), esc(t), esc(GAME_NAMES[r.game] || ""), esc(TYPE_LABEL[r.type] || r.type), esc(r.detail), r.opening, r.wagered, r.won, r.net, r.closing, esc(r.status)].join(","));
  }
  res.writeHead(200, { "content-type": "text/csv; charset=utf-8", "content-disposition": `attachment; filename="${name}"`, "cache-control": "no-store" }), res.end("\uFEFF" + lines.join(`\r
`));
}
route("GET", "/api/history", async ({ user, url: url2 }) => {
  let game = url2.searchParams.get("game");
  return { rows: (await q(`SELECT id, game, bet, payout, result, settled_at FROM game_rounds WHERE user_id=$1 AND status='settled' ${game && GAMES.includes(game) ? "AND game=$2" : ""} ORDER BY id DESC LIMIT 30`, game && GAMES.includes(game) ? [user.id, game] : [user.id])).map((r) => ({ ...roundSummary(r), result: r.result })) };
}, "user");
route("POST", "/api/games/slots", async ({ user, body }) => (rateLimit("play:" + user.id, 12, 1e3), playSlots(user.id, body)), "user");
route("POST", "/api/games/roulette", async ({ user, body }) => (rateLimit("play:" + user.id, 12, 1e3), playRoulette(user.id, body)), "user");
route("POST", "/api/games/baccarat", async ({ user, body }) => (rateLimit("play:" + user.id, 12, 1e3), playBaccarat(user.id, body)), "user");
route("POST", "/api/games/blackjack/start", async ({ user, body }) => (rateLimit("play:" + user.id, 12, 1e3), bjStart(user.id, body)), "user");
route("POST", "/api/games/blackjack/action", async ({ user, body }) => (rateLimit("play:" + user.id, 20, 1e3), bjAction(user.id, body)), "user");
route("GET", "/api/games/blackjack/current", async ({ user }) => ({ round: await bjCurrent(user.id) }), "user");
route("POST", "/api/tables", async ({ user, body }) => (rateLimit("table:" + user.id, 6, 6e4), createTable(user, body)), "user");
route("GET", "/api/tables/mine", async ({ user }) => ({ tables: myTables(user.id) }), "user");
route("GET", "/api/tables/:code", async ({ params }) => {
  let r = getRoom(params.code);
  if (!r || r.mode === "house") throw new HttpError(404, "No open table with that code");
  return { code: r.code, game: r.game, title: r.title, theme: r.theme, host: r.hostName, players: r.seats.filter(Boolean).length, max: r.settings.maxPlayers, minBuy: r.settings.minBuy, maxBuy: r.settings.maxBuy, blinds: [r.settings.smallBlind, r.settings.bigBlind], bets: [r.settings.minBet, r.settings.maxBet] };
}, "user");
var TOPICS = ["General", "My account", "My ledger", "A game", "Private tables", "Responsible gaming"];
route("POST", "/api/contact", async ({ req, user, body }) => {
  rateLimit("contact:" + (user?.id ?? clientIpOf(req)), 3, 10 * 6e4);
  let name = str(body.name, "Name", 2, 60).replace(/[<>]/g, ""), email = str(body.email, "Email", 5, 120);
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) throw bad("Please enter a valid email");
  let topic = TOPICS.includes(body.topic) ? body.topic : "General", message = str(body.message, "Message", 10, 2e3);
  return await q("INSERT INTO contact_messages (user_id, name, email, topic, message) VALUES ($1,$2,$3,$4,$5)", [user?.id ?? null, name, email, topic, message]), { ok: !0 };
});
var clientIpOf = (req) => String(req.headers["x-forwarded-for"] || "").split(",")[0].trim() || req.socket.remoteAddress || "ip";
async function audit(adminId, action, targetId, detail) {
  await q("INSERT INTO audit_log (admin_id, action, target_id, detail) VALUES ($1,$2,$3,$4)", [adminId, action, targetId, JSON.stringify(detail)]);
}
route("GET", "/api/admin/overview", async () => adminOverview(), "admin");
route("GET", "/api/admin/health", async () => health(), "admin");
route("GET", "/api/admin/users", async ({ url: url2 }) => {
  let s = (url2.searchParams.get("search") || "").slice(0, 60), off = Math.max(0, Number(url2.searchParams.get("offset")) || 0), rows = await q(`SELECT u.id, u.username, u.email, u.role, u.status, u.created_at, u.last_seen_at, w.balance,
      (SELECT count(*) FROM game_rounds r WHERE r.user_id=u.id AND r.status='settled')::bigint AS rounds
      FROM users u JOIN wallets w ON w.user_id=u.id
      ${s ? "WHERE u.username ILIKE $1 OR u.email ILIKE $1" : ""} ORDER BY u.id DESC LIMIT 50 OFFSET ${off}`, s ? ["%" + s + "%"] : []), total2 = await q1(`SELECT count(*)::bigint AS n FROM users ${s ? "WHERE username ILIKE $1 OR email ILIKE $1" : ""}`, s ? ["%" + s + "%"] : []);
  return { rows, total: Number(total2.n) };
}, "admin");
route("POST", "/api/admin/users/:id/status", async ({ user, params, body }) => {
  let id = int(params.id, "User", 1, 1e15), status = body.status === "suspended" ? "suspended" : "active";
  if (id === user.id) throw bad("You cannot suspend your own account");
  return await q("UPDATE users SET status=$2 WHERE id=$1", [id, status]), status === "suspended" && await q("DELETE FROM sessions WHERE user_id=$1", [id]), await audit(user.id, status === "suspended" ? "suspend_user" : "activate_user", id, {}), { ok: !0 };
}, "admin");
route("POST", "/api/admin/users/:id/role", async ({ user, params, body }) => {
  let id = int(params.id, "User", 1, 1e15), role = body.role === "admin" ? "admin" : "player";
  if (id === user.id) throw bad("You cannot change your own role");
  return await q("UPDATE users SET role=$2 WHERE id=$1", [id, role]), await audit(user.id, "set_role", id, { role }), { ok: !0 };
}, "admin");
route("POST", "/api/admin/users/:id/grant", async ({ user, params, body }) => {
  let id = int(params.id, "User", 1, 1e15), amount = int(body.amount, "Amount", 1, 1e7), note = typeof body.note == "string" && body.note.trim() ? body.note.trim().slice(0, 80) : "Promotional credits from ROYALE21CASINO", t = await withTx((c) => post(c, { userId: id, type: "promotional", credit: amount, detail: note }));
  return await audit(user.id, "grant_credits", id, { amount, note, txn: t.txn_ref }), { ok: !0, txn: t.txn_ref, balance: t.closing };
}, "admin");
route("POST", "/api/admin/users/:id/reset-link", async ({ user, params, req }) => {
  let id = int(params.id, "User", 1, 1e15), t = await createResetToken(id, "admin");
  return await audit(user.id, "reset_link", id, {}), { link: `${origin(req)}/reset-password?token=${t}`, expiresInMinutes: 60 };
}, "admin");
route("GET", "/api/admin/transactions", async ({ url: url2 }) => {
  let f = ledgerFilters(url2), uid = url2.searchParams.get("user");
  return listLedger(uid ? Number(uid) : null, f);
}, "admin");
route("GET", "/api/admin/transactions/export.csv", async ({ url: url2, res }) => {
  let { rows } = await listLedger(null, { ...ledgerFilters(url2), limit: 5e3, offset: 0 });
  csv(res, `royale21casino-all-transactions-${(/* @__PURE__ */ new Date()).toISOString().slice(0, 10)}.csv`, rows, !0);
}, "admin");
route("GET", "/api/admin/games", async () => ({ games: await gameSettings() }), "admin");
route("POST", "/api/admin/games", async ({ user, body }) => {
  let g = await saveGameSettings(body.games || {});
  return await audit(user.id, "game_settings", null, body.games), { games: g };
}, "admin");
route("GET", "/api/admin/verify", async () => verifyLedger(), "admin");
route("GET", "/api/admin/messages", async () => ({ rows: await q("SELECT m.*, u.username FROM contact_messages m LEFT JOIN users u ON u.id=m.user_id ORDER BY m.id DESC LIMIT 100") }), "admin");
route("POST", "/api/admin/messages/:id/handled", async ({ params, body }) => (await q("UPDATE contact_messages SET handled=$2 WHERE id=$1", [int(params.id, "Message", 1, 1e15), body.handled !== !1]), { ok: !0 }), "admin");
route("GET", "/api/admin/audit", async () => ({ rows: await q("SELECT a.*, u.username AS admin FROM audit_log a LEFT JOIN users u ON u.id=a.admin_id ORDER BY a.id DESC LIMIT 60") }), "admin");
var assets = /* @__PURE__ */ new Map(), TYPES = { ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8", ".svg": "image/svg+xml", ".png": "image/png", ".ico": "image/x-icon", ".woff2": "font/woff2", ".woff": "font/woff", ".json": "application/json", ".webmanifest": "application/manifest+json", ".txt": "text/plain; charset=utf-8", ".xml": "application/xml", ".jpg": "image/jpeg", ".webp": "image/webp" };
function addAsset(p, body) {
  let ext = import_node_path.default.extname(p), type = TYPES[ext] || "application/octet-stream", a = { type, body, immutable: /\/assets\//.test(p) };
  /text|javascript|svg|json|xml|manifest/.test(type) && body.length > 1024 && (a.gz = import_node_zlib.default.gzipSync(body, { level: 9 }), a.br = import_node_zlib.default.brotliCompressSync(body, { params: { [import_node_zlib.default.constants.BROTLI_PARAM_QUALITY]: 11 } })), assets.set(p, a);
}
function loadAssets() {
  let here = import_node_path.default.dirname(process.argv[1] || "."), bundle = [import_node_path.default.join(here, "site.bundle"), import_node_path.default.join(process.cwd(), "site.bundle")].find((f) => import_node_fs.default.existsSync(f));
  if (bundle) {
    let map = JSON.parse(import_node_fs.default.readFileSync(bundle, "utf8"));
    for (let [p, b64] of Object.entries(map)) addAsset(p, Buffer.from(b64, "base64"));
    console.log(`Loaded ${assets.size} site files from ${import_node_path.default.basename(bundle)}`);
    return;
  }
  let dir = [import_node_path.default.join(here, "../dist/client"), import_node_path.default.join(process.cwd(), "dist/client")].find((d) => import_node_fs.default.existsSync(d));
  if (!dir) {
    console.warn("No site files found (site.bundle missing)");
    return;
  }
  let walk = (d) => {
    for (let f of import_node_fs.default.readdirSync(d)) {
      let full = import_node_path.default.join(d, f);
      import_node_fs.default.statSync(full).isDirectory() ? walk(full) : addAsset("/" + import_node_path.default.relative(dir, full).split(import_node_path.default.sep).join("/"), import_node_fs.default.readFileSync(full));
    }
  };
  walk(dir), console.log(`Loaded ${assets.size} site files from ${dir}`);
}
var CSP = "default-src 'self'; img-src 'self' data: blob:; style-src 'self' 'unsafe-inline'; font-src 'self' data:; script-src 'self'; connect-src 'self' ws: wss:; frame-ancestors 'none'; base-uri 'self'; form-action 'self'";
function serveStatic(req, res, pathname) {
  if (pathname === "/google743a355452be7de8.html") {
    res.writeHead(200, { "content-type": "text/html" }), res.end("google-site-verification: google743a355452be7de8.html");
    return;
  }
  let a = assets.get(pathname), isIndex = !1;
  if (!a && !import_node_path.default.extname(pathname) && (a = assets.get("/index.html"), isIndex = !0), !a) {
    send(res, 404, "Not found");
    return;
  }
  let enc = String(req.headers["accept-encoding"] || ""), body = a.body, h = { "content-type": a.type, vary: "Accept-Encoding", "x-content-type-options": "nosniff" };
  a.br && /\bbr\b/.test(enc) ? (body = a.br, h["content-encoding"] = "br") : a.gz && /gzip/.test(enc) && (body = a.gz, h["content-encoding"] = "gzip"), h["cache-control"] = a.immutable ? "public, max-age=31536000, immutable" : a.type.startsWith("text/html") ? "no-cache" : "public, max-age=86400", (a.type.startsWith("text/html") || isIndex) && (h["content-security-policy"] = CSP, h["x-frame-options"] = "DENY", h["referrer-policy"] = "strict-origin-when-cross-origin", h["permissions-policy"] = "camera=(), microphone=(), geolocation=()", String(req.headers["x-forwarded-proto"] || "").startsWith("https") && (h["strict-transport-security"] = "max-age=31536000")), res.writeHead(200, h), res.end(body);
}
var server = import_node_http.default.createServer(async (req, res) => {
  let url2 = new URL(req.url || "/", "http://x"), pathname = decodeURIComponent(url2.pathname);
  if (pathname === "/health") {
    send(res, 200, { ok: !0 });
    return;
  }
  if (!pathname.startsWith("/api/")) {
    req.method === "GET" || req.method === "HEAD" ? serveStatic(req, res, pathname) : send(res, 405, "Method not allowed");
    return;
  }
  try {
    let r = routes.find((x) => x.method === req.method && x.re.test(pathname));
    if (!r) throw new HttpError(404, "Not found");
    if (req.method !== "GET") {
      if (req.headers["x-r21"] !== "1") throw new HttpError(403, "Request blocked");
      let o = req.headers.origin;
      if (o && new URL(o).host !== req.headers.host) throw new HttpError(403, "Request blocked");
    }
    let user = await userFromRequest(req);
    if (r.auth && !user) throw new HttpError(401, "Please sign in", "AUTH");
    if (r.auth === "admin" && user.role !== "admin") throw new HttpError(403, "Administrators only");
    let m = r.re.exec(pathname), params = {};
    r.keys.forEach((k, i) => params[k] = m[i + 1]);
    let body = req.method === "GET" ? {} : await readJson(req), out = await r.h({ req, res, url: url2, user, body, params });
    out !== void 0 && !res.headersSent && send(res, 200, out);
  } catch (e) {
    let status = e instanceof HttpError ? e.status : e.code === "23505" ? 409 : 500;
    status === 500 && console.error(req.method, pathname, e), res.headersSent || send(res, status, { error: status === 500 ? "Something went wrong. Please try again." : e.message, code: e.code });
  }
}), wss = new import_websocket_server.default({ noServer: !0, maxPayload: 16 * 1024, perMessageDeflate: !1 });
server.on("upgrade", async (req, socket, head) => {
  let url2 = new URL(req.url || "/", "http://x");
  try {
    let o = req.headers.origin;
    if (o && new URL(o).host !== req.headers.host) throw new Error("origin");
    let user = await userFromRequest(req);
    url2.pathname === "/live/crash" ? wss.handleUpgrade(req, socket, head, (ws) => {
      req.socket.setNoDelay?.(!0), crashConnect(ws, user);
    }) : url2.pathname === "/live/table" && user ? wss.handleUpgrade(req, socket, head, (ws) => {
      req.socket.setNoDelay?.(!0), tableConnect(ws, user, url2.searchParams.get("code") || "");
    }) : socket.destroy();
  } catch {
    socket.destroy();
  }
});
setInterval(() => {
  for (let ws of wss.clients) {
    if (ws.isAlive === !1) {
      ws.terminate();
      continue;
    }
    ws.isAlive = !1, ws.ping(), ws.once("pong", () => ws.isAlive = !0);
  }
}, 25e3).unref();
async function boot() {
  process.env.DATABASE_URL || console.error("Missing DATABASE_URL. The casino needs a PostgreSQL database."), await migrate(), process.env.ADMIN_EMAIL && await q("UPDATE users SET role='admin' WHERE email=$1", [process.env.ADMIN_EMAIL.trim().toLowerCase()]);
  let rec = await recoverTables();
  (rec.tables || rec.crash) && console.log(`Returned chips for ${rec.tables} table seats and ${rec.crash} crash bets after restart`), await q("DELETE FROM sessions WHERE expires_at < now()"), loadAssets(), await startCrashEngine(), server.listen(PORT, () => console.log(`ROYALE21CASINO running on port ${PORT}`));
}
boot().catch((e) => {
  console.error("Startup failed:", e), process.exit(1);
});
process.on("unhandledRejection", (e) => console.error("unhandled", e?.message || e));
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  fmt,
  pool
});
