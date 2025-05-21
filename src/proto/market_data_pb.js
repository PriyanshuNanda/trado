/*eslint-disable block-scoped-var, id-length, no-control-regex, no-magic-numbers, no-prototype-builtins, no-redeclare, no-shadow, no-var, sort-vars*/
"use strict";

var $protobuf = require("protobufjs/minimal");

// Common aliases
var $Reader = $protobuf.Reader, $Writer = $protobuf.Writer, $util = $protobuf.util;

// Exported root namespace
var $root = $protobuf.roots["default"] || ($protobuf.roots["default"] = {});

$root.marketdata = (function() {

    /**
     * Namespace marketdata.
     * @exports marketdata
     * @namespace
     */
    var marketdata = {};

    marketdata.MarketData = (function() {

        /**
         * Properties of a MarketData.
         * @memberof marketdata
         * @interface IMarketData
         * @property {number|null} [ltp] MarketData ltp
         * @property {number|null} [cp] MarketData cp
         * @property {string|null} [token] MarketData token
         * @property {number|Long|null} [timestamp] MarketData timestamp
         */

        /**
         * Constructs a new MarketData.
         * @memberof marketdata
         * @classdesc Represents a MarketData.
         * @implements IMarketData
         * @constructor
         * @param {marketdata.IMarketData=} [properties] Properties to set
         */
        function MarketData(properties) {
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null)
                        this[keys[i]] = properties[keys[i]];
        }

        /**
         * MarketData ltp.
         * @member {number} ltp
         * @memberof marketdata.MarketData
         * @instance
         */
        MarketData.prototype.ltp = 0;

        /**
         * MarketData cp.
         * @member {number} cp
         * @memberof marketdata.MarketData
         * @instance
         */
        MarketData.prototype.cp = 0;

        /**
         * MarketData token.
         * @member {string} token
         * @memberof marketdata.MarketData
         * @instance
         */
        MarketData.prototype.token = "";

        /**
         * MarketData timestamp.
         * @member {number|Long} timestamp
         * @memberof marketdata.MarketData
         * @instance
         */
        MarketData.prototype.timestamp = $util.Long ? $util.Long.fromBits(0,0,false) : 0;

        /**
         * Creates a new MarketData instance using the specified properties.
         * @function create
         * @memberof marketdata.MarketData
         * @static
         * @param {marketdata.IMarketData=} [properties] Properties to set
         * @returns {marketdata.MarketData} MarketData instance
         */
        MarketData.create = function create(properties) {
            return new MarketData(properties);
        };

        /**
         * Encodes the specified MarketData message. Does not implicitly {@link marketdata.MarketData.verify|verify} messages.
         * @function encode
         * @memberof marketdata.MarketData
         * @static
         * @param {marketdata.IMarketData} message MarketData message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        MarketData.encode = function encode(message, writer) {
            if (!writer)
                writer = $Writer.create();
            if (message.ltp != null && Object.hasOwnProperty.call(message, "ltp"))
                writer.uint32(/* id 1, wireType 1 =*/9).double(message.ltp);
            if (message.cp != null && Object.hasOwnProperty.call(message, "cp"))
                writer.uint32(/* id 2, wireType 1 =*/17).double(message.cp);
            if (message.token != null && Object.hasOwnProperty.call(message, "token"))
                writer.uint32(/* id 3, wireType 2 =*/26).string(message.token);
            if (message.timestamp != null && Object.hasOwnProperty.call(message, "timestamp"))
                writer.uint32(/* id 4, wireType 0 =*/32).int64(message.timestamp);
            return writer;
        };

        /**
         * Encodes the specified MarketData message, length delimited. Does not implicitly {@link marketdata.MarketData.verify|verify} messages.
         * @function encodeDelimited
         * @memberof marketdata.MarketData
         * @static
         * @param {marketdata.IMarketData} message MarketData message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        MarketData.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a MarketData message from the specified reader or buffer.
         * @function decode
         * @memberof marketdata.MarketData
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {marketdata.MarketData} MarketData
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        MarketData.decode = function decode(reader, length, error) {
            if (!(reader instanceof $Reader))
                reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length, message = new $root.marketdata.MarketData();
            while (reader.pos < end) {
                var tag = reader.uint32();
                if (tag === error)
                    break;
                switch (tag >>> 3) {
                case 1: {
                        message.ltp = reader.double();
                        break;
                    }
                case 2: {
                        message.cp = reader.double();
                        break;
                    }
                case 3: {
                        message.token = reader.string();
                        break;
                    }
                case 4: {
                        message.timestamp = reader.int64();
                        break;
                    }
                default:
                    reader.skipType(tag & 7);
                    break;
                }
            }
            return message;
        };

        /**
         * Decodes a MarketData message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof marketdata.MarketData
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {marketdata.MarketData} MarketData
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        MarketData.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader))
                reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a MarketData message.
         * @function verify
         * @memberof marketdata.MarketData
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        MarketData.verify = function verify(message) {
            if (typeof message !== "object" || message === null)
                return "object expected";
            if (message.ltp != null && message.hasOwnProperty("ltp"))
                if (typeof message.ltp !== "number")
                    return "ltp: number expected";
            if (message.cp != null && message.hasOwnProperty("cp"))
                if (typeof message.cp !== "number")
                    return "cp: number expected";
            if (message.token != null && message.hasOwnProperty("token"))
                if (!$util.isString(message.token))
                    return "token: string expected";
            if (message.timestamp != null && message.hasOwnProperty("timestamp"))
                if (!$util.isInteger(message.timestamp) && !(message.timestamp && $util.isInteger(message.timestamp.low) && $util.isInteger(message.timestamp.high)))
                    return "timestamp: integer|Long expected";
            return null;
        };

        /**
         * Creates a MarketData message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof marketdata.MarketData
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {marketdata.MarketData} MarketData
         */
        MarketData.fromObject = function fromObject(object) {
            if (object instanceof $root.marketdata.MarketData)
                return object;
            var message = new $root.marketdata.MarketData();
            if (object.ltp != null)
                message.ltp = Number(object.ltp);
            if (object.cp != null)
                message.cp = Number(object.cp);
            if (object.token != null)
                message.token = String(object.token);
            if (object.timestamp != null)
                if ($util.Long)
                    (message.timestamp = $util.Long.fromValue(object.timestamp)).unsigned = false;
                else if (typeof object.timestamp === "string")
                    message.timestamp = parseInt(object.timestamp, 10);
                else if (typeof object.timestamp === "number")
                    message.timestamp = object.timestamp;
                else if (typeof object.timestamp === "object")
                    message.timestamp = new $util.LongBits(object.timestamp.low >>> 0, object.timestamp.high >>> 0).toNumber();
            return message;
        };

        /**
         * Creates a plain object from a MarketData message. Also converts values to other types if specified.
         * @function toObject
         * @memberof marketdata.MarketData
         * @static
         * @param {marketdata.MarketData} message MarketData
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        MarketData.toObject = function toObject(message, options) {
            if (!options)
                options = {};
            var object = {};
            if (options.defaults) {
                object.ltp = 0;
                object.cp = 0;
                object.token = "";
                if ($util.Long) {
                    var long = new $util.Long(0, 0, false);
                    object.timestamp = options.longs === String ? long.toString() : options.longs === Number ? long.toNumber() : long;
                } else
                    object.timestamp = options.longs === String ? "0" : 0;
            }
            if (message.ltp != null && message.hasOwnProperty("ltp"))
                object.ltp = options.json && !isFinite(message.ltp) ? String(message.ltp) : message.ltp;
            if (message.cp != null && message.hasOwnProperty("cp"))
                object.cp = options.json && !isFinite(message.cp) ? String(message.cp) : message.cp;
            if (message.token != null && message.hasOwnProperty("token"))
                object.token = message.token;
            if (message.timestamp != null && message.hasOwnProperty("timestamp"))
                if (typeof message.timestamp === "number")
                    object.timestamp = options.longs === String ? String(message.timestamp) : message.timestamp;
                else
                    object.timestamp = options.longs === String ? $util.Long.prototype.toString.call(message.timestamp) : options.longs === Number ? new $util.LongBits(message.timestamp.low >>> 0, message.timestamp.high >>> 0).toNumber() : message.timestamp;
            return object;
        };

        /**
         * Converts this MarketData to JSON.
         * @function toJSON
         * @memberof marketdata.MarketData
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        MarketData.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        /**
         * Gets the default type url for MarketData
         * @function getTypeUrl
         * @memberof marketdata.MarketData
         * @static
         * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
         * @returns {string} The default type url
         */
        MarketData.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
            if (typeUrlPrefix === undefined) {
                typeUrlPrefix = "type.googleapis.com";
            }
            return typeUrlPrefix + "/marketdata.MarketData";
        };

        return MarketData;
    })();

    marketdata.MarketDataBatch = (function() {

        /**
         * Properties of a MarketDataBatch.
         * @memberof marketdata
         * @interface IMarketDataBatch
         * @property {Array.<marketdata.IMarketData>|null} [data] MarketDataBatch data
         */

        /**
         * Constructs a new MarketDataBatch.
         * @memberof marketdata
         * @classdesc Represents a MarketDataBatch.
         * @implements IMarketDataBatch
         * @constructor
         * @param {marketdata.IMarketDataBatch=} [properties] Properties to set
         */
        function MarketDataBatch(properties) {
            this.data = [];
            if (properties)
                for (var keys = Object.keys(properties), i = 0; i < keys.length; ++i)
                    if (properties[keys[i]] != null)
                        this[keys[i]] = properties[keys[i]];
        }

        /**
         * MarketDataBatch data.
         * @member {Array.<marketdata.IMarketData>} data
         * @memberof marketdata.MarketDataBatch
         * @instance
         */
        MarketDataBatch.prototype.data = $util.emptyArray;

        /**
         * Creates a new MarketDataBatch instance using the specified properties.
         * @function create
         * @memberof marketdata.MarketDataBatch
         * @static
         * @param {marketdata.IMarketDataBatch=} [properties] Properties to set
         * @returns {marketdata.MarketDataBatch} MarketDataBatch instance
         */
        MarketDataBatch.create = function create(properties) {
            return new MarketDataBatch(properties);
        };

        /**
         * Encodes the specified MarketDataBatch message. Does not implicitly {@link marketdata.MarketDataBatch.verify|verify} messages.
         * @function encode
         * @memberof marketdata.MarketDataBatch
         * @static
         * @param {marketdata.IMarketDataBatch} message MarketDataBatch message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        MarketDataBatch.encode = function encode(message, writer) {
            if (!writer)
                writer = $Writer.create();
            if (message.data != null && message.data.length)
                for (var i = 0; i < message.data.length; ++i)
                    $root.marketdata.MarketData.encode(message.data[i], writer.uint32(/* id 1, wireType 2 =*/10).fork()).ldelim();
            return writer;
        };

        /**
         * Encodes the specified MarketDataBatch message, length delimited. Does not implicitly {@link marketdata.MarketDataBatch.verify|verify} messages.
         * @function encodeDelimited
         * @memberof marketdata.MarketDataBatch
         * @static
         * @param {marketdata.IMarketDataBatch} message MarketDataBatch message or plain object to encode
         * @param {$protobuf.Writer} [writer] Writer to encode to
         * @returns {$protobuf.Writer} Writer
         */
        MarketDataBatch.encodeDelimited = function encodeDelimited(message, writer) {
            return this.encode(message, writer).ldelim();
        };

        /**
         * Decodes a MarketDataBatch message from the specified reader or buffer.
         * @function decode
         * @memberof marketdata.MarketDataBatch
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @param {number} [length] Message length if known beforehand
         * @returns {marketdata.MarketDataBatch} MarketDataBatch
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        MarketDataBatch.decode = function decode(reader, length, error) {
            if (!(reader instanceof $Reader))
                reader = $Reader.create(reader);
            var end = length === undefined ? reader.len : reader.pos + length, message = new $root.marketdata.MarketDataBatch();
            while (reader.pos < end) {
                var tag = reader.uint32();
                if (tag === error)
                    break;
                switch (tag >>> 3) {
                case 1: {
                        if (!(message.data && message.data.length))
                            message.data = [];
                        message.data.push($root.marketdata.MarketData.decode(reader, reader.uint32()));
                        break;
                    }
                default:
                    reader.skipType(tag & 7);
                    break;
                }
            }
            return message;
        };

        /**
         * Decodes a MarketDataBatch message from the specified reader or buffer, length delimited.
         * @function decodeDelimited
         * @memberof marketdata.MarketDataBatch
         * @static
         * @param {$protobuf.Reader|Uint8Array} reader Reader or buffer to decode from
         * @returns {marketdata.MarketDataBatch} MarketDataBatch
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        MarketDataBatch.decodeDelimited = function decodeDelimited(reader) {
            if (!(reader instanceof $Reader))
                reader = new $Reader(reader);
            return this.decode(reader, reader.uint32());
        };

        /**
         * Verifies a MarketDataBatch message.
         * @function verify
         * @memberof marketdata.MarketDataBatch
         * @static
         * @param {Object.<string,*>} message Plain object to verify
         * @returns {string|null} `null` if valid, otherwise the reason why it is not
         */
        MarketDataBatch.verify = function verify(message) {
            if (typeof message !== "object" || message === null)
                return "object expected";
            if (message.data != null && message.hasOwnProperty("data")) {
                if (!Array.isArray(message.data))
                    return "data: array expected";
                for (var i = 0; i < message.data.length; ++i) {
                    var error = $root.marketdata.MarketData.verify(message.data[i]);
                    if (error)
                        return "data." + error;
                }
            }
            return null;
        };

        /**
         * Creates a MarketDataBatch message from a plain object. Also converts values to their respective internal types.
         * @function fromObject
         * @memberof marketdata.MarketDataBatch
         * @static
         * @param {Object.<string,*>} object Plain object
         * @returns {marketdata.MarketDataBatch} MarketDataBatch
         */
        MarketDataBatch.fromObject = function fromObject(object) {
            if (object instanceof $root.marketdata.MarketDataBatch)
                return object;
            var message = new $root.marketdata.MarketDataBatch();
            if (object.data) {
                if (!Array.isArray(object.data))
                    throw TypeError(".marketdata.MarketDataBatch.data: array expected");
                message.data = [];
                for (var i = 0; i < object.data.length; ++i) {
                    if (typeof object.data[i] !== "object")
                        throw TypeError(".marketdata.MarketDataBatch.data: object expected");
                    message.data[i] = $root.marketdata.MarketData.fromObject(object.data[i]);
                }
            }
            return message;
        };

        /**
         * Creates a plain object from a MarketDataBatch message. Also converts values to other types if specified.
         * @function toObject
         * @memberof marketdata.MarketDataBatch
         * @static
         * @param {marketdata.MarketDataBatch} message MarketDataBatch
         * @param {$protobuf.IConversionOptions} [options] Conversion options
         * @returns {Object.<string,*>} Plain object
         */
        MarketDataBatch.toObject = function toObject(message, options) {
            if (!options)
                options = {};
            var object = {};
            if (options.arrays || options.defaults)
                object.data = [];
            if (message.data && message.data.length) {
                object.data = [];
                for (var j = 0; j < message.data.length; ++j)
                    object.data[j] = $root.marketdata.MarketData.toObject(message.data[j], options);
            }
            return object;
        };

        /**
         * Converts this MarketDataBatch to JSON.
         * @function toJSON
         * @memberof marketdata.MarketDataBatch
         * @instance
         * @returns {Object.<string,*>} JSON object
         */
        MarketDataBatch.prototype.toJSON = function toJSON() {
            return this.constructor.toObject(this, $protobuf.util.toJSONOptions);
        };

        /**
         * Gets the default type url for MarketDataBatch
         * @function getTypeUrl
         * @memberof marketdata.MarketDataBatch
         * @static
         * @param {string} [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
         * @returns {string} The default type url
         */
        MarketDataBatch.getTypeUrl = function getTypeUrl(typeUrlPrefix) {
            if (typeUrlPrefix === undefined) {
                typeUrlPrefix = "type.googleapis.com";
            }
            return typeUrlPrefix + "/marketdata.MarketDataBatch";
        };

        return MarketDataBatch;
    })();

    return marketdata;
})();

module.exports = $root;
