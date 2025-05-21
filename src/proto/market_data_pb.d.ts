import * as $protobuf from "protobufjs";
import Long = require("long");
/** Namespace marketdata. */
export namespace marketdata {

    /** Properties of a MarketData. */
    interface IMarketData {

        /** MarketData ltp */
        ltp?: (number|null);

        /** MarketData cp */
        cp?: (number|null);

        /** MarketData token */
        token?: (string|null);

        /** MarketData timestamp */
        timestamp?: (number|Long|null);
    }

    /** Represents a MarketData. */
    class MarketData implements IMarketData {

        /**
         * Constructs a new MarketData.
         * @param [properties] Properties to set
         */
        constructor(properties?: marketdata.IMarketData);

        /** MarketData ltp. */
        public ltp: number;

        /** MarketData cp. */
        public cp: number;

        /** MarketData token. */
        public token: string;

        /** MarketData timestamp. */
        public timestamp: (number|Long);

        /**
         * Creates a new MarketData instance using the specified properties.
         * @param [properties] Properties to set
         * @returns MarketData instance
         */
        public static create(properties?: marketdata.IMarketData): marketdata.MarketData;

        /**
         * Encodes the specified MarketData message. Does not implicitly {@link marketdata.MarketData.verify|verify} messages.
         * @param message MarketData message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: marketdata.IMarketData, writer?: $protobuf.Writer): $protobuf.Writer;

        /**
         * Encodes the specified MarketData message, length delimited. Does not implicitly {@link marketdata.MarketData.verify|verify} messages.
         * @param message MarketData message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: marketdata.IMarketData, writer?: $protobuf.Writer): $protobuf.Writer;

        /**
         * Decodes a MarketData message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns MarketData
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): marketdata.MarketData;

        /**
         * Decodes a MarketData message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns MarketData
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): marketdata.MarketData;

        /**
         * Verifies a MarketData message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates a MarketData message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns MarketData
         */
        public static fromObject(object: { [k: string]: any }): marketdata.MarketData;

        /**
         * Creates a plain object from a MarketData message. Also converts values to other types if specified.
         * @param message MarketData
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: marketdata.MarketData, options?: $protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this MarketData to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };

        /**
         * Gets the default type url for MarketData
         * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
         * @returns The default type url
         */
        public static getTypeUrl(typeUrlPrefix?: string): string;
    }

    /** Properties of a MarketDataBatch. */
    interface IMarketDataBatch {

        /** MarketDataBatch data */
        data?: (marketdata.IMarketData[]|null);
    }

    /** Represents a MarketDataBatch. */
    class MarketDataBatch implements IMarketDataBatch {

        /**
         * Constructs a new MarketDataBatch.
         * @param [properties] Properties to set
         */
        constructor(properties?: marketdata.IMarketDataBatch);

        /** MarketDataBatch data. */
        public data: marketdata.IMarketData[];

        /**
         * Creates a new MarketDataBatch instance using the specified properties.
         * @param [properties] Properties to set
         * @returns MarketDataBatch instance
         */
        public static create(properties?: marketdata.IMarketDataBatch): marketdata.MarketDataBatch;

        /**
         * Encodes the specified MarketDataBatch message. Does not implicitly {@link marketdata.MarketDataBatch.verify|verify} messages.
         * @param message MarketDataBatch message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encode(message: marketdata.IMarketDataBatch, writer?: $protobuf.Writer): $protobuf.Writer;

        /**
         * Encodes the specified MarketDataBatch message, length delimited. Does not implicitly {@link marketdata.MarketDataBatch.verify|verify} messages.
         * @param message MarketDataBatch message or plain object to encode
         * @param [writer] Writer to encode to
         * @returns Writer
         */
        public static encodeDelimited(message: marketdata.IMarketDataBatch, writer?: $protobuf.Writer): $protobuf.Writer;

        /**
         * Decodes a MarketDataBatch message from the specified reader or buffer.
         * @param reader Reader or buffer to decode from
         * @param [length] Message length if known beforehand
         * @returns MarketDataBatch
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        public static decode(reader: ($protobuf.Reader|Uint8Array), length?: number): marketdata.MarketDataBatch;

        /**
         * Decodes a MarketDataBatch message from the specified reader or buffer, length delimited.
         * @param reader Reader or buffer to decode from
         * @returns MarketDataBatch
         * @throws {Error} If the payload is not a reader or valid buffer
         * @throws {$protobuf.util.ProtocolError} If required fields are missing
         */
        public static decodeDelimited(reader: ($protobuf.Reader|Uint8Array)): marketdata.MarketDataBatch;

        /**
         * Verifies a MarketDataBatch message.
         * @param message Plain object to verify
         * @returns `null` if valid, otherwise the reason why it is not
         */
        public static verify(message: { [k: string]: any }): (string|null);

        /**
         * Creates a MarketDataBatch message from a plain object. Also converts values to their respective internal types.
         * @param object Plain object
         * @returns MarketDataBatch
         */
        public static fromObject(object: { [k: string]: any }): marketdata.MarketDataBatch;

        /**
         * Creates a plain object from a MarketDataBatch message. Also converts values to other types if specified.
         * @param message MarketDataBatch
         * @param [options] Conversion options
         * @returns Plain object
         */
        public static toObject(message: marketdata.MarketDataBatch, options?: $protobuf.IConversionOptions): { [k: string]: any };

        /**
         * Converts this MarketDataBatch to JSON.
         * @returns JSON object
         */
        public toJSON(): { [k: string]: any };

        /**
         * Gets the default type url for MarketDataBatch
         * @param [typeUrlPrefix] your custom typeUrlPrefix(default "type.googleapis.com")
         * @returns The default type url
         */
        public static getTypeUrl(typeUrlPrefix?: string): string;
    }
}
