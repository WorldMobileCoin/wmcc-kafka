/*!
 * Copyright (c) 2018, Park Alter (pseudonym)
 * Distributed under the MIT software license, see the accompanying
 * file COPYING or http://www.opensource.org/licenses/mit-license.php
 *
 * https://github.com/worldmobilecoin/wmcc-kafka
 * client.js - kafka client for wmcc
 */
'use strict';

const Assert = require('assert');
const Events = require('events');
const FS = require('fs');
const OS = require('os');
const Path = require('path');

const Logger = require('wmcc-logger');
const Kafka = require('kafka-node');

const Errors = require('./errors');

/**
 * Client
 * Represents a kafka client
 * @alias module:Kafka.Client
 * @extends EventEmitter
 */
class Client extends Events {
  /**
   * Create a client.
   * @constructor
   * @param {Object} options
   */
  constructor(options) {
    super();

    this.options = new ClientOptions(options);
    this.kafka = new Kafka.KafkaClient(this.options);

    this._autoConnect = this.options.autoConnect;
    this._opened = false;

    this._initLogger(this.options.logger);
    this._init();
  }

  /**
   * Initiate a client, handle events
   * @private
   */
  _init() {
    this.kafka.on('ready', () => {
      this.emit('ready');
    });

    this.kafka.on('error', (err) => {
      this.emit('error', err);
    });
  }

  /**
   * Initiate logger
   * @private
   */
  _initLogger(logger) {
    if (!logger)
      return;

    if (logger.debug)
      console.debug.bind(logger.debug);

    if (logger.info)
      console.info.bind(logger.info);

    const warning = logger.warn || logger.warning;
    if (warning)
      console.warn.bind(warning);

    if (logger.error)
      console.error.bind(logger.error);
  }

  /**
   * Connect to kafka server
   * @return {Promise|Number} errno
   */
  connect() {
    if (this._autoConnect || this._opened)
      return Errors.ERRNO.KAFKA_OPENED;

    return new Promise((resolve) => {
      this.kafka.once('connect', () => {
        this._opened = true;
        resolve(0);
      });

      this.kafka.connect();
    });
  }
}

/**
 * Client Options
 * @internal
 * @alias module:Kafka.ClientOptions
 */
class ClientOptions {
  /**
   * Create client options.
   * @constructor
   * @param {Object} options
   */
  constructor(options) {
    this.logger = Logger.global;
    this.kafkaHost = '127.0.0.1:9092';
    this.connectTimeout = 10000;
    this.requestTimeout = 30000;
    this.autoConnect = false;
    this.connectRetryOptions = {
      retries: 5,
      factor: 2,
      minTimeout: 1 * 1000,
      maxTimeout: 60 * 1000,
      randomize: true
    };
    this.maxAsyncRequests = 10;

    this.prefix = Path.resolve(OS.homedir(), '.wmcc-exchange');
    this.ssl = false;
    this.sslOptions = null;
    this.sasl = null;

    if (options)
      this.fromOptions(options);
  }

  /**
   * Inject properties from object.
   * @private
   * @param {Object} options
   * @returns {Object} ClientOptions
   */
  fromOptions(options) {
    if (options.logger != null) {
      Assert(typeof options.logger === 'object');
      this.logger = options.logger;
    }

    if (options.hosts == null && options.host != null && options.port != null) {
      Assert(typeof options.host === 'string');
      Assert(typeof options.port === 'number');
      this.kafkaHost = `${options.host}:${options.port}`;
    }

    // override if multihost
    if (options.hosts != null) {
      if (Array.isArray(options.hosts)) {
        for (let key in options.hosts) {
          let val = options.hosts[key];
          if (typeof host === 'object') {
            Assert(typeof val.host === 'string');
            Assert(typeof val.port === 'number');

            options.hosts[key] = `${val.host}:${val.port}`;
          }
        }
        options.hosts = options.hosts.join(',');
      }

      if (typeof options.hosts === 'object') {
        let val = options.hosts;
        Assert(typeof val.host === 'string');
        Assert(typeof val.port === 'number');

        options.hosts = `${val.host}:${val.port}`;
      }

      Assert(typeof options.hosts === 'string');
      this.kafkaHost = options.hosts;
    }

    if (options.connTimeout != null) {
      Assert(typeof options.connTimeout === 'number');
      this.connectTimeout = options.connTimeout;
    }

    if (options.reqTimeout != null) {
      Assert(typeof options.reqTimeout === 'number');
      this.requestTimeout = options.reqTimeout;
    }

    if (options.autoConnect != null) {
      Assert(typeof options.autoConnect === 'boolean');
      this.autoConnect = options.autoConnect;
    }

    if (options.retryOptions != null) {
      Assert(typeof options.retryOptions === 'object');
      for (let opt of Object.values(options.retryOptions))
        Assert(typeof opt === 'number');

      this.connectRetryOptions = options.retryOptions;
    }

    if (options.maxAsyncReq != null) {
      Assert(typeof options.maxAsyncReq === 'number');
      this.maxAsyncRequests = options.maxAsyncReq;
    }

    if (options.prefix != null) {
      Assert(typeof options.prefix === 'string');
      this.prefix = options.prefix;
    }

    if (options.ssl) {
      Assert(typeof options.ssl === 'boolean');
      let key = Path.join(this.prefix, 'certificate', 'key.pem');
      let cert = Path.join(this.prefix, 'certificate', 'cert.pem');
      let ca = Path.join(this.prefix, 'certificate', 'ca.pem');

      this.ssl = true;

      if (options.keyFile != null && options.certFile != null) {
        Assert(typeof options.keyFile === 'string');
        Assert(typeof options.certFile === 'string');
        key = Path.join(this.prefix, 'certificate', options.keyFile);
        cert = Path.join(this.prefix, 'certificate', options.certFile);

        this.sslOptions = {
          key: FS.readFileSync(key),
          cert: FS.readFileSync(cert)
        }
      }

      if (options.caFile != null) {
        Assert(typeof options.caFile === 'string');
        ca = Path.join(this.prefix, 'certificate', options.caFile);
        this.sslOptions.ca = [FS.readFileSync(ca)];
      }
    }

    if (options.sasl) {
      Assert(typeof options.sasl === 'boolean');
      Assert(typeof options.saslUser === 'string');
      Assert(typeof options.saslPass === 'string');

      this.sasl = {
        mechanism: 'plain',
        username: options.saslUser,
        password: options.saslPass
      }
    }

    return this;
  }

  /**
   * Instantiate client options from object.
   * @param {Object} options
   * @returns {Object} ClientOptions
   */
  static fromOptions(options) {
    return new ClientOptions().fromOptions(options);
  }
};

/**
 * Expose
 */
module.exports = Client;