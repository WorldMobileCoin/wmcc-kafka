/*!
 * Copyright (c) 2018, Park Alter (pseudonym)
 * Distributed under the MIT software license, see the accompanying
 * file COPYING or http://www.opensource.org/licenses/mit-license.php
 *
 * https://github.com/worldmobilecoin/wmcc-kafka
 * consumer.js - kafka consumer for wmcc
 */
 'use strict';

const Assert = require('assert');

const Logger = require('wmcc-logger');
const Kafka = require('kafka-node');

const Client = require('./client');
const Errors = require('./errors');

const {
  ERRNO
} = Errors;

/**
 * Consumer
 * A consumer of kafka client for handling topic/message.
 * @alias module:Kafka.Consumer
 */
class Consumer {
  /**
   * Create a consumer.
   * @constructor
   * @param {Object} options
   */
  constructor(options) {
    this.options = new ConsumerOptions(options);

    this.logger = this.options.logger.context('consumer');
    this.client = new Client(options);

    this.consumer = new Kafka.Consumer(
      this.client.kafka,
      [],
      this.options
    );

    this._opened = false;
    this.topics = {};
    this.maxqueue = this.options.maxqueue;
    this.maxretry = this.options.maxretry;

    this._init();
  }

  /**
   * Initiate consumer, handle events
   * @private
   */
  _init() {
    this.consumer.on('error', (err) => {
      this._handleError(err);
    });

    this.consumer.on('offsetOutOfRange', (err) => {
      ; // abstract
    });

    this.consumer.on('message', (msg) => {
      this._handleMessage(msg);
    });
  }

  /**
   * Open a consumer
   * @return {Promise|Number} errno
   */
  async open() {
    if (this._opened)
      return ERRNO.CONSUMER_OPENED;

    this._opened = true;
    await this.client.connect();
    await this._handleTopic();
  }

  /**
   * Close a consumer
   * @return {Promise}
   */
  async close() {
    return new Promise((resolve) => {
      this.consumer.close(resolve());
    });
  }

  /**
   * @abstract
   */
  _handleError(err) {}

  /**
   * @param {String} message
   */
  _handleMessage(msg) {
    const topic = this.getTopic(msg.topic);

    if (!this._opened)
      return ERRNO.CONSUMER_NOT_OPENED;

    let message;
    try {
      message = JSON.parse(msg.value);
    } catch (e) {
      this.logger.error(Errors.format(ERRNO.PARSE_MESSAGE_FAIL, message))
      return ERRNO.PARSE_MESSAGE_FAIL;
    }

    return this._queue(topic, message, msg.offset);
  }

  _queue(topic, message, offset) {
    topic.queuesize++;

    topic.jobs.push(() => {
      topic.jobs.shift();

      topic.cb(topic, message, offset);

      topic.idle = true;
      this._next(topic);

      return true;
    });
    this._next(topic);

    return topic.queuesize;
  }

  /**
   * Process next job
   * @param {Object} Topic
   */
  _next(topic) {
    if(!topic.idle) return;

    const job = topic.jobs[0];
    if(!job) return;

    topic.idle = job();
  }

  /**
   * Create topics from topics entries
   * @private
   * @return {Promise|Number} errno
   */
  async _handleTopic() {
    if (!this._opened)
      return ERRNO.CONSUMER_OPENED;

    for (let [name, topic] of Object.entries(this.topics)) {
      if (topic.created)
        continue;

      await this.createTopic(topic);
    }
  }

  /**
   * Create a topic
   * @param {Object} Topic
   * @return {Promise|Number} errno
   */
  async createTopic(topic) {
    if (typeof topic !== 'object')
      return ERRNO.INVALID_ARGUMENT_TYPE;

    for (;;) {
      const ret = await this._createTopic(topic);
      if (ret < 0) {
        if (!topic.retries) {
          this.removeTopic(topic.name);
          return ret;
        }
        topic.retries--;
      } else {
        topic.created = true;
        break;
      }
    }

    return 0;
  }

  /**
   * Create a topic
   * @private
   * @param {String} topic
   * @return {Promise|Number} errno
   */
  _createTopic(topic) {
    return new Promise((resolve) => {
      const payloads = {        
        topic: topic.name,
        partition: topic.partition,
        offset: topic.offset
      };

      this.consumer.addTopics([payloads], (err, data) => {
        if (err) {
          this.logger.error(err);
          resolve(ERRNO.UNABLE_CREATE_TOPIC);
        }

        resolve(0);
      }, true);
    });
  }

  /**
   * Test a topic
   * @param {String} topic
   * @return {Boolean}
   */
  hasTopic(name) {
    return this.topics.hasOwnProperty(name) ? true : false;
  }

  /**
   * Get a topic
   * @param {String} topic
   * @return {Object|Number} Topic | errno
   */
  getTopic(name) {
    if (!this.hasTopic(name))
      return ERRNO.TOPIC_NOT_EXISTS;

    return this.topics[name];
  }

  /**
   * Add a topic
   * @param {Object} message topic object
   * @param {Function} message callback
   * @param {Number} partition
   * @param {Number} offset
   * @return {Object|Number} Topic | errno
   */
  addTopic(mtopic) {
    if (this.hasTopic(mtopic.name))
      return ERRNO.TOPIC_EXISTS;

    this.topics[mtopic.name] = {
      self: mtopic.self,
      name: mtopic.name,
      partition: mtopic.partition,
      offset: mtopic.offset,
      cb: mtopic.cb,
      idle: true,
      queuesize: 0,
      created: false,
      retries: this.maxretry,
      jobs: []
    };

    this._handleTopic();

    return this.topics[mtopic.name];
  }

  /**
   * Remove a topic
   * @param {String} topic
   * @return {Object|Number} Topic | errno
   */
  removeTopic(name) {
    if (!this.hasTopic(name))
      return ERRNO.TOPIC_NOT_EXISTS;

    delete this.topics[name];
    return 0;
  }
}

/**
 * Consumer Options
 * @internal
 * @alias module:Kafka.ConsumerOptions
 */
class ConsumerOptions {
  /**
   * Create consumer options.
   * @constructor
   * @param {Object} options
   */
  constructor(options) {
    this.logger = Logger.global;
    this.maxqueue = 1000;
    this.maxretry = 3;
    // kafka-node consumer options
    this.groupId = 'kafka-node-group';
    this.autoCommitIntervalMs = 5000;
    this.fetchMaxWaitMs = 100;
    this.fetchMinBytes = 1;
    this.fetchMaxBytes = 1024 * 1024;
    this.fromOffset = true;
    this.encoding = 'utf8'; // easier to parse than buffer
    this.keyEncoding = 'utf8';

    if (options)
      this.fromOptions(options);
  }

  /**
   * Inject properties from object.
   * @private
   * @param {Object} options
   * @returns {Object} ConsumerOptions
   */
  fromOptions(options) {
    if (options.logger != null) {
      Assert(typeof options.logger === 'object');
      this.logger = options.logger;
    }

    if (options.maxqueue != null) {
      Assert(typeof options.maxqueue === 'number');
      this.maxqueue = options.maxqueue;
    }

    if (options.maxretry != null) {
      Assert(typeof options.maxretry === 'number');
      this.maxretry = options.maxretry;
    }

    if (options.groupId != null) {
      Assert(typeof options.groupId === 'string');
      this.groupId = options.groupId;
    }

    if (options.autoCommitIntervalMs != null) {
      Assert(typeof options.autoCommitIntervalMs === 'number');
      this.autoCommitIntervalMs = options.autoCommitIntervalMs;
    }

    if (options.fetchMaxWaitMs != null) {
      Assert(typeof options.fetchMaxWaitMs === 'number');
      this.fetchMaxWaitMs = options.fetchMaxWaitMs;
    }

    if (options.fetchMinBytes != null) {
      Assert(typeof options.fetchMinBytes === 'number');
      this.fetchMinBytes = options.fetchMinBytes;
    }

    if (options.fromOffset != null) {
      Assert(typeof options.fromOffset === 'boolean');
      this.fromOffset = options.fromOffset;
    }

    if (options.encoding != null) {
      Assert(typeof options.encoding === 'string');
      this.encoding = options.encoding;
    }

    if (options.keyEncoding != null) {
      Assert(typeof options.keyEncoding === 'string');
      this.keyEncoding = options.keyEncoding;
    }

    return this;
  }

  /**
   * Instantiate consumer options from object.
   * @param {Object} options
   * @returns {Object} ConsumerOptions
   */
  static fromOptions(options) {
    return new ConsumerOptions().fromOptions(options);
  }
}

/**
 * Expose
 */
module.exports = Consumer;