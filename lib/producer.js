/*!
 * Copyright (c) 2018, Park Alter (pseudonym)
 * Distributed under the MIT software license, see the accompanying
 * file COPYING or http://www.opensource.org/licenses/mit-license.php
 *
 * https://github.com/worldmobilecoin/wmcc-kafka
 * producer.js - kafka producer for wmcc
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
 * Producer
 * A producer of kafka client for handling topic/message.
 * @alias module:Kafka.Producer
 */
class Producer {
  /**
   * Create a producer.
   * @constructor
   * @param {Object} options
   */
  constructor(options) {
    this.options = new ProducerOptions(options);

    this.logger = this.options.logger.context('producer'); // check if context applied to client
    this.client = new Client(options);

    this.producer = new Kafka.Producer(this.client.kafka, this.options);

    this._opened = false;
    this.topics = {};
    this.maxretry = this.options.maxretry;
    this.maxqueue = this.options.maxqueue;

    this._init();
  }

  /**
   * Initiate producer, handle events
   * @private
   */
  _init() {
    this.producer.on('error', (err) => {
      this._handleError(err);
    });
  }

  /**
   * Open a producer
   * @return {Promise|Number} errno
   */
  open() {
    if (this._opened)
      return ERRNO.PRODUSER_OPENED;

    return new Promise(async (resolve) => {
      this.producer.on('ready', () => {
        this._handleTopic();
        this._opened = true;
        resolve(0);
      });

      await this.client.connect();
    });
  }

  /**
   * Close a producer
   * @abstract
   */
  async close() {}

  /**
   * @abstract
   */
  _handleError(err) {}

  /**
   * Create topics from topics entries
   * @private
   * @return {Promise|Number} errno
   */
  async _handleTopic() {
    if (!this._opened)
      return ERRNO.PRODUSER_OPENED;

    for (let [name, topic] of Object.entries(this.topics)) {
      if (topic.created/* && topic.skip*/)
        continue;

      await this.createTopic(topic);
    }
  }

  /**
   * Produce a message for topic
   * @param {Object} Topic
   * @param {Object} Message
   * @return {Number} errno | queue size
   */
  produce(topic, messages) {
    if (!this._opened)
      return ERRNO.PRODUSER_NOT_OPENED;

    if (typeof topic !== 'object')
      return ERRNO.INVALID_ARGUMENT_TYPE;

    if (!Array.isArray(messages))
      messages = [messages];

    for (let index in messages) {
      if (typeof messages[index] === 'object')
        messages[index] = JSON.stringify(messages[index]);

      if (typeof messages[index] !== 'string')
        return ERRNO.INVALID_ARGUMENT_TYPE;
    }

    return this._queue(this.__produce, this, topic, messages);
  }

  /**
   * Queue callback
   * @internal_cb
   * @param {Instance} Producer
   * @param {Object} Topic
   * @param {Object} Messages
   */
  __produce(self, topic, messages) {
    const payloads = {
      topic: topic.name,
      messages: messages
    };

    self.producer.send([payloads], async (err, data) => {
      if (err)
        self.logger.error(err);

      topic.queuesize--;
    });
  }

  /**
   * Put a message to queue job
   * To make sure messages added in sequence/parallel
   * @param {Function} callback
   * @param {Instance} Producer
   * @param {Object} Topic
   * @param {Object} Message
   * @return {Number} errno | queue size
   */
  _queue(cb, self, topic, message) {
    if (this.maxqueue && topic.queuesize >= this.maxqueue)
      return ERRNO.QUEUE_FULL;

    topic.queuesize ++;

    topic.jobs.push(() => {
      topic.jobs.shift();

      cb(self, topic, message);

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
   * Create a topic
   * @param {Object} Topic
   * @return {Promise|Number} errno
   */
  async createTopic(topic) {
    if (typeof topic !== 'object')
      return ERRNO.INVALID_ARGUMENT_TYPE;

    for (;;) {
      const ret = await this._createTopic(topic.name);
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
  _createTopic(name) {
    return new Promise((resolve) => {
      this.producer.createTopics([name], false, (err, data) => {
        if (err) {
          this.logger.error(err);
          resolve(ERRNO.UNABLE_CREATE_TOPIC);
        }

        resolve(0);
      });
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
   * @param {String} topic
   * @return {Object|Number} Topic | errno
   */
  addTopic(name/*, skip*/) {
    if (this.hasTopic(name))
      return ERRNO.TOPIC_EXISTS;

    this.topics[name] = {
      idle: true,
      queuesize: 0,
      //skip: skip,
      created: false,
      retries: this.maxretry,
      jobs: [],
      name: name
    };

    this._handleTopic();

    return this.topics[name];
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

  /**
   * Check if producer is blocked
   * @return {Boolean}
   */
  isBlock() {
    for (let topic of Object.values(this.topics)) {
      if (topic.queuesize >= this.limit)
        return true;
    }

    return false;
  }

  /**
   * Retrieve producer status for each topic
   * @return {Object} Status
   */
  status() {
    const status = {};

    for (let topic of Object.values(this.topics)) {
      status[topic.name] = topic.queuesize;
    }

    return status;
  }
}

/**
 * Producer Options
 * @internal
 * @alias module:Kafka.ProducerOptions
 */
class ProducerOptions {
  /**
   * Create producer options.
   * @constructor
   * @param {Object} options
   */
  constructor(options) {
    this.logger = Logger.global;
    this.maxretry = 3;
    this.maxqueue = 1000;
    // kafka-node producer options
    this.requireAcks = 1;
    this.ackTimeoutMs = 100;
    this.partitionerType = 0;

    if (options)
      this.fromOptions(options);
  }

  /**
   * Inject properties from object.
   * @private
   * @param {Object} options
   * @returns {Object} ProducerOptions
   */
  fromOptions(options) {
    if (options.logger != null) {
      Assert(typeof options.logger === 'object');
      this.logger = options.logger;
    }

    if (options.maxretry != null) {
      Assert(typeof options.maxretry === 'number');
      this.maxretry = options.maxretry;
    }

    if (options.maxqueue != null) {
      Assert(typeof options.maxqueue === 'number');
      this.maxqueue = options.maxqueue;
    }

    if (options.requireAcks != null) {
      Assert(typeof options.requireAcks === 'number');
      this.requireAcks = options.requireAcks;
    }

    if (options.ackTimeoutMs != null) {
      Assert(typeof options.ackTimeoutMs === 'number');
      this.ackTimeoutMs = options.ackTimeoutMs;
    }

    if (options.partitionerType != null) {
      Assert(typeof options.partitionerType === 'number');
      this.partitionerType = options.partitionerType;
    }

    return this;
  }

  /**
   * Instantiate producer options from object.
   * @param {Object} options
   * @returns {Object} ProducerOptions
   */
  static fromOptions(options) {
    return new ProducerOptions().fromOptions(options);
  }
}

/**
 * Expose
 */
module.exports = Producer;