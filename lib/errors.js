/*!
 * Copyright (c) 2018, Park Alter (pseudonym)
 * Distributed under the MIT software license, see the accompanying
 * file COPYING or http://www.opensource.org/licenses/mit-license.php
 *
 * https://github.com/worldmobilecoin/wmcc-kafka
 * errors.js - errors parser/handler for wmcc-kafka
 */
'use strict';

const Util = require('util');

const errnos = {};

/**
 * Expose
 */
exports.ERRORS = {
  // CLIENT
  KAFKA_OPENED: [-2500, 'Kafka client already opened'],
  INVALID_ARGUMENT_TYPE: [-2501, 'Invalid argument data type'],
  TOPIC_NOT_EXISTS: [-2502, 'Topic not exists'],
  TOPIC_EXISTS: [-2503, 'Topic exists'],
  UNABLE_CREATE_TOPIC: [-2504, 'Unable to create topic'],
  PARSE_MESSAGE_FAIL: [-2505, 'Parse message fail, message: %s'],
  // PRODUCER
  PRODUSER_NOT_OPENED: [-2520, 'Produser not opened'],
  PRODUSER_OPENED: [-2521, 'Produser already opened'],
  QUEUE_FULL: [-2522, 'Queue is full'],
  // CONSUMER
  CONSUMER_NOT_OPENED: [-2530, 'Consumer not opened'],
  CONSUMER_OPENED: [-2531, 'Consumer already opened']
};

exports.ERRNO = {};
exports.ERROR = {};

exports.get = function(errno) {
  return errnos[errno];
}

exports.format = function(errno, ...ext) {
  return Util.format(errnos[errno], ...ext);
}

/**
 * Initiate errors
 */
function _init() {
  for(let [name, error] of Object.entries(exports.ERRORS)) {
    exports.ERRNO[name] = error[0];
    exports.ERROR[name] = error[1];
    errnos[error[0]] = error[1];
  }
};

_init();