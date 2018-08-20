/*!
 * Copyright (c) 2018, Park Alter (pseudonym)
 * Distributed under the MIT software license, see the accompanying
 * file COPYING or http://www.opensource.org/licenses/mit-license.php
 *
 * https://github.com/worldmobilecoin/wmcc-kafka
 */
'use strict';

/**
 * A wmcc kafka "environment" which exposes all
 * constructors for client, producer and consumer
 *
 * @exports wmcc_kafka
 * @type {Object}
 */
const wmcc_kafka = exports;

/**
 * Define a module for lazy loading.
 * @param {String} name
 * @param {String} path
 */
wmcc_kafka.define = function define(name, path) {
  let cache = null;
  Object.defineProperty(wmcc_kafka, name, {
    get() {
      if (!cache)
        cache = require(path);
      return cache;
    }
  });
};

// WMCC-Kafka
wmcc_kafka.define('Client', './client');
wmcc_kafka.define('Consumer', './consumer');
wmcc_kafka.define('Producer', './producer');
wmcc_kafka.define('Errors', './errors');

// ALL REQUIRED MODULES

// Nodejs modules
//const Assert = require('assert');
//const Events = require('events');
//const FS = require('fs');
//const OS = require('os');
//const Path = require('path');
//const Util = require('util');

// Dependencies
//const Kafka = require('kafka-node');

// WMCC Modules
//const Logger = require('wmcc-logger');