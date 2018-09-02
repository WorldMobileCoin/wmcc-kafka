/* todo: add proper unit test */
'use strict';
/* basic test only */
const Kafka = require('../');

const {
  Producer,
  Consumer
} = Kafka;

const producer = new Producer();
const consumer = new Consumer();

(async() =>{
  await producer.open();
  await consumer.open();
  const t1 = producer.addTopic('test1');
  const t2 = producer.addTopic('test2');
  const t3 = producer.addTopic('test3');
  const topic2 = producer.getTopic('test2');

  const test1 = new Test();

  const ct1 = consumer.addTopic({
      self: test1,
      name: 'test1',
      cb: test1.__test,
      partition: 0,
      offset: test1._offset + 1
    });

  for (let i=0; i< 10; i++) {
    setTimeout(()=>{
      const size = producer.produce(t2, {test: 'test message'});
      //console.log('Test 2: '+producer.status().test2 + ':' + size);
    }, 100*i);
  }
  for (let i=0; i< 10; i++) {
    setTimeout(()=>{
      const msg = {test: 'test message'};
      const size = producer.produce(t1, msg);
      //console.log('Test 1: '+producer.status().test1 + ':' + size);
      console.log(`Producer message: ${JSON.stringify(msg)}`);
    }, 100*i);
  }
})();

class Test {
  constructor() {
    this._offset = 0; // for testing only, should retrieve this first
    this._printed = false;
  }

  __test(topic, message, offset) {
    console.log(`Consumer message: ${JSON.stringify(message)} @ ${offset}`);
    const self = topic.self;
    self._offset = offset;
    self.__printself();
  }

  __printself() {
    if (this._printed)
      return;

    console.log(`Self call, offset: ${this._offset}`);
    this._printed = true;
  }
}