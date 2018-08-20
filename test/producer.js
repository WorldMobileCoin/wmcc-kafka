/* todo: add proper unit test */
'use strict';
/* basic test only */
const Kafka = require('../');

const {
  Producer
} = Kafka;

const producer = new Producer();

(async() =>{
  await producer.open();
  const t1 = producer.addTopic('test1');
  const t2 = producer.addTopic('test2');
  const t3 = producer.addTopic('test3');
  const topic2 = producer.getTopic('test2');

  for (let i=0; i< 10; i++) {
    setTimeout(()=>{
      const size = producer.produce(t2, {test: 'test message'});
      console.log('Test 2: '+producer.status().test2 + ':' + size)
    }, 100*i);
  }
  for (let i=0; i< 10; i++) {
    setTimeout(()=>{
      const size = producer.produce(t1, {test: 'test message'});
      console.log('Test 1: '+producer.status().test1 + ':' + size)
    }, 100*i);
  }
})();