# wmcc-kafka (WorldMobileCoin)

__NOTE__: The first release of wmcc-kafka.

---

## WMCC Kafka

Messaging for WMCC-Exchange.

### Usage:
```js
const Kafka = require('wmcc-kafka');

const {
  Producer
} = Kafka;


(async() =>{
  const producer = new Producer();
  await producer.open();

  const topic = producer.addTopic('foo');
  for (let i=0; i< 10; i++) {
    const size = producer.produce(topic, {bar: `baz_${i}`});
    console.log(`Status: ${producer.status()}`);
  }
})();
...
```

**WorldMobileCoin** is a new generation of cryptocurrency.

## Disclaimer

WorldMobileCoin does not guarantee you against theft or lost funds due to bugs, mishaps,
or your own incompetence. You and you alone are responsible for securing your money.

## Contribution and License Agreement

If you contribute code to this project, you are implicitly allowing your code
to be distributed under the MIT license. You are also implicitly verifying that
all code is your original work.

## License

--Copyright (c) 2018, Park Alter (pseudonym)  
--Distributed under the MIT software license, see the accompanying  
--file COPYING or http://www.opensource.org/licenses/mit-license.php