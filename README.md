# Nakadion

A client for the [Nakadi](https://github.com/zalando/nakadi) Event Broker.

Nakadion uses the Subscription API of Nakadi.


## Consuming

Nakadion supports two modes of consuming events. A sequuential one and a cuncurrent one.

### Sequential Consumption

In this mode Nakadion will read a batch from Nakadi then call a handler on theese and afterwards try to commit the batch.
This mode of operation is simple, straight forward and should be sufficient for most scenarios.

### Concurrent Consumption

In this mode Nakadion will spawn a number of worker threads and distribute work among them based on 
the `partion id` of a batch. The workers are not dedidacted to a partition. Work is rather distributed based
on a hash of the `partition id`. 

## Performance

This library is not meant to be used in a high performance scenario. It uses synchronous IO.

## Documentation

Documenatation can be found at [docs.rs](https://docs.rs/nakadion)

## License

Nakadion is distributed under the terms of both the MIT license and the Apache License (Version 2.0).

See LICENSE-APACHE and LICENSE-MIT for details.
