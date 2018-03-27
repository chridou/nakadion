# Nakadion

A client for the [Nakadi](http://nakadi.io) Event Broker.


## Summary

Nakadion processes batches from Nakadi on a worker per partition basis. A worker is
instantiated for each partion discovered. These workers are guaranteed to be run on
one thread at a time. To process batches of events a handler factory has to be implemented
which is creates handlers that are executed by the workers.

Nakadion is almost completely configurable with environment variables.

## How to use

1. Implement a handler that contains all your batch processing logic

2. Implement a handler factory that creates handlers for the workers.

3. Configure Nakadion

5. Hand your worker factory over to Nakadion

6. Consume events!

## Performance

This library is not meant to be used in a high performance scenario. It uses synchronous IO.
Nevertheless it is easily possible to consume tens of thousands events per second depending
on the complexity of your processing logic.

## Documentation

Detailed documenatation can be found at [docs.rs](https://docs.rs/nakadion)

## Recent Changes

* 0.8.3
    * Added code examples to the documentation
    * Added JSON serialization examples

## License

Nakadion is distributed under the terms of both the MIT license and the Apache License (Version 2.0).

See LICENSE-APACHE and LICENSE-MIT for details.
