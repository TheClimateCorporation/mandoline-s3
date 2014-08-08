# mandoline-s3

S3 backend for Mandoline.

## About concurrency

Mandoline supports multiple concurrent writers. However, limitations imposed by
Amazon S3 prevent a formal guarantee of safety when multiple writers are ingesting
at the same time. This backend attempts to minimize the possiblity of conflict
with an optimistic concurrency control strategy, and empirically this is effective.

We have conducted several multi-threaded write tests, and no conflicts occurred. However,
your mileage may vary.

Because of this limitation, this S3 backend might not be suitable for your use case if
you perform data ingests with many writers and you cannot guarantee that they will not
write simultaneously to the same index location.

Note that the number of concurrent readers is unlimited.

## About the tests

All the tests for this project are integration tests that need to interact with S3.
Export the `MANDOLINE_S3_TEST_PATH` environment variable to set an S3 path to which
you have access (example: "mycorp.mybucket/testing/s3backend").

You will also need Amazon credentials (`AMAZON_ACCESS_KEY` and
`AMAZON_SECRET_ACCESS_KEY` in your environment.)
