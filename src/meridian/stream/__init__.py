"""Streaming: topics, producer, two consumer groups, and a dead letter queue.

CONTRACTS.md §4 makes `contracts/topics.yml` the single source of truth and says
the broker init and the client constants are **generated from it**. That is not
a style preference. The design this replaced had a shell script that created
topics, a producer with the topic name in a string literal, a consumer with its
own copy, and a dashboard with a third — four places that had to agree about a
partition count, and no mechanism that made them.

Here there is one file and one loader. `meridian.stream.admin` creates what the
manifest declares; the producer and consumers address topics through the same
objects. A partition count can be wrong, but it cannot be *inconsistently*
wrong, which is the failure that takes a day to find.
"""
