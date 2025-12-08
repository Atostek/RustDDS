# RustDDS
[![Static Checks](https://github.com/Atostek/RustDDS/actions/workflows/static-checks.yml/badge.svg)](https://github.com/Atostek/RustDDS/actions/workflows/static-checks.yml)
[![Tests on Ubuntu](https://github.com/Atostek/RustDDS/actions/workflows/tests.yml/badge.svg)](https://github.com/Atostek/RustDDS/actions/workflows/tests.yml)
[![Tests on macOS](https://github.com/Atostek/RustDDS/actions/workflows/tests-macos.yml/badge.svg)](https://github.com/Atostek/RustDDS/actions/workflows/tests-macos.yml)
[![Security audit](https://github.com/Atostek/RustDDS/actions/workflows/audit.yml/badge.svg)](https://github.com/Atostek/RustDDS/actions/workflows/audit.yml)


[RustDDS][rustdds-url] is a pure Rust implementation of [Data Distribution Service](https://www.omg.org/spec/DDS/). The latest [released version](https://crates.io/crates/rustdds) 
is available on [crates.io](https://crates.io/) and API documentation on [docs.rs](https://docs.rs/rustdds/latest/rustdds/). The [GitHub repository](https://github.com/Atostek/RustDDS) tracks development.

RustDDS is developed by [Atostek Oy][atostek-url]. Atostek provides support and software development services related to DDS, ROS2, and robotics software in general. As a part of our work, we have open-sourced the RustDDS implementation.

We have tried to translate the key ideas of the DDS application interface to Rust concepts, but also follow Rust conventions. Consequently, the API is not exactly as written in the DDS specification, but a functionally equivalent approximation using Rust concepts and conventions.

# Data Distribution Service

The Data Distribution Service for real-time systems (DDS) is an Object Management Group (OMG) machine-to-machine connectivity framework that aims to enable scalable, real-time, dependable, high-performance and interoperable data exchanges using a publish–subscribe pattern. DDS addresses the needs of applications like air-traffic control, smart grid management, autonomous vehicles, robotics, transportation systems, power generation, medical devices, simulation and testing, aerospace and defense, and other applications that require real-time data exchange [[Wiki]][wiki-dds-url].

# Current implementation status

Currently, the implementation is complete enough to do data exchange with [ROS2][ros2-url] software. 

The [ros2-client](https://crates.io/crates/ros2-client) is recommended for talking to ROS components. The `ros2` module within RustDDS should not be used anymore.


## Features Status

* Discovery ✅
* Reliability QoS: Reliable and Best Effort ✅
* History QoS ✅
* RTPS over UDP ✅
* Broadcast UDP ✅
* Non-blocking I/O  ✅
* Topics kinds: with_key and no_key ✅
* Zero-copy receive path ✅
* Zero-copy transmit path
* Topic creation ✅
* Topic finding ✅
* Partition QoS
* Time-based filter QoS
* Ownership QoS
* Presentation QoS: Coherent/atomic sample sets and ordering
* Deadline and Latency budget QoS
* Sample fragmentation (large object exchange) ✅
* `wait_for_acknowledgments` ✅
* Listener (or equivalent) for DomainParticipants ✅
* Listener (or equivalent) for Topics
* Alternative API using Rust `async` tasks ✅
* Shared-memory transport for local connections

## Interoperability

Using "Shapes" demo programs available. Data exchange worked in both directions:

* RTI Connext
* eProsima FastRTPS
* OpenDDS
* CycloneDDS
* Twin Oaks Computing

# Usage

Please see the examples included within the crate and also [Interoperability test](https://github.com/jhelovuo/dds-rtps) .


# Data serialization and keying

Some existing DDS implementations use code generation to implement DataReader and DataWriter classes for each payload type.

We do not rely on code generation, but Rust generic programming instead: There is a generic DataReader and DataWriter, parameterized with the payload type D and a serializer adapter type SA. The [Serde][serde-url] library is used for payload data serialization/deserialization.

The payload type D is required to implement `serde::Serialize` when used with a DataWriter, and 
`serde::DeserializeOwned` when used with a DataReader. Many existing Rust types and libraries already support Serde, so they are good to go as-is.

In DDS, a WITH_KEY topic contains multiple different instances, that are distinguished by a key. The key must be somehow embedded into the data samples. In our implementation, if the payload type D is communicated in a WITH_KEY topic, then D is additionally required to implement trait `Keyed`.

The trait `Keyed` requires one method: `key(&self) -> Self::K` , which is used to extract a key of an associated type `K` from `D`. They key type `K` must implement trait `Key`, which is a combination of pre-existing traits `Eq + 
PartialEq + PartialOrd + Ord + Hash + Clone + Serialize + DeserializeOwned` and no additional methods.

A serializer adapter type SA (wrapper for a Serde data format) is provided for OMG Common Data Representation (CDR), as this is the default serialization format used by DDS/RTPS. It is possible to use another serialization format for the objects communicated over DDS by providing a Serde [data format][serde-data-format-url] implementation.

# Intentional deviations from DDS specification

## Rationale

The [DDS][omg-dds-spec-url] 1.4 specification specifies an object model and a set of APIs for those objects that constitute the DDS specification. The design of these APIs in, e.g., naming conventions and memory management semantics, does not quite fit the Rust world. We have tried to create a design where important DDS ideas are preserved and implemented, but in a manner suitable to Rust. These design compromises should be apparent only on the application-facing API of DDS. The network side is still aiming to be fully interoperable with existing DDS implementations.

## Class Hierarchy

The DDS specifies a class hierarchy, which is part of the API. That hierarchy is not necessarily followed, because Rust does not use inheritance and derived classes in the same sense as e.g. C++.

## Naming Conventions

We have tried to follow Rust naming conventions.

## Data listeners and WaitSets

DDS provides two alternative methods for waiting arriving data, namely WaitSets and Listeners. We have chosen to replace these by using the non-blocking IO API from [mio][metal-io-url] crate. The DDS DataReader objects can be directly used with the mio `Poll` interface. It should be possible to implement other APIs, such as an async API on top of that.

## Instance Handles

DDS uses "instance handles", which behave like pointers to objects managed by the DDS implementation. This does not seem to mix well with Rust memory handling, so we have chosen to not implement those.

An instance handle can be used to refer to refer to data values (samples) with a specific key. We have written the API to use directly the key instead, as that seems semantically equivalent.

## Return codes

The list of standard method return codes specified by DDS (section 2.2.1.1) is modified, in particular:

* The `OK` code is not used to indicate successful operation. Success or failure is indicated using the standard `Result` type.
* The `TIMEOUT` code is not used. Timeouts should be indicated as `Result::Err` or `Option::None`.
* The generic `ERROR` code should not be used, but a more specific value instead.
* `NO_DATA` is not used. The absence of data should be encoded as `Option::None`.

## DataReader and DataWriter interfaces

The DDS specification specifies multiple functions to read received data samples out of a DataReader:

* `read`: Accesses deserialized data objects from a DataReader, and marks them read. Same samples can be read again, if already read samples are requested.
* `take`: Like read, but removes returned objects from DataReader, so they cannot be accessed again.
* `read_w_condition`, `take_w_condition`: Read/take samples that match specified condition.
* `read_next_sample`, `take_next_sample`: Read/take next non-previously accessed sample.
* `read_instance`, `take_instance`: Read/take samples belonging to a single instance (having the same key).
* `read_next_instance`, `take_next_instance`: Combination of `_next` and `_instance`.
* `read_next_instance_w_condition`, `take_next_instance_w_condition`: Combination of `_next` , `_instance` , and `_w_condition`.

We have decided to not implement all 12 of these. Instead, we implement smaller collection of methods:

* `read` : Borrows data from the DataReader.
* `take` : Moves data from the DataReader.
* `read_instance`, `take_instance`: Access samples belonging to a single key.

All of the methods above require a ReadCondition to specify which samples to access, but it is very easy to specify "any" condition, i.e. access unconditionally.

There are also methods  `read_next_sample`, `take_next_sample` , but these are essentially simplification wrappers for read/take.

In addition to these, we also provide a Rust Iterator interface for reading data.

## Memory management

The DDS specification specifies manual memory management in the sense that many object types are created with a 
`create_` method call and destroyed with a matching `delete_` method call. We have opted to rely on Rust memory management wherever possible, including handling of payload data.

# Release history

## Version 0.11.0
* `DataReaderStream` sample stream now returns full `DataSample`, including sample metadata 
(`SampleInfo`). The old version returned only bare data value. You can access the old (simpler and faster) async stream as `BareDataReaderStream`.

### Version 0.11.1
* Improve interoperability in DDS-RPC. Inline QoS parameter `RELATED_SAMPLE_IDENTITY` apparently has two encodings in practical use. Fixes interoperability with new eProsima FastDDS versions.

### Version 0.11.2
* Fix possible panic if system clock jumps backward.
* Fix Writer history garbage collection.

### Version 0.11.3
* Fix bug revealed by improvements in 0.11.2

### Version 0.11.4
* Faster response to SPDP (Discovery) Announcements. Makes connection forming faster.
* Update dependency versions

### Version 0.11.5
* Fix private type leak, which prevented NO_KEY topics working with mio-0.8.x. Discovered by @garamgim.
* Properly check for multicast when enumerating network interfaces. Fix contributed by @onkoe.

### Version 0.11.6
* New test program `ddsperf`
* Fix bug / race condition in Discovery: Previously known Endpoints were ignored.
* Bug fix: CDR serializer endianness was indicated wrong.

### Version 0.11.7
* More correct QoS settings for built-in (Discovery) topics.

### Version 0.11.8
* Fix race condition in Discovery.


## Version 0.10.0
The `DeserializerAdpter` interface for attaching serialization formats to RTPS was extended
to support deserialization with a "seed" value. This allows the deserialization process
to input other run-time data besides the incoming byte stream.

### 0.10.1
* Make RTPS Timestamp's native tick count publicly accessible

### 0.10.2
* Bug fix: Reliable DataReader delivered data samples in the wrong order, if they have arrived out-of-order.
* Change socket initialization behaviour on Windows.
* [CDR serialization](https://crates.io/crates/cdr-encoding) is now a separate Rust crate.

### 0.10.3
* Bug fixes to improve interoperability with CycloneDDS
* Two example programs to test against CycloneDDS examples

## Version 0.9.2
* Redesign internal caching to resolve bugs in connecting.

## Version 0.9.1
* Numerous bug fixes
  * Memory leak in DDSCache
  * Reliable receiver could get stuck
* DDS Security interoperability with FastDDS improved
* New security features, e.g. PKCS#11 support, RSA authentication support

## Version 0.9
* New release to enable new features in `ros2-client`
* DDS Security is under interoperability testing.
* Support for DomainParticipnnt status events, mostly Discovery-related.
* Small API changes
  * Simplify naming
  * QoS objects are now serializable
* Protocol bug fixes with Realible connections.

## Version 0.8.6

* Feature `security` is nearing completion. RustDDS can securely talk to itself, but interoperability testing against other DDS implementaitons is still in progress.
* Fix several bugs in SequenceNumber handling.
* RTPS Writer data sending rewritten.
* Fixed bug: Source timestamps were missing on retransmitted data.


## Version 0.8.5

* Feature `security` merged to master, but it is still work-in-progress, so does not work yet.
* Should work on Windows again
* Less strict lifetime bound in deserialization
* Simplify Key trait usage

## Version 0.8

New features:

* Async API is available.
* Polling using either mio-0.6 or mio-0.8.
* Simplified DataReader `SimpleDataReader` is available. It supports only `.take()` calls, but
should be lighter and faster than regular DataReader. It is designed to have just enough
functionality to implement a ROS2 Subscriber.

This release breaks compatibility:

* Naming of data returned from `read()` / `.take()` calls has been changed from `Result` to 
`Sample`. This was done to reduce confusing naming, because in the previous usage the `Err`
 variant of `Result` did not mean an actual error condition, but a data instance disposal 
 operation.
* Error types are reworked to better reflect what errors can actually result, rather than having
 one complex error type for the entire API. This is an intentional deviation from the DDS
 Specification to make the implementation more Rust-like.


## Version 0.6

This release breaks compatibility with 0.5.x. There are some minor differences in public API names. Changes were made to follow Rust naming conventions. Version 0.6.0 fixes a regression, where communication with eProsima FastRTPS was only possible for a short time.

## Version 0.5

This release breaks compatibility with 0.4.0. Differences are
* Naming convention is more Rust-like, instead of DDS convention - mostly capitalization and underscores.
* Some functions new require owned `String` instead of `&str`. Just add `.to_string()` to fix.
* Key size detection (is it over 16 bytes?) is now implemented in a trait with derive macro.


# Based on rtps-rs

The RTPS implementation used here is derived from [rtps-rs][klapeyron-rtps-rs-url].

[wiki-dds-url]: https://en.wikipedia.org/wiki/Data_Distribution_Service
[omg-rtps-url]: https://www.omg.org/spec/DDSI-RTPS/2.3
[omg-dds-spec-url]: https://www.omg.org/spec/DDS/About-DDS/
[klapeyron-rtps-rs-url]: https://github.com/Klapeyron/rtps-rs
[ros2-url]: https://index.ros.org/doc/ros2/
[metal-io-url]: https://docs.rs/mio/0.6.22/mio/
[serde-url]: https://serde.rs/
[serde-data-format-url]: https://serde.rs/data-format.html
[rustdds-url]: https://atostek.com/en/products/rustdds/
[atostek-url]: https://atostek.com/en/
