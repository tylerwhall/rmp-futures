# rmp-futures

Async encode/decode for msgpack and msgpack-rpc. Intended to allow
deterministic memory use, making the client responsible for allocation. Since
msgpack is a streamable format, the ability to serialize on the fly as the
writer becomes writable is exposed. Similarly, the reader can be written to
deal with individual encoded objects as they are encountered in the stream.
No memory allocations are performed in the library unless requested, such as
returning a string as a Rust `String` or returning an entire object as a
dynamic `rmpv::Value`.

## Theory of operation

Every time some data is read and there's a decision to make, the code returns
a new object with the result of that read back to the client. So a client can
have a static decoding sequence for an expected message with the storage
requirements known up front, and it can simply bail on the message if it
can't be decoded. Every parsing state is a new data type or enum, so keeping
track of the next type of data to decode is enforced at compile time. The
code mostly writes itself with auto-complete since only operations valid at
that time are defined for the data type you get back.

Recursive objects yield recursive types. Simple example for an array containing on element: `[true]`

- R: Initial plain async reader
- MsgPackFuture<R>: wrapped with MsgPackFuture::new() to indicate that this reader is at the start of a message
- ValueFuture::Array(ArrayFuture<R>): decode() returns an enum
- ArrayFuture<R>: client matches on the enum and we're in the client's code for handling Array
- MsgPackFuture<ArrayFuture<R>>: client calls next(), unwraps the MsgPackOption. Now we're decoding a new msg that's an element of an array, as indicated by the data type
- ValueFuture::Boolean(true, ArrayFuture<R>): message decoded as "true"
- ArrayFuture<R>: unpack the ArrayFuture, ready to read the next element
- MsgPackOption::End(R): ArrayFuture::next() returns
- R: unpack the option to get back the original reader

Encode and Decode both support dynamic `write_value()` and `into_value()` functions that deal with heap-allocated messages. This is easier to use at the cost of memory, and 


## TODO

- Add another layer to RPC to manage concurrent operations (e.g multiple outstanding methods). rmp-rpc uses unbounded queues for messages to send. Goal is to have each object that wants to be written have a shared reference (Rc<RefCell<Option<W>>) to the writer, and poll() to take exclusive ownership. Then it would poll() to perform its writing as the writer allows, and when finished, give the writer back to the shared reference, allowing the next future to take exclusive ownership. This allows the client to manage how many outstanding write operations are pending (sending method calls, notifications, and method responses). If it wanted to be fully dynamic and unbounded, it could simply spawn() each write future as a separate task, or it could manually manage a limit of pending messages to keep memory use deterministic. This also allows generating the message as it is sent, rather than allocating it up-front like with rmpv::Value.

- RpcMessage::Request should produce a handle to be used to send the message back. Needs to remember the id and contain a shared reference to the writer (layer above).

## License

Licensed under either of

* Apache License, Version 2.0 http://www.apache.org/licenses/LICENSE-2.0
* MIT license http://opensource.org/licenses/MIT

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
