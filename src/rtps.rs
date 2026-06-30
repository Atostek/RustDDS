#[allow(dead_code)] // We allow this, since extra constants are not too harmful.
pub(crate) mod constant;

pub(crate) mod dp_event_loop;
pub(crate) mod fragment_assembler;
pub(crate) mod message_receiver;
pub(crate) mod reader;
pub(crate) mod rtps_reader_proxy;
pub(crate) mod rtps_writer_proxy;
pub(crate) mod timed_event;
pub(crate) mod writer;
pub(crate) mod writer_send_buffer;

pub(crate) mod message;
pub(crate) use message::{Message, MessageBuilder};

pub(crate) mod submessage;
pub(crate) use submessage::{Submessage, SubmessageBody};
