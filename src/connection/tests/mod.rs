//! Test groups follow the production responsibilities; shared fixtures live in support.

use super::*;
use crate::pending::Completion;
use crate::protocol::{FrameError, ParseError, TcpRequest};
use serde_json::json;
use std::cell::Cell;
use std::collections::VecDeque;
use std::io::{BufRead, BufReader};
use std::net::{Shutdown, TcpListener};
use std::sync::mpsc;
use std::time::{Duration, UNIX_EPOCH};

use crate::model::Value;

use support::*;

mod backend;
mod boolean;
mod outbound;
mod pending;
mod session;
mod streaming;
mod subscriptions;
mod support;
mod tcp;
