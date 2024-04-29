use std::{
    collections::HashMap,
    str,
    time::{Duration, Instant},
};

use anyhow::{Ok, Result};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::resp::RespType;

pub enum RedisCommand {
    Set(Vec<RespType>),
    Get(RespType),
}

pub struct RedisCommandResponse(pub Vec<RespType>);

struct Redis {
    map: HashMap<RespType, (RespType, Option<Instant>)>,
}

impl Redis {
    fn new() -> Self {
        Redis {
            map: HashMap::new(),
        }
    }

    fn redis_set(&mut self, mut args: Vec<RespType>) -> Result<RedisCommandResponse> {
        if args.len() < 2 {
            unreachable!();
        }
        let (k, v) = (std::mem::take(&mut args[0]), std::mem::take(&mut args[1]));
        let mut duration = Duration::default();
        let mut option = Vec::new();
        for arg in args[2..].iter() {
            match &option {
                option if option.eq_ignore_ascii_case(b"EX") => {
                    if let RespType::Integers(seconds) = arg {
                        duration += Duration::from_secs(*seconds as u64);
                        continue;
                    } else if let RespType::BulkStrings(seconds) = arg {
                        duration +=
                            Duration::from_secs(str::from_utf8(seconds).unwrap().parse().unwrap());
                        continue;
                    }
                }
                option if option.eq_ignore_ascii_case(b"PX") => {
                    if let RespType::Integers(millis) = arg {
                        duration += Duration::from_millis(*millis as u64);
                        continue;
                    } else if let RespType::BulkStrings(millis) = arg {
                        duration +=
                            Duration::from_millis(str::from_utf8(millis).unwrap().parse().unwrap());
                        continue;
                    }
                }
                _ => (),
            }
            if let RespType::BulkStrings(x) = arg {
                option = x.to_owned();
            }
        }
        let expire_date = if !duration.is_zero() {
            Some(Instant::now().checked_add(duration).unwrap())
        } else {
            None
        };
        let _ = self.map.insert(k, (v, expire_date));
        Ok(RedisCommandResponse(vec![RespType::SimpleStrings(
            "OK".to_string(),
        )]))
    }

    fn redis_get(&mut self, key: RespType) -> Result<RedisCommandResponse> {
        if let Some((value, expire_date)) = self.map.get(&key) {
            if let Some(expire_date) = expire_date {
                if Instant::now() >= *expire_date {
                    self.map.remove(&key);
                } else {
                    return Ok(RedisCommandResponse(vec![value.to_owned()]));
                }
            } else {
                return Ok(RedisCommandResponse(vec![value.to_owned()]));
            }
        }
        Ok(RedisCommandResponse(vec![RespType::BulkStrings(vec![])]))
    }
}

pub async fn redis_run(mut rx: Receiver<(RedisCommand, Sender<RedisCommandResponse>)>) {
    let mut redis = Redis::new();
    while let Some((message, reply_tx)) = rx.recv().await {
        let response = match message {
            RedisCommand::Set(args) => redis.redis_set(args),
            RedisCommand::Get(arg) => redis.redis_get(arg),
        }
        .unwrap();
        let _ = reply_tx.send(response).await;
    }
}
