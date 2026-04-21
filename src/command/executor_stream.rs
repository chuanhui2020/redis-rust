use super::*;

use crate::error::Result;
use crate::protocol::RespValue;
use super::executor::CommandExecutor;

pub(crate) fn execute_x_add(executor: &CommandExecutor, key: String, id: String, fields: Vec<(String, String)>, nomkstream: bool, max_len: Option<usize>, min_id: Option<String>) -> Result<RespValue> {
                let min_id_parsed = match min_id {
                    Some(s) => Some(crate::storage::StreamId::parse(s.as_str())?),
                    None => None,
                };
                match executor.storage.xadd(&key, &id, fields, nomkstream, max_len, min_id_parsed)? {
                    Some(new_id) => Ok(RespValue::BulkString(Some(Bytes::from(new_id)))),
                    None => Ok(RespValue::BulkString(None)),
                }
}

pub(crate) fn execute_x_len(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let len = executor.storage.xlen(&key)?;
                Ok(RespValue::Integer(len as i64))
}

pub(crate) fn execute_x_range(executor: &CommandExecutor, key: String, start: String, end: String, count: Option<usize>) -> Result<RespValue> {
                let entries = executor.storage.xrange(&key, &start, &end, count)?;
                let mut resp_values = Vec::new();
                for (id, fields) in entries {
                    let mut field_values = Vec::new();
                    for (f, v) in fields {
                        field_values.push(RespValue::BulkString(Some(Bytes::from(f))));
                        field_values.push(RespValue::BulkString(Some(Bytes::from(v))));
                    }
                    resp_values.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                        RespValue::Array(field_values),
                    ]));
                }
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_x_rev_range(executor: &CommandExecutor, key: String, end: String, start: String, count: Option<usize>) -> Result<RespValue> {
                let entries = executor.storage.xrevrange(&key, &end, &start, count)?;
                let mut resp_values = Vec::new();
                for (id, fields) in entries {
                    let mut field_values = Vec::new();
                    for (f, v) in fields {
                        field_values.push(RespValue::BulkString(Some(Bytes::from(f))));
                        field_values.push(RespValue::BulkString(Some(Bytes::from(v))));
                    }
                    resp_values.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                        RespValue::Array(field_values),
                    ]));
                }
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_x_trim(executor: &CommandExecutor, key: String, strategy: String, threshold: String) -> Result<RespValue> {
                let removed = executor.storage.xtrim(&key, &strategy, &threshold, false)?;
                Ok(RespValue::Integer(removed as i64))
}

pub(crate) fn execute_x_del(executor: &CommandExecutor, key: String, ids: Vec<String>) -> Result<RespValue> {
                let id_vec: Vec<crate::storage::StreamId> = ids
                    .iter()
                    .map(|s| crate::storage::StreamId::parse(s))
                    .collect::<Result<Vec<_>>>()?;
                let removed = executor.storage.xdel(&key, &id_vec)?;
                Ok(RespValue::Integer(removed as i64))
}

pub(crate) fn execute_x_read(executor: &CommandExecutor, keys: Vec<String>, ids: Vec<String>, count: Option<usize>) -> Result<RespValue> {
                let result = executor.storage.xread(&keys, &ids, count)?;
                let mut resp_values = Vec::new();
                for (key, entries) in result {
                    let mut stream_entries = Vec::new();
                    for (id, fields) in entries {
                        let mut field_values = Vec::new();
                        for (f, v) in fields {
                            field_values.push(RespValue::BulkString(Some(Bytes::from(f))));
                            field_values.push(RespValue::BulkString(Some(Bytes::from(v))));
                        }
                        stream_entries.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::Array(field_values),
                        ]));
                    }
                    resp_values.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(key))),
                        RespValue::Array(stream_entries),
                    ]));
                }
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_x_set_id(executor: &CommandExecutor, key: String, id: String) -> Result<RespValue> {
                let sid = crate::storage::StreamId::parse(&id)?;
                let ok = executor.storage.xsetid(&key, sid)?;
                if ok {
                    Ok(RespValue::SimpleString("OK".to_string()))
                } else {
                    Ok(RespValue::Error("ERR 键不存在".to_string()))
                }
}

pub(crate) fn execute_x_group_create(executor: &CommandExecutor, key: String, group: String, id: String, mkstream: bool) -> Result<RespValue> {
                executor.storage.xgroup_create(&key, &group, &id, mkstream)?;
                Ok(RespValue::SimpleString("OK".to_string()))
}

pub(crate) fn execute_x_group_destroy(executor: &CommandExecutor, key: String, group: String) -> Result<RespValue> {
                let removed = executor.storage.xgroup_destroy(&key, &group)?;
                Ok(RespValue::Integer(if removed { 1 } else { 0 }))
}

pub(crate) fn execute_x_group_set_id(executor: &CommandExecutor, key: String, group: String, id: String) -> Result<RespValue> {
                executor.storage.xgroup_setid(&key, &group, &id)?;
                Ok(RespValue::SimpleString("OK".to_string()))
}

pub(crate) fn execute_x_group_del_consumer(executor: &CommandExecutor, key: String, group: String, consumer: String) -> Result<RespValue> {
                let pending = executor.storage.xgroup_delconsumer(&key, &group, &consumer)?;
                Ok(RespValue::Integer(pending as i64))
}

pub(crate) fn execute_x_group_create_consumer(executor: &CommandExecutor, key: String, group: String, consumer: String) -> Result<RespValue> {
                let created = executor.storage.xgroup_createconsumer(&key, &group, &consumer)?;
                Ok(RespValue::Integer(if created { 1 } else { 0 }))
}

pub(crate) fn execute_x_read_group(executor: &CommandExecutor, group: String, consumer: String, keys: Vec<String>, ids: Vec<String>, count: Option<usize>, noack: bool) -> Result<RespValue> {
                let result = executor.storage.xreadgroup(&group, &consumer, &keys, &ids, count, noack)?;
                let mut resp_values = Vec::new();
                for (key, entries) in result {
                    let mut stream_entries = Vec::new();
                    for (id, fields) in entries {
                        let mut field_values = Vec::new();
                        for (f, v) in fields {
                            field_values.push(RespValue::BulkString(Some(Bytes::from(f))));
                            field_values.push(RespValue::BulkString(Some(Bytes::from(v))));
                        }
                        stream_entries.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::Array(field_values),
                        ]));
                    }
                    resp_values.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from(key))),
                        RespValue::Array(stream_entries),
                    ]));
                }
                Ok(RespValue::Array(resp_values))
}

pub(crate) fn execute_x_ack(executor: &CommandExecutor, key: String, group: String, ids: Vec<String>) -> Result<RespValue> {
                let id_vec: Vec<crate::storage::StreamId> = ids
                    .iter()
                    .map(|s| crate::storage::StreamId::parse(s))
                    .collect::<Result<Vec<_>>>()?;
                let acked = executor.storage.xack(&key, &group, &id_vec)?;
                Ok(RespValue::Integer(acked as i64))
}

pub(crate) fn execute_x_claim(executor: &CommandExecutor, key: String, group: String, consumer: String, min_idle: u64, ids: Vec<String>, justid: bool) -> Result<RespValue> {
                let id_vec: Vec<crate::storage::StreamId> = ids
                    .iter()
                    .map(|s| crate::storage::StreamId::parse(s))
                    .collect::<Result<Vec<_>>>()?;
                let claimed = executor.storage.xclaim(&key, &group, &consumer, min_idle, &id_vec, justid)?;
                let mut resp = Vec::new();
                for (id, fields) in claimed {
                    if justid {
                        resp.push(RespValue::BulkString(Some(Bytes::from(id.to_string()))));
                    } else if let Some(flds) = fields {
                        let mut fv = Vec::new();
                        for (f, v) in flds {
                            fv.push(RespValue::BulkString(Some(Bytes::from(f))));
                            fv.push(RespValue::BulkString(Some(Bytes::from(v))));
                        }
                        resp.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::Array(fv),
                        ]));
                    }
                }
                Ok(RespValue::Array(resp))
}

pub(crate) fn execute_x_auto_claim(executor: &CommandExecutor, key: String, group: String, consumer: String, min_idle: u64, start: String, count: usize, justid: bool) -> Result<RespValue> {
                let start_id = crate::storage::StreamId::parse(&start)?;
                let (next_id, claimed) = executor.storage.xautoclaim(&key, &group, &consumer, min_idle, start_id, count, justid)?;
                let mut entries = Vec::new();
                for (id, fields) in claimed {
                    if justid {
                        entries.push(RespValue::BulkString(Some(Bytes::from(id.to_string()))));
                    } else if let Some(flds) = fields {
                        let mut fv = Vec::new();
                        for (f, v) in flds {
                            fv.push(RespValue::BulkString(Some(Bytes::from(f))));
                            fv.push(RespValue::BulkString(Some(Bytes::from(v))));
                        }
                        entries.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::Array(fv),
                        ]));
                    }
                }
                Ok(RespValue::Array(vec![
                    RespValue::BulkString(Some(Bytes::from(next_id.to_string()))),
                    RespValue::Array(entries),
                    RespValue::Array(vec![]),
                ]))
}

pub(crate) fn execute_x_pending(executor: &CommandExecutor, key: String, group: String, start: Option<String>, end: Option<String>, count: Option<usize>, consumer: Option<String>) -> Result<RespValue> {
                let start_id = match &start {
                    Some(s) => {
                        if s == "-" { Some(crate::storage::StreamId::new(0, 0)) }
                        else { Some(crate::storage::StreamId::parse(s)?) }
                    }
                    None => None,
                };
                let end_id = match &end {
                    Some(s) => {
                        if s == "+" { Some(crate::storage::StreamId::new(u64::MAX, u64::MAX)) }
                        else { Some(crate::storage::StreamId::parse(s)?) }
                    }
                    None => None,
                };
                let (total, min_id, max_id, consumers, details) = executor.storage.xpending(
                    &key, &group, start_id, end_id, count, consumer.as_deref(),
                )?;
                if start.is_none() {
                    let mut resp = vec![RespValue::Integer(total as i64)];
                    match min_id {
                        Some(id) => resp.push(RespValue::BulkString(Some(Bytes::from(id.to_string())))),
                        None => resp.push(RespValue::BulkString(None)),
                    }
                    match max_id {
                        Some(id) => resp.push(RespValue::BulkString(Some(Bytes::from(id.to_string())))),
                        None => resp.push(RespValue::BulkString(None)),
                    }
                    if consumers.is_empty() {
                        resp.push(RespValue::BulkString(None));
                    } else {
                        let mut consumer_list = Vec::new();
                        for (name, cnt) in consumers {
                            consumer_list.push(RespValue::Array(vec![
                                RespValue::BulkString(Some(Bytes::from(name))),
                                RespValue::BulkString(Some(Bytes::from(cnt.to_string()))),
                            ]));
                        }
                        resp.push(RespValue::Array(consumer_list));
                    }
                    Ok(RespValue::Array(resp))
                } else {
                    let mut resp = Vec::new();
                    for (id, consumer_name, idle, delivery_count) in details {
                        resp.push(RespValue::Array(vec![
                            RespValue::BulkString(Some(Bytes::from(id.to_string()))),
                            RespValue::BulkString(Some(Bytes::from(consumer_name))),
                            RespValue::Integer(idle as i64),
                            RespValue::Integer(delivery_count as i64),
                        ]));
                    }
                    Ok(RespValue::Array(resp))
                }
}

pub(crate) fn execute_x_info_stream(executor: &CommandExecutor, key: String, full: bool) -> Result<RespValue> {
                match executor.storage.xinfo_stream(&key, full)? {
                    Some((length, groups, last_id, first_id, _group_names)) => {
                        let resp = vec![
                            RespValue::BulkString(Some(Bytes::from("length"))),
                            RespValue::Integer(length as i64),
                            RespValue::BulkString(Some(Bytes::from("groups"))),
                            RespValue::Integer(groups as i64),
                            RespValue::BulkString(Some(Bytes::from("last-generated-id"))),
                            RespValue::BulkString(Some(Bytes::from(last_id.to_string()))),
                            RespValue::BulkString(Some(Bytes::from("first-entry"))),
                            RespValue::BulkString(Some(Bytes::from(first_id.to_string()))),
                        ];
                        Ok(RespValue::Array(resp))
                    }
                    None => Ok(RespValue::Error("ERR no such key".to_string())),
                }
}

pub(crate) fn execute_x_info_groups(executor: &CommandExecutor, key: String) -> Result<RespValue> {
                let groups = executor.storage.xinfo_groups(&key)?;
                let mut resp = Vec::new();
                for (name, consumers, pending, last_id, entries_read) in groups {
                    resp.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from("name"))),
                        RespValue::BulkString(Some(Bytes::from(name))),
                        RespValue::BulkString(Some(Bytes::from("consumers"))),
                        RespValue::Integer(consumers as i64),
                        RespValue::BulkString(Some(Bytes::from("pending"))),
                        RespValue::Integer(pending as i64),
                        RespValue::BulkString(Some(Bytes::from("last-delivered-id"))),
                        RespValue::BulkString(Some(Bytes::from(last_id.to_string()))),
                        RespValue::BulkString(Some(Bytes::from("entries-read"))),
                        RespValue::Integer(entries_read as i64),
                    ]));
                }
                Ok(RespValue::Array(resp))
}

pub(crate) fn execute_x_info_consumers(executor: &CommandExecutor, key: String, group: String) -> Result<RespValue> {
                let consumers = executor.storage.xinfo_consumers(&key, &group)?;
                let mut resp = Vec::new();
                for (name, pending, idle, inactive) in consumers {
                    resp.push(RespValue::Array(vec![
                        RespValue::BulkString(Some(Bytes::from("name"))),
                        RespValue::BulkString(Some(Bytes::from(name))),
                        RespValue::BulkString(Some(Bytes::from("pending"))),
                        RespValue::Integer(pending as i64),
                        RespValue::BulkString(Some(Bytes::from("idle"))),
                        RespValue::Integer(idle as i64),
                        RespValue::BulkString(Some(Bytes::from("inactive"))),
                        RespValue::Integer(inactive as i64),
                    ]));
                }
                Ok(RespValue::Array(resp))
}

