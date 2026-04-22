use super::*;

use crate::protocol::RespValue;

impl Command {
    /// 将命令转换为 RESP 值，用于 AOF 持久化
    pub fn to_resp_value(&self) -> RespValue {
        match self {
            Command::Ping(..) => resp_admin::to_resp_ping(self),
            Command::Get(..) => resp_string::to_resp_get(self),
            Command::Set(..) => resp_string::to_resp_set(self),
            Command::SetEx(..) => resp_string::to_resp_set_ex(self),
            Command::Del(..) => resp_string::to_resp_del(self),
            Command::Exists(..) => resp_string::to_resp_exists(self),
            Command::FlushAll => resp_string::to_resp_flush_all(self),
            Command::Expire(key, seconds) => {
                RespValue::Array(vec![
                    bulk("EXPIRE"),
                    bulk(key),
                    bulk(&seconds.to_string()),
                ])
            }
            Command::Ttl(key) => {
                RespValue::Array(vec![bulk("TTL"), bulk(key)])
            }
            Command::ConfigGet(..) => resp_admin::to_resp_config_get(self),
            Command::ConfigSet(..) => resp_admin::to_resp_config_set(self),
            Command::ConfigRewrite => resp_admin::to_resp_config_rewrite(self),
            Command::ConfigResetStat => resp_admin::to_resp_config_reset_stat(self),
            Command::MemoryUsage(..) => resp_admin::to_resp_memory_usage(self),
            Command::MemoryDoctor => resp_admin::to_resp_memory_doctor(self),
            Command::LatencyLatest => resp_admin::to_resp_latency_latest(self),
            Command::LatencyHistory(..) => resp_admin::to_resp_latency_history(self),
            Command::LatencyReset(..) => resp_admin::to_resp_latency_reset(self),
            Command::Reset => resp_admin::to_resp_reset(self),
            Command::Hello(..) => resp_admin::to_resp_hello(self),
            Command::Monitor => resp_admin::to_resp_monitor(self),
            Command::CommandInfo => resp_admin::to_resp_command_info(self),
            Command::CommandCount => resp_admin::to_resp_command_count(self),
            Command::CommandList(..) => resp_admin::to_resp_command_list(self),
            Command::CommandDocs(..) => resp_admin::to_resp_command_docs(self),
            Command::CommandGetKeys(..) => resp_admin::to_resp_command_get_keys(self),
            Command::MGet(..) => resp_string::to_resp_m_get(self),
            Command::MSet(..) => resp_string::to_resp_m_set(self),
            Command::Incr(..) => resp_string::to_resp_incr(self),
            Command::Decr(..) => resp_string::to_resp_decr(self),
            Command::IncrBy(..) => resp_string::to_resp_incr_by(self),
            Command::DecrBy(..) => resp_string::to_resp_decr_by(self),
            Command::Append(..) => resp_string::to_resp_append(self),
            Command::SetNx(..) => resp_string::to_resp_set_nx(self),
            Command::SetExCmd(..) => resp_string::to_resp_set_ex_cmd(self),
            Command::PSetEx(..) => resp_string::to_resp_p_set_ex(self),
            Command::GetSet(..) => resp_string::to_resp_get_set(self),
            Command::GetDel(..) => resp_string::to_resp_get_del(self),
            Command::GetEx(..) => resp_string::to_resp_get_ex(self),
            Command::MSetNx(..) => resp_string::to_resp_m_set_nx(self),
            Command::IncrByFloat(..) => resp_string::to_resp_incr_by_float(self),
            Command::SetRange(..) => resp_string::to_resp_set_range(self),
            Command::GetRange(..) => resp_string::to_resp_get_range(self),
            Command::StrLen(..) => resp_string::to_resp_str_len(self),
            Command::LPush(..) => resp_list::to_resp_l_push(self),
            Command::RPush(..) => resp_list::to_resp_r_push(self),
            Command::LPop(..) => resp_list::to_resp_l_pop(self),
            Command::RPop(..) => resp_list::to_resp_r_pop(self),
            Command::LLen(..) => resp_list::to_resp_l_len(self),
            Command::LRange(..) => resp_list::to_resp_l_range(self),
            Command::LIndex(..) => resp_list::to_resp_l_index(self),
            Command::LSet(..) => resp_list::to_resp_l_set(self),
            Command::LInsert(..) => resp_list::to_resp_l_insert(self),
            Command::LRem(..) => resp_list::to_resp_l_rem(self),
            Command::LTrim(..) => resp_list::to_resp_l_trim(self),
            Command::LPos(..) => resp_list::to_resp_l_pos(self),
            Command::BLPop(..) => resp_list::to_resp_b_l_pop(self),
            Command::BRPop(..) => resp_list::to_resp_b_r_pop(self),
            Command::HSet(..) => resp_hash::to_resp_h_set(self),
            Command::HGet(..) => resp_hash::to_resp_h_get(self),
            Command::HDel(..) => resp_hash::to_resp_h_del(self),
            Command::HExists(..) => resp_hash::to_resp_h_exists(self),
            Command::HGetAll(..) => resp_hash::to_resp_h_get_all(self),
            Command::HLen(..) => resp_hash::to_resp_h_len(self),
            Command::HMSet(..) => resp_hash::to_resp_h_m_set(self),
            Command::HMGet(..) => resp_hash::to_resp_h_m_get(self),
            Command::HIncrBy(..) => resp_hash::to_resp_h_incr_by(self),
            Command::HIncrByFloat(..) => resp_hash::to_resp_h_incr_by_float(self),
            Command::HKeys(..) => resp_hash::to_resp_h_keys(self),
            Command::HVals(..) => resp_hash::to_resp_h_vals(self),
            Command::HSetNx(..) => resp_hash::to_resp_h_set_nx(self),
            Command::HRandField(..) => resp_hash::to_resp_h_rand_field(self),
            Command::HScan(..) => resp_hash::to_resp_h_scan(self),
            Command::HExpire(..) => resp_hash::to_resp_h_expire(self),
            Command::HPExpire(..) => resp_hash::to_resp_h_p_expire(self),
            Command::HExpireAt(..) => resp_hash::to_resp_h_expire_at(self),
            Command::HPExpireAt(..) => resp_hash::to_resp_h_p_expire_at(self),
            Command::HTtl(..) => resp_hash::to_resp_h_ttl(self),
            Command::HPTtl(..) => resp_hash::to_resp_h_p_ttl(self),
            Command::HExpireTime(..) => resp_hash::to_resp_h_expire_time(self),
            Command::HPExpireTime(..) => resp_hash::to_resp_h_p_expire_time(self),
            Command::HPersist(..) => resp_hash::to_resp_h_persist(self),
            Command::SAdd(..) => resp_set::to_resp_s_add(self),
            Command::SRem(..) => resp_set::to_resp_s_rem(self),
            Command::SMembers(..) => resp_set::to_resp_s_members(self),
            Command::SIsMember(..) => resp_set::to_resp_s_is_member(self),
            Command::SCard(..) => resp_set::to_resp_s_card(self),
            Command::SInter(..) => resp_set::to_resp_s_inter(self),
            Command::SUnion(..) => resp_set::to_resp_s_union(self),
            Command::SDiff(..) => resp_set::to_resp_s_diff(self),
            Command::SPop(..) => resp_set::to_resp_s_pop(self),
            Command::SRandMember(..) => resp_set::to_resp_s_rand_member(self),
            Command::SMove(..) => resp_set::to_resp_s_move(self),
            Command::SInterStore(..) => resp_set::to_resp_s_inter_store(self),
            Command::SUnionStore(..) => resp_set::to_resp_s_union_store(self),
            Command::SDiffStore(..) => resp_set::to_resp_s_diff_store(self),
            Command::SScan(..) => resp_set::to_resp_s_scan(self),
            Command::ZAdd(..) => resp_zset::to_resp_z_add(self),
            Command::ZRem(..) => resp_zset::to_resp_z_rem(self),
            Command::ZScore(..) => resp_zset::to_resp_z_score(self),
            Command::ZRank(..) => resp_zset::to_resp_z_rank(self),
            Command::ZRange(..) => resp_zset::to_resp_z_range(self),
            Command::ZRangeByScore(..) => resp_zset::to_resp_z_range_by_score(self),
            Command::ZCard(..) => resp_zset::to_resp_z_card(self),
            Command::ZRevRange(..) => resp_zset::to_resp_z_rev_range(self),
            Command::ZRevRank(..) => resp_zset::to_resp_z_rev_rank(self),
            Command::ZIncrBy(..) => resp_zset::to_resp_z_incr_by(self),
            Command::ZCount(..) => resp_zset::to_resp_z_count(self),
            Command::ZPopMin(..) => resp_zset::to_resp_z_pop_min(self),
            Command::ZPopMax(..) => resp_zset::to_resp_z_pop_max(self),
            Command::ZUnionStore(..) => resp_zset::to_resp_z_union_store(self),
            Command::ZInterStore(..) => resp_zset::to_resp_z_inter_store(self),
            Command::ZScan(..) => resp_zset::to_resp_z_scan(self),
            Command::ZRangeByLex(..) => resp_zset::to_resp_z_range_by_lex(self),
            Command::SInterCard(keys, limit) => {
                let mut parts = vec![bulk("SINTERCARD"), bulk(&keys.len().to_string())];
                for key in keys {
                    parts.push(bulk(key));
                }
                if *limit > 0 {
                    parts.push(bulk("LIMIT"));
                    parts.push(bulk(&limit.to_string()));
                }
                RespValue::Array(parts)
            }
            Command::SMisMember(key, members) => {
                let mut parts = vec![bulk("SMISMEMBER"), bulk(key)];
                for m in members {
                    parts.push(bulk_bytes(m));
                }
                RespValue::Array(parts)
            }
            Command::ZRandMember(..) => resp_zset::to_resp_z_rand_member(self),
            Command::ZDiff(..) => resp_zset::to_resp_z_diff(self),
            Command::ZDiffStore(..) => resp_zset::to_resp_z_diff_store(self),
            Command::ZInter(..) => resp_zset::to_resp_z_inter(self),
            Command::ZUnion(..) => resp_zset::to_resp_z_union(self),
            Command::ZRangeStore(..) => resp_zset::to_resp_z_range_store(self),
            Command::ZMpop(..) => resp_zset::to_resp_z_mpop(self),
            Command::BZMpop(..) => resp_zset::to_resp_b_z_mpop(self),
            Command::BZPopMin(..) => resp_zset::to_resp_b_z_pop_min(self),
            Command::BZPopMax(..) => resp_zset::to_resp_b_z_pop_max(self),
            Command::ZRevRangeByScore(..) => resp_zset::to_resp_z_rev_range_by_score(self),
            Command::ZRevRangeByLex(..) => resp_zset::to_resp_z_rev_range_by_lex(self),
            Command::ZMScore(..) => resp_zset::to_resp_z_m_score(self),
            Command::ZLexCount(..) => resp_zset::to_resp_z_lex_count(self),
            Command::ZRangeUnified(key, min, max, by_score, by_lex, rev, with_scores, limit_offset, limit_count) => {
                let mut parts = vec![bulk("ZRANGE"), bulk(key), bulk(min), bulk(max)];
                if *by_score {
                    parts.push(bulk("BYSCORE"));
                }
                if *by_lex {
                    parts.push(bulk("BYLEX"));
                }
                if *rev {
                    parts.push(bulk("REV"));
                }
                if *limit_count > 0 {
                    parts.push(bulk("LIMIT"));
                    parts.push(bulk(&limit_offset.to_string()));
                    parts.push(bulk(&limit_count.to_string()));
                }
                if *with_scores {
                    parts.push(bulk("WITHSCORES"));
                }
                RespValue::Array(parts)
            }
            Command::Keys(..) => resp_admin::to_resp_keys(self),
            Command::Scan(..) => resp_admin::to_resp_scan(self),
            Command::Rename(..) => resp_admin::to_resp_rename(self),
            Command::Type(..) => resp_admin::to_resp_type(self),
            Command::Persist(..) => resp_admin::to_resp_persist(self),
            Command::PExpire(..) => resp_admin::to_resp_p_expire(self),
            Command::PTtl(..) => resp_admin::to_resp_p_ttl(self),
            Command::DbSize => {
                RespValue::Array(vec![bulk("DBSIZE")])
            }
            Command::Info(..) => resp_admin::to_resp_info(self),
            Command::Subscribe(channels) => {
                let mut parts = vec![bulk("SUBSCRIBE")];
                for ch in channels {
                    parts.push(bulk(ch));
                }
                RespValue::Array(parts)
            }
            Command::Unsubscribe(channels) => {
                let mut parts = vec![bulk("UNSUBSCRIBE")];
                for ch in channels {
                    parts.push(bulk(ch));
                }
                RespValue::Array(parts)
            }
            Command::Publish(channel, message) => {
                RespValue::Array(vec![
                    bulk("PUBLISH"),
                    bulk(channel),
                    bulk_bytes(message),
                ])
            }
            Command::PSubscribe(patterns) => {
                let mut parts = vec![bulk("PSUBSCRIBE")];
                for pat in patterns {
                    parts.push(bulk(pat));
                }
                RespValue::Array(parts)
            }
            Command::PUnsubscribe(patterns) => {
                let mut parts = vec![bulk("PUNSUBSCRIBE")];
                for pat in patterns {
                    parts.push(bulk(pat));
                }
                RespValue::Array(parts)
            }
            Command::PubSubChannels(pattern) => {
                let mut parts = vec![bulk("PUBSUB"), bulk("CHANNELS")];
                if let Some(pat) = pattern {
                    parts.push(bulk(pat));
                }
                RespValue::Array(parts)
            }
            Command::PubSubNumSub(channels) => {
                let mut parts = vec![bulk("PUBSUB"), bulk("NUMSUB")];
                for ch in channels {
                    parts.push(bulk(ch));
                }
                RespValue::Array(parts)
            }
            Command::PubSubNumPat => {
                RespValue::Array(vec![bulk("PUBSUB"), bulk("NUMPAT")])
            }
            Command::SSubscribe(channels) => {
                let mut parts = vec![bulk("SSUBSCRIBE")];
                for ch in channels {
                    parts.push(bulk(ch));
                }
                RespValue::Array(parts)
            }
            Command::SUnsubscribe(channels) => {
                let mut parts = vec![bulk("SUNSUBSCRIBE")];
                for ch in channels {
                    parts.push(bulk(ch));
                }
                RespValue::Array(parts)
            }
            Command::SPublish(channel, message) => {
                RespValue::Array(vec![
                    bulk("SPUBLISH"),
                    bulk(channel),
                    bulk_bytes(message),
                ])
            }
            Command::PubSubShardChannels(pattern) => {
                let mut parts = vec![bulk("PUBSUB"), bulk("SHARDCHANNELS")];
                if let Some(pat) = pattern {
                    parts.push(bulk(pat));
                }
                RespValue::Array(parts)
            }
            Command::PubSubShardNumSub(channels) => {
                let mut parts = vec![bulk("PUBSUB"), bulk("SHARDNUMSUB")];
                for ch in channels {
                    parts.push(bulk(ch));
                }
                RespValue::Array(parts)
            }
            Command::Multi => {
                RespValue::Array(vec![bulk("MULTI")])
            }
            Command::Exec => {
                RespValue::Array(vec![bulk("EXEC")])
            }
            Command::Discard => {
                RespValue::Array(vec![bulk("DISCARD")])
            }
            Command::Watch(keys) => {
                let mut parts = vec![bulk("WATCH")];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::Unwatch => {
                RespValue::Array(vec![bulk("UNWATCH")])
            }
            Command::BgRewriteAof => resp_admin::to_resp_bg_rewrite_aof(self),
            Command::SetBit(..) => resp_bitmap::to_resp_set_bit(self),
            Command::GetBit(..) => resp_bitmap::to_resp_get_bit(self),
            Command::BitCount(..) => resp_bitmap::to_resp_bit_count(self),
            Command::BitOp(..) => resp_bitmap::to_resp_bit_op(self),
            Command::BitPos(..) => resp_bitmap::to_resp_bit_pos(self),
            Command::BitField(..) => resp_bitmap::to_resp_bit_field(self),
            Command::BitFieldRo(..) => resp_bitmap::to_resp_bit_field_ro(self),
            Command::XAdd(..) => resp_stream::to_resp_x_add(self),
            Command::XLen(..) => resp_stream::to_resp_x_len(self),
            Command::XRange(..) => resp_stream::to_resp_x_range(self),
            Command::XRevRange(..) => resp_stream::to_resp_x_rev_range(self),
            Command::XTrim(..) => resp_stream::to_resp_x_trim(self),
            Command::XDel(..) => resp_stream::to_resp_x_del(self),
            Command::XRead(..) => resp_stream::to_resp_x_read(self),
            Command::XSetId(..) => resp_stream::to_resp_x_set_id(self),
            Command::XGroupCreate(..) => resp_stream::to_resp_x_group_create(self),
            Command::XGroupDestroy(..) => resp_stream::to_resp_x_group_destroy(self),
            Command::XGroupSetId(..) => resp_stream::to_resp_x_group_set_id(self),
            Command::XGroupDelConsumer(..) => resp_stream::to_resp_x_group_del_consumer(self),
            Command::XGroupCreateConsumer(..) => resp_stream::to_resp_x_group_create_consumer(self),
            Command::XReadGroup(..) => resp_stream::to_resp_x_read_group(self),
            Command::XAck(..) => resp_stream::to_resp_x_ack(self),
            Command::XClaim(..) => resp_stream::to_resp_x_claim(self),
            Command::XAutoClaim(..) => resp_stream::to_resp_x_auto_claim(self),
            Command::XPending(..) => resp_stream::to_resp_x_pending(self),
            Command::XInfoStream(..) => resp_stream::to_resp_x_info_stream(self),
            Command::XInfoGroups(..) => resp_stream::to_resp_x_info_groups(self),
            Command::XInfoConsumers(..) => resp_stream::to_resp_x_info_consumers(self),
            Command::PfAdd(key, elements) => {
                let mut parts = vec![bulk("PFADD"), bulk(key)];
                for element in elements {
                    parts.push(bulk(element));
                }
                RespValue::Array(parts)
            }
            Command::PfCount(keys) => {
                let mut parts = vec![bulk("PFCOUNT")];
                for key in keys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::PfMerge(destkey, sourcekeys) => {
                let mut parts = vec![bulk("PFMERGE"), bulk(destkey)];
                for key in sourcekeys {
                    parts.push(bulk(key));
                }
                RespValue::Array(parts)
            }
            Command::GeoAdd(..) => resp_geo::to_resp_geo_add(self),
            Command::GeoDist(..) => resp_geo::to_resp_geo_dist(self),
            Command::GeoHash(..) => resp_geo::to_resp_geo_hash(self),
            Command::GeoPos(..) => resp_geo::to_resp_geo_pos(self),
            Command::GeoSearch(_, _, _, _, _, _, _, _, _, _) => {
                // GEOSEARCH 命令较复杂，简化序列化
                RespValue::Array(vec![bulk("GEOSEARCH")])
            }
            Command::GeoSearchStore(_, _, _, _, _, _, _, _, _) => {
                RespValue::Array(vec![bulk("GEOSEARCHSTORE")])
            }
            Command::Select(..) => resp_admin::to_resp_select(self),
            Command::Auth(..) => resp_admin::to_resp_auth(self),
            Command::ClientSetName(name) => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("SETNAME"), bulk(name)])
            }
            Command::ClientGetName => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("GETNAME")])
            }
            Command::ClientList => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("LIST")])
            }
            Command::ClientId => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("ID")])
            }
            Command::ClientInfo => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("INFO")])
            }
            Command::ClientKill { id, addr, user, skipme } => {
                let mut parts = vec![bulk("CLIENT"), bulk("KILL")];
                if let Some(i) = id {
                    parts.push(bulk("ID"));
                    parts.push(bulk(&i.to_string()));
                }
                if let Some(a) = addr {
                    parts.push(bulk("ADDR"));
                    parts.push(bulk(a));
                }
                if let Some(u) = user {
                    parts.push(bulk("USER"));
                    parts.push(bulk(u));
                }
                if !skipme {
                    parts.push(bulk("SKIPME"));
                    parts.push(bulk("no"));
                }
                RespValue::Array(parts)
            }
            Command::ClientPause(timeout, mode) => {
                RespValue::Array(vec![
                    bulk("CLIENT"), bulk("PAUSE"), bulk(&timeout.to_string()), bulk(mode),
                ])
            }
            Command::ClientUnpause => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("UNPAUSE")])
            }
            Command::ClientNoEvict(flag) => {
                RespValue::Array(vec![
                    bulk("CLIENT"), bulk("NO-EVICT"), bulk(if *flag { "ON" } else { "OFF" }),
                ])
            }
            Command::ClientNoTouch(flag) => {
                RespValue::Array(vec![
                    bulk("CLIENT"), bulk("NO-TOUCH"), bulk(if *flag { "ON" } else { "OFF" }),
                ])
            }
            Command::ClientReply(mode) => {
                let mode_str = match mode {
                    crate::server::ReplyMode::On => "ON",
                    crate::server::ReplyMode::Off => "OFF",
                    crate::server::ReplyMode::Skip => "SKIP",
                };
                RespValue::Array(vec![bulk("CLIENT"), bulk("REPLY"), bulk(mode_str)])
            }
            Command::ClientUnblock(id, reason) => {
                RespValue::Array(vec![
                    bulk("CLIENT"), bulk("UNBLOCK"), bulk(&id.to_string()), bulk(reason),
                ])
            }
            Command::ClientTracking { on, redirect, bcast, prefixes, optin, optout, noloop } => {
                let mut parts = vec![bulk("CLIENT"), bulk("TRACKING"), bulk(if *on { "ON" } else { "OFF" })];
                if let Some(r) = redirect {
                    parts.push(bulk("REDIRECT"));
                    parts.push(bulk(&r.to_string()));
                }
                if *bcast {
                    parts.push(bulk("BCAST"));
                }
                for p in prefixes {
                    parts.push(bulk("PREFIX"));
                    parts.push(bulk(p));
                }
                if *optin {
                    parts.push(bulk("OPTIN"));
                }
                if *optout {
                    parts.push(bulk("OPTOUT"));
                }
                if *noloop {
                    parts.push(bulk("NOLOOP"));
                }
                RespValue::Array(parts)
            }
            Command::ClientCaching(flag) => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("CACHING"), bulk(if *flag { "YES" } else { "NO" })])
            }
            Command::ClientGetRedir => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("GETREDIR")])
            }
            Command::ClientTrackingInfo => {
                RespValue::Array(vec![bulk("CLIENT"), bulk("TRACKINGINFO")])
            }
            Command::AclSetUser(username, rules) => {
                let mut parts = vec![bulk("ACL"), bulk("SETUSER"), bulk(username)];
                for r in rules {
                    parts.push(bulk(r));
                }
                RespValue::Array(parts)
            }
            Command::AclGetUser(username) => {
                RespValue::Array(vec![bulk("ACL"), bulk("GETUSER"), bulk(username)])
            }
            Command::AclDelUser(names) => {
                let mut parts = vec![bulk("ACL"), bulk("DELUSER")];
                for n in names {
                    parts.push(bulk(n));
                }
                RespValue::Array(parts)
            }
            Command::AclList => {
                RespValue::Array(vec![bulk("ACL"), bulk("LIST")])
            }
            Command::AclCat(category) => {
                let mut parts = vec![bulk("ACL"), bulk("CAT")];
                if let Some(cat) = category {
                    parts.push(bulk(cat));
                }
                RespValue::Array(parts)
            }
            Command::AclWhoAmI => {
                RespValue::Array(vec![bulk("ACL"), bulk("WHOAMI")])
            }
            Command::AclLog(arg) => {
                let mut parts = vec![bulk("ACL"), bulk("LOG")];
                if let Some(a) = arg {
                    parts.push(bulk(a));
                }
                RespValue::Array(parts)
            }
            Command::AclGenPass(bits) => {
                let mut parts = vec![bulk("ACL"), bulk("GENPASS")];
                if let Some(b) = bits {
                    parts.push(bulk(&b.to_string()));
                }
                RespValue::Array(parts)
            }
            Command::AclSave => {
                RespValue::Array(vec![bulk("ACL"), bulk("SAVE")])
            }
            Command::AclLoad => {
                RespValue::Array(vec![bulk("ACL"), bulk("LOAD")])
            }
            Command::AclDryRun { username, command } => {
                let mut parts = vec![bulk("ACL"), bulk("DRYRUN"), bulk(username)];
                for arg in command {
                    parts.push(bulk(arg));
                }
                RespValue::Array(parts)
            }
            Command::Sort(key, _, _, _, _, _, _, _) => {
                let mut parts = vec![bulk("SORT"), bulk(key)];
                parts.push(bulk("ASC"));
                RespValue::Array(parts)
            }
            Command::Unlink(..) => resp_admin::to_resp_unlink(self),
            Command::Copy(source, destination, _) => {
                RespValue::Array(vec![bulk("COPY"), bulk(source), bulk(destination)])
            }
            Command::Dump(..) => resp_admin::to_resp_dump(self),
            Command::Restore(key, ttl, _, _) => {
                RespValue::Array(vec![bulk("RESTORE"), bulk(key), bulk(&ttl.to_string())])
            }
            Command::Eval(..) => resp_admin::to_resp_eval(self),
            Command::EvalSha(..) => resp_admin::to_resp_eval_sha(self),
            Command::ScriptLoad(..) => resp_admin::to_resp_script_load(self),
            Command::ScriptExists(..) => resp_admin::to_resp_script_exists(self),
            Command::ScriptFlush => resp_admin::to_resp_script_flush(self),
            Command::ScriptDebug(mode) => {
                RespValue::Array(vec![bulk("SCRIPT"), bulk("DEBUG"), bulk(mode)])
            }
            Command::ScriptHelp => {
                RespValue::Array(vec![bulk("SCRIPT"), bulk("HELP")])
            }
            Command::FunctionLoad(..) => resp_admin::to_resp_function_load(self),
            Command::FunctionDelete(..) => resp_admin::to_resp_function_delete(self),
            Command::FunctionList(..) => resp_admin::to_resp_function_list(self),
            Command::FunctionDump => resp_admin::to_resp_function_dump(self),
            Command::FunctionRestore(..) => resp_admin::to_resp_function_restore(self),
            Command::FunctionStats => resp_admin::to_resp_function_stats(self),
            Command::FunctionFlush(..) => resp_admin::to_resp_function_flush(self),
            Command::FCall(..) => resp_admin::to_resp_f_call(self),
            Command::FCallRO(..) => resp_admin::to_resp_f_call_r_o(self),
            Command::EvalRO(..) => resp_admin::to_resp_eval_r_o(self),
            Command::EvalShaRO(..) => resp_admin::to_resp_eval_sha_r_o(self),
            Command::Save => {
                RespValue::Array(vec![bulk("SAVE")])
            }
            Command::BgSave => {
                RespValue::Array(vec![bulk("BGSAVE")])
            }
            Command::SlowLogGet(..) => resp_admin::to_resp_slow_log_get(self),
            Command::SlowLogLen => resp_admin::to_resp_slow_log_len(self),
            Command::SlowLogReset => resp_admin::to_resp_slow_log_reset(self),
            Command::ObjectEncoding(key) => {
                RespValue::Array(vec![bulk("OBJECT"), bulk("ENCODING"), bulk(key)])
            }
            Command::ObjectRefCount(key) => {
                RespValue::Array(vec![bulk("OBJECT"), bulk("REFCOUNT"), bulk(key)])
            }
            Command::ObjectIdleTime(key) => {
                RespValue::Array(vec![bulk("OBJECT"), bulk("IDLETIME"), bulk(key)])
            }
            Command::ObjectHelp => {
                RespValue::Array(vec![bulk("OBJECT"), bulk("HELP")])
            }
            Command::DebugSetActiveExpire(flag) => {
                RespValue::Array(vec![bulk("DEBUG"), bulk("SET-ACTIVE-EXPIRE"), bulk(if *flag { "1" } else { "0" })])
            }
            Command::DebugSleep(seconds) => {
                RespValue::Array(vec![bulk("DEBUG"), bulk("SLEEP"), bulk(&seconds.to_string())])
            }
            Command::DebugObject(..) => resp_admin::to_resp_debug_object(self),
            Command::Echo(msg) => {
                RespValue::Array(vec![bulk("ECHO"), bulk(msg)])
            }
            Command::Time => {
                RespValue::Array(vec![bulk("TIME")])
            }
            Command::RandomKey => {
                RespValue::Array(vec![bulk("RANDOMKEY")])
            }
            Command::Touch(..) => resp_admin::to_resp_touch(self),
            Command::ExpireAt(..) => resp_admin::to_resp_expire_at(self),
            Command::PExpireAt(..) => resp_admin::to_resp_p_expire_at(self),
            Command::ExpireTime(..) => resp_admin::to_resp_expire_time(self),
            Command::PExpireTime(..) => resp_admin::to_resp_p_expire_time(self),
            Command::RenameNx(..) => resp_admin::to_resp_rename_nx(self),
            Command::SwapDb(..) => resp_admin::to_resp_swap_db(self),
            Command::FlushDb => resp_admin::to_resp_flush_db(self),
            Command::Shutdown(..) => resp_admin::to_resp_shutdown(self),
            Command::LastSave => {
                RespValue::Array(vec![bulk("LASTSAVE")])
            }
            Command::SubStr(key, start, end) => {
                RespValue::Array(vec![bulk("SUBSTR"), bulk(key), bulk(&start.to_string()), bulk(&end.to_string())])
            }
            Command::Lcs(..) => resp_string::to_resp_lcs(self),
            Command::Lmove(source, dest, left_from, left_to) => {
                RespValue::Array(vec![
                    bulk("LMOVE"),
                    bulk(source),
                    bulk(dest),
                    bulk(if *left_from { "LEFT" } else { "RIGHT" }),
                    bulk(if *left_to { "LEFT" } else { "RIGHT" }),
                ])
            }
            Command::Rpoplpush(source, dest) => {
                RespValue::Array(vec![bulk("RPOPLPUSH"), bulk(source), bulk(dest)])
            }
            Command::Lmpop(..) => resp_list::to_resp_lmpop(self),
            Command::BLmove(source, dest, left_from, left_to, timeout) => {
                RespValue::Array(vec![
                    bulk("BLMOVE"),
                    bulk(source),
                    bulk(dest),
                    bulk(if *left_from { "LEFT" } else { "RIGHT" }),
                    bulk(if *left_to { "LEFT" } else { "RIGHT" }),
                    bulk(&timeout.to_string()),
                ])
            }
            Command::BLmpop(..) => resp_list::to_resp_b_lmpop(self),
            Command::BRpoplpush(source, dest, timeout) => {
                RespValue::Array(vec![bulk("BRPOPLPUSH"), bulk(source), bulk(dest), bulk(&timeout.to_string())])
            }
            Command::Quit => {
                RespValue::Array(vec![bulk("QUIT")])
            }
            Command::Role => {
                RespValue::Array(vec![bulk("ROLE")])
            }
            Command::ReplicaOf { host, port } => {
                RespValue::Array(vec![bulk("REPLICAOF"), bulk(host), bulk(&port.to_string())])
            }
            Command::ReplicaOfNoOne => {
                RespValue::Array(vec![bulk("REPLICAOF"), bulk("NO"), bulk("ONE")])
            }
            Command::ReplConf { args } => {
                let mut arr = vec![bulk("REPLCONF")];
                for arg in args {
                    arr.push(bulk(arg));
                }
                RespValue::Array(arr)
            }
            Command::Sync => {
                RespValue::Array(vec![bulk("SYNC")])
            }
            Command::Psync { replid, offset } => {
                RespValue::Array(vec![bulk("PSYNC"), bulk(replid), bulk(&offset.to_string())])
            }
            Command::Wait { numreplicas, timeout } => {
                RespValue::Array(vec![bulk("WAIT"), bulk(&numreplicas.to_string()), bulk(&timeout.to_string())])
            }
            Command::Failover { host, port, timeout, force } => {
                let mut parts = vec![bulk("FAILOVER")];
                if let Some(h) = host {
                    parts.push(bulk("TO"));
                    parts.push(bulk(h));
                    if let Some(p) = port {
                        parts.push(bulk(&p.to_string()));
                    }
                }
                if *timeout >= 0 {
                    parts.push(bulk("TIMEOUT"));
                    parts.push(bulk(&timeout.to_string()));
                }
                if *force {
                    parts.push(bulk("FORCE"));
                }
                RespValue::Array(parts)
            }
            Command::FailoverAbort => {
                RespValue::Array(vec![bulk("FAILOVER"), bulk("ABORT")])
            }
            Command::SentinelMasters => {
                RespValue::Array(vec![bulk("SENTINEL"), bulk("MASTERS")])
            }
            Command::SentinelMaster(name) => {
                RespValue::Array(vec![bulk("SENTINEL"), bulk("MASTER"), bulk(name)])
            }
            Command::SentinelReplicas(name) => {
                RespValue::Array(vec![bulk("SENTINEL"), bulk("REPLICAS"), bulk(name)])
            }
            Command::SentinelSentinels(name) => {
                RespValue::Array(vec![bulk("SENTINEL"), bulk("SENTINELS"), bulk(name)])
            }
            Command::SentinelGetMasterAddrByName(name) => {
                RespValue::Array(vec![bulk("SENTINEL"), bulk("GET-MASTER-ADDR-BY-NAME"), bulk(name)])
            }
            Command::SentinelMonitor { name, ip, port, quorum } => {
                RespValue::Array(vec![
                    bulk("SENTINEL"), bulk("MONITOR"), bulk(name), bulk(ip),
                    bulk(&port.to_string()), bulk(&quorum.to_string()),
                ])
            }
            Command::SentinelRemove(name) => {
                RespValue::Array(vec![bulk("SENTINEL"), bulk("REMOVE"), bulk(name)])
            }
            Command::SentinelSet { name, option, value } => {
                RespValue::Array(vec![bulk("SENTINEL"), bulk("SET"), bulk(name), bulk(option), bulk(value)])
            }
            Command::SentinelFailover(name) => {
                RespValue::Array(vec![bulk("SENTINEL"), bulk("FAILOVER"), bulk(name)])
            }
            Command::SentinelReset(pattern) => {
                RespValue::Array(vec![bulk("SENTINEL"), bulk("RESET"), bulk(pattern)])
            }
            Command::SentinelCkquorum(name) => {
                RespValue::Array(vec![bulk("SENTINEL"), bulk("CKQUORUM"), bulk(name)])
            }
            Command::SentinelMyId => {
                RespValue::Array(vec![bulk("SENTINEL"), bulk("MYID")])
            }
            Command::ClusterInfo => {
                RespValue::Array(vec![bulk("CLUSTER"), bulk("INFO")])
            }
            Command::ClusterNodes => {
                RespValue::Array(vec![bulk("CLUSTER"), bulk("NODES")])
            }
            Command::ClusterMyId => {
                RespValue::Array(vec![bulk("CLUSTER"), bulk("MYID")])
            }
            Command::ClusterSlots => {
                RespValue::Array(vec![bulk("CLUSTER"), bulk("SLOTS")])
            }
            Command::ClusterShards => {
                RespValue::Array(vec![bulk("CLUSTER"), bulk("SHARDS")])
            }
            Command::ClusterMeet { ip, port } => {
                RespValue::Array(vec![bulk("CLUSTER"), bulk("MEET"), bulk(ip), bulk(&port.to_string())])
            }
            Command::ClusterAddSlots(slots) => {
                let mut parts = vec![bulk("CLUSTER"), bulk("ADDSLOTS")];
                for slot in slots {
                    parts.push(bulk(&slot.to_string()));
                }
                RespValue::Array(parts)
            }
            Command::ClusterDelSlots(slots) => {
                let mut parts = vec![bulk("CLUSTER"), bulk("DELSLOTS")];
                for slot in slots {
                    parts.push(bulk(&slot.to_string()));
                }
                RespValue::Array(parts)
            }
            Command::ClusterSetSlot { slot, state, node_id } => {
                let mut parts = vec![bulk("CLUSTER"), bulk("SETSLOT"), bulk(&slot.to_string()), bulk(state)];
                if let Some(id) = node_id {
                    parts.push(bulk(id));
                }
                RespValue::Array(parts)
            }
            Command::ClusterReplicate(node_id) => {
                RespValue::Array(vec![bulk("CLUSTER"), bulk("REPLICATE"), bulk(node_id)])
            }
            Command::ClusterFailover(opt) => {
                let mut parts = vec![bulk("CLUSTER"), bulk("FAILOVER")];
                if let Some(o) = opt {
                    parts.push(bulk(o));
                }
                RespValue::Array(parts)
            }
            Command::ClusterReset(opt) => {
                let mut parts = vec![bulk("CLUSTER"), bulk("RESET")];
                if let Some(o) = opt {
                    parts.push(bulk(o));
                }
                RespValue::Array(parts)
            }
            Command::ClusterKeySlot(key) => {
                RespValue::Array(vec![bulk("CLUSTER"), bulk("KEYSLOT"), bulk(key)])
            }
            Command::ClusterCountKeysInSlot(slot) => {
                RespValue::Array(vec![bulk("CLUSTER"), bulk("COUNTKEYSINSLOT"), bulk(&slot.to_string())])
            }
            Command::ClusterGetKeysInSlot(slot, count) => {
                RespValue::Array(vec![bulk("CLUSTER"), bulk("GETKEYSINSLOT"), bulk(&slot.to_string()), bulk(&count.to_string())])
            }
            Command::Migrate { host, port, keys, db, timeout, copy, replace } => {
                let mut parts = vec![bulk("MIGRATE"), bulk(host), bulk(&port.to_string())];
                if keys.len() == 1 {
                    parts.push(bulk(&keys[0]));
                } else {
                    parts.push(bulk(""));
                }
                parts.push(bulk(&db.to_string()));
                parts.push(bulk(&timeout.to_string()));
                if *copy { parts.push(bulk("COPY")); }
                if *replace { parts.push(bulk("REPLACE")); }
                if keys.len() > 1 {
                    parts.push(bulk("KEYS"));
                    for k in keys { parts.push(bulk(k)); }
                }
                RespValue::Array(parts)
            }
            Command::Asking => {
                RespValue::Array(vec![bulk("ASKING")])
            }
            Command::Unknown(cmd_name) => {
                RespValue::Array(vec![bulk(cmd_name)])
            }
        }
    }

    /// 判断是否为写操作（需要记录到 AOF）
    pub fn is_write_command(&self) -> bool {
        matches!(
            self,
            Command::Set(_, _, _)
                | Command::SetEx(_, _, _)
                | Command::Del(_)
                | Command::FlushAll
                | Command::Expire(_, _)
                | Command::MSet(_)
                | Command::Incr(_)
                | Command::Decr(_)
                | Command::IncrBy(_, _)
                | Command::DecrBy(_, _)
                | Command::Append(_, _)
                | Command::SetNx(_, _)
                | Command::SetExCmd(_, _, _)
                | Command::PSetEx(_, _, _)
                | Command::GetSet(_, _)
                | Command::GetDel(_)
                | Command::GetEx(_, _)
                | Command::MSetNx(_)
                | Command::IncrByFloat(_, _)
                | Command::SetRange(_, _, _)
                | Command::LPush(_, _)
                | Command::RPush(_, _)
                | Command::LPop(_)
                | Command::RPop(_)
                | Command::LSet(_, _, _)
                | Command::LInsert(_, _, _, _)
                | Command::LRem(_, _, _)
                | Command::LTrim(_, _, _)
                | Command::HSet(_, _)
                | Command::HDel(_, _)
                | Command::HMSet(_, _)
                | Command::HIncrBy(_, _, _)
                | Command::HIncrByFloat(_, _, _)
                | Command::HSetNx(_, _, _)
                | Command::HExpire(_, _, _)
                | Command::HPExpire(_, _, _)
                | Command::HExpireAt(_, _, _)
                | Command::HPExpireAt(_, _, _)
                | Command::HPersist(_, _)
                | Command::SAdd(_, _)
                | Command::SRem(_, _)
                | Command::SPop(_, _)
                | Command::SMove(_, _, _)
                | Command::SInterStore(_, _)
                | Command::SUnionStore(_, _)
                | Command::SDiffStore(_, _)
                | Command::ZAdd(_, _)
                | Command::ZRem(_, _)
                | Command::ZIncrBy(_, _, _)
                | Command::ZPopMin(_, _)
                | Command::ZPopMax(_, _)
                | Command::ZUnionStore(_, _, _, _)
                | Command::ZInterStore(_, _, _, _)
                | Command::ZDiffStore(_, _)
                | Command::ZRangeStore(_, _, _, _, _, _, _, _, _)
                | Command::ZMpop(_, _, _)
                | Command::Rename(_, _)
                | Command::Persist(_)
                | Command::PExpire(_, _)
                | Command::SetBit(_, _, _)
                | Command::BitOp(_, _, _)
                | Command::BitField(_, _)
                | Command::XAdd(_, _, _, _, _, _)
                | Command::XTrim(_, _, _)
                | Command::XDel(_, _)
                | Command::XSetId(_, _)
                | Command::XGroupCreate(_, _, _, _)
                | Command::XGroupDestroy(_, _)
                | Command::XGroupSetId(_, _, _)
                | Command::XGroupDelConsumer(_, _, _)
                | Command::XGroupCreateConsumer(_, _, _)
                | Command::XReadGroup(_, _, _, _, _, _)
                | Command::XAck(_, _, _)
                | Command::XClaim(_, _, _, _, _, _)
                | Command::XAutoClaim(_, _, _, _, _, _, _)
                | Command::PfAdd(_, _)
                | Command::PfMerge(_, _)
                | Command::GeoAdd(_, _)
                | Command::GeoSearchStore(_, _, _, _, _, _, _, _, _)
                | Command::Sort(_, _, _, _, _, _, _, Some(_))
                | Command::Unlink(_)
                | Command::Copy(_, _, _)
                | Command::Restore(_, _, _, _)
                | Command::Eval(_, _, _)
                | Command::EvalSha(_, _, _)
                | Command::Save
                | Command::BgSave
                | Command::ExpireAt(_, _)
                | Command::PExpireAt(_, _)
                | Command::RenameNx(_, _)
                | Command::SwapDb(_, _)
                | Command::FlushDb
                | Command::Shutdown(_)
                | Command::Lmove(_, _, _, _)
                | Command::Rpoplpush(_, _)
                | Command::Lmpop(_, _, _)
                | Command::FunctionLoad(_, _)
                | Command::FunctionDelete(_)
                | Command::FunctionFlush(_)
                | Command::FunctionRestore(_, _)
                | Command::FCall(_, _, _)
                | Command::FCallRO(_, _, _)
        )
    }
}
