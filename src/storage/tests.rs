#[allow(unused_imports)]
use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let engine = StorageEngine::new();
        engine
            .set("name".to_string(), Bytes::from("redis"))
            .unwrap();
        let value = engine.get("name").unwrap();
        assert_eq!(value, Some(Bytes::from("redis")));
    }

    #[test]
    fn test_get_nonexistent() {
        let engine = StorageEngine::new();
        let value = engine.get("missing").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_del() {
        let engine = StorageEngine::new();
        engine.set("key".to_string(), Bytes::from("value")).unwrap();
        assert!(engine.exists("key").unwrap());
        assert!(engine.del("key").unwrap());
        assert!(!engine.exists("key").unwrap());
        assert_eq!(engine.get("key").unwrap(), None);
    }

    #[test]
    fn test_del_nonexistent() {
        let engine = StorageEngine::new();
        assert!(!engine.del("missing").unwrap());
    }

    #[test]
    fn test_exists() {
        let engine = StorageEngine::new();
        assert!(!engine.exists("key").unwrap());
        engine.set("key".to_string(), Bytes::from("value")).unwrap();
        assert!(engine.exists("key").unwrap());
    }

    #[test]
    fn test_flush() {
        let engine = StorageEngine::new();
        engine.set("a".to_string(), Bytes::from("1")).unwrap();
        engine.set("b".to_string(), Bytes::from("2")).unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.get("a").unwrap(), None);
        assert_eq!(engine.get("b").unwrap(), None);
    }

    #[test]
    fn test_ttl_not_expired() {
        let engine = StorageEngine::new();
        engine
            .set_with_ttl("temp".to_string(), Bytes::from("data"), 10_000)
            .unwrap();
        assert!(engine.exists("temp").unwrap());
        assert_eq!(engine.get("temp").unwrap(), Some(Bytes::from("data")));
    }

    #[test]
    fn test_ttl_expired() {
        let engine = StorageEngine::new();
        engine
            .set_with_ttl("temp".to_string(), Bytes::from("data"), 1)
            .unwrap();
        // 等待 20 毫秒确保过期
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(!engine.exists("temp").unwrap());
        assert_eq!(engine.get("temp").unwrap(), None);
        assert!(!engine.del("temp").unwrap());
    }

    #[test]
    fn test_overwrite() {
        let engine = StorageEngine::new();
        engine.set("key".to_string(), Bytes::from("old")).unwrap();
        engine.set("key".to_string(), Bytes::from("new")).unwrap();
        assert_eq!(engine.get("key").unwrap(), Some(Bytes::from("new")));
    }

    #[test]
    fn test_set_with_ttl_overwrites_plain() {
        let engine = StorageEngine::new();
        engine.set("key".to_string(), Bytes::from("plain")).unwrap();
        engine
            .set_with_ttl("key".to_string(), Bytes::from("ttl"), 10_000)
            .unwrap();
        assert_eq!(engine.get("key").unwrap(), Some(Bytes::from("ttl")));
    }

    #[test]
    fn test_expire() {
        let engine = StorageEngine::new();
        engine.set("key".to_string(), Bytes::from("value")).unwrap();
        assert!(engine.expire("key", 10).unwrap());
        let ttl = engine.ttl("key").unwrap();
        assert!(ttl > 9000 && ttl <= 10000);
    }

    #[test]
    fn test_expire_nonexistent() {
        let engine = StorageEngine::new();
        assert!(!engine.expire("missing", 10).unwrap());
        assert_eq!(engine.ttl("missing").unwrap(), -2);
    }

    #[test]
    fn test_ttl_no_expire() {
        let engine = StorageEngine::new();
        engine.set("key".to_string(), Bytes::from("value")).unwrap();
        assert_eq!(engine.ttl("key").unwrap(), -1);
    }

    #[test]
    fn test_ttl_expired_key() {
        let engine = StorageEngine::new();
        engine
            .set_with_ttl("key".to_string(), Bytes::from("value"), 1)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert_eq!(engine.ttl("key").unwrap(), -2);
        assert_eq!(engine.get("key").unwrap(), None);
    }

    #[test]
    fn test_expire_updates_ttl() {
        let engine = StorageEngine::new();
        engine
            .set_with_ttl("key".to_string(), Bytes::from("value"), 10_000)
            .unwrap();
        let ttl1 = engine.ttl("key").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(engine.expire("key", 20).unwrap());
        let ttl2 = engine.ttl("key").unwrap();
        // 重新设置后 TTL 应该接近 20 秒（20000 毫秒）
        assert!(ttl2 > 19500 && ttl2 <= 20000);
        // 新的 TTL 应该比原来的大
        assert!(ttl2 > ttl1);
    }

    // ---------- 并发测试 ----------

    #[test]
    fn test_concurrent_set_different_keys() {
        let engine = StorageEngine::new();
        let mut handles = vec![];

        for i in 0..100 {
            let e = engine.clone();
            handles.push(std::thread::spawn(move || {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                e.set(key.clone(), Bytes::from(value.clone())).unwrap();
                let result = e.get(&key).unwrap();
                assert_eq!(result, Some(Bytes::from(value)));
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_set_same_key() {
        let engine = StorageEngine::new();
        let mut handles = vec![];

        for i in 0..100 {
            let e = engine.clone();
            handles.push(std::thread::spawn(move || {
                e.set("shared".to_string(), Bytes::from(format!("val{}", i)))
                    .unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let result = engine.get("shared").unwrap();
        assert!(result.is_some());
        let data = result.unwrap();
        let val = String::from_utf8_lossy(&data);
        assert!(val.starts_with("val"), "最终值不以 val 开头: {}", val);
    }

    #[test]
    fn test_mget_mset() {
        let engine = StorageEngine::new();
        engine
            .mset(&[
                ("a".to_string(), Bytes::from("1")),
                ("b".to_string(), Bytes::from("2")),
            ])
            .unwrap();

        let results = engine
            .mget(&["a".to_string(), "b".to_string(), "c".to_string()])
            .unwrap();
        assert_eq!(
            results,
            vec![Some(Bytes::from("1")), Some(Bytes::from("2")), None,]
        );
    }

    #[test]
    fn test_incr_new_key() {
        let engine = StorageEngine::new();
        assert_eq!(engine.incr("counter").unwrap(), 1);
        assert_eq!(engine.incr("counter").unwrap(), 2);
    }

    #[test]
    fn test_incr_existing_string() {
        let engine = StorageEngine::new();
        engine.set("n".to_string(), Bytes::from("100")).unwrap();
        assert_eq!(engine.incr("n").unwrap(), 101);
        assert_eq!(engine.incrby("n", 10).unwrap(), 111);
    }

    #[test]
    fn test_incr_non_integer() {
        let engine = StorageEngine::new();
        engine.set("x".to_string(), Bytes::from("abc")).unwrap();
        assert!(engine.incr("x").is_err());
    }

    #[test]
    fn test_decr() {
        let engine = StorageEngine::new();
        assert_eq!(engine.decr("c").unwrap(), -1);
        engine.set("c".to_string(), Bytes::from("10")).unwrap();
        assert_eq!(engine.decr("c").unwrap(), 9);
        assert_eq!(engine.decrby("c", 5).unwrap(), 4);
    }

    #[test]
    fn test_append_new_key() {
        let engine = StorageEngine::new();
        assert_eq!(engine.append("s", Bytes::from("hello")).unwrap(), 5);
        assert_eq!(engine.append("s", Bytes::from(" world")).unwrap(), 11);
        assert_eq!(engine.get("s").unwrap(), Some(Bytes::from("hello world")));
    }

    #[test]
    fn test_setnx() {
        let engine = StorageEngine::new();
        assert!(
            engine
                .setnx("key".to_string(), Bytes::from("first"))
                .unwrap()
        );
        assert!(
            !engine
                .setnx("key".to_string(), Bytes::from("second"))
                .unwrap()
        );
        assert_eq!(engine.get("key").unwrap(), Some(Bytes::from("first")));
    }

    #[test]
    fn test_getrange() {
        let engine = StorageEngine::new();
        engine
            .set("s".to_string(), Bytes::from("hello world"))
            .unwrap();
        assert_eq!(
            engine.getrange("s", 0, 4).unwrap(),
            Some(Bytes::from("hello"))
        );
        assert_eq!(
            engine.getrange("s", 6, 10).unwrap(),
            Some(Bytes::from("world"))
        );
        assert_eq!(
            engine.getrange("s", -5, -1).unwrap(),
            Some(Bytes::from("world"))
        );
        assert_eq!(
            engine.getrange("s", 0, -1).unwrap(),
            Some(Bytes::from("hello world"))
        );
        assert_eq!(engine.getrange("s", 3, 0).unwrap(), Some(Bytes::new()));
    }

    #[test]
    fn test_strlen() {
        let engine = StorageEngine::new();
        assert_eq!(engine.strlen("missing").unwrap(), 0);
        engine.set("s".to_string(), Bytes::from("hello")).unwrap();
        assert_eq!(engine.strlen("s").unwrap(), 5);
    }

    // ---------- List 测试 ----------

    #[test]
    fn test_lpush_rpush() {
        let engine = StorageEngine::new();
        assert_eq!(
            engine
                .lpush("list", vec![Bytes::from("a"), Bytes::from("b")])
                .unwrap(),
            2
        );
        assert_eq!(engine.rpush("list", vec![Bytes::from("c")]).unwrap(), 3);
        assert_eq!(engine.llen("list").unwrap(), 3);
    }

    #[test]
    fn test_lpush_order() {
        let engine = StorageEngine::new();
        engine
            .lpush("list", vec![Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        let vals = engine.lrange("list", 0, -1).unwrap();
        assert_eq!(vals, vec![Bytes::from("b"), Bytes::from("a")]);
    }

    #[test]
    fn test_lpop_rpop() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();

        assert_eq!(engine.lpop("list").unwrap(), Some(Bytes::from("a")));
        assert_eq!(engine.rpop("list").unwrap(), Some(Bytes::from("c")));
        assert_eq!(engine.llen("list").unwrap(), 1);
        assert_eq!(engine.lpop("list").unwrap(), Some(Bytes::from("b")));
        assert_eq!(engine.lpop("list").unwrap(), None);
    }

    #[test]
    fn test_lrange() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![
                    Bytes::from("a"),
                    Bytes::from("b"),
                    Bytes::from("c"),
                    Bytes::from("d"),
                ],
            )
            .unwrap();

        assert_eq!(
            engine.lrange("list", 0, 1).unwrap(),
            vec![Bytes::from("a"), Bytes::from("b")]
        );
        assert_eq!(
            engine.lrange("list", -2, -1).unwrap(),
            vec![Bytes::from("c"), Bytes::from("d")]
        );
        assert_eq!(
            engine.lrange("list", 0, -1).unwrap(),
            vec![
                Bytes::from("a"),
                Bytes::from("b"),
                Bytes::from("c"),
                Bytes::from("d"),
            ]
        );
        assert!(engine.lrange("list", 3, 0).unwrap().is_empty());
    }

    #[test]
    fn test_lindex() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();

        assert_eq!(engine.lindex("list", 0).unwrap(), Some(Bytes::from("a")));
        assert_eq!(engine.lindex("list", 2).unwrap(), Some(Bytes::from("c")));
        assert_eq!(engine.lindex("list", -1).unwrap(), Some(Bytes::from("c")));
        assert_eq!(engine.lindex("list", 5).unwrap(), None);
    }

    #[test]
    fn test_lpush_wrongtype() {
        let engine = StorageEngine::new();
        engine.set("s".to_string(), Bytes::from("string")).unwrap();
        assert!(engine.lpush("s", vec![Bytes::from("x")]).is_err());
        assert!(engine.rpop("s").is_err());
    }

    #[test]
    fn test_string_op_on_list() {
        let engine = StorageEngine::new();
        engine.lpush("list", vec![Bytes::from("a")]).unwrap();
        assert!(engine.get("list").is_err());
        assert!(engine.incr("list").is_err());
        assert!(engine.append("list", Bytes::from("x")).is_err());
    }

    // ---------- Hash 测试 ----------

    #[test]
    fn test_hset_hget() {
        let engine = StorageEngine::new();
        assert_eq!(
            engine
                .hset("hash", "name".to_string(), Bytes::from("redis"))
                .unwrap(),
            1
        );
        assert_eq!(
            engine
                .hset("hash", "name".to_string(), Bytes::from("redis2"))
                .unwrap(),
            0
        );
        assert_eq!(
            engine.hget("hash", "name").unwrap(),
            Some(Bytes::from("redis2"))
        );
        assert_eq!(engine.hget("hash", "missing").unwrap(), None);
    }

    #[test]
    fn test_hdel() {
        let engine = StorageEngine::new();
        engine
            .hset("hash", "a".to_string(), Bytes::from("1"))
            .unwrap();
        engine
            .hset("hash", "b".to_string(), Bytes::from("2"))
            .unwrap();
        engine
            .hset("hash", "c".to_string(), Bytes::from("3"))
            .unwrap();

        assert_eq!(
            engine
                .hdel("hash", &["a".to_string(), "missing".to_string()])
                .unwrap(),
            1
        );
        assert_eq!(
            engine
                .hdel("hash", &["b".to_string(), "c".to_string()])
                .unwrap(),
            2
        );
        assert_eq!(engine.hlen("hash").unwrap(), 0);
    }

    #[test]
    fn test_hexists() {
        let engine = StorageEngine::new();
        engine
            .hset("hash", "field".to_string(), Bytes::from("val"))
            .unwrap();
        assert!(engine.hexists("hash", "field").unwrap());
        assert!(!engine.hexists("hash", "missing").unwrap());
    }

    #[test]
    fn test_hgetall() {
        let engine = StorageEngine::new();
        engine
            .hset("hash", "a".to_string(), Bytes::from("1"))
            .unwrap();
        engine
            .hset("hash", "b".to_string(), Bytes::from("2"))
            .unwrap();

        let mut pairs = engine.hgetall("hash").unwrap();
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            pairs,
            vec![
                ("a".to_string(), Bytes::from("1")),
                ("b".to_string(), Bytes::from("2")),
            ]
        );
    }

    #[test]
    fn test_hlen() {
        let engine = StorageEngine::new();
        assert_eq!(engine.hlen("missing").unwrap(), 0);
        engine
            .hset("hash", "a".to_string(), Bytes::from("1"))
            .unwrap();
        assert_eq!(engine.hlen("hash").unwrap(), 1);
    }

    #[test]
    fn test_hmset_hmget() {
        let engine = StorageEngine::new();
        engine
            .hmset(
                "hash",
                &[
                    ("a".to_string(), Bytes::from("1")),
                    ("b".to_string(), Bytes::from("2")),
                ],
            )
            .unwrap();

        let results = engine
            .hmget(
                "hash",
                &["a".to_string(), "missing".to_string(), "b".to_string()],
            )
            .unwrap();
        assert_eq!(
            results,
            vec![Some(Bytes::from("1")), None, Some(Bytes::from("2")),]
        );
    }

    #[test]
    fn test_hash_op_on_string() {
        let engine = StorageEngine::new();
        engine.set("s".to_string(), Bytes::from("hello")).unwrap();
        assert!(engine.hget("s", "field").is_err());
        assert!(
            engine
                .hset("s", "field".to_string(), Bytes::from("val"))
                .is_err()
        );
    }

    // ---------- Set 测试 ----------

    #[test]
    fn test_sadd_srem() {
        let engine = StorageEngine::new();
        assert_eq!(
            engine
                .sadd("set", vec![Bytes::from("a"), Bytes::from("b")])
                .unwrap(),
            2
        );
        assert_eq!(
            engine
                .sadd("set", vec![Bytes::from("a"), Bytes::from("c")])
                .unwrap(),
            1
        );
        assert_eq!(
            engine
                .srem("set", &[Bytes::from("a"), Bytes::from("missing")])
                .unwrap(),
            1
        );
        assert_eq!(engine.scard("set").unwrap(), 2);
    }

    #[test]
    fn test_smembers_sismember() {
        let engine = StorageEngine::new();
        engine
            .sadd("set", vec![Bytes::from("a"), Bytes::from("b")])
            .unwrap();

        let mut members = engine.smembers("set").unwrap();
        members.sort();
        assert_eq!(members, vec![Bytes::from("a"), Bytes::from("b")]);

        assert!(engine.sismember("set", &Bytes::from("a")).unwrap());
        assert!(!engine.sismember("set", &Bytes::from("c")).unwrap());
    }

    #[test]
    fn test_set_ops() {
        let engine = StorageEngine::new();
        engine
            .sadd(
                "a",
                vec![Bytes::from("1"), Bytes::from("2"), Bytes::from("3")],
            )
            .unwrap();
        engine
            .sadd(
                "b",
                vec![Bytes::from("2"), Bytes::from("3"), Bytes::from("4")],
            )
            .unwrap();
        engine
            .sadd(
                "c",
                vec![Bytes::from("3"), Bytes::from("4"), Bytes::from("5")],
            )
            .unwrap();

        let mut inter = engine.sinter(&["a".to_string(), "b".to_string()]).unwrap();
        inter.sort();
        assert_eq!(inter, vec![Bytes::from("2"), Bytes::from("3")]);

        let mut union = engine.sunion(&["a".to_string(), "b".to_string()]).unwrap();
        union.sort();
        assert_eq!(
            union,
            vec![
                Bytes::from("1"),
                Bytes::from("2"),
                Bytes::from("3"),
                Bytes::from("4")
            ]
        );

        let mut diff = engine.sdiff(&["a".to_string(), "b".to_string()]).unwrap();
        diff.sort();
        assert_eq!(diff, vec![Bytes::from("1")]);
    }

    #[test]
    fn test_set_op_on_string() {
        let engine = StorageEngine::new();
        engine.set("s".to_string(), Bytes::from("hello")).unwrap();
        assert!(engine.sadd("s", vec![Bytes::from("x")]).is_err());
        assert!(engine.smembers("s").is_err());
    }

    // ---------- ZSet 测试 ----------

    #[test]
    fn test_zadd_zrem() {
        let engine = StorageEngine::new();
        assert_eq!(
            engine
                .zadd("zset", vec![(1.0, "a".to_string()), (2.0, "b".to_string())])
                .unwrap(),
            2
        );
        assert_eq!(
            engine
                .zadd("zset", vec![(3.0, "c".to_string()), (2.0, "b".to_string())])
                .unwrap(),
            1
        );
        assert_eq!(
            engine
                .zrem("zset", &["a".to_string(), "missing".to_string()])
                .unwrap(),
            1
        );
        assert_eq!(engine.zcard("zset").unwrap(), 2);
    }

    #[test]
    fn test_zscore_zrank() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "zset",
                vec![
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                    (30.0, "c".to_string()),
                ],
            )
            .unwrap();

        assert_eq!(engine.zscore("zset", "b").unwrap(), Some(20.0));
        assert_eq!(engine.zscore("zset", "missing").unwrap(), None);

        assert_eq!(engine.zrank("zset", "a").unwrap(), Some(0));
        assert_eq!(engine.zrank("zset", "b").unwrap(), Some(1));
        assert_eq!(engine.zrank("zset", "c").unwrap(), Some(2));
        assert_eq!(engine.zrank("zset", "missing").unwrap(), None);
    }

    #[test]
    fn test_zrange() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "zset",
                vec![
                    (30.0, "c".to_string()),
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                ],
            )
            .unwrap();

        let range = engine.zrange("zset", 0, 1, false).unwrap();
        assert_eq!(
            range,
            vec![("a".to_string(), 10.0), ("b".to_string(), 20.0)]
        );

        let range = engine.zrange("zset", 0, -1, false).unwrap();
        assert_eq!(
            range,
            vec![
                ("a".to_string(), 10.0),
                ("b".to_string(), 20.0),
                ("c".to_string(), 30.0),
            ]
        );

        let range = engine.zrange("zset", -2, -1, false).unwrap();
        assert_eq!(
            range,
            vec![("b".to_string(), 20.0), ("c".to_string(), 30.0)]
        );
    }

    #[test]
    fn test_zrangebyscore() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "zset",
                vec![
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                    (30.0, "c".to_string()),
                    (40.0, "d".to_string()),
                ],
            )
            .unwrap();

        let range = engine.zrangebyscore("zset", 15.0, 35.0, false).unwrap();
        assert_eq!(
            range,
            vec![("b".to_string(), 20.0), ("c".to_string(), 30.0)]
        );
    }

    #[test]
    fn test_zadd_update_score() {
        let engine = StorageEngine::new();
        engine.zadd("zset", vec![(1.0, "a".to_string())]).unwrap();
        engine.zadd("zset", vec![(5.0, "a".to_string())]).unwrap();
        assert_eq!(engine.zscore("zset", "a").unwrap(), Some(5.0));
        assert_eq!(engine.zrank("zset", "a").unwrap(), Some(0));
    }

    #[test]
    fn test_zset_op_on_string() {
        let engine = StorageEngine::new();
        engine.set("s".to_string(), Bytes::from("hello")).unwrap();
        assert!(engine.zadd("s", vec![(1.0, "x".to_string())]).is_err());
        assert!(engine.zscore("s", "x").is_err());
    }

    #[test]
    fn test_zrevrange() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "zset",
                vec![
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                    (30.0, "c".to_string()),
                ],
            )
            .unwrap();

        let range = engine.zrevrange("zset", 0, 1, false).unwrap();
        assert_eq!(
            range,
            vec![("c".to_string(), 30.0), ("b".to_string(), 20.0)]
        );
    }

    #[test]
    fn test_zrevrank() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "zset",
                vec![
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                    (30.0, "c".to_string()),
                ],
            )
            .unwrap();

        assert_eq!(engine.zrevrank("zset", "a").unwrap(), Some(2));
        assert_eq!(engine.zrevrank("zset", "c").unwrap(), Some(0));
        assert_eq!(engine.zrevrank("zset", "missing").unwrap(), None);
    }

    #[test]
    fn test_zincrby() {
        let engine = StorageEngine::new();
        engine.zadd("zset", vec![(10.0, "a".to_string())]).unwrap();

        let score = engine.zincrby("zset", 5.5, "a".to_string()).unwrap();
        assert_eq!(score, "15.5");
        assert_eq!(engine.zscore("zset", "a").unwrap(), Some(15.5));

        let score = engine.zincrby("zset", 2.0, "b".to_string()).unwrap();
        assert_eq!(score, "2");
        assert_eq!(engine.zscore("zset", "b").unwrap(), Some(2.0));
    }

    #[test]
    fn test_zcount() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "zset",
                vec![
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                    (30.0, "c".to_string()),
                ],
            )
            .unwrap();

        assert_eq!(engine.zcount("zset", 15.0, 25.0).unwrap(), 1);
        assert_eq!(engine.zcount("zset", 5.0, 35.0).unwrap(), 3);
        assert_eq!(engine.zcount("zset", 100.0, 200.0).unwrap(), 0);
    }

    #[test]
    fn test_zpopmin() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "zset",
                vec![
                    (30.0, "c".to_string()),
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                ],
            )
            .unwrap();

        let popped = engine.zpopmin("zset", 2).unwrap();
        assert_eq!(
            popped,
            vec![("a".to_string(), 10.0), ("b".to_string(), 20.0)]
        );
        assert_eq!(engine.zcard("zset").unwrap(), 1);
    }

    #[test]
    fn test_zpopmax() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "zset",
                vec![
                    (30.0, "c".to_string()),
                    (10.0, "a".to_string()),
                    (20.0, "b".to_string()),
                ],
            )
            .unwrap();

        let popped = engine.zpopmax("zset", 1).unwrap();
        assert_eq!(popped, vec![("c".to_string(), 30.0)]);
        assert_eq!(engine.zcard("zset").unwrap(), 2);
    }

    #[test]
    fn test_zunionstore() {
        let engine = StorageEngine::new();
        engine
            .zadd("z1", vec![(1.0, "a".to_string()), (2.0, "b".to_string())])
            .unwrap();
        engine
            .zadd("z2", vec![(2.0, "b".to_string()), (3.0, "c".to_string())])
            .unwrap();

        let count = engine
            .zunionstore("z3", &["z1".to_string(), "z2".to_string()], None, "SUM")
            .unwrap();
        assert_eq!(count, 3);
        assert_eq!(engine.zscore("z3", "a").unwrap(), Some(1.0));
        assert_eq!(engine.zscore("z3", "b").unwrap(), Some(4.0));
        assert_eq!(engine.zscore("z3", "c").unwrap(), Some(3.0));
    }

    #[test]
    fn test_zinterstore() {
        let engine = StorageEngine::new();
        engine
            .zadd("z1", vec![(1.0, "a".to_string()), (2.0, "b".to_string())])
            .unwrap();
        engine
            .zadd("z2", vec![(2.0, "b".to_string()), (3.0, "c".to_string())])
            .unwrap();

        let count = engine
            .zinterstore("z3", &["z1".to_string(), "z2".to_string()], None, "SUM")
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(engine.zscore("z3", "b").unwrap(), Some(4.0));
    }

    #[test]
    fn test_zscan() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "zset",
                vec![(1.0, "alpha".to_string()), (2.0, "beta".to_string())],
            )
            .unwrap();

        let (cursor, items) = engine.zscan("zset", 0, "a*", 10).unwrap();
        assert_eq!(cursor, 0);
        assert_eq!(items.len(), 1);
        assert_eq!(items[0], ("alpha".to_string(), 1.0));
    }

    #[test]
    fn test_zrangebylex() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "zset",
                vec![
                    (0.0, "a".to_string()),
                    (0.0, "b".to_string()),
                    (0.0, "c".to_string()),
                    (0.0, "d".to_string()),
                ],
            )
            .unwrap();

        let members = engine.zrangebylex("zset", "[b", "[c").unwrap();
        assert_eq!(members, vec!["b".to_string(), "c".to_string()]);
    }

    #[test]
    fn test_keys() {
        let engine = StorageEngine::new();
        engine.set("hello".to_string(), Bytes::from("v1")).unwrap();
        engine.set("hallo".to_string(), Bytes::from("v2")).unwrap();
        engine.set("world".to_string(), Bytes::from("v3")).unwrap();

        let keys = engine.keys("h*lo").unwrap();
        assert_eq!(keys, vec!["hallo", "hello"]);

        let keys = engine.keys("*").unwrap();
        assert_eq!(keys, vec!["hallo", "hello", "world"]);

        let keys = engine.keys("nope").unwrap();
        assert!(keys.is_empty());
    }

    #[test]
    fn test_scan() {
        let engine = StorageEngine::new();
        engine.set("a1".to_string(), Bytes::from("v1")).unwrap();
        engine.set("a2".to_string(), Bytes::from("v2")).unwrap();
        engine.set("b1".to_string(), Bytes::from("v3")).unwrap();

        let (cursor, keys) = engine.scan(0, "a*", 1).unwrap();
        assert_eq!(keys, vec!["a1"]);
        assert_ne!(cursor, 0);

        let (cursor, keys) = engine.scan(cursor, "a*", 1).unwrap();
        assert_eq!(keys, vec!["a2"]);
        assert_eq!(cursor, 0);

        let (cursor, keys) = engine.scan(0, "*", 10).unwrap();
        assert_eq!(keys, vec!["a1", "a2", "b1"]);
        assert_eq!(cursor, 0);
    }

    #[test]
    fn test_rename() {
        let engine = StorageEngine::new();
        engine.set("old".to_string(), Bytes::from("value")).unwrap();
        engine.rename("old", "new").unwrap();
        assert_eq!(engine.get("old").unwrap(), None);
        assert_eq!(engine.get("new").unwrap(), Some(Bytes::from("value")));
    }

    #[test]
    fn test_rename_overwrite() {
        let engine = StorageEngine::new();
        engine.set("a".to_string(), Bytes::from("va")).unwrap();
        engine.set("b".to_string(), Bytes::from("vb")).unwrap();
        engine.rename("a", "b").unwrap();
        assert_eq!(engine.get("a").unwrap(), None);
        assert_eq!(engine.get("b").unwrap(), Some(Bytes::from("va")));
    }

    #[test]
    fn test_key_type() {
        let engine = StorageEngine::new();
        assert_eq!(engine.key_type("missing").unwrap(), "none");

        engine.set("s".to_string(), Bytes::from("v")).unwrap();
        assert_eq!(engine.key_type("s").unwrap(), "string");

        engine.lpush("l", vec![Bytes::from("v")]).unwrap();
        assert_eq!(engine.key_type("l").unwrap(), "list");

        engine.hset("h", "f".to_string(), Bytes::from("v")).unwrap();
        assert_eq!(engine.key_type("h").unwrap(), "hash");

        engine.sadd("set", vec![Bytes::from("v")]).unwrap();
        assert_eq!(engine.key_type("set").unwrap(), "set");

        engine.zadd("z", vec![(1.0, "m".to_string())]).unwrap();
        assert_eq!(engine.key_type("z").unwrap(), "zset");
    }

    #[test]
    fn test_persist() {
        let engine = StorageEngine::new();
        engine
            .set_with_ttl("k".to_string(), Bytes::from("v"), 10000)
            .unwrap();
        assert!(engine.persist("k").unwrap());
        assert_eq!(engine.ttl("k").unwrap(), -1);

        assert!(!engine.persist("missing").unwrap());
    }

    #[test]
    fn test_pexpire() {
        let engine = StorageEngine::new();
        engine.set("k".to_string(), Bytes::from("v")).unwrap();
        assert!(engine.pexpire("k", 100).unwrap());
        let ttl = engine.pttl("k").unwrap();
        assert!(ttl > 0 && ttl <= 100);

        assert!(!engine.pexpire("missing", 100).unwrap());
    }

    #[test]
    fn test_pttl() {
        let engine = StorageEngine::new();
        assert_eq!(engine.pttl("missing").unwrap(), -2);

        engine.set("k".to_string(), Bytes::from("v")).unwrap();
        assert_eq!(engine.pttl("k").unwrap(), -1);

        engine
            .set_with_ttl("k2".to_string(), Bytes::from("v"), 10000)
            .unwrap();
        let ttl = engine.pttl("k2").unwrap();
        assert!(ttl > 0 && ttl <= 10000);
    }

    #[test]
    fn test_dbsize() {
        let engine = StorageEngine::new();
        assert_eq!(engine.dbsize().unwrap(), 0);
        engine.set("a".to_string(), Bytes::from("v")).unwrap();
        engine.set("b".to_string(), Bytes::from("v")).unwrap();
        assert_eq!(engine.dbsize().unwrap(), 2);
    }

    #[test]
    fn test_info() {
        let engine = StorageEngine::new();
        let info = engine.info(None).unwrap();
        assert!(info.contains("redis_version:0.1.0"));
        assert!(info.contains("db0:keys="));
    }

    #[test]
    fn test_version_bump() {
        let engine = StorageEngine::new();
        engine.watch_count.fetch_add(1, Ordering::Relaxed);
        assert_eq!(engine.get_version("k").unwrap(), 0);

        engine.set("k".to_string(), Bytes::from("v1")).unwrap();
        let v1 = engine.get_version("k").unwrap();
        assert!(v1 > 0);

        engine.set("k".to_string(), Bytes::from("v2")).unwrap();
        let v2 = engine.get_version("k").unwrap();
        assert!(v2 > v1);

        // 读操作不应改变版本号
        let _ = engine.get("k").unwrap();
        assert_eq!(engine.get_version("k").unwrap(), v2);
    }

    #[test]
    fn test_watch_check() {
        let engine = StorageEngine::new();
        engine.watch_count.fetch_add(1, Ordering::Relaxed);
        let mut watched = HashMap::new();

        // WATCH 不存在的 key
        watched.insert("k".to_string(), 0u64);
        assert!(engine.watch_check(&watched).unwrap());

        // 修改 key 后版本号变化
        engine.set("k".to_string(), Bytes::from("v")).unwrap();
        assert!(!engine.watch_check(&watched).unwrap());

        // 重新 WATCH 最新版本号
        let new_version = engine.get_version("k").unwrap();
        watched.insert("k".to_string(), new_version);
        assert!(engine.watch_check(&watched).unwrap());
    }

    #[test]
    fn test_watch_check_multiple_keys() {
        let engine = StorageEngine::new();
        engine.watch_count.fetch_add(1, Ordering::Relaxed);
        engine.set("a".to_string(), Bytes::from("1")).unwrap();
        engine.set("b".to_string(), Bytes::from("2")).unwrap();

        let v_a = engine.get_version("a").unwrap();
        let v_b = engine.get_version("b").unwrap();

        let mut watched = HashMap::new();
        watched.insert("a".to_string(), v_a);
        watched.insert("b".to_string(), v_b);
        assert!(engine.watch_check(&watched).unwrap());

        // 只修改其中一个
        engine.set("a".to_string(), Bytes::from("x")).unwrap();
        assert!(!engine.watch_check(&watched).unwrap());
    }

    #[test]
    fn test_watch_check_fails_after_lazy_expire() {
        let engine = StorageEngine::new();
        engine.watch_count.fetch_add(1, Ordering::Relaxed);
        engine
            .set_with_ttl("k".to_string(), Bytes::from("v"), 5)
            .unwrap();

        let mut watched = HashMap::new();
        watched.insert("k".to_string(), engine.get_version("k").unwrap());
        engine.watch_count.fetch_add(1, Ordering::Relaxed);

        std::thread::sleep(std::time::Duration::from_millis(20));
        assert_eq!(engine.get("k").unwrap(), None);
        assert!(!engine.watch_check(&watched).unwrap());

        engine.watch_count.fetch_sub(1, Ordering::Relaxed);
    }

    #[test]
    fn test_memory_usage() {
        let engine = StorageEngine::new();
        let usage0 = engine.memory_usage().unwrap();
        assert_eq!(usage0, 0);

        engine
            .set("hello".to_string(), Bytes::from("world"))
            .unwrap();
        let usage1 = engine.memory_usage().unwrap();
        assert!(usage1 > 0);

        engine.set("foo".to_string(), Bytes::from("bar")).unwrap();
        let usage2 = engine.memory_usage().unwrap();
        assert!(usage2 > usage1);
    }

    #[test]
    fn test_maxmemory_eviction() {
        let engine = StorageEngine::new();
        // 先设置上限，确保 touch() 会记录访问时间
        engine.set_maxmemory(70);
        engine.set("a".to_string(), Bytes::from("1")).unwrap();
        assert!(engine.get("a").unwrap().is_some());

        // 第二个 key 写入时应该触发淘汰
        engine.set("b".to_string(), Bytes::from("2")).unwrap();
        // 由于内存限制，a 应该被淘汰（LRU 最久未访问）
        assert!(engine.get("a").unwrap().is_none());
        assert!(engine.get("b").unwrap().is_some());
    }

    #[test]
    fn test_watch_check_fails_after_eviction() {
        let engine = StorageEngine::new();
        engine.watch_count.fetch_add(1, Ordering::Relaxed);
        engine.set_maxmemory(70);
        engine.set("a".to_string(), Bytes::from("1")).unwrap();

        let mut watched = HashMap::new();
        watched.insert("a".to_string(), engine.get_version("a").unwrap());
        engine.watch_count.fetch_add(1, Ordering::Relaxed);

        engine.set("b".to_string(), Bytes::from("2")).unwrap();
        assert!(engine.get("a").unwrap().is_none());
        assert!(!engine.watch_check(&watched).unwrap());

        engine.watch_count.fetch_sub(1, Ordering::Relaxed);
    }

    #[test]
    fn test_lru_eviction_order() {
        let engine = StorageEngine::new();
        // 先设置一个较大的上限，确保 touch() 会记录访问时间
        engine.set_maxmemory(500);
        engine.set("a".to_string(), Bytes::from("va")).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        engine.set("b".to_string(), Bytes::from("vb")).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        engine.set("c".to_string(), Bytes::from("vc")).unwrap();

        // 访问 a，更新它的访问时间
        std::thread::sleep(std::time::Duration::from_millis(10));
        let _ = engine.get("a").unwrap();

        // 降低上限为刚好能保留 2 个 key 的值，写入 d 时触发淘汰 1 个
        let usage_before = engine.memory_usage().unwrap();
        let maxmem = usage_before + 40;
        engine.set_maxmemory(maxmem as u64);
        engine.set("d".to_string(), Bytes::from("vd")).unwrap();

        // a 因为最近被访问，不应被淘汰；b 最久未访问，应被淘汰
        assert!(
            engine.get("a").unwrap().is_some(),
            "a 最近被访问，不应被淘汰"
        );
        assert!(engine.get("b").unwrap().is_none(), "b 最久未访问，应被淘汰");
    }

    #[test]
    fn test_setex() {
        let engine = StorageEngine::new();
        engine.setex("k".to_string(), 10, Bytes::from("v")).unwrap();
        assert_eq!(engine.get("k").unwrap(), Some(Bytes::from("v")));
        // ttl() 返回毫秒，10秒 = 10000ms
        let ttl = engine.ttl("k").unwrap();
        assert!(ttl >= 9000 && ttl <= 10000, "ttl={}", ttl);
    }

    #[test]
    fn test_psetex() {
        let engine = StorageEngine::new();
        engine
            .psetex("k".to_string(), 10, Bytes::from("v"))
            .unwrap();
        assert_eq!(engine.get("k").unwrap(), Some(Bytes::from("v")));
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(engine.get("k").unwrap(), None);
    }

    #[test]
    fn test_getset() {
        let engine = StorageEngine::new();
        engine.set("k".to_string(), Bytes::from("old")).unwrap();
        let old = engine.getset("k", Bytes::from("new")).unwrap();
        assert_eq!(old, Some(Bytes::from("old")));
        assert_eq!(engine.get("k").unwrap(), Some(Bytes::from("new")));

        let none = engine.getset("missing", Bytes::from("val")).unwrap();
        assert_eq!(none, None);
        assert_eq!(engine.get("missing").unwrap(), Some(Bytes::from("val")));
    }

    #[test]
    fn test_getdel() {
        let engine = StorageEngine::new();
        engine.set("k".to_string(), Bytes::from("v")).unwrap();
        let val = engine.getdel("k").unwrap();
        assert_eq!(val, Some(Bytes::from("v")));
        assert_eq!(engine.get("k").unwrap(), None);

        let none = engine.getdel("missing").unwrap();
        assert_eq!(none, None);
    }

    #[test]
    fn test_getex_persist() {
        let engine = StorageEngine::new();
        engine
            .set_with_ttl("k".to_string(), Bytes::from("v"), 10_000)
            .unwrap();
        let val = engine.getex("k", GetExOption::Persist).unwrap();
        assert_eq!(val, Some(Bytes::from("v")));
        // 持久化后 ttl 应该为 -1
        assert_eq!(engine.ttl("k").unwrap(), -1);
    }

    #[test]
    fn test_getex_ex() {
        let engine = StorageEngine::new();
        engine.set("k".to_string(), Bytes::from("v")).unwrap();
        engine.getex("k", GetExOption::Ex(3600)).unwrap();
        let ttl = engine.ttl("k").unwrap();
        assert!(ttl >= 3_599_000 && ttl <= 3_600_000, "ttl={}", ttl);
    }

    #[test]
    fn test_getex_px() {
        let engine = StorageEngine::new();
        engine.set("k".to_string(), Bytes::from("v")).unwrap();
        engine.getex("k", GetExOption::Px(10)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert_eq!(engine.get("k").unwrap(), None);
    }

    #[test]
    fn test_getex_nonexistent() {
        let engine = StorageEngine::new();
        let val = engine.getex("missing", GetExOption::Persist).unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_geoadd_geodist() {
        let engine = StorageEngine::new();

        // 添加北京和上海的位置
        let count = engine
            .geoadd(
                "cities",
                vec![
                    (116.4074, 39.9042, "北京".to_string()),
                    (121.4737, 31.2304, "上海".to_string()),
                ],
            )
            .unwrap();
        assert_eq!(count, 2);

        // 计算距离（约 1067 km）
        let dist = engine.geodist("cities", "北京", "上海", "km").unwrap();
        assert!(dist.is_some());
        let dist_km = dist.unwrap();
        assert!(
            (dist_km - 1067.0).abs() < 20.0,
            "距离误差过大: {} km",
            dist_km
        );

        // 测试不同单位
        let dist_m = engine
            .geodist("cities", "北京", "上海", "m")
            .unwrap()
            .unwrap();
        assert!((dist_m - 1067000.0).abs() < 20000.0);
    }

    #[test]
    fn test_geohash() {
        let engine = StorageEngine::new();
        engine
            .geoadd("cities", vec![(116.4074, 39.9042, "北京".to_string())])
            .unwrap();

        let hashes = engine.geohash("cities", &["北京".to_string()]).unwrap();
        assert_eq!(hashes.len(), 1);
        assert!(hashes[0].is_some());
        assert_eq!(hashes[0].as_ref().unwrap().len(), 11); // 11 位 base32
    }

    #[test]
    fn test_geopos() {
        let engine = StorageEngine::new();
        engine
            .geoadd("cities", vec![(116.4074, 39.9042, "北京".to_string())])
            .unwrap();

        let positions = engine.geopos("cities", &["北京".to_string()]).unwrap();
        assert_eq!(positions.len(), 1);
        assert!(positions[0].is_some());
        let (lon, lat) = positions[0].unwrap();
        assert!((lon - 116.4074).abs() < 0.001, "经度解码误差过大: {}", lon);
        assert!((lat - 39.9042).abs() < 0.001, "纬度解码误差过大: {}", lat);
    }

    #[test]
    fn test_geosearch_byradius() {
        let engine = StorageEngine::new();
        engine
            .geoadd(
                "cities",
                vec![
                    (116.4074, 39.9042, "北京".to_string()),
                    (121.4737, 31.2304, "上海".to_string()),
                    (113.2644, 23.1291, "广州".to_string()),
                    (114.0579, 22.5431, "深圳".to_string()),
                ],
            )
            .unwrap();

        // 以北京为中心，500km 范围内
        let results = engine
            .geosearch(
                "cities",
                116.4074,
                39.9042,
                Some(500000.0),
                None,
                Some("ASC"),
                0,
            )
            .unwrap();
        assert!(!results.is_empty());
        // 北京自己应该在范围内
        assert_eq!(results[0].0, "北京");
    }

    #[test]
    fn test_geosearch_bybox() {
        let engine = StorageEngine::new();
        engine
            .geoadd(
                "cities",
                vec![
                    (116.4074, 39.9042, "北京".to_string()),
                    (121.4737, 31.2304, "上海".to_string()),
                ],
            )
            .unwrap();

        // 以北京为中心，2000km x 2000km 矩形
        let results = engine
            .geosearch(
                "cities",
                116.4074,
                39.9042,
                None,
                Some((2000000.0, 2000000.0)),
                Some("ASC"),
                0,
            )
            .unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_geosearch_asc_desc() {
        let engine = StorageEngine::new();
        engine
            .geoadd(
                "cities",
                vec![
                    (116.4074, 39.9042, "北京".to_string()),
                    (121.4737, 31.2304, "上海".to_string()),
                    (113.2644, 23.1291, "广州".to_string()),
                ],
            )
            .unwrap();

        // ASC 排序
        let asc = engine
            .geosearch(
                "cities",
                116.4074,
                39.9042,
                Some(2000000.0),
                None,
                Some("ASC"),
                0,
            )
            .unwrap();
        assert_eq!(asc[0].0, "北京");

        // DESC 排序
        let desc = engine
            .geosearch(
                "cities",
                116.4074,
                39.9042,
                Some(2000000.0),
                None,
                Some("DESC"),
                0,
            )
            .unwrap();
        assert_eq!(desc[0].0, "广州");
    }

    #[test]
    fn test_geosearchstore() {
        let engine = StorageEngine::new();
        engine
            .geoadd(
                "cities",
                vec![
                    (116.4074, 39.9042, "北京".to_string()),
                    (121.4737, 31.2304, "上海".to_string()),
                ],
            )
            .unwrap();

        let count = engine
            .geosearchstore(
                "near",
                "cities",
                116.4074,
                39.9042,
                Some(500000.0),
                None,
                Some("ASC"),
                0,
                false,
            )
            .unwrap();
        assert_eq!(count, 1);

        let near_pos = engine.geopos("near", &["北京".to_string()]).unwrap();
        assert!(near_pos[0].is_some());
    }

    #[test]
    fn test_geoadd_wrongtype() {
        let engine = StorageEngine::new();
        engine.set("s".to_string(), Bytes::from("hello")).unwrap();
        let result = engine.geoadd("s", vec![(0.0, 0.0, "a".to_string())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_msetnx() {
        let engine = StorageEngine::new();
        let result = engine
            .msetnx(&[
                ("a".to_string(), Bytes::from("1")),
                ("b".to_string(), Bytes::from("2")),
            ])
            .unwrap();
        assert_eq!(result, 1);
        assert_eq!(engine.get("a").unwrap(), Some(Bytes::from("1")));
        assert_eq!(engine.get("b").unwrap(), Some(Bytes::from("2")));

        // 再次设置，应该失败
        let result = engine
            .msetnx(&[
                ("a".to_string(), Bytes::from("x")),
                ("c".to_string(), Bytes::from("3")),
            ])
            .unwrap();
        assert_eq!(result, 0);
        // a 保持原值，c 未被设置
        assert_eq!(engine.get("a").unwrap(), Some(Bytes::from("1")));
        assert_eq!(engine.get("c").unwrap(), None);
    }

    #[test]
    fn test_msetnx_with_expired_key() {
        let engine = StorageEngine::new();
        engine
            .set_with_ttl("a".to_string(), Bytes::from("old"), 1)
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        let result = engine
            .msetnx(&[("a".to_string(), Bytes::from("new"))])
            .unwrap();
        assert_eq!(result, 1);
        assert_eq!(engine.get("a").unwrap(), Some(Bytes::from("new")));
    }

    #[test]
    fn test_incrbyfloat() {
        let engine = StorageEngine::new();
        let v = engine.incrbyfloat("k", 0.5).unwrap();
        assert_eq!(v, "0.5");
        let v = engine.incrbyfloat("k", 0.25).unwrap();
        assert_eq!(v, "0.75");
        let v = engine.incrbyfloat("k", -0.3).unwrap();
        assert_eq!(v, "0.45");
    }

    #[test]
    fn test_incrbyfloat_nonexistent() {
        let engine = StorageEngine::new();
        let v = engine.incrbyfloat("k", 3.14).unwrap();
        assert_eq!(v, "3.14");
    }

    #[test]
    fn test_setrange() {
        let engine = StorageEngine::new();
        engine
            .set("k".to_string(), Bytes::from("Hello World"))
            .unwrap();
        let len = engine.setrange("k", 6, Bytes::from("Redis")).unwrap();
        assert_eq!(len, 11);
        assert_eq!(engine.get("k").unwrap(), Some(Bytes::from("Hello Redis")));
    }

    #[test]
    fn test_setrange_padding() {
        let engine = StorageEngine::new();
        let len = engine.setrange("k", 6, Bytes::from("Redis")).unwrap();
        assert_eq!(len, 11);
        let val = engine.get("k").unwrap().unwrap();
        assert_eq!(&val[..6], &[0, 0, 0, 0, 0, 0]);
        assert_eq!(&val[6..], b"Redis");
    }

    #[test]
    fn test_setrange_overwrite_beginning() {
        let engine = StorageEngine::new();
        engine
            .set("k".to_string(), Bytes::from("Hello World"))
            .unwrap();
        let len = engine.setrange("k", 0, Bytes::from("Hola")).unwrap();
        assert_eq!(len, 11);
        assert_eq!(engine.get("k").unwrap(), Some(Bytes::from("Holao World")));
    }

    #[test]
    fn test_setbit_getbit() {
        let engine = StorageEngine::new();

        // 设置位并返回旧值
        let old = engine.setbit("k", 0, true).unwrap();
        assert_eq!(old, 0);
        let old = engine.setbit("k", 0, false).unwrap();
        assert_eq!(old, 1);

        // 获取位
        assert_eq!(engine.getbit("k", 0).unwrap(), 0);
        assert_eq!(engine.getbit("k", 1).unwrap(), 0);

        // 设置多个位
        engine.setbit("k", 7, true).unwrap(); // 第一个字节的最低位
        assert_eq!(engine.getbit("k", 7).unwrap(), 1);
        assert_eq!(engine.getbit("k", 6).unwrap(), 0);
    }

    #[test]
    fn test_setbit_auto_expand() {
        let engine = StorageEngine::new();

        // 在远超出当前长度的位置设置位
        let old = engine.setbit("k", 23, true).unwrap();
        assert_eq!(old, 0);

        // 验证中间字节被填充为 0
        assert_eq!(engine.getbit("k", 8).unwrap(), 0);
        assert_eq!(engine.getbit("k", 15).unwrap(), 0);
        assert_eq!(engine.getbit("k", 16).unwrap(), 0);
        assert_eq!(engine.getbit("k", 23).unwrap(), 1);
    }

    #[test]
    fn test_getbit_nonexistent() {
        let engine = StorageEngine::new();
        assert_eq!(engine.getbit("missing", 0).unwrap(), 0);
        assert_eq!(engine.getbit("missing", 100).unwrap(), 0);
    }

    #[test]
    fn test_bitcount() {
        let engine = StorageEngine::new();
        // 二进制: 0b10101010 = 170, 0b11110000 = 240
        // 总共 4 + 4 = 8 个 1
        engine
            .set("k".to_string(), Bytes::from(vec![0b10101010, 0b11110000]))
            .unwrap();

        // 全范围
        assert_eq!(engine.bitcount("k", 0, -1, true).unwrap(), 8);

        // 指定字节范围
        assert_eq!(engine.bitcount("k", 0, 0, true).unwrap(), 4);
        assert_eq!(engine.bitcount("k", 1, 1, true).unwrap(), 4);
        assert_eq!(engine.bitcount("k", 0, 1, true).unwrap(), 8);

        // 负数字节索引
        assert_eq!(engine.bitcount("k", -1, -1, true).unwrap(), 4);

        // 位范围
        assert_eq!(engine.bitcount("k", 0, 7, false).unwrap(), 4);
        assert_eq!(engine.bitcount("k", 8, 15, false).unwrap(), 4);
    }

    #[test]
    fn test_bitop_and_or_xor() {
        let engine = StorageEngine::new();
        engine
            .set("a".to_string(), Bytes::from(vec![0b11110000]))
            .unwrap();
        engine
            .set("b".to_string(), Bytes::from(vec![0b10101010]))
            .unwrap();

        // AND
        let len = engine
            .bitop("AND", "dest", &["a".to_string(), "b".to_string()])
            .unwrap();
        assert_eq!(len, 1);
        assert_eq!(
            engine.get("dest").unwrap(),
            Some(Bytes::from(vec![0b10100000]))
        );

        // OR
        let len = engine
            .bitop("OR", "dest", &["a".to_string(), "b".to_string()])
            .unwrap();
        assert_eq!(len, 1);
        assert_eq!(
            engine.get("dest").unwrap(),
            Some(Bytes::from(vec![0b11111010]))
        );

        // XOR
        let len = engine
            .bitop("XOR", "dest", &["a".to_string(), "b".to_string()])
            .unwrap();
        assert_eq!(len, 1);
        assert_eq!(
            engine.get("dest").unwrap(),
            Some(Bytes::from(vec![0b01011010]))
        );
    }

    #[test]
    fn test_bitop_not() {
        let engine = StorageEngine::new();
        engine
            .set("a".to_string(), Bytes::from(vec![0b11110000]))
            .unwrap();

        let len = engine.bitop("NOT", "dest", &["a".to_string()]).unwrap();
        assert_eq!(len, 1);
        assert_eq!(
            engine.get("dest").unwrap(),
            Some(Bytes::from(vec![0b00001111]))
        );
    }

    #[test]
    fn test_bitop_different_lengths() {
        let engine = StorageEngine::new();
        engine
            .set("a".to_string(), Bytes::from(vec![0xFF, 0xFF]))
            .unwrap();
        engine
            .set("b".to_string(), Bytes::from(vec![0x00]))
            .unwrap();

        let len = engine
            .bitop("AND", "dest", &["a".to_string(), "b".to_string()])
            .unwrap();
        assert_eq!(len, 2);
        assert_eq!(
            engine.get("dest").unwrap(),
            Some(Bytes::from(vec![0x00, 0x00]))
        );
    }

    #[test]
    fn test_bitpos() {
        let engine = StorageEngine::new();
        // 0b10101010 = 170
        engine
            .set("k".to_string(), Bytes::from(vec![0b10101010]))
            .unwrap();

        // 查找第一个 1
        assert_eq!(engine.bitpos("k", 1, 0, -1, true).unwrap(), 0);
        // 查找第一个 0
        assert_eq!(engine.bitpos("k", 0, 0, -1, true).unwrap(), 1);

        // 指定字节范围查找第一个 1
        // 第二个字节 0b11110000
        engine
            .set("k".to_string(), Bytes::from(vec![0b00000000, 0b11110000]))
            .unwrap();
        assert_eq!(engine.bitpos("k", 1, 0, -1, true).unwrap(), 8);
        assert_eq!(engine.bitpos("k", 1, 1, 1, true).unwrap(), 8);

        // 查找不存在的 bit
        engine
            .set("k".to_string(), Bytes::from(vec![0b00000000]))
            .unwrap();
        assert_eq!(engine.bitpos("k", 1, 0, -1, true).unwrap(), -1);
    }

    #[test]
    fn test_pfadd_pfcount() {
        let engine = StorageEngine::new();

        // 添加元素
        let updated = engine
            .pfadd("hll", &["a".to_string(), "b".to_string(), "c".to_string()])
            .unwrap();
        assert_eq!(updated, 1);

        // 估算基数
        let count = engine.pfcount(&["hll".to_string()]).unwrap();
        assert!(count >= 3);
        assert!(count <= 10); // 少量元素估算应该接近实际值

        // 重复元素不增长
        let updated = engine
            .pfadd("hll", &["a".to_string(), "b".to_string()])
            .unwrap();
        assert_eq!(updated, 0);

        let count2 = engine.pfcount(&["hll".to_string()]).unwrap();
        assert_eq!(count, count2);
    }

    #[test]
    fn test_pfcount_large_set() {
        let engine = StorageEngine::new();

        // 添加 10000 个唯一元素
        let mut elements = Vec::new();
        for i in 0..10000 {
            elements.push(format!("element_{}", i));
        }
        engine.pfadd("hll", &elements).unwrap();

        let count = engine.pfcount(&["hll".to_string()]).unwrap();
        let actual = 10000u64;
        let diff = if count > actual {
            count - actual
        } else {
            actual - count
        };
        let error_rate = diff as f64 / actual as f64;
        assert!(
            error_rate < 0.05,
            "估算误差 {}% 超过 5%，实际估算值: {}，期望值: {}",
            error_rate * 100.0,
            count,
            actual
        );
    }

    #[test]
    fn test_pfmerge() {
        let engine = StorageEngine::new();

        engine
            .pfadd("hll1", &["a".to_string(), "b".to_string(), "c".to_string()])
            .unwrap();
        engine
            .pfadd("hll2", &["c".to_string(), "d".to_string(), "e".to_string()])
            .unwrap();

        engine
            .pfmerge("merged", &["hll1".to_string(), "hll2".to_string()])
            .unwrap();

        let count = engine.pfcount(&["merged".to_string()]).unwrap();
        assert!(count >= 5); // a, b, c, d, e = 5 个唯一元素
    }

    #[test]
    fn test_pfcount_multiple_keys() {
        let engine = StorageEngine::new();

        engine
            .pfadd("hll1", &["a".to_string(), "b".to_string()])
            .unwrap();
        engine
            .pfadd("hll2", &["b".to_string(), "c".to_string()])
            .unwrap();

        let count = engine
            .pfcount(&["hll1".to_string(), "hll2".to_string()])
            .unwrap();
        assert!(count >= 3); // a, b, c
    }

    #[test]
    fn test_pfadd_wrongtype() {
        let engine = StorageEngine::new();
        engine.set("s".to_string(), Bytes::from("hello")).unwrap();
        let result = engine.pfadd("s", &["a".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_pfcount_nonexistent() {
        let engine = StorageEngine::new();
        let count = engine.pfcount(&["missing".to_string()]).unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_lset() {
        let engine = StorageEngine::new();
        engine
            .lpush(
                "list",
                vec![Bytes::from("c"), Bytes::from("b"), Bytes::from("a")],
            )
            .unwrap();
        engine.lset("list", 1, Bytes::from("x")).unwrap();
        assert_eq!(engine.lindex("list", 1).unwrap(), Some(Bytes::from("x")));
        // 负数索引
        engine.lset("list", -1, Bytes::from("z")).unwrap();
        assert_eq!(engine.lindex("list", 2).unwrap(), Some(Bytes::from("z")));
    }

    #[test]
    fn test_lset_out_of_range() {
        let engine = StorageEngine::new();
        engine.lpush("list", vec![Bytes::from("a")]).unwrap();
        let result = engine.lset("list", 5, Bytes::from("x"));
        assert!(result.is_err());
        let result = engine.lset("list", -5, Bytes::from("x"));
        assert!(result.is_err());
    }

    #[test]
    fn test_lset_no_key() {
        let engine = StorageEngine::new();
        let result = engine.lset("missing", 0, Bytes::from("x"));
        assert!(result.is_err());
    }

    #[test]
    fn test_linsert() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        let len = engine
            .linsert(
                "list",
                LInsertPosition::Before,
                Bytes::from("b"),
                Bytes::from("x"),
            )
            .unwrap();
        assert_eq!(len, 4);
        assert_eq!(engine.lindex("list", 0).unwrap(), Some(Bytes::from("a")));
        assert_eq!(engine.lindex("list", 1).unwrap(), Some(Bytes::from("x")));
        assert_eq!(engine.lindex("list", 2).unwrap(), Some(Bytes::from("b")));

        let len = engine
            .linsert(
                "list",
                LInsertPosition::After,
                Bytes::from("b"),
                Bytes::from("y"),
            )
            .unwrap();
        assert_eq!(len, 5);
        assert_eq!(engine.lindex("list", 3).unwrap(), Some(Bytes::from("y")));
    }

    #[test]
    fn test_linsert_pivot_not_found() {
        let engine = StorageEngine::new();
        engine.rpush("list", vec![Bytes::from("a")]).unwrap();
        let len = engine
            .linsert(
                "list",
                LInsertPosition::Before,
                Bytes::from("z"),
                Bytes::from("x"),
            )
            .unwrap();
        assert_eq!(len, -1);
    }

    #[test]
    fn test_lrem_positive_count() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![
                    Bytes::from("a"),
                    Bytes::from("b"),
                    Bytes::from("a"),
                    Bytes::from("c"),
                    Bytes::from("a"),
                ],
            )
            .unwrap();
        let removed = engine.lrem("list", 2, Bytes::from("a")).unwrap();
        assert_eq!(removed, 2);
        assert_eq!(engine.llen("list").unwrap(), 3);
        // 删除前2个 a (索引0和2)，剩余 [b, c, a]
        assert_eq!(engine.lindex("list", 0).unwrap(), Some(Bytes::from("b")));
        assert_eq!(engine.lindex("list", 1).unwrap(), Some(Bytes::from("c")));
        assert_eq!(engine.lindex("list", 2).unwrap(), Some(Bytes::from("a")));
    }

    #[test]
    fn test_lrem_negative_count() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![
                    Bytes::from("a"),
                    Bytes::from("b"),
                    Bytes::from("a"),
                    Bytes::from("c"),
                    Bytes::from("a"),
                ],
            )
            .unwrap();
        let removed = engine.lrem("list", -1, Bytes::from("a")).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(engine.llen("list").unwrap(), 4);
        assert_eq!(engine.lindex("list", 3).unwrap(), Some(Bytes::from("c")));
    }

    #[test]
    fn test_lrem_zero_count() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![
                    Bytes::from("a"),
                    Bytes::from("b"),
                    Bytes::from("a"),
                    Bytes::from("c"),
                ],
            )
            .unwrap();
        let removed = engine.lrem("list", 0, Bytes::from("a")).unwrap();
        assert_eq!(removed, 2);
        assert_eq!(engine.llen("list").unwrap(), 2);
        assert_eq!(engine.lindex("list", 0).unwrap(), Some(Bytes::from("b")));
        assert_eq!(engine.lindex("list", 1).unwrap(), Some(Bytes::from("c")));
    }

    #[test]
    fn test_ltrim() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![
                    Bytes::from("a"),
                    Bytes::from("b"),
                    Bytes::from("c"),
                    Bytes::from("d"),
                    Bytes::from("e"),
                ],
            )
            .unwrap();
        engine.ltrim("list", 1, 3).unwrap();
        assert_eq!(engine.llen("list").unwrap(), 3);
        assert_eq!(engine.lindex("list", 0).unwrap(), Some(Bytes::from("b")));
        assert_eq!(engine.lindex("list", 2).unwrap(), Some(Bytes::from("d")));
    }

    #[test]
    fn test_ltrim_negative_indices() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![
                    Bytes::from("a"),
                    Bytes::from("b"),
                    Bytes::from("c"),
                    Bytes::from("d"),
                    Bytes::from("e"),
                ],
            )
            .unwrap();
        engine.ltrim("list", -3, -1).unwrap();
        assert_eq!(engine.llen("list").unwrap(), 3);
        assert_eq!(engine.lindex("list", 0).unwrap(), Some(Bytes::from("c")));
    }

    #[test]
    fn test_ltrim_out_of_range() {
        let engine = StorageEngine::new();
        engine
            .rpush("list", vec![Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        engine.ltrim("list", 5, 10).unwrap();
        assert_eq!(engine.llen("list").unwrap(), 0);
    }

    #[test]
    fn test_lpos() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![
                    Bytes::from("a"),
                    Bytes::from("b"),
                    Bytes::from("a"),
                    Bytes::from("c"),
                    Bytes::from("a"),
                ],
            )
            .unwrap();
        let pos = engine.lpos("list", Bytes::from("a"), 1, 0, 0).unwrap();
        assert_eq!(pos, vec![0]); // rank=1 count=0 -> 第一个匹配
        let pos = engine.lpos("list", Bytes::from("a"), 2, 0, 0).unwrap();
        assert_eq!(pos, vec![2]); // rank=2 count=0 -> 第二个匹配
        let pos = engine.lpos("list", Bytes::from("a"), -1, 0, 0).unwrap();
        assert_eq!(pos, vec![4]); // rank=-1 count=0 -> 最后一个匹配
    }

    #[test]
    fn test_lpos_not_found() {
        let engine = StorageEngine::new();
        engine
            .rpush("list", vec![Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        let pos = engine.lpos("list", Bytes::from("z"), 1, 0, 0).unwrap();
        assert!(pos.is_empty());
    }

    #[test]
    fn test_lpos_count() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![
                    Bytes::from("a"),
                    Bytes::from("b"),
                    Bytes::from("a"),
                    Bytes::from("c"),
                    Bytes::from("a"),
                ],
            )
            .unwrap();
        let pos = engine.lpos("list", Bytes::from("a"), 1, 2, 0).unwrap();
        assert_eq!(pos, vec![0, 2]);
    }

    #[tokio::test]
    async fn test_blpop_immediate() {
        let engine = StorageEngine::new();
        engine
            .rpush("list", vec![Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        let result = engine.blpop(&["list".to_string()], 1.0).await.unwrap();
        assert_eq!(result, Some(("list".to_string(), Bytes::from("a"))));
        assert_eq!(engine.llen("list").unwrap(), 1);
    }

    #[tokio::test]
    async fn test_blpop_blocking() {
        let engine = StorageEngine::new();
        let engine_clone = engine.clone();

        // 在另一个任务中延迟 push
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            engine_clone.lpush("list", vec![Bytes::from("x")]).unwrap();
        });

        let result = engine.blpop(&["list".to_string()], 5.0).await.unwrap();
        assert_eq!(result, Some(("list".to_string(), Bytes::from("x"))));
    }

    #[tokio::test]
    async fn test_blpop_timeout() {
        let engine = StorageEngine::new();
        let result = engine.blpop(&["list".to_string()], 0.05).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_brpop_blocking() {
        let engine = StorageEngine::new();
        let engine_clone = engine.clone();

        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            engine_clone.rpush("list", vec![Bytes::from("y")]).unwrap();
        });

        let result = engine.brpop(&["list".to_string()], 5.0).await.unwrap();
        assert_eq!(result, Some(("list".to_string(), Bytes::from("y"))));
    }

    #[test]
    fn test_hincrby() {
        let engine = StorageEngine::new();
        let v = engine.hincrby("hash", "field".to_string(), 5).unwrap();
        assert_eq!(v, 5);
        let v = engine.hincrby("hash", "field".to_string(), 3).unwrap();
        assert_eq!(v, 8);
        let v = engine.hincrby("hash", "field".to_string(), -2).unwrap();
        assert_eq!(v, 6);
    }

    #[test]
    fn test_hincrby_non_integer() {
        let engine = StorageEngine::new();
        engine
            .hset("hash", "field".to_string(), Bytes::from("abc"))
            .unwrap();
        let result = engine.hincrby("hash", "field".to_string(), 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_hincrbyfloat() {
        let engine = StorageEngine::new();
        let v = engine
            .hincrbyfloat("hash", "field".to_string(), 0.5)
            .unwrap();
        assert_eq!(v, "0.5");
        let v = engine
            .hincrbyfloat("hash", "field".to_string(), 0.25)
            .unwrap();
        assert_eq!(v, "0.75");
    }

    #[test]
    fn test_hkeys() {
        let engine = StorageEngine::new();
        engine
            .hset("hash", "b".to_string(), Bytes::from("2"))
            .unwrap();
        engine
            .hset("hash", "a".to_string(), Bytes::from("1"))
            .unwrap();
        let keys = engine.hkeys("hash").unwrap();
        assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn test_hvals() {
        let engine = StorageEngine::new();
        engine
            .hset("hash", "b".to_string(), Bytes::from("2"))
            .unwrap();
        engine
            .hset("hash", "a".to_string(), Bytes::from("1"))
            .unwrap();
        let vals = engine.hvals("hash").unwrap();
        assert_eq!(vals, vec![Bytes::from("1"), Bytes::from("2")]);
    }

    #[test]
    fn test_hsetnx() {
        let engine = StorageEngine::new();
        let result = engine
            .hsetnx("hash", "field".to_string(), Bytes::from("v1"))
            .unwrap();
        assert_eq!(result, 1);
        let result = engine
            .hsetnx("hash", "field".to_string(), Bytes::from("v2"))
            .unwrap();
        assert_eq!(result, 0);
        let val = engine.hget("hash", "field").unwrap();
        assert_eq!(val, Some(Bytes::from("v1")));
    }

    #[test]
    fn test_hrandfield_single() {
        let engine = StorageEngine::new();
        engine
            .hset("hash", "a".to_string(), Bytes::from("1"))
            .unwrap();
        let result = engine.hrandfield("hash", 1, false).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].0 == "a");
        assert!(result[0].1.is_none());
    }

    #[test]
    fn test_hrandfield_multiple() {
        let engine = StorageEngine::new();
        engine
            .hset("hash", "a".to_string(), Bytes::from("1"))
            .unwrap();
        engine
            .hset("hash", "b".to_string(), Bytes::from("2"))
            .unwrap();
        engine
            .hset("hash", "c".to_string(), Bytes::from("3"))
            .unwrap();
        let result = engine.hrandfield("hash", 2, false).unwrap();
        assert_eq!(result.len(), 2);
        let fields: Vec<String> = result.iter().map(|(k, _)| k.clone()).collect();
        assert_ne!(fields[0], fields[1]);
    }

    #[test]
    fn test_hrandfield_negative() {
        let engine = StorageEngine::new();
        engine
            .hset("hash", "a".to_string(), Bytes::from("1"))
            .unwrap();
        engine
            .hset("hash", "b".to_string(), Bytes::from("2"))
            .unwrap();
        let result = engine.hrandfield("hash", -5, false).unwrap();
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn test_hrandfield_with_values() {
        let engine = StorageEngine::new();
        engine
            .hset("hash", "a".to_string(), Bytes::from("1"))
            .unwrap();
        let result = engine.hrandfield("hash", 1, true).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "a");
        assert_eq!(result[0].1, Some(Bytes::from("1")));
    }

    #[test]
    fn test_hscan() {
        let engine = StorageEngine::new();
        engine
            .hset("hash", "a".to_string(), Bytes::from("1"))
            .unwrap();
        engine
            .hset("hash", "b".to_string(), Bytes::from("2"))
            .unwrap();
        engine
            .hset("hash", "c".to_string(), Bytes::from("3"))
            .unwrap();

        let (cursor, fields) = engine.hscan("hash", 0, "*", 2).unwrap();
        assert_eq!(fields.len(), 2);
        assert!(cursor > 0);

        let (cursor2, fields2) = engine.hscan("hash", cursor, "*", 2).unwrap();
        assert_eq!(fields2.len(), 1);
        assert_eq!(cursor2, 0);

        // 合并结果应包含所有字段
        let mut all_fields: Vec<String> = fields
            .iter()
            .chain(fields2.iter())
            .map(|(k, _)| k.clone())
            .collect();
        all_fields.sort();
        assert_eq!(
            all_fields,
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn test_hscan_match() {
        let engine = StorageEngine::new();
        engine
            .hset("hash", "foo".to_string(), Bytes::from("1"))
            .unwrap();
        engine
            .hset("hash", "bar".to_string(), Bytes::from("2"))
            .unwrap();
        engine
            .hset("hash", "foobar".to_string(), Bytes::from("3"))
            .unwrap();

        let (cursor, fields) = engine.hscan("hash", 0, "foo*", 10).unwrap();
        assert_eq!(cursor, 0);
        assert_eq!(fields.len(), 2);
    }

    #[test]
    fn test_spop() {
        let engine = StorageEngine::new();
        engine
            .sadd(
                "set",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        let popped = engine.spop("set", 2).unwrap();
        assert_eq!(popped.len(), 2);
        assert_eq!(engine.scard("set").unwrap(), 1);
    }

    #[test]
    fn test_spop_empty() {
        let engine = StorageEngine::new();
        let popped = engine.spop("set", 1).unwrap();
        assert!(popped.is_empty());
    }

    #[test]
    fn test_srandmember() {
        let engine = StorageEngine::new();
        engine
            .sadd(
                "set",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        let members = engine.srandmember("set", 2).unwrap();
        assert_eq!(members.len(), 2);
        assert_eq!(engine.scard("set").unwrap(), 3); // 不删除
    }

    #[test]
    fn test_srandmember_negative() {
        let engine = StorageEngine::new();
        engine
            .sadd("set", vec![Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        let members = engine.srandmember("set", -5).unwrap();
        assert_eq!(members.len(), 5); // 可重复
    }

    #[test]
    fn test_smove() {
        let engine = StorageEngine::new();
        engine
            .sadd("s1", vec![Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        engine.sadd("s2", vec![Bytes::from("c")]).unwrap();
        let result = engine.smove("s1", "s2", Bytes::from("a")).unwrap();
        assert!(result);
        assert_eq!(engine.scard("s1").unwrap(), 1);
        assert_eq!(engine.scard("s2").unwrap(), 2);
        assert!(engine.sismember("s2", &Bytes::from("a")).unwrap());
    }

    #[test]
    fn test_smove_not_exist() {
        let engine = StorageEngine::new();
        engine.sadd("s1", vec![Bytes::from("a")]).unwrap();
        let result = engine.smove("s1", "s2", Bytes::from("z")).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_sinterstore() {
        let engine = StorageEngine::new();
        engine
            .sadd(
                "s1",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        engine
            .sadd(
                "s2",
                vec![Bytes::from("b"), Bytes::from("c"), Bytes::from("d")],
            )
            .unwrap();
        let count = engine
            .sinterstore("dest", &["s1".to_string(), "s2".to_string()])
            .unwrap();
        assert_eq!(count, 2);
        assert!(engine.sismember("dest", &Bytes::from("b")).unwrap());
        assert!(engine.sismember("dest", &Bytes::from("c")).unwrap());
    }

    #[test]
    fn test_sunionstore() {
        let engine = StorageEngine::new();
        engine
            .sadd("s1", vec![Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        engine
            .sadd("s2", vec![Bytes::from("b"), Bytes::from("c")])
            .unwrap();
        let count = engine
            .sunionstore("dest", &["s1".to_string(), "s2".to_string()])
            .unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_sdiffstore() {
        let engine = StorageEngine::new();
        engine
            .sadd(
                "s1",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        engine
            .sadd("s2", vec![Bytes::from("b"), Bytes::from("c")])
            .unwrap();
        let count = engine
            .sdiffstore("dest", &["s1".to_string(), "s2".to_string()])
            .unwrap();
        assert_eq!(count, 1);
        assert!(engine.sismember("dest", &Bytes::from("a")).unwrap());
    }

    #[test]
    fn test_sscan() {
        let engine = StorageEngine::new();
        engine
            .sadd(
                "set",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();

        let (cursor, members) = engine.sscan("set", 0, "*", 2).unwrap();
        assert_eq!(members.len(), 2);
        assert!(cursor > 0);

        let (cursor2, members2) = engine.sscan("set", cursor, "*", 2).unwrap();
        assert_eq!(members2.len(), 1);
        assert_eq!(cursor2, 0);

        let mut all: Vec<String> = members
            .iter()
            .chain(members2.iter())
            .map(|m| String::from_utf8_lossy(m).to_string())
            .collect();
        all.sort();
        assert_eq!(all, vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    }

    #[test]
    fn test_sscan_match() {
        let engine = StorageEngine::new();
        engine
            .sadd(
                "set",
                vec![
                    Bytes::from("foo"),
                    Bytes::from("bar"),
                    Bytes::from("foobar"),
                ],
            )
            .unwrap();

        let (cursor, members) = engine.sscan("set", 0, "foo*", 10).unwrap();
        assert_eq!(cursor, 0);
        assert_eq!(members.len(), 2);
    }

    // ---------- SORT 测试 ----------

    #[test]
    fn test_sort_list_numeric() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![Bytes::from("3"), Bytes::from("1"), Bytes::from("2")],
            )
            .unwrap();
        let result = engine
            .sort("list", None, Vec::new(), None, None, true, false, None)
            .unwrap();
        assert_eq!(result, vec!["1", "2", "3"]);
    }

    #[test]
    fn test_sort_list_alpha() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![Bytes::from("c"), Bytes::from("a"), Bytes::from("b")],
            )
            .unwrap();
        let result = engine
            .sort("list", None, Vec::new(), None, None, true, true, None)
            .unwrap();
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_sort_desc() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![Bytes::from("1"), Bytes::from("3"), Bytes::from("2")],
            )
            .unwrap();
        let result = engine
            .sort("list", None, Vec::new(), None, None, false, false, None)
            .unwrap();
        assert_eq!(result, vec!["3", "2", "1"]);
    }

    #[test]
    fn test_sort_limit() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![
                    Bytes::from("5"),
                    Bytes::from("1"),
                    Bytes::from("3"),
                    Bytes::from("2"),
                    Bytes::from("4"),
                ],
            )
            .unwrap();
        let result = engine
            .sort(
                "list",
                None,
                Vec::new(),
                Some(1),
                Some(2),
                true,
                false,
                None,
            )
            .unwrap();
        assert_eq!(result, vec!["2", "3"]);
    }

    #[test]
    fn test_sort_store() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![Bytes::from("3"), Bytes::from("1"), Bytes::from("2")],
            )
            .unwrap();
        let result = engine
            .sort(
                "list",
                None,
                Vec::new(),
                None,
                None,
                true,
                false,
                Some("dest".to_string()),
            )
            .unwrap();
        // STORE 时返回空，数据存入 dest
        assert!(result.is_empty());
        let stored = engine.lrange("dest", 0, -1).unwrap();
        let stored_str: Vec<String> = stored
            .iter()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .collect();
        assert_eq!(stored_str, vec!["1", "2", "3"]);
    }

    #[test]
    fn test_sort_set() {
        let engine = StorageEngine::new();
        engine
            .sadd(
                "set",
                vec![Bytes::from("10"), Bytes::from("2"), Bytes::from("1")],
            )
            .unwrap();
        let result = engine
            .sort("set", None, Vec::new(), None, None, true, false, None)
            .unwrap();
        assert_eq!(result, vec!["1", "2", "10"]);
    }

    // ---------- UNLINK 测试 ----------

    #[test]
    fn test_unlink() {
        let engine = StorageEngine::new();
        engine.set("a".to_string(), Bytes::from("1")).unwrap();
        engine.set("b".to_string(), Bytes::from("2")).unwrap();
        let count = engine
            .unlink(&["a".to_string(), "b".to_string(), "c".to_string()])
            .unwrap();
        assert_eq!(count, 2);
        assert!(!engine.exists("a").unwrap());
        assert!(!engine.exists("b").unwrap());
    }

    // ---------- COPY 测试 ----------

    #[test]
    fn test_copy_basic() {
        let engine = StorageEngine::new();
        engine.set("src".to_string(), Bytes::from("value")).unwrap();
        let ok = engine.copy("src", "dest", false).unwrap();
        assert!(ok);
        assert_eq!(engine.get("dest").unwrap(), Some(Bytes::from("value")));
    }

    #[test]
    fn test_copy_no_replace() {
        let engine = StorageEngine::new();
        engine.set("src".to_string(), Bytes::from("new")).unwrap();
        engine.set("dest".to_string(), Bytes::from("old")).unwrap();
        let ok = engine.copy("src", "dest", false).unwrap();
        assert!(!ok);
        assert_eq!(engine.get("dest").unwrap(), Some(Bytes::from("old")));
    }

    #[test]
    fn test_copy_replace() {
        let engine = StorageEngine::new();
        engine.set("src".to_string(), Bytes::from("new")).unwrap();
        engine.set("dest".to_string(), Bytes::from("old")).unwrap();
        let ok = engine.copy("src", "dest", true).unwrap();
        assert!(ok);
        assert_eq!(engine.get("dest").unwrap(), Some(Bytes::from("new")));
    }

    // ---------- DUMP/RESTORE 测试 ----------

    #[test]
    fn test_dump_restore_string() {
        let engine = StorageEngine::new();
        engine.set("key".to_string(), Bytes::from("hello")).unwrap();
        let data = engine.dump("key").unwrap().unwrap();
        engine.restore("restored", 0, &data, false).unwrap();
        assert_eq!(engine.get("restored").unwrap(), Some(Bytes::from("hello")));
    }

    #[test]
    fn test_dump_restore_list() {
        let engine = StorageEngine::new();
        engine
            .rpush(
                "list",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        let data = engine.dump("list").unwrap().unwrap();
        engine.restore("restored", 0, &data, false).unwrap();
        let restored = engine.lrange("restored", 0, -1).unwrap();
        let restored_str: Vec<String> = restored
            .iter()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .collect();
        assert_eq!(restored_str, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_dump_restore_hash() {
        let engine = StorageEngine::new();
        engine
            .hset("hash", "f1".to_string(), Bytes::from("v1"))
            .unwrap();
        engine
            .hset("hash", "f2".to_string(), Bytes::from("v2"))
            .unwrap();
        let data = engine.dump("hash").unwrap().unwrap();
        engine.restore("restored", 0, &data, false).unwrap();
        assert_eq!(
            engine.hget("restored", "f1").unwrap(),
            Some(Bytes::from("v1"))
        );
        assert_eq!(
            engine.hget("restored", "f2").unwrap(),
            Some(Bytes::from("v2"))
        );
    }

    #[test]
    fn test_dump_restore_set() {
        let engine = StorageEngine::new();
        engine
            .sadd("set", vec![Bytes::from("x"), Bytes::from("y")])
            .unwrap();
        let data = engine.dump("set").unwrap().unwrap();
        engine.restore("restored", 0, &data, false).unwrap();
        assert!(engine.sismember("restored", &Bytes::from("x")).unwrap());
        assert!(engine.sismember("restored", &Bytes::from("y")).unwrap());
    }

    #[test]
    fn test_dump_restore_zset() {
        let engine = StorageEngine::new();
        engine
            .zadd("zset", vec![(1.0, "a".to_string()), (2.0, "b".to_string())])
            .unwrap();
        let data = engine.dump("zset").unwrap().unwrap();
        engine.restore("restored", 0, &data, false).unwrap();
        assert_eq!(engine.zscore("restored", "a").unwrap(), Some(1.0));
        assert_eq!(engine.zscore("restored", "b").unwrap(), Some(2.0));
    }

    #[test]
    fn test_dump_restore_replace() {
        let engine = StorageEngine::new();
        engine.set("key".to_string(), Bytes::from("old")).unwrap();
        let data = engine.dump("key").unwrap().unwrap();
        engine.set("key".to_string(), Bytes::from("new")).unwrap();
        engine.restore("key", 0, &data, true).unwrap();
        assert_eq!(engine.get("key").unwrap(), Some(Bytes::from("old")));
    }

    #[test]
    fn test_dump_restore_no_replace_error() {
        let engine = StorageEngine::new();
        engine.set("key".to_string(), Bytes::from("old")).unwrap();
        let data = engine.dump("key").unwrap().unwrap();
        let result = engine.restore("key", 0, &data, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_object_encoding_string() {
        let engine = StorageEngine::new();
        // 整数编码
        engine
            .set("int_key".to_string(), Bytes::from("42"))
            .unwrap();
        assert_eq!(
            engine.object_encoding("int_key").unwrap(),
            Some("int".to_string())
        );

        // embstr（≤44 字节）
        engine
            .set("embstr_key".to_string(), Bytes::from("hello world"))
            .unwrap();
        assert_eq!(
            engine.object_encoding("embstr_key").unwrap(),
            Some("embstr".to_string())
        );

        // raw（>44 字节）
        engine
            .set("raw_key".to_string(), Bytes::from("a".repeat(100)))
            .unwrap();
        assert_eq!(
            engine.object_encoding("raw_key").unwrap(),
            Some("raw".to_string())
        );

        // 不存在的 key
        assert_eq!(engine.object_encoding("missing").unwrap(), None);
    }

    #[test]
    fn test_object_encoding_list() {
        let engine = StorageEngine::new();
        // 小列表 → listpack
        engine
            .lpush("small_list", vec![Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        assert_eq!(
            engine.object_encoding("small_list").unwrap(),
            Some("listpack".to_string())
        );

        // 大列表 → quicklist
        let big_values: Vec<Bytes> = (0..3).map(|_| Bytes::from("x".repeat(100))).collect();
        engine.lpush("big_list", big_values).unwrap();
        assert_eq!(
            engine.object_encoding("big_list").unwrap(),
            Some("quicklist".to_string())
        );
    }

    #[test]
    fn test_object_encoding_hash() {
        let engine = StorageEngine::new();
        // 小哈希 → listpack
        engine
            .hset("small_hash", "f1".to_string(), Bytes::from("v1"))
            .unwrap();
        assert_eq!(
            engine.object_encoding("small_hash").unwrap(),
            Some("listpack".to_string())
        );

        // 大哈希 → hashtable
        for i in 0..3usize {
            engine
                .hset(
                    "big_hash",
                    format!("field{}", i),
                    Bytes::from("x".repeat(100)),
                )
                .unwrap();
        }
        assert_eq!(
            engine.object_encoding("big_hash").unwrap(),
            Some("hashtable".to_string())
        );
    }

    #[test]
    fn test_object_encoding_set() {
        let engine = StorageEngine::new();
        // 整数集合 → intset
        engine
            .sadd("int_set", vec![Bytes::from("1"), Bytes::from("2")])
            .unwrap();
        assert_eq!(
            engine.object_encoding("int_set").unwrap(),
            Some("intset".to_string())
        );

        // 非整数集合 → hashtable
        engine
            .sadd("str_set", vec![Bytes::from("hello"), Bytes::from("world")])
            .unwrap();
        assert_eq!(
            engine.object_encoding("str_set").unwrap(),
            Some("hashtable".to_string())
        );
    }

    #[test]
    fn test_object_encoding_zset() {
        let engine = StorageEngine::new();
        // 小有序集合 → listpack
        engine
            .zadd("small_zset", vec![(1.0, "a".to_string())])
            .unwrap();
        assert_eq!(
            engine.object_encoding("small_zset").unwrap(),
            Some("listpack".to_string())
        );

        // 大有序集合 → skiplist
        let members: Vec<(f64, String)> = (0..130).map(|i| (i as f64, format!("m{}", i))).collect();
        engine.zadd("big_zset", members).unwrap();
        assert_eq!(
            engine.object_encoding("big_zset").unwrap(),
            Some("skiplist".to_string())
        );
    }

    #[test]
    fn test_object_refcount() {
        let engine = StorageEngine::new();
        engine.set("k".to_string(), Bytes::from("v")).unwrap();
        assert_eq!(engine.object_refcount("k").unwrap(), Some(1));
        assert_eq!(engine.object_refcount("missing").unwrap(), None);
    }

    #[test]
    fn test_object_idletime() {
        let engine = StorageEngine::new();
        engine.set("k".to_string(), Bytes::from("v")).unwrap();
        // 访问一次更新时间
        let _ = engine.get("k").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        let idle = engine.object_idletime("k").unwrap();
        assert!(idle.is_some());
        assert!(idle.is_some());
        assert_eq!(engine.object_idletime("missing").unwrap(), None);
    }

    #[test]
    fn test_object_help() {
        let help = StorageEngine::object_help();
        assert!(!help.is_empty());
        assert!(help.iter().any(|s| s.contains("ENCODING")));
        assert!(help.iter().any(|s| s.contains("REFCOUNT")));
        assert!(help.iter().any(|s| s.contains("IDLETIME")));
        assert!(help.iter().any(|s| s.contains("FREQ")));
    }

    #[test]
    fn test_object_freq() {
        let engine = StorageEngine::new();
        engine.set_maxmemory(1024 * 1024); // 启用访问计数
        engine.set("k".to_string(), Bytes::from("v")).unwrap();
        // 访问一次增加计数
        let _ = engine.get("k").unwrap();
        let freq = engine.object_freq("k").unwrap();
        assert!(freq.is_some() && freq.unwrap() >= 1, "OBJECT FREQ 应 >= 1");
        assert_eq!(engine.object_freq("missing").unwrap(), None);
    }

    #[test]
    fn test_active_expire_switch() {
        let engine = StorageEngine::new();
        assert!(engine.active_expire_enabled());
        engine.set_active_expire(false);
        assert!(!engine.active_expire_enabled());
        engine.set_active_expire(true);
        assert!(engine.active_expire_enabled());
    }

    #[test]
    fn test_randomkey() {
        let engine = StorageEngine::new();
        assert_eq!(engine.randomkey().unwrap(), None);
        engine.set("a".to_string(), Bytes::from("1")).unwrap();
        engine.set("b".to_string(), Bytes::from("2")).unwrap();
        let key = engine.randomkey().unwrap();
        assert!(key.is_some());
        let k = key.unwrap();
        assert!(k == "a" || k == "b");
    }

    #[test]
    fn test_touch_keys() {
        let engine = StorageEngine::new();
        engine.set("a".to_string(), Bytes::from("1")).unwrap();
        engine.set("b".to_string(), Bytes::from("2")).unwrap();
        let count = engine
            .touch_keys(&["a".to_string(), "b".to_string(), "c".to_string()])
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_expire_at() {
        let engine = StorageEngine::new();
        engine.set("k".to_string(), Bytes::from("v")).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(engine.expire_at("k", now + 10).unwrap());
        let ttl = engine.ttl("k").unwrap();
        assert!(ttl > 0 && ttl <= 10000);
        assert!(!engine.expire_at("missing", now + 10).unwrap());
    }

    #[test]
    fn test_pexpire_at() {
        let engine = StorageEngine::new();
        engine.set("k".to_string(), Bytes::from("v")).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!(engine.pexpire_at("k", now + 10000).unwrap());
        let pttl = engine.pttl("k").unwrap();
        assert!(pttl > 0 && pttl <= 10000);
    }

    #[test]
    fn test_expire_time() {
        let engine = StorageEngine::new();
        engine.set("k".to_string(), Bytes::from("v")).unwrap();
        assert_eq!(engine.expire_time("k").unwrap(), -1);
        assert_eq!(engine.expire_time("missing").unwrap(), -2);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        engine.expire_at("k", now + 10).unwrap();
        let et = engine.expire_time("k").unwrap();
        assert!(et >= now as i64 && et <= (now + 10) as i64);
    }

    #[test]
    fn test_pexpire_time() {
        let engine = StorageEngine::new();
        engine.set("k".to_string(), Bytes::from("v")).unwrap();
        assert_eq!(engine.pexpire_time("k").unwrap(), -1);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        engine.pexpire_at("k", now + 10000).unwrap();
        let pet = engine.pexpire_time("k").unwrap();
        assert!(pet >= now as i64 && pet <= (now + 10000) as i64);
    }

    #[test]
    fn test_renamenx() {
        let engine = StorageEngine::new();
        engine.set("a".to_string(), Bytes::from("1")).unwrap();
        engine.set("b".to_string(), Bytes::from("2")).unwrap();
        assert!(!engine.renamenx("a", "b").unwrap());
        assert!(engine.renamenx("a", "c").unwrap());
        assert_eq!(engine.get("c").unwrap(), Some(Bytes::from("1")));
        assert_eq!(engine.get("a").unwrap(), None);
    }

    #[test]
    fn test_swap_db() {
        let engine = StorageEngine::new();
        engine.set("k".to_string(), Bytes::from("db0")).unwrap();
        engine.select(1).unwrap();
        engine.set("k".to_string(), Bytes::from("db1")).unwrap();

        engine.swap_db(0, 1).unwrap();

        engine.select(0).unwrap();
        assert_eq!(engine.get("k").unwrap(), Some(Bytes::from("db1")));
        engine.select(1).unwrap();
        assert_eq!(engine.get("k").unwrap(), Some(Bytes::from("db0")));
    }

    #[test]
    fn test_flush_db() {
        let engine = StorageEngine::new();
        engine.set("a".to_string(), Bytes::from("1")).unwrap();
        engine.select(1).unwrap();
        engine.set("b".to_string(), Bytes::from("2")).unwrap();

        engine.select(0).unwrap();
        engine.flush_db(0).unwrap();
        assert_eq!(engine.get("a").unwrap(), None);

        engine.select(1).unwrap();
        assert_eq!(engine.get("b").unwrap(), Some(Bytes::from("2")));
    }

    #[test]
    fn test_last_save_time() {
        let engine = StorageEngine::new();
        assert_eq!(engine.get_last_save_time(), 0);
        engine.set_last_save_time(12345);
        assert_eq!(engine.get_last_save_time(), 12345);
    }

    #[test]
    fn test_lmove_left_left() {
        let engine = StorageEngine::new();
        engine
            .lpush(
                "src",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        let v = engine.lmove("src", "dst", true, true).unwrap();
        assert_eq!(v, Some(Bytes::from("c")));
        assert_eq!(
            engine.lrange("src", 0, -1).unwrap(),
            vec![Bytes::from("b"), Bytes::from("a")]
        );
        assert_eq!(engine.lrange("dst", 0, -1).unwrap(), vec![Bytes::from("c")]);
    }

    #[test]
    fn test_lmove_left_right() {
        let engine = StorageEngine::new();
        engine
            .lpush(
                "src",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        let v = engine.lmove("src", "dst", true, false).unwrap();
        assert_eq!(v, Some(Bytes::from("c")));
        assert_eq!(
            engine.lrange("src", 0, -1).unwrap(),
            vec![Bytes::from("b"), Bytes::from("a")]
        );
        assert_eq!(engine.lrange("dst", 0, -1).unwrap(), vec![Bytes::from("c")]);
    }

    #[test]
    fn test_lmove_right_left() {
        let engine = StorageEngine::new();
        engine
            .lpush(
                "src",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        let v = engine.lmove("src", "dst", false, true).unwrap();
        assert_eq!(v, Some(Bytes::from("a")));
        assert_eq!(
            engine.lrange("src", 0, -1).unwrap(),
            vec![Bytes::from("c"), Bytes::from("b")]
        );
        assert_eq!(engine.lrange("dst", 0, -1).unwrap(), vec![Bytes::from("a")]);
    }

    #[test]
    fn test_lmove_right_right() {
        let engine = StorageEngine::new();
        engine
            .lpush(
                "src",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        let v = engine.lmove("src", "dst", false, false).unwrap();
        assert_eq!(v, Some(Bytes::from("a")));
        assert_eq!(
            engine.lrange("src", 0, -1).unwrap(),
            vec![Bytes::from("c"), Bytes::from("b")]
        );
        assert_eq!(engine.lrange("dst", 0, -1).unwrap(), vec![Bytes::from("a")]);
    }

    #[test]
    fn test_lmove_same_key_rotation() {
        let engine = StorageEngine::new();
        engine
            .lpush(
                "list",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        let v = engine.lmove("list", "list", true, false).unwrap();
        assert_eq!(v, Some(Bytes::from("c")));
        assert_eq!(
            engine.lrange("list", 0, -1).unwrap(),
            vec![Bytes::from("b"), Bytes::from("a"), Bytes::from("c")]
        );
    }

    #[test]
    fn test_rpoplpush() {
        let engine = StorageEngine::new();
        engine
            .lpush(
                "src",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        let v = engine.rpoplpush("src", "dst").unwrap();
        assert_eq!(v, Some(Bytes::from("a")));
        assert_eq!(
            engine.lrange("src", 0, -1).unwrap(),
            vec![Bytes::from("c"), Bytes::from("b")]
        );
        assert_eq!(engine.lrange("dst", 0, -1).unwrap(), vec![Bytes::from("a")]);
    }

    #[test]
    fn test_lmpop_single_key() {
        let engine = StorageEngine::new();
        engine
            .lpush(
                "list",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        let result = engine.lmpop(&["list".to_string()], true, 1).unwrap();
        assert!(result.is_some());
        let (key, vals) = result.unwrap();
        assert_eq!(key, "list");
        assert_eq!(vals, vec![Bytes::from("c")]);
    }

    #[test]
    fn test_lmpop_multiple_keys() {
        let engine = StorageEngine::new();
        engine.lpush("l2", vec![Bytes::from("x")]).unwrap();
        engine
            .lpush("l1", vec![Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        let result = engine
            .lmpop(&["l1".to_string(), "l2".to_string()], true, 1)
            .unwrap();
        assert!(result.is_some());
        let (key, vals) = result.unwrap();
        assert_eq!(key, "l1");
        assert_eq!(vals, vec![Bytes::from("b")]);
    }

    #[test]
    fn test_lmpop_count() {
        let engine = StorageEngine::new();
        engine
            .lpush(
                "list",
                vec![
                    Bytes::from("a"),
                    Bytes::from("b"),
                    Bytes::from("c"),
                    Bytes::from("d"),
                ],
            )
            .unwrap();
        let result = engine.lmpop(&["list".to_string()], true, 2).unwrap();
        assert!(result.is_some());
        let (key, vals) = result.unwrap();
        assert_eq!(key, "list");
        assert_eq!(vals, vec![Bytes::from("d"), Bytes::from("c")]);
    }

    #[test]
    fn test_lmpop_empty() {
        let engine = StorageEngine::new();
        let result = engine.lmpop(&["empty".to_string()], true, 1).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_blmove_immediate() {
        let engine = StorageEngine::new();
        engine
            .lpush("src", vec![Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(async { engine.blmove("src", "dst", true, true, 1.0).await })
            .unwrap();
        assert_eq!(result, Some(Bytes::from("b")));
    }

    #[test]
    fn test_blmove_timeout() {
        let engine = StorageEngine::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(async { engine.blmove("empty", "dst", true, true, 0.1).await })
            .unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_blmpop_immediate() {
        let engine = StorageEngine::new();
        engine
            .lpush("list", vec![Bytes::from("a"), Bytes::from("b")])
            .unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(async { engine.blmpop(&["list".to_string()], true, 1, 1.0).await })
            .unwrap();
        assert!(result.is_some());
        let (key, vals) = result.unwrap();
        assert_eq!(key, "list");
        assert_eq!(vals, vec![Bytes::from("b")]);
    }

    #[test]
    fn test_blmpop_timeout() {
        let engine = StorageEngine::new();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt
            .block_on(async { engine.blmpop(&["empty".to_string()], true, 1, 0.1).await })
            .unwrap();
        assert_eq!(result, None);
    }

    // ---------- Hash 字段级过期测试 ----------

    #[test]
    fn test_hexpire_basic() {
        let engine = StorageEngine::new();
        engine
            .hset("h", "f1".to_string(), Bytes::from("v1"))
            .unwrap();
        engine
            .hset("h", "f2".to_string(), Bytes::from("v2"))
            .unwrap();

        let result = engine.hexpire("h", &["f1".to_string()], 3600).unwrap();
        assert_eq!(result, vec![1]);

        let ttls = engine
            .httl("h", &["f1".to_string(), "f2".to_string()])
            .unwrap();
        assert!(ttls[0] > 3500 && ttls[0] <= 3600);
        assert_eq!(ttls[1], -1);
    }

    #[test]
    fn test_hpexpire_and_hpttl() {
        let engine = StorageEngine::new();
        engine
            .hset("h", "f1".to_string(), Bytes::from("v1"))
            .unwrap();

        let result = engine.hpexpire("h", &["f1".to_string()], 5000).unwrap();
        assert_eq!(result, vec![1]);

        let ttls = engine.hpttl("h", &["f1".to_string()]).unwrap();
        assert!(ttls[0] > 4900 && ttls[0] <= 5000);
    }

    #[test]
    fn test_hexpireat_and_hexpiretime() {
        let engine = StorageEngine::new();
        engine
            .hset("h", "f1".to_string(), Bytes::from("v1"))
            .unwrap();

        let future_ts = StorageEngine::now_millis() / 1000 + 100;
        let result = engine
            .hexpireat("h", &["f1".to_string()], future_ts)
            .unwrap();
        assert_eq!(result, vec![1]);

        let times = engine.hexpiretime("h", &["f1".to_string()]).unwrap();
        assert_eq!(times[0] as u64, future_ts);
    }

    #[test]
    fn test_hpexpireat_and_hpexpiretime() {
        let engine = StorageEngine::new();
        engine
            .hset("h", "f1".to_string(), Bytes::from("v1"))
            .unwrap();

        let future_ts = StorageEngine::now_millis() + 100_000;
        let result = engine
            .hpexpireat("h", &["f1".to_string()], future_ts)
            .unwrap();
        assert_eq!(result, vec![1]);

        let times = engine.hpexpiretime("h", &["f1".to_string()]).unwrap();
        assert_eq!(times[0] as u64, future_ts);
    }

    #[test]
    fn test_hpersist() {
        let engine = StorageEngine::new();
        engine
            .hset("h", "f1".to_string(), Bytes::from("v1"))
            .unwrap();

        engine.hexpire("h", &["f1".to_string()], 3600).unwrap();
        let ttls = engine.httl("h", &["f1".to_string()]).unwrap();
        assert!(ttls[0] > 0);

        let result = engine.hpersist("h", &["f1".to_string()]).unwrap();
        assert_eq!(result, vec![1]);

        let ttls = engine.httl("h", &["f1".to_string()]).unwrap();
        assert_eq!(ttls[0], -1);
    }

    #[test]
    fn test_hexpire_field_not_exists() {
        let engine = StorageEngine::new();
        engine
            .hset("h", "f1".to_string(), Bytes::from("v1"))
            .unwrap();

        let result = engine.hexpire("h", &["fx".to_string()], 3600).unwrap();
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_hexpire_key_not_exists() {
        let engine = StorageEngine::new();

        let result = engine
            .hexpire("missing", &["f1".to_string()], 3600)
            .unwrap();
        assert_eq!(result, vec![-1]);

        let ttls = engine.httl("missing", &["f1".to_string()]).unwrap();
        assert_eq!(ttls, vec![-2]);
    }

    #[test]
    fn test_hash_field_lazy_expiration() {
        let engine = StorageEngine::new();
        engine
            .hset("h", "f1".to_string(), Bytes::from("v1"))
            .unwrap();
        engine
            .hset("h", "f2".to_string(), Bytes::from("v2"))
            .unwrap();

        // 设置 f1 立即过期（1毫秒）
        engine.hpexpire("h", &["f1".to_string()], 1).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));

        // hget 应该触发惰性清理
        let v = engine.hget("h", "f1").unwrap();
        assert_eq!(v, None);

        // hlen 应该只返回 1
        let len = engine.hlen("h").unwrap();
        assert_eq!(len, 1);

        // hgetall 应该只返回 f2
        let all = engine.hgetall("h").unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].0, "f2");
    }

    #[test]
    fn test_hash_all_fields_expire_removes_key() {
        let engine = StorageEngine::new();
        engine
            .hset("h", "f1".to_string(), Bytes::from("v1"))
            .unwrap();

        engine.hpexpire("h", &["f1".to_string()], 1).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(50));

        // 触发清理
        let v = engine.hget("h", "f1").unwrap();
        assert_eq!(v, None);

        // key 应该被完全删除
        assert_eq!(engine.exists("h").unwrap(), false);
    }

    #[test]
    fn test_hash_expiration_with_hdel() {
        let engine = StorageEngine::new();
        engine
            .hset("h", "f1".to_string(), Bytes::from("v1"))
            .unwrap();
        engine
            .hset("h", "f2".to_string(), Bytes::from("v2"))
            .unwrap();

        engine.hexpire("h", &["f1".to_string()], 3600).unwrap();
        let count = engine.hdel("h", &["f1".to_string()]).unwrap();
        assert_eq!(count, 1);

        let ttls = engine.httl("h", &["f1".to_string()]).unwrap();
        assert_eq!(ttls[0], -2);
    }

    #[test]
    fn test_hash_expiration_wrong_type() {
        let engine = StorageEngine::new();
        engine.set("s".to_string(), Bytes::from("val")).unwrap();

        let result = engine.hexpire("s", &["f1".to_string()], 3600);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("WRONGTYPE"));
    }

    #[test]
    fn test_hpersist_no_expiration() {
        let engine = StorageEngine::new();
        engine
            .hset("h", "f1".to_string(), Bytes::from("v1"))
            .unwrap();

        let result = engine.hpersist("h", &["f1".to_string()]).unwrap();
        assert_eq!(result, vec![0]);
    }

    #[test]
    fn test_multiple_fields_hexpire() {
        let engine = StorageEngine::new();
        engine
            .hset("h", "f1".to_string(), Bytes::from("v1"))
            .unwrap();
        engine
            .hset("h", "f2".to_string(), Bytes::from("v2"))
            .unwrap();
        engine
            .hset("h", "f3".to_string(), Bytes::from("v3"))
            .unwrap();

        let result = engine
            .hexpire(
                "h",
                &["f1".to_string(), "f2".to_string(), "fx".to_string()],
                3600,
            )
            .unwrap();
        assert_eq!(result, vec![1, 1, 0]);

        let ttls = engine
            .httl("h", &["f1".to_string(), "f2".to_string(), "f3".to_string()])
            .unwrap();
        assert!(ttls[0] > 0);
        assert!(ttls[1] > 0);
        assert_eq!(ttls[2], -1);
    }

    // ---------- Set 补全测试 ----------

    #[test]
    fn test_sintercard() {
        let engine = StorageEngine::new();
        engine
            .sadd(
                "s1",
                vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        engine
            .sadd(
                "s2",
                vec![Bytes::from("b"), Bytes::from("c"), Bytes::from("d")],
            )
            .unwrap();

        let count = engine
            .sintercard(&["s1".to_string(), "s2".to_string()], 0)
            .unwrap();
        assert_eq!(count, 2);

        let count = engine
            .sintercard(&["s1".to_string(), "s2".to_string()], 1)
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_smismember() {
        let engine = StorageEngine::new();
        engine
            .sadd("s1", vec![Bytes::from("a"), Bytes::from("b")])
            .unwrap();

        let result = engine
            .smismember(
                "s1",
                &[Bytes::from("a"), Bytes::from("b"), Bytes::from("c")],
            )
            .unwrap();
        assert_eq!(result, vec![1, 1, 0]);
    }

    // ---------- Sorted Set 补全测试 ----------

    #[test]
    fn test_zrandmember() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "z1",
                vec![
                    (1.0, "a".to_string()),
                    (2.0, "b".to_string()),
                    (3.0, "c".to_string()),
                ],
            )
            .unwrap();

        let result = engine.zrandmember("z1", 2, false).unwrap();
        assert_eq!(result.len(), 2);

        let result = engine.zrandmember("z1", -3, false).unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_zdiff() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "z1",
                vec![
                    (1.0, "a".to_string()),
                    (2.0, "b".to_string()),
                    (3.0, "c".to_string()),
                ],
            )
            .unwrap();
        engine
            .zadd(
                "z2",
                vec![
                    (2.0, "b".to_string()),
                    (3.0, "c".to_string()),
                    (4.0, "d".to_string()),
                ],
            )
            .unwrap();

        let result = engine
            .zdiff(&["z1".to_string(), "z2".to_string()], false)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "a");
    }

    #[test]
    fn test_zdiffstore() {
        let engine = StorageEngine::new();
        engine
            .zadd("z1", vec![(1.0, "a".to_string()), (2.0, "b".to_string())])
            .unwrap();
        engine
            .zadd("z2", vec![(2.0, "b".to_string()), (3.0, "c".to_string())])
            .unwrap();

        let count = engine
            .zdiffstore("z3", &["z1".to_string(), "z2".to_string()])
            .unwrap();
        assert_eq!(count, 1);

        let score = engine.zscore("z3", "a").unwrap();
        assert_eq!(score, Some(1.0));
    }

    #[test]
    fn test_zinter() {
        let engine = StorageEngine::new();
        engine
            .zadd("z1", vec![(1.0, "a".to_string()), (2.0, "b".to_string())])
            .unwrap();
        engine
            .zadd("z2", vec![(2.0, "b".to_string()), (3.0, "c".to_string())])
            .unwrap();

        let result = engine
            .zinter(&["z1".to_string(), "z2".to_string()], None, "SUM", false)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "b");
        assert_eq!(result[0].1, 4.0);
    }

    #[test]
    fn test_zunion() {
        let engine = StorageEngine::new();
        engine.zadd("z1", vec![(1.0, "a".to_string())]).unwrap();
        engine.zadd("z2", vec![(2.0, "a".to_string())]).unwrap();

        let result = engine
            .zunion(&["z1".to_string(), "z2".to_string()], None, "SUM", false)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "a");
        assert_eq!(result[0].1, 3.0);
    }

    #[test]
    fn test_zrangestore() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "z1",
                vec![
                    (1.0, "a".to_string()),
                    (2.0, "b".to_string()),
                    (3.0, "c".to_string()),
                ],
            )
            .unwrap();

        let count = engine
            .zrangestore("z2", "z1", "0", "1", false, false, false, 0, 0)
            .unwrap();
        assert_eq!(count, 2);

        let score = engine.zscore("z2", "a").unwrap();
        assert_eq!(score, Some(1.0));
    }

    #[test]
    fn test_zmpop() {
        let engine = StorageEngine::new();
        engine
            .zadd("z1", vec![(1.0, "a".to_string()), (2.0, "b".to_string())])
            .unwrap();

        let result = engine.zmpop(&["z1".to_string()], true, 1).unwrap();
        assert!(result.is_some());
        let (key, pairs) = result.unwrap();
        assert_eq!(key, "z1");
        assert_eq!(pairs[0].0, "a");
        assert_eq!(pairs[0].1, 1.0);
    }

    #[test]
    fn test_zrevrangebyscore() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "z1",
                vec![
                    (1.0, "a".to_string()),
                    (2.0, "b".to_string()),
                    (3.0, "c".to_string()),
                ],
            )
            .unwrap();

        let result = engine
            .zrevrangebyscore("z1", 3.0, 1.5, false, 0, 0)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "c");
        assert_eq!(result[1].0, "b");
    }

    #[test]
    fn test_zrevrangebylex() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "z1",
                vec![
                    (1.0, "a".to_string()),
                    (1.0, "b".to_string()),
                    (1.0, "c".to_string()),
                ],
            )
            .unwrap();

        let result = engine.zrevrangebylex("z1", "[c", "[a", 0, 0).unwrap();
        assert_eq!(
            result,
            vec!["c".to_string(), "b".to_string(), "a".to_string()]
        );
    }

    #[test]
    fn test_zmscore() {
        let engine = StorageEngine::new();
        engine
            .zadd("z1", vec![(1.0, "a".to_string()), (2.0, "b".to_string())])
            .unwrap();

        let result = engine
            .zmscore("z1", &["a".to_string(), "b".to_string(), "c".to_string()])
            .unwrap();
        assert_eq!(result[0], Some(1.0));
        assert_eq!(result[1], Some(2.0));
        assert_eq!(result[2], None);
    }

    #[test]
    fn test_zlexcount() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "z1",
                vec![
                    (1.0, "a".to_string()),
                    (1.0, "b".to_string()),
                    (1.0, "c".to_string()),
                ],
            )
            .unwrap();

        let count = engine.zlexcount("z1", "[a", "[c").unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn test_zrange_unified() {
        let engine = StorageEngine::new();
        engine
            .zadd(
                "z1",
                vec![
                    (1.0, "a".to_string()),
                    (2.0, "b".to_string()),
                    (3.0, "c".to_string()),
                ],
            )
            .unwrap();

        let result = engine
            .zrange_unified("z1", "0", "1", false, false, false, false, 0, 0)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "a");
        assert_eq!(result[1].0, "b");

        let result = engine
            .zrange_unified("z1", "1.5", "3.0", true, false, false, false, 0, 0)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "b");
        assert_eq!(result[1].0, "c");

        let result = engine
            .zrange_unified("z1", "0", "-1", false, false, true, false, 0, 0)
            .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0, "c");
    }

    // ---------- BITFIELD 测试 ----------

    #[test]
    fn test_bitfield_get_u8() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0x12, 0x34]))
            .unwrap();

        let ops = vec![BitFieldOp::Get(
            BitFieldEncoding {
                signed: false,
                bits: 8,
            },
            BitFieldOffset::Num(0),
        )];
        let result = engine.bitfield("bf", &ops).unwrap();
        assert_eq!(result, vec![BitFieldResult::Value(0x12)]);
    }

    #[test]
    fn test_bitfield_get_u16() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0x12, 0x34]))
            .unwrap();

        let ops = vec![BitFieldOp::Get(
            BitFieldEncoding {
                signed: false,
                bits: 16,
            },
            BitFieldOffset::Num(0),
        )];
        let result = engine.bitfield("bf", &ops).unwrap();
        // 大端序：0x12 0x34 -> 0x1234 = 4660
        assert_eq!(result, vec![BitFieldResult::Value(0x1234)]);
    }

    #[test]
    fn test_bitfield_get_i8_positive() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0x7F]))
            .unwrap();

        let ops = vec![BitFieldOp::Get(
            BitFieldEncoding {
                signed: true,
                bits: 8,
            },
            BitFieldOffset::Num(0),
        )];
        let result = engine.bitfield("bf", &ops).unwrap();
        assert_eq!(result, vec![BitFieldResult::Value(127)]);
    }

    #[test]
    fn test_bitfield_get_i8_negative() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0xFF]))
            .unwrap();

        let ops = vec![BitFieldOp::Get(
            BitFieldEncoding {
                signed: true,
                bits: 8,
            },
            BitFieldOffset::Num(0),
        )];
        let result = engine.bitfield("bf", &ops).unwrap();
        assert_eq!(result, vec![BitFieldResult::Value(-1)]);
    }

    #[test]
    fn test_bitfield_get_i16_negative() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0xFF, 0x80]))
            .unwrap();

        let ops = vec![BitFieldOp::Get(
            BitFieldEncoding {
                signed: true,
                bits: 16,
            },
            BitFieldOffset::Num(0),
        )];
        let result = engine.bitfield("bf", &ops).unwrap();
        // 大端序：0xFF 0x80 -> -128
        assert_eq!(result, vec![BitFieldResult::Value(-128)]);
    }

    #[test]
    fn test_bitfield_set() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0x00]))
            .unwrap();

        let ops = vec![BitFieldOp::Set(
            BitFieldEncoding {
                signed: false,
                bits: 8,
            },
            BitFieldOffset::Num(0),
            0x42,
        )];
        let result = engine.bitfield("bf", &ops).unwrap();
        assert_eq!(result, vec![BitFieldResult::Value(0x00)]);

        // 验证新值
        let ops2 = vec![BitFieldOp::Get(
            BitFieldEncoding {
                signed: false,
                bits: 8,
            },
            BitFieldOffset::Num(0),
        )];
        let result2 = engine.bitfield("bf", &ops2).unwrap();
        assert_eq!(result2, vec![BitFieldResult::Value(0x42)]);
    }

    #[test]
    fn test_bitfield_incrby() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0x05]))
            .unwrap();

        let ops = vec![BitFieldOp::IncrBy(
            BitFieldEncoding {
                signed: false,
                bits: 8,
            },
            BitFieldOffset::Num(0),
            3,
        )];
        let result = engine.bitfield("bf", &ops).unwrap();
        assert_eq!(result, vec![BitFieldResult::Value(0x08)]);
    }

    #[test]
    fn test_bitfield_overflow_wrap() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0xFE]))
            .unwrap();

        let ops = vec![
            BitFieldOp::Overflow(BitFieldOverflow::Wrap),
            BitFieldOp::IncrBy(
                BitFieldEncoding {
                    signed: false,
                    bits: 8,
                },
                BitFieldOffset::Num(0),
                5,
            ),
        ];
        let result = engine.bitfield("bf", &ops).unwrap();
        // 0xFE + 5 = 259, wrap -> 259 % 256 = 3
        assert_eq!(result, vec![BitFieldResult::Value(3)]);
    }

    #[test]
    fn test_bitfield_overflow_sat() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0xFE]))
            .unwrap();

        let ops = vec![
            BitFieldOp::Overflow(BitFieldOverflow::Sat),
            BitFieldOp::IncrBy(
                BitFieldEncoding {
                    signed: false,
                    bits: 8,
                },
                BitFieldOffset::Num(0),
                5,
            ),
        ];
        let result = engine.bitfield("bf", &ops).unwrap();
        // 0xFE + 5 = 259, sat -> 255
        assert_eq!(result, vec![BitFieldResult::Value(255)]);
    }

    #[test]
    fn test_bitfield_overflow_fail() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0xFE]))
            .unwrap();

        let ops = vec![
            BitFieldOp::Overflow(BitFieldOverflow::Fail),
            BitFieldOp::IncrBy(
                BitFieldEncoding {
                    signed: false,
                    bits: 8,
                },
                BitFieldOffset::Num(0),
                5,
            ),
        ];
        let result = engine.bitfield("bf", &ops).unwrap();
        // 溢出，返回 nil
        assert_eq!(result, vec![BitFieldResult::Nil]);

        // 值不应被修改
        let ops2 = vec![BitFieldOp::Get(
            BitFieldEncoding {
                signed: false,
                bits: 8,
            },
            BitFieldOffset::Num(0),
        )];
        let result2 = engine.bitfield("bf", &ops2).unwrap();
        assert_eq!(result2, vec![BitFieldResult::Value(0xFE)]);
    }

    #[test]
    fn test_bitfield_hash_offset() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0x12, 0x34, 0x56, 0x78]))
            .unwrap();

        // #1 u16 -> offset = 1 * 16 = 16，读取字节 2,3 -> 0x5678
        let ops = vec![BitFieldOp::Get(
            BitFieldEncoding {
                signed: false,
                bits: 16,
            },
            BitFieldOffset::Hash(1),
        )];
        let result = engine.bitfield("bf", &ops).unwrap();
        assert_eq!(result, vec![BitFieldResult::Value(0x5678)]);
    }

    #[test]
    fn test_bitfield_cross_byte_boundary() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0xAB, 0xCD]))
            .unwrap();

        // 从位偏移 4 开始读取 12 位
        // 0xAB = 1010 1011, 0xCD = 1100 1101
        // 从第 4 位（0-indexed）开始，即从 0xAB 的 bit 3 开始（大端序）
        // 位偏移 4：byte 0 的 bit 3（因为大端序 bit 7 是 offset 0）
        // offset 4 -> byte 0, bit 7-4 = 3
        // 读取 12 位：1011 1100 1101 -> 0xBCD = 3021
        let ops = vec![BitFieldOp::Get(
            BitFieldEncoding {
                signed: false,
                bits: 12,
            },
            BitFieldOffset::Num(4),
        )];
        let result = engine.bitfield("bf", &ops).unwrap();
        assert_eq!(result, vec![BitFieldResult::Value(0xBCD)]);
    }

    #[test]
    fn test_bitfield_auto_extend() {
        let engine = StorageEngine::new();
        // key 不存在
        let ops = vec![BitFieldOp::Set(
            BitFieldEncoding {
                signed: false,
                bits: 16,
            },
            BitFieldOffset::Num(16),
            0x1234,
        )];
        let result = engine.bitfield("bf", &ops).unwrap();
        assert_eq!(result, vec![BitFieldResult::Value(0x00)]);

        let ops2 = vec![BitFieldOp::Get(
            BitFieldEncoding {
                signed: false,
                bits: 16,
            },
            BitFieldOffset::Num(16),
        )];
        let result2 = engine.bitfield("bf", &ops2).unwrap();
        assert_eq!(result2, vec![BitFieldResult::Value(0x1234)]);
    }

    #[test]
    fn test_bitfield_ro() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0x12, 0x34]))
            .unwrap();

        let ops = vec![BitFieldOp::Get(
            BitFieldEncoding {
                signed: false,
                bits: 8,
            },
            BitFieldOffset::Num(0),
        )];
        let result = engine.bitfield_ro("bf", &ops).unwrap();
        assert_eq!(result, vec![BitFieldResult::Value(0x12)]);
    }

    #[test]
    fn test_bitfield_ro_reject_set() {
        let engine = StorageEngine::new();

        let ops = vec![BitFieldOp::Set(
            BitFieldEncoding {
                signed: false,
                bits: 8,
            },
            BitFieldOffset::Num(0),
            1,
        )];
        let result = engine.bitfield_ro("bf", &ops);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("只支持 GET"));
    }

    #[test]
    fn test_bitfield_signed_overflow_wrap() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0x7F]))
            .unwrap();

        let ops = vec![
            BitFieldOp::Overflow(BitFieldOverflow::Wrap),
            BitFieldOp::IncrBy(
                BitFieldEncoding {
                    signed: true,
                    bits: 8,
                },
                BitFieldOffset::Num(0),
                1,
            ),
        ];
        let result = engine.bitfield("bf", &ops).unwrap();
        // 127 + 1 = 128, i8 wrap -> -128
        assert_eq!(result, vec![BitFieldResult::Value(-128)]);
    }

    #[test]
    fn test_bitfield_signed_overflow_sat() {
        let engine = StorageEngine::new();
        engine
            .set("bf".to_string(), Bytes::from(vec![0x7F]))
            .unwrap();

        let ops = vec![
            BitFieldOp::Overflow(BitFieldOverflow::Sat),
            BitFieldOp::IncrBy(
                BitFieldEncoding {
                    signed: true,
                    bits: 8,
                },
                BitFieldOffset::Num(0),
                10,
            ),
        ];
        let result = engine.bitfield("bf", &ops).unwrap();
        // 127 + 10 = 137, i8 sat -> 127
        assert_eq!(result, vec![BitFieldResult::Value(127)]);
    }

    #[test]
    fn test_bitfield_wrong_type() {
        let engine = StorageEngine::new();
        engine.lpush("list", vec![Bytes::from("a")]).unwrap();

        let ops = vec![BitFieldOp::Get(
            BitFieldEncoding {
                signed: false,
                bits: 8,
            },
            BitFieldOffset::Num(0),
        )];
        let result = engine.bitfield("list", &ops);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("WRONGTYPE"));
    }

    // ---------- Stream 测试 ----------

    #[test]
    fn test_xadd_auto_id() {
        let engine = StorageEngine::new();
        let id = engine
            .xadd(
                "stream",
                "*",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        assert!(id.is_some());
        let id_str = id.unwrap();
        assert!(id_str.contains('-'));
    }

    #[test]
    fn test_xadd_specified_id() {
        let engine = StorageEngine::new();
        let id = engine
            .xadd(
                "stream",
                "1234567890-1",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        assert_eq!(id, Some("1234567890-1".to_string()));
    }

    #[test]
    fn test_xadd_id_increasing() {
        let engine = StorageEngine::new();
        let id1 = engine
            .xadd(
                "stream",
                "*",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap()
            .unwrap();
        let id2 = engine
            .xadd(
                "stream",
                "*",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap()
            .unwrap();
        let sid1 = StreamId::parse(&id1).unwrap();
        let sid2 = StreamId::parse(&id2).unwrap();
        assert!(sid2 > sid1);
    }

    #[test]
    fn test_xadd_partial_auto() {
        let engine = StorageEngine::new();
        let id = engine
            .xadd(
                "stream",
                "1234567890-*",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        assert_eq!(id, Some("1234567890-0".to_string()));
    }

    #[test]
    fn test_xlen() {
        let engine = StorageEngine::new();
        assert_eq!(engine.xlen("stream").unwrap(), 0);
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        assert_eq!(engine.xlen("stream").unwrap(), 2);
    }

    #[test]
    fn test_xrange() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-2",
                vec![("f3".to_string(), "v3".to_string())],
                false,
                None,
                None,
            )
            .unwrap();

        let result = engine.xrange("stream", "-", "+", None).unwrap();
        assert_eq!(result.len(), 3);

        let result = engine.xrange("stream", "1000-1", "1000-2", None).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, StreamId::new(1000, 1));

        let result = engine.xrange("stream", "-", "+", Some(2)).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_xrevrange() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-2",
                vec![("f3".to_string(), "v3".to_string())],
                false,
                None,
                None,
            )
            .unwrap();

        let result = engine.xrevrange("stream", "+", "-", None).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].0, StreamId::new(1000, 2));
        assert_eq!(result[2].0, StreamId::new(1000, 0));
    }

    #[test]
    fn test_xtrim_maxlen() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-2",
                vec![("f3".to_string(), "v3".to_string())],
                false,
                None,
                None,
            )
            .unwrap();

        let removed = engine.xtrim("stream", "MAXLEN", "2", false).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(engine.xlen("stream").unwrap(), 2);
    }

    #[test]
    fn test_xtrim_minid() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-2",
                vec![("f3".to_string(), "v3".to_string())],
                false,
                None,
                None,
            )
            .unwrap();

        let removed = engine.xtrim("stream", "MINID", "1000-1", false).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(engine.xlen("stream").unwrap(), 2);
    }

    #[test]
    fn test_xdel() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();

        let removed = engine.xdel("stream", &[StreamId::new(1000, 0)]).unwrap();
        assert_eq!(removed, 1);
        assert_eq!(engine.xlen("stream").unwrap(), 1);

        let removed = engine.xdel("stream", &[StreamId::new(1000, 99)]).unwrap();
        assert_eq!(removed, 0);
    }

    #[test]
    fn test_xread() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "s1",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "s1",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "s2",
                "2000-0",
                vec![("f3".to_string(), "v3".to_string())],
                false,
                None,
                None,
            )
            .unwrap();

        let result = engine
            .xread(
                &["s1".to_string(), "s2".to_string()],
                &["1000-0".to_string(), "2000-0".to_string()],
                None,
            )
            .unwrap();
        assert_eq!(result.len(), 1); // 只有 s1 有新消息
        assert_eq!(result[0].0, "s1");
        assert_eq!(result[0].1.len(), 1);
        assert_eq!(result[0].1[0].0, StreamId::new(1000, 1));
    }

    #[test]
    fn test_xread_dollar() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "s1",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "s1",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();

        let result = engine
            .xread(&["s1".to_string()], &["$".to_string()], None)
            .unwrap();
        // $ 表示从最后一条之后读，所以没有新消息
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_xsetid() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();

        let ok = engine.xsetid("stream", StreamId::new(2000, 0)).unwrap();
        assert!(ok);

        // 尝试设置更小的 ID，应该返回错误
        let result = engine.xsetid("stream", StreamId::new(1000, 0));
        assert!(result.is_err());

        // 不存在的 key 返回 false
        let ok = engine.xsetid("missing", StreamId::new(1000, 0)).unwrap();
        assert!(!ok);
    }

    #[test]
    fn test_stream_wrong_type() {
        let engine = StorageEngine::new();
        engine.set("s".to_string(), Bytes::from("val")).unwrap();

        let result = engine.xadd(
            "s",
            "*",
            vec![("f1".to_string(), "v1".to_string())],
            false,
            None,
            None,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("WRONGTYPE"));
    }

    #[test]
    fn test_xadd_maxlen() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                Some(2),
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                Some(2),
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-2",
                vec![("f3".to_string(), "v3".to_string())],
                false,
                Some(2),
                None,
            )
            .unwrap();

        assert_eq!(engine.xlen("stream").unwrap(), 2);
        let result = engine.xrange("stream", "-", "+", None).unwrap();
        assert_eq!(result[0].0, StreamId::new(1000, 1));
        assert_eq!(result[1].0, StreamId::new(1000, 2));
    }

    #[test]
    fn test_xadd_nostream() {
        let engine = StorageEngine::new();
        let result = engine
            .xadd(
                "stream",
                "*",
                vec![("f1".to_string(), "v1".to_string())],
                true,
                None,
                None,
            )
            .unwrap();
        assert!(result.is_none());
    }

    // ---------- Stream 消费者组测试 ----------

    #[test]
    fn test_xgroup_create_destroy() {
        let engine = StorageEngine::new();
        // 创建流和消费者组
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        let ok = engine.xgroup_create("stream", "g1", "0-0", false).unwrap();
        assert!(ok);

        // 重复创建应失败
        let result = engine.xgroup_create("stream", "g1", "0-0", false);
        assert!(result.is_err());

        // 销毁消费者组
        let ok = engine.xgroup_destroy("stream", "g1").unwrap();
        assert!(ok);

        // 再次销毁返回 false
        let ok = engine.xgroup_destroy("stream", "g1").unwrap();
        assert!(!ok);
    }

    #[test]
    fn test_xgroup_mkstream() {
        let engine = StorageEngine::new();
        // 不创建流直接创建组，mkstream=true
        let ok = engine.xgroup_create("stream", "g1", "$", true).unwrap();
        assert!(ok);
        assert_eq!(engine.xlen("stream").unwrap(), 0);

        // mkstream=false 应该失败
        let result = engine.xgroup_create("stream2", "g1", "$", false);
        assert!(result.is_err());
    }

    #[test]
    fn test_xgroup_setid() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine.xgroup_create("stream", "g1", "0-0", false).unwrap();

        let ok = engine.xgroup_setid("stream", "g1", "1000-1").unwrap();
        assert!(ok);

        // 不存在的组
        let result = engine.xgroup_setid("stream", "g2", "0-0");
        assert!(result.is_err());
    }

    #[test]
    fn test_xgroup_consumer_management() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine.xgroup_create("stream", "g1", "0-0", false).unwrap();

        // 显式创建消费者
        let ok = engine.xgroup_createconsumer("stream", "g1", "c1").unwrap();
        assert!(ok);

        // 重复创建返回 false
        let ok = engine.xgroup_createconsumer("stream", "g1", "c1").unwrap();
        assert!(!ok);

        // XREADGROUP 自动创建消费者
        let result = engine
            .xreadgroup(
                "g1",
                "c2",
                &["stream".to_string()],
                &[">".to_string()],
                None,
                false,
            )
            .unwrap();
        assert_eq!(result.len(), 1);

        // 删除消费者
        let count = engine.xgroup_delconsumer("stream", "g1", "c1").unwrap();
        assert_eq!(count, 0); // c1 没有 PEL

        // 删除不存在的消费者
        let count = engine
            .xgroup_delconsumer("stream", "g1", "missing")
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_xreadgroup_basic() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine.xgroup_create("stream", "g1", "0-0", false).unwrap();

        // 消费者 c1 读取新消息
        let result = engine
            .xreadgroup(
                "g1",
                "c1",
                &["stream".to_string()],
                &[">".to_string()],
                None,
                false,
            )
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1.len(), 2);
        assert_eq!(result[0].1[0].0, StreamId::new(1000, 0));
        assert_eq!(result[0].1[1].0, StreamId::new(1000, 1));

        // 再次读取，没有新消息了（last_delivered_id 已更新）
        let result = engine
            .xreadgroup(
                "g1",
                "c1",
                &["stream".to_string()],
                &[">".to_string()],
                None,
                false,
            )
            .unwrap();
        assert_eq!(result.len(), 0);

        // 添加新消息后再次读取
        engine
            .xadd(
                "stream",
                "1000-2",
                vec![("f3".to_string(), "v3".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        let result = engine
            .xreadgroup(
                "g1",
                "c1",
                &["stream".to_string()],
                &[">".to_string()],
                None,
                false,
            )
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1.len(), 1);
        assert_eq!(result[0].1[0].0, StreamId::new(1000, 2));
    }

    #[test]
    fn test_xreadgroup_noack() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine.xgroup_create("stream", "g1", "0-0", false).unwrap();

        let result = engine
            .xreadgroup(
                "g1",
                "c1",
                &["stream".to_string()],
                &[">".to_string()],
                None,
                true,
            )
            .unwrap();
        assert_eq!(result.len(), 1);

        // NOACK 模式下消息不应进入 PEL
        let (total, _, _, _, _) = engine
            .xpending("stream", "g1", None, None, None, None)
            .unwrap();
        assert_eq!(total, 0);
    }

    #[test]
    fn test_xreadgroup_pel_read() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine.xgroup_create("stream", "g1", "0-0", false).unwrap();

        // 读取新消息到 PEL
        let result = engine
            .xreadgroup(
                "g1",
                "c1",
                &["stream".to_string()],
                &[">".to_string()],
                None,
                false,
            )
            .unwrap();
        assert_eq!(result.len(), 1);

        // 使用非 ">" 的 ID 读取该消费者 PEL 中的消息
        let result = engine
            .xreadgroup(
                "g1",
                "c1",
                &["stream".to_string()],
                &["1000-0".to_string()],
                None,
                false,
            )
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].1.len(), 1);
        assert_eq!(result[0].1[0].0, StreamId::new(1000, 1));
    }

    #[test]
    fn test_xack() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine.xgroup_create("stream", "g1", "0-0", false).unwrap();

        engine
            .xreadgroup(
                "g1",
                "c1",
                &["stream".to_string()],
                &[">".to_string()],
                None,
                false,
            )
            .unwrap();

        // 确认一条消息
        let acked = engine
            .xack("stream", "g1", &[StreamId::new(1000, 0)])
            .unwrap();
        assert_eq!(acked, 1);

        // PEL 中还剩一条
        let (total, _, _, _, _) = engine
            .xpending("stream", "g1", None, None, None, None)
            .unwrap();
        assert_eq!(total, 1);

        // 确认不存在消息
        let acked = engine
            .xack("stream", "g1", &[StreamId::new(9999, 0)])
            .unwrap();
        assert_eq!(acked, 0);

        // 确认最后一条
        let acked = engine
            .xack("stream", "g1", &[StreamId::new(1000, 1)])
            .unwrap();
        assert_eq!(acked, 1);
        let (total, _, _, _, _) = engine
            .xpending("stream", "g1", None, None, None, None)
            .unwrap();
        assert_eq!(total, 0);
    }

    #[test]
    fn test_xclaim() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine.xgroup_create("stream", "g1", "0-0", false).unwrap();

        // c1 读取消息
        engine
            .xreadgroup(
                "g1",
                "c1",
                &["stream".to_string()],
                &[">".to_string()],
                None,
                false,
            )
            .unwrap();
        let (total, _, _, consumers, _) = engine
            .xpending("stream", "g1", None, None, None, None)
            .unwrap();
        assert_eq!(total, 1);
        assert_eq!(consumers.len(), 1);
        assert_eq!(consumers[0].0, "c1");

        // c2 用 min_idle_time=0 claim 消息
        let result = engine
            .xclaim("stream", "g1", "c2", 0, &[StreamId::new(1000, 0)], false)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, StreamId::new(1000, 0));

        let (total, _, _, consumers, _) = engine
            .xpending("stream", "g1", None, None, None, None)
            .unwrap();
        assert_eq!(total, 1);
        assert_eq!(consumers[0].0, "c2");

        // justid 模式
        let result = engine
            .xclaim("stream", "g1", "c2", 0, &[StreamId::new(1000, 0)], true)
            .unwrap();
        assert_eq!(result[0].1, None);
    }

    #[test]
    fn test_xautoclaim() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine.xgroup_create("stream", "g1", "0-0", false).unwrap();

        // c1 读取两条消息
        engine
            .xreadgroup(
                "g1",
                "c1",
                &["stream".to_string()],
                &[">".to_string()],
                None,
                false,
            )
            .unwrap();

        // c2 自动 claim 从 0-0 开始的所有消息（min_idle_time=0）
        let (_next_id, result) = engine
            .xautoclaim("stream", "g1", "c2", 0, StreamId::new(0, 0), 10, false)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, StreamId::new(1000, 0));
        assert_eq!(result[1].0, StreamId::new(1000, 1));

        let (total, _, _, consumers, _) = engine
            .xpending("stream", "g1", None, None, None, None)
            .unwrap();
        assert_eq!(total, 2);
        assert_eq!(consumers[0].0, "c2");
    }

    #[test]
    fn test_xpending() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-2",
                vec![("f3".to_string(), "v3".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine.xgroup_create("stream", "g1", "0-0", false).unwrap();

        // c1 读取前两条
        engine
            .xreadgroup(
                "g1",
                "c1",
                &["stream".to_string()],
                &[">".to_string()],
                Some(2),
                false,
            )
            .unwrap();
        // c2 读取第三条
        engine
            .xreadgroup(
                "g1",
                "c2",
                &["stream".to_string()],
                &[">".to_string()],
                Some(1),
                false,
            )
            .unwrap();

        // 概览查询
        let (total, min_id, max_id, consumers, _) = engine
            .xpending("stream", "g1", None, None, None, None)
            .unwrap();
        assert_eq!(total, 3);
        assert_eq!(min_id, Some(StreamId::new(1000, 0)));
        assert_eq!(max_id, Some(StreamId::new(1000, 2)));
        assert_eq!(consumers.len(), 2);

        // 详细查询
        let (total, _, _, _, details) = engine
            .xpending(
                "stream",
                "g1",
                Some(StreamId::new(1000, 0)),
                Some(StreamId::new(1000, 2)),
                Some(10),
                Some("c1"),
            )
            .unwrap();
        assert_eq!(total, 3);
        assert_eq!(details.len(), 2);
        assert_eq!(details[0].1, "c1");
    }

    #[test]
    fn test_xinfo() {
        let engine = StorageEngine::new();
        engine
            .xadd(
                "stream",
                "1000-0",
                vec![("f1".to_string(), "v1".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine
            .xadd(
                "stream",
                "1000-1",
                vec![("f2".to_string(), "v2".to_string())],
                false,
                None,
                None,
            )
            .unwrap();
        engine.xgroup_create("stream", "g1", "0-0", false).unwrap();
        engine.xgroup_create("stream", "g2", "$", false).unwrap();

        // XINFO STREAM
        let info = engine.xinfo_stream("stream", false).unwrap().unwrap();
        assert_eq!(info.0, 2); // length
        assert_eq!(info.1, 2); // groups count
        assert_eq!(info.2, StreamId::new(1000, 1)); // last_id

        // XINFO GROUPS
        let groups = engine.xinfo_groups("stream").unwrap();
        assert_eq!(groups.len(), 2);

        // XREADGROUP 创建消费者
        engine
            .xreadgroup(
                "g1",
                "c1",
                &["stream".to_string()],
                &[">".to_string()],
                None,
                false,
            )
            .unwrap();

        // XINFO CONSUMERS
        let consumers = engine.xinfo_consumers("stream", "g1").unwrap();
        assert_eq!(consumers.len(), 1);
        assert_eq!(consumers[0].0, "c1");
        assert_eq!(consumers[0].1, 2); // pel count
    }
}

