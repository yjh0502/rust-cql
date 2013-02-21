extern mod std;
extern mod cql_client;

use std::time;
use std::task;

const str_seq:&static/str = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";
const str_seq_len:uint = 26 * 2;

fn rand_str(size: uint) -> ~str {
    let mut bytes:~[u8] = ~[];

    for uint::range(0, size) |i| {
        bytes.push(str_seq[rand::random() % str_seq_len]);
    }
    str::from_bytes(bytes)
}

fn time_diff(start: time::Timespec, end: time::Timespec) -> float {
    return (end.sec - start.sec) as float + (end.nsec - start.nsec) as float/1e9f;
}

fn main() {
    for uint::range(0, 8) |_| {
        do task::spawn {
            let res = cql_client::connect(~"127.0.0.1", 9042, None);
            if res.is_err() {
                io::println(fmt!("%?", res.get_err()));
                fail!(~"Failed to connect");
            }

            let client = res.get();
            let start = time::get_time();
            let queries = 10000;

            for uint::range(0, queries) |_| {
                let id = rand_str(10);
                let pw = rand_str(10);
                let res = client.query(fmt!("insert into test.test (id, pw) values ('%?', '%?')", id, pw),
                     cql_client::ConsistencyOne);
            }

            let end = time::get_time();
            io::println(fmt!("qps: %?",  (queries as float) / time_diff(start, end)));
        }
    }
}

/*
    client.query(~"create keyspace test with replication = \
        {'CLASS': 'SimpleStrategy', 'replication_factor':1}", cql_client::ConsistencyOne);
    client.query(~"create table test.test (id text primary key, pw text)",
         cql_client::ConsistencyOne);
    let res = client.query(~"select * from test.test", 
        cql_client::ConsistencyOne);

    match res.body {
        cql_client::ResultRows(ref rows) => {
            for rows.rows.each |row| {
                io::println(fmt!("%?", row.get_column(~"id")));
            }
        },
        _ => (),
    };
    */


