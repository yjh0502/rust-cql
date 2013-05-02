extern mod std;
extern mod cql_client (name="cql_client", vers="0.0.1");

use core::rand;
use std::time;

static str_seq:&'static str = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";
static str_seq_len:uint = 26 * 2;

fn rand_str(size: uint) -> ~str {
    let mut bytes:~[u8] = ~[];
    for uint::range(0, size) |_| {
        bytes.push(str_seq[rand::random::<uint>() % str_seq_len]);
    }
    str::from_bytes(bytes)
}

fn time_diff(start: time::Timespec, end: time::Timespec) -> float {
    return (end.sec - start.sec) as float + (end.nsec - start.nsec) as float/1e9f;
}

fn main() {

/*
    let res = cql_client::connect(~"127.0.0.1", 9042, None);
    if res.is_err() {
        io::println(fmt!("%?", res.get_err()));
        fail!(~"Failed to connect");
    }

    let client = res.get();

    let mut res;

    res = client.query(~"create keyspace test with replication = \
        {'CLASS': 'SimpleStrategy', 'replication_factor':1}", cql_client::ConsistencyOne);

    res = client.query(~"create table test.test2 (id text primary key, value float)",
         cql_client::ConsistencyOne);

    res = client.query(~"insert into test.test2(id, pw) values ('asdf', 1.2345)",
         cql_client::ConsistencyOne);

    res = client.query(~"select * from test.test3",
         cql_client::ConsistencyOne);
    io::println(fmt!("%?", res));
    */


}
