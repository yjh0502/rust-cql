extern mod std;
extern mod cql_client;


fn main() {
    let res = cql_client::connect(~"127.0.0.1", 9042, None);
    if res.is_err() {
        io::println(fmt!("%?", res.get_err()));
        return;
    }

    let client = res.get();

    let res = client.query(~"create keyspace test with replication = {'CLASS': 'SimpleStrategy', 'replication_factor':1}", cql_client::ConsistencyOne);
    io::println(fmt!("%?", res));

    let res = client.query(~"select id, email from test.test", cql_client::ConsistencyOne);
    let msg = match copy res.body {
        cql_client::ResponseEmpty() => ~"empty",
        cql_client::ResultRows(rows) => ~"rows",
        _ => ~"etc",
    };

    io::println(fmt!("%?", msg));

    io::println(fmt!("%?", res));
}
