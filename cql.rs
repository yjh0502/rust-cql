extern mod std;
extern mod cql_client;


fn main() {
    let res = cql_client::connect(~"127.0.0.1", 9042, None);
    if res.is_err() {
        io::println(fmt!("%?", res.get_err()));
        return;
    }

    let client = res.get();

    client.query(~"create keyspace test with replication = \
        {'CLASS': 'SimpleStrategy', 'replication_factor':1}", cql_client::ConsistencyOne);
    client.query(~"create table test.test (id text primary key, pw text)",
         cql_client::ConsistencyOne);
    client.query(~"insert into test.test (id, pw) values ('adsf', 'asdf')",
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
}
