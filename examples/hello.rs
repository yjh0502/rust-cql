
extern crate cql;

use cql::CqlResult;

fn run() -> CqlResult<()> {
    let mut client = try!(cql::connect("localhost:9042"));

    let mut res;
    res = try!(client.query("create keyspace rust with replication = \
        {'class': 'SimpleStrategy', 'replication_factor':1}", cql::Consistency::One));
    println!("{:?}", res);

    res = try!(client.query("create table rust.test (id text primary key, value float)",
         cql::Consistency::One));
    println!("{:?}", res);

    res = try!(client.query("insert into rust.test (id, value) values ('asdf', 1.2345)",
         cql::Consistency::One));
    println!("{:?}", res);

    res = try!(client.query("select * from rust.test",
         cql::Consistency::One));
    println!("{:?}", res);

    Ok(())
}

fn main() {
    match run() {
        Ok(()) => {}
        Err(e) => {
            println!("Error while running CQL: {:?}", e);
        }
    }
}
