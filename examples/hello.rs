extern crate cql;

fn run() -> cql::Result<()> {
    let mut client = cql::Client::new("localhost:9042")?;

    eprintln!("ready");

    let res = client.options();
    println!("options: {:?}", res);

    let res = client.query(
        "create keyspace rust with replication = \
         {'class': 'SimpleStrategy', 'replication_factor':1}",
        cql::Consistency::One,
        Vec::new(),
    )?;
    println!("create: {:?}", res);

    let res = client.query(
        "create table rust.test (v1 text primary key, v2 float, v3 list<boolean>, v4 varint)",
        cql::Consistency::One,
        Vec::new(),
    )?;
    println!("create table: {:?}", res);

    let prepare_id =
        client.prepare("insert into rust.test (v1, v2, v3, v4) values ('asdf', ?, ?, ?)")?;
    println!("prepare: {:?}", prepare_id);

    let res = client.execute(
        prepare_id,
        cql::Consistency::One,
        vec![
            cql::Value::CqlFloat(1.2345),
            cql::Value::CqlList(vec![cql::Value::CqlBoolean(false)]),
            cql::Value::CqlVarInt(123),
        ],
    )?;
    println!("execute: {:?}", res);

    let res = client.query("select * from rust.test", cql::Consistency::One, Vec::new())?;
    println!("select: {:?}", res);

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
