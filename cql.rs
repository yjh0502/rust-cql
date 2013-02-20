extern mod std;

use core::result;
use core::str;
use core::vec;
use core::io;
use core::io::ReaderUtil;
use core::io::WriterUtil;
use core::task;
use core::pipes;

use std::net_ip;
use std::net_tcp;
use std::net_tcp::TcpSocketBuf;
use std::uv_global_loop;
use std::uv_iotask::IoTask;
use std::bigint;

const CQL_VERSION:u8 = 0x01;

enum cql_opcode {
    OPCODE_ERROR = 0x00,
    OPCODE_STARTUP = 0x01,
    OPCODE_READY = 0x02,
    OPCODE_AUTHENTICATE = 0x03,
    OPCODE_CREDENTIALS = 0x04,
    OPCODE_OPTIONS = 0x05,
    OPCODE_SUPPORTED = 0x06,
    OPCODE_QUERY = 0x07,
    OPCODE_RESULT = 0x08,
    OPCODE_PREPARE = 0x09,
    OPCODE_EXECUTE = 0x0A,
    OPCODE_REGISTER = 0x0B,
    OPCODE_EVENT = 0x0C,
    OPCODE_UNKNOWN
}

fn cql_opcode(val: u8) -> cql_opcode {
    match val {
        0x00 => OPCODE_ERROR,
        0x01 => OPCODE_STARTUP,
        0x02 => OPCODE_READY,
        0x03 => OPCODE_AUTHENTICATE,
        0x04 => OPCODE_CREDENTIALS,
        0x05 => OPCODE_OPTIONS, 
        0x06 => OPCODE_SUPPORTED,
        0x07 => OPCODE_QUERY,
        0x08 => OPCODE_RESULT,
        0x09 => OPCODE_PREPARE,
        0x0A => OPCODE_EXECUTE,
        0x0B => OPCODE_REGISTER,
        0x0C => OPCODE_EVENT,
        _ => OPCODE_UNKNOWN
    }
}

enum cql_consistency {
    CONSISTENCY_ANY = 0x0000,
    CONSISTENCY_ONE = 0x0001,
    CONSISTENCY_TWO = 0x0002,
    CONSISTENCY_THREE = 0x0003,
    CONSISTENCY_QUORUM = 0x0004,
    CONSISTENCY_ALL = 0x0005,
    CONSISTENCY_LOCAL_QUORUM = 0x0006,
    CONSISTENCY_EACH_QUORUM = 0x0007,
    CONSISTENCY_UNKNOWN,
}

fn cql_consistency(val: u16) -> cql_consistency {
    match val {
        0 => CONSISTENCY_ANY,
        1 => CONSISTENCY_ONE,
        2 => CONSISTENCY_TWO,
        3 => CONSISTENCY_THREE,
        4 => CONSISTENCY_QUORUM,
        5 => CONSISTENCY_ALL,
        6 => CONSISTENCY_LOCAL_QUORUM,
        7 => CONSISTENCY_EACH_QUORUM,
        _ => CONSISTENCY_UNKNOWN
    }
}

enum cql_column_type {
    COLUMN_CUSTOM = 0x0000,
    COLUMN_ASCII = 0x0001,
    COLUMN_BIGINT = 0x0002,
    COLUMN_BLOB = 0x0003,
    COLUMN_BOOLEAN = 0x0004,
    COLUMN_COUNTER = 0x0005,
    COLUMN_DECIMAL = 0x0006,
    COLUMN_DOUBLE = 0x0007,
    COLUMN_FLOAT = 0x0008,
    COLUMN_INT = 0x0009,
    COLUMN_TEXT = 0x000A,
    COLUMN_TIMESTAMP = 0x000B,
    COLUMN_UUID = 0x000C,
    COLUMN_VARCHAR = 0x000D,
    COLUMN_VARINT = 0x000E,
    COLUMN_TIMEUUID = 0x000F,
    COLUMN_INET = 0x0010,
    COLUMN_LIST = 0x0020,
    COLUMN_MAP = 0x0021,
    COLUMN_SET = 0x0022,
    COLUMN_UNKNOWN,
}

fn cql_column_type(val: u16) -> cql_column_type {
    match val {
        0x0000 => COLUMN_CUSTOM,
        0x0001 => COLUMN_ASCII,
        0x0002 => COLUMN_BIGINT,
        0x0003 => COLUMN_BLOB,
        0x0004 => COLUMN_BOOLEAN,
        0x0005 => COLUMN_COUNTER,
        0x0006 => COLUMN_DECIMAL,
        0x0007 => COLUMN_DOUBLE,
        0x0008 => COLUMN_FLOAT,
        0x0009 => COLUMN_INT,
        0x000A => COLUMN_TEXT,
        0x000B => COLUMN_TIMESTAMP,
        0x000C => COLUMN_UUID,
        0x000D => COLUMN_VARCHAR,
        0x000E => COLUMN_VARINT,
        0x000F => COLUMN_TIMEUUID,
        0x0010 => COLUMN_INET,
        0x0020 => COLUMN_LIST,
        0x0021 => COLUMN_MAP,
        0x0022 => COLUMN_SET,
        _ => COLUMN_UNKNOWN
    }
}

struct cql_err {
    err_name: ~str,
    err_msg: ~str,
}

fn cql_err(name: ~str, msg: ~str) -> cql_err {
    return cql_err{err_name: name, err_msg: msg};
}


trait CqlSerializable {
    fn len(&self) -> uint;
    fn serialize<T: io::Writer>(&self, buf: &T);
}

trait CqlReader {
    fn read_cql_str(&self) -> ~str;
    fn read_cql_long_str(&self) -> ~str;
    fn read_cql_rows(&self) -> cql_rows;
    fn read_cql_message(&self) -> cql_message;
}

impl<T: ReaderUtil> CqlReader for T {
    fn read_cql_str(&self) -> ~str {
        let len = self.read_be_u16() as uint;
        str::from_bytes(self.read_bytes(len))
    }

    fn read_cql_long_str(&self) -> ~str {
        let len = self.read_be_u32() as uint;
        str::from_bytes(self.read_bytes(len))
    }

    fn read_cql_rows(&self) -> cql_rows {
        let flags = self.read_be_u32();
        let column_count = self.read_be_u32();
        let (keyspace, table) = 
            if flags == 0x0001 {
                let keyspace_str = self.read_cql_str();
                let table_str = self.read_cql_str();
                (keyspace_str, table_str)
            } else {
                (~"", ~"")
            };

        let mut row_metadata:~[cql_row_metadata] = ~[];
        for u32::range(0, column_count) |_| {
            let (keyspace, table) = 
                if flags == 0x0001 {
                    (~"", ~"")
                } else {
                    let keyspace_str = self.read_cql_str();
                    let table_str = self.read_cql_str();
                    (keyspace_str, table_str)
                };
            let col_name = self.read_cql_str();
            let type_key = self.read_be_u16();
            let type_name = 
                if type_key >= 0x20 {
                    self.read_cql_str()
                } else {
                    ~""
                };

            row_metadata.push(cql_row_metadata {
                keyspace: keyspace,
                table: table,
                col_name: col_name,
                col_type: cql_column_type(type_key),
                col_type_name: type_name
            });
        }

        let rows_count = self.read_be_u32();

        let mut rows:~[cql_row] = ~[];
        for u32::range(0, rows_count) |_| {
            let mut row: cql_row = cql_row{ cols: ~[] };
            for row_metadata.each |meta| {
                let col = match meta.col_type {
                    COLUMN_ASCII => cql_string(self.read_cql_long_str()),
                    COLUMN_VARCHAR => cql_string(self.read_cql_long_str()),
                    COLUMN_TEXT => cql_string(self.read_cql_long_str()),

                    COLUMN_INT => cql_i32(self.read_be_i32()),
                    COLUMN_BIGINT => cql_i64(self.read_be_i64()),
                    COLUMN_FLOAT => cql_f32(self.read_be_u32() as f32),
                    COLUMN_DOUBLE => cql_f64(self.read_be_u64() as f64),

    /*
                    COLUMN_CUSTOM => ,
                    COLUMN_BLOB => ,
                    COLUMN_BOOLEAN => ,
                    COLUMN_COUNTER => ,
                    COLUMN_DECIMAL => ,
                    COLUMN_TIMESTAMP => ,
                    COLUMN_UUID => ,
                    COLUMN_VARINT => ,
                    COLUMN_TIMEUUID => ,
                    COLUMN_INET => ,
                    COLUMN_LIST => ,
                    COLUMN_MAP => ,
                    COLUMN_SET => ,
                    */
                    _ => cql_i32(0),
                };

                row.cols.push(col);
            }
            rows.push(row);
        }

        cql_rows {
            flags: flags,
            column_count: column_count,
            keyspace: keyspace,
            table: table,
            row_metadata: row_metadata,
            rows_count: rows_count,
            rows: rows,
        }
    }

    fn read_cql_message(&self) -> cql_message {
        let version = self.read_u8();
        let flags = self.read_u8();
        let stream = self.read_i8();
        let opcode:cql_opcode = cql_opcode(self.read_u8());
        self.read_be_u32();

        let payload = match opcode {
            OPCODE_READY => empty,
            OPCODE_ERROR => {
                let code = self.read_be_u32();
                let msg = self.read_cql_str();
                error(code, msg)
            },
            OPCODE_RESULT => {
                let code = self.read_be_u32();
                match code {
                    0x0001 => {
                        result_void
                    },
                    0x0002 => {
                        result_rows(self.read_cql_rows())
                    },
                    0x0003 => {
                        let msg = self.read_cql_str();
                        result_keyspace(msg)
                    },
                    _ => empty
                }
            }
            _ => empty,
        };


        return cql_message {
            version: version,
            flags: flags,
            stream: stream,
            opcode: opcode,
            payload: payload,
        };
    }
}

struct pair {
    key: ~str,
    value: ~str,
}

impl CqlSerializable for pair {
    fn serialize<T: io::Writer>(&self, buf: &T) {
        buf.write_be_u16(self.key.len() as u16);
        buf.write(str::to_bytes(self.key));
        buf.write_be_u16(self.value.len() as u16);
        buf.write(str::to_bytes(self.value));
    }
    
    fn len(&self) -> uint {
        return 4 + self.key.len() + self.value.len();
    }
}

struct string_map {
    pairs: ~[pair],
}

impl CqlSerializable for string_map {
    fn serialize<T: io::Writer>(&self, buf: &T) {
        buf.write_be_u16(self.pairs.len() as u16);
        for self.pairs.each |pair| {
            pair.serialize(buf);
        }
    }
    
    fn len(&self) -> uint {
        let mut len = 2u;
        for self.pairs.each |pair| {
            len += pair.len();
        }
        len
    }
}

struct cql_row_metadata {
    keyspace: ~str,
    table: ~str,
    col_name: ~str,
    col_type: cql_column_type,
    col_type_name: ~str,
}

enum cql_col {
    cql_string(~str),

    cql_i32(i32),
    cql_i64(i64),

    cql_blob(~[u8]),
    cql_bool(bool),

    cql_counter(u64),

    cql_f32(f32),
    cql_f64(f64),

    cql_timestamp(u64),
    cql_bigint(bigint::BigInt),
}

struct cql_row {
    cols: ~[cql_col],
}

struct cql_rows {
    flags: u32,
    column_count: u32,
    keyspace: ~str,
    table: ~str,
    row_metadata: ~[cql_row_metadata],
    rows_count: u32,
    rows: ~[cql_row],
}

enum cql_payload {
    startup(string_map),
    query(~str, cql_consistency),
    error(u32, ~str),

    result_void(),
    result_rows(cql_rows),
    result_keyspace(~str),
    result_schema_change(),

    empty(),
}

struct cql_message {
    version: u8,
    flags: u8,
    stream: i8,
    opcode: cql_opcode,
    payload: cql_payload,
}

impl CqlSerializable for cql_message {
    fn serialize<T: io::Writer>(&self, buf: &T) {
        buf.write_u8(self.version);
        buf.write_u8(self.flags);
        buf.write_i8(self.stream);
        buf.write_u8(self.opcode as u8);
        buf.write_be_u32((self.len()-8) as u32);

        match copy self.payload {
            startup(map) => {
                map.serialize(buf)
            },
            query(query_str, consistency) => {
                buf.write_be_u32(query_str.len() as u32);
                buf.write(str::to_bytes(query_str));
                buf.write_be_u16(consistency as u16);
            },
            _ => (),
        }
    }
    fn len(&self) -> uint {
        8 + match copy self.payload {
            startup(map) => {
                map.len()
            },
            query(query_str, _) => {
                4 + query_str.len() + 2
            },
            _ => {
                0
            }
        }
    }
}


fn Options() -> cql_message {
    return cql_message {
        version: CQL_VERSION,
        flags: 0x00,
        stream: 0x01,
        opcode: OPCODE_OPTIONS,
        payload: empty,
    };
}

fn Startup() -> cql_message {
    let payload = string_map {
            pairs:~[pair{key: ~"CQL_VERSION", value: ~"3.0.0"}],
        };
    return cql_message {
        version: CQL_VERSION,
        flags: 0x00,
        stream: 0x01,
        opcode: OPCODE_STARTUP,
        payload: startup(payload),
    };
}

fn Query(stream: i8, query_str: ~str, con: cql_consistency) -> cql_message {
    return cql_message {
        version: CQL_VERSION,
        flags: 0x00,
        stream: stream,
        opcode: OPCODE_QUERY,
        payload: query(query_str, con),
    };
}

struct cql_loop {
    socket: @net_tcp::TcpSocketBuf,
    mut tasks: [Option<cql_task>*128],
    port: pipes::Port<cql_message>,
}

struct cql_task {
    cb: @fn(cql_message),
}

impl cql_loop {
    fn get_empty_stream(&self, cb: @fn(cql_message)) -> i8 {
        for i8::range(0, 128) |i| {
            match self.tasks[i] {
                None => { 
                    self.tasks[i] = Some(cql_task{cb: cb});
                    return i; 
                },
                _ => ()
            }
        }
        return -1;
    }

    fn query(&self, query_str: ~str, con: cql_consistency, 
            f: @fn(cql_message)) {
        let stream = self.get_empty_stream(f);
        let q = Query(stream, query_str, con);

        q.serialize::<net_tcp::TcpSocketBuf>(self.socket);
        /*
        let msg = (*self.socket).read_cql_message();
        io::println(fmt!("%?", msg));
        */
    }

    fn start(&self) {
        loop {
            /*
            let one = port.try_recv();
            match one {
                Some(msg) => {
                    io::println(fmt!("%?", msg));
                }
                None => {
                    io::println("Port closed");
                    break;
                }
            }
            let stream = msg.stream;
            match self.tasks[stream] {
                Some(task) => {
                    self.tasks[stream] = None;
                    let cb = task.cb;
                    cb(msg);
                }
                _ => {
                    io::println(fmt!("Invalid stream id: %?", stream));
                }
            }
            */
        }
    }
}

fn get_sock(ip: ~str, port: uint) -> result::Result<net_tcp::TcpSocket, cql_err> {
    let task = @uv_global_loop::get();
    let addr = net_ip::v4::parse_addr(ip);

    let res = net_tcp::connect(addr, port, task);
    if(res.is_err()) {
        return result::Err(cql_err(~"Error", ~"Failed to connect to server"));
    }

    result::Ok(res.unwrap())
}

struct cql_client {
    chan: pipes::Chan<(~cql_message, ~fn(cql_message))>,
}

impl cql_client {
    fn send_msg(&self, msg: ~cql_message,  cb: ~fn(cql_message)) {
        self.chan.send((msg, cb));
    }
}


fn cql_loop_create(ip: ~str, port: uint) -> result::Result<~cql_loop, cql_err> {
    //let (p, c) = pipes::stream::<~cql_message>();
    let res = get_sock(ip, port);
    if res.is_err() {
        return result::Err(res.get_err());
    }
    let socket = res.unwrap();

    let sock2 = copy socket;
    /*
    let reader = net_tcp::socket_buf(socket);
    let writer = copy reader;


    let msg_startup = Startup();
    msg_startup.serialize::<net_tcp::TcpSocketBuf>(&writer);


    let x = ~reader;
    do task::spawn {
        //loop {
            let y = copy x;
            let r = copy reader;
            let msg = (*r).read_cql_message();
            io::println(fmt!("%?", msg));
            let s = socket;
            let msg = s.read_cql_message();
            io::println(fmt!("%?", msg));
            cql_send_msg::client::send(port, msg, |m| {
            });
        //}
    }
    */

/*
    let client = ~cql_loop {
        socket: @(*buf), 
        tasks: [None, ..128],
        port: port,
    };
    */

    //result::Ok(client)
    result::Err(cql_err(~"", ~""))
/*
    let response = (*buf).read_cql_message();
    match response.opcode {
        OPCODE_READY => {
            let (port, chan) = pipes::stream::<cql_message>();
            do task::spawn {
                loop {
                    let ch = chan;
                    let msg = (*buf).read_cql_message();
                    ch.send(msg);
                }
            }

            let client = ~cql_loop {
                socket: buf, 
                tasks: [None, ..128],
                port: port,
            };

            result::Ok(client)
        },
        _ => result::Err(cql_err(fmt!("Invalid opcode: %?", response.opcode), ~""))
    }
    */

}

fn main() {
    let res = ~cql_loop_create(~"127.0.0.1", 9042);
    if res.is_err() {
        io::println(fmt!("%?", res.get_err()));
        return;
    }

    /*
    let client = res.get_ref();
    //client.query(~"use test");
    client.query(~"select id, email from test.test", 
            CONSISTENCY_ONE, |res| {
        io::println(fmt!("%?", res));
    });
    */
}
