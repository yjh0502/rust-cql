#![feature(core)]
#![feature(test)]
#![feature(io)]
#![feature(net)]

extern crate num;
extern crate byteorder;
extern crate test;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use std::rc::Rc;
use std::error::FromError;
use std::borrow::ToOwned;
use std::io;
use std::io::{Write,Read};
use std::net::TcpStream;
use std::intrinsics::transmute;
use std::string::FromUtf8Error;

pub static CQL_VERSION:u8 = 0x01;

#[derive(Clone, Debug)]
enum OpcodeReq {
    //requests
    Startup = 0x01,
    Cred = 0x04,
    Opts = 0x05,
    Query = 0x07,
    Prepare = 0x09,
    Register = 0x0B,
}

#[derive(Debug)]
enum OpcodeResp {
    //responces
    Error = 0x00,
    Ready = 0x02,
    Auth = 0x03,
    Supported = 0x06,
    Result = 0x08,
    Exec = 0x0A,
    Event = 0x0C,
}

fn opcode(val: u8) -> OpcodeResp {
    match val {
//        0x01 => Startup,
//        0x04 => Cred,
//        0x05 => Opts,
//        0x07 => Query,
//        0x09 => Prepare,
//        0x0B => Register,

        0x00 => OpcodeResp::Error,
        0x02 => OpcodeResp::Ready,
        0x03 => OpcodeResp::Auth,
        0x06 => OpcodeResp::Supported,
        0x08 => OpcodeResp::Result,
        0x0A => OpcodeResp::Exec,
        0x0C => OpcodeResp::Event,
        _ => OpcodeResp::Error
    }
}

#[derive(Clone, Debug)]
pub enum Consistency {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    Unknown,
}

pub fn consistency(val: u16) -> Consistency {
    match val {
        0 => Consistency::Any,
        1 => Consistency::One,
        2 => Consistency::Two,
        3 => Consistency::Three,
        4 => Consistency::Quorum,
        5 => Consistency::All,
        6 => Consistency::LocalQuorum,
        7 => Consistency::EachQuorum,
        _ => Consistency::Unknown
    }
}

#[derive(Clone, Debug)]
pub enum ColumnType {
    Custom = 0x0000,
    Ascii = 0x0001,
    Bigint = 0x0002,
    Blob = 0x0003,
    Boolean = 0x0004,
    Counter = 0x0005,
    Decimal = 0x0006,
    Double = 0x0007,
    Float = 0x0008,
    Int = 0x0009,
    Text = 0x000A,
    Timestamp = 0x000B,
    UUID = 0x000C,
    VarChar = 0x000D,
    Varint = 0x000E,
    TimeUUID = 0x000F,
    Inet = 0x0010,
    List = 0x0020,
    Map = 0x0021,
    Set = 0x0022,
    Unknown = 0xffff,
}

fn column_type(val: u16) -> ColumnType {
    match val {
        0x0000 => ColumnType::Custom,
        0x0001 => ColumnType::Ascii,
        0x0002 => ColumnType::Bigint,
        0x0003 => ColumnType::Blob,
        0x0004 => ColumnType::Boolean,
        0x0005 => ColumnType::Counter,
        0x0006 => ColumnType::Decimal,
        0x0007 => ColumnType::Double,
        0x0008 => ColumnType::Float,
        0x0009 => ColumnType::Int,
        0x000A => ColumnType::Text,
        0x000B => ColumnType::Timestamp,
        0x000C => ColumnType::UUID,
        0x000D => ColumnType::VarChar,
        0x000E => ColumnType::Varint,
        0x000F => ColumnType::TimeUUID,
        0x0010 => ColumnType::Inet,
        0x0020 => ColumnType::List,
        0x0021 => ColumnType::Map,
        0x0022 => ColumnType::Set,
        _ => ColumnType::Unknown
    }
}

#[derive(Debug)]
pub enum Error {
    UnexpectedEOF,
    Io(io::Error),
    ByteOrder(byteorder::Error),
    Utf8(FromUtf8Error),
}

impl FromError<io::Error> for Error {
    fn from_error(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl FromError<byteorder::Error> for Error {
    fn from_error(err: byteorder::Error) -> Error {
        match err {
            byteorder::Error::UnexpectedEOF => Error::UnexpectedEOF,
            byteorder::Error::Io(e) => Error::Io(e),
        }
    }
}

impl FromError<FromUtf8Error> for Error {
    fn from_error(err: FromUtf8Error) -> Error {
        Error::Utf8(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

trait CqlSerializable {
    fn len(&self) -> usize;
    fn serialize<T: Write>(&self, buf: &mut T) -> Result<()>;
}

trait CqlReader {
    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>>;
    fn read_cql_str(&mut self) -> Result<String>;
    fn read_cql_long_str(&mut self) -> Result<Option<String>>;
    fn read_cql_rows(&mut self) -> Result<Rows>;

    fn read_cql_metadata(&mut self) -> Result<Metadata>;
    fn read_cql_response(&mut self) -> Result<Response>;
}

fn read_full<R: io::Read>(rdr: &mut R, buf: &mut [u8]) -> Result<()> {
    let mut nread = 0usize;
    while nread < buf.len() {
        match try!(rdr.read(&mut buf[nread..])) {
            0 => return Err(Error::UnexpectedEOF),
            n => nread += n,
        }
    }
    Ok(())
}

impl<'a, T: Read> CqlReader for T {
    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        let mut vec = vec![0; len];
        try!(read_full(self, vec.as_mut_slice()));
        Ok(vec)
    }

    fn read_cql_str(&mut self) -> Result<String> {
        let len = try!(self.read_u16::<BigEndian>());
        let bytes = try!(self.read_bytes(len as usize));
        let s = try!(String::from_utf8(bytes));
        Ok(s)
    }

    fn read_cql_long_str(&mut self) -> Result<Option<String>> {
        match try!(self.read_i32::<BigEndian>()) {
            -1 => Ok(None),
            len => {
                let bytes = try!(self.read_bytes(len as usize));
                let s = try!(String::from_utf8(bytes));
                Ok(Some(s))
            }
        }
    }

    fn read_cql_metadata(&mut self) -> Result<Metadata> {
        let flags = try!(self.read_u32::<BigEndian>());
        let column_count = try!(self.read_u32::<BigEndian>());
        let (keyspace, table) =
            if flags == 0x0001 {
                let keyspace_str = try!(self.read_cql_str());
                let table_str = try!(self.read_cql_str());
                (Some(keyspace_str), Some(table_str))
            } else {
                (None, None)
            };

        let mut row_metadata:Vec<CqlColMetadata> = Vec::new();
        for _ in (0 .. column_count) {
            let (keyspace, table) =
                if flags == 0x0001 {
                    (None, None)
                } else {
                    let keyspace_str = try!(self.read_cql_str());
                    let table_str = try!(self.read_cql_str());
                    (Some(keyspace_str), Some(table_str))
                };
            let col_name = try!(self.read_cql_str());
            let type_key = try!(self.read_u16::<BigEndian>());
            let type_name =
                if type_key >= 0x20 {
                    column_type(try!(self.read_u16::<BigEndian>()))
                } else {
                    ColumnType::Unknown
                };

            row_metadata.push(CqlColMetadata {
                keyspace: keyspace,
                table: table,
                col_name: col_name,
                col_type: column_type(type_key),
                col_type_name: type_name
            });
        }

        Ok(Metadata {
            flags: flags,
            column_count: column_count,
            keyspace: keyspace,
            table: table,
            row_metadata: row_metadata,
        })
    }

    fn read_cql_rows(&mut self) -> Result<Rows> {
        let metadata = Rc::new(try!(self.read_cql_metadata()));
        let rows_count = try!(self.read_u32::<BigEndian>());

        let mut rows:Vec<Row> = Vec::new();
        for _ in (0 .. rows_count) {
            let mut row = Row{ cols: Vec::new(), metadata: metadata.clone() };
            for meta in row.metadata.row_metadata.iter() {
                let col = match meta.col_type.clone() {
                    ColumnType::Ascii => Cql::CqlString(try!(self.read_cql_long_str())),
                    ColumnType::VarChar => Cql::CqlString(try!(self.read_cql_long_str())),
                    ColumnType::Text => Cql::CqlString(try!(self.read_cql_long_str())),

                    ColumnType::Int => Cql::Cqli32(match try!(self.read_i32::<BigEndian>()) {
                            -1 => None,
                            4 => Some(try!(self.read_i32::<BigEndian>())),
                            len => panic!("Invalid length with i32: {}", len),
                        }),
                    ColumnType::Bigint => Cql::Cqli64(Some(try!(self.read_i64::<BigEndian>()))),
                    ColumnType::Float => Cql::Cqlf32(unsafe{
                        match try!(self.read_i32::<BigEndian>()) {
                            -1 => None,
                            4 => Some(transmute(try!(self.read_u32::<BigEndian>()))),
                            len => panic!("Invalid length with f32: {}", len),
                        }
                    }),
                    ColumnType::Double => Cql::Cqlf64(unsafe{
                        match try!(self.read_i32::<BigEndian>()) {
                            -1 => None,
                            4 => Some(transmute(try!(self.read_u64::<BigEndian>()))),
                            len => panic!("Invalid length with f64: {}", len),
                        }
                    }),

                    ColumnType::List => Cql::CqlList({
                        match try!(self.read_i32::<BigEndian>()) {
                            -1 => None,
                            _ => {
                                //let data = self.read_bytes(len as usize);
                                panic!("List parse not implemented: {}");
                            },
                        }
                    }),


//                    Custom => ,
//                    Blob => ,
//                    Boolean => ,
//                    Counter => ,
//                    Decimal => ,
//                    Timestamp => ,
//                    UUID => ,
//                    Varint => ,
//                    TimeUUID => ,
//                    Inet => ,
//                    List => ,
//                    Map => ,
//                    Set => ,

                    _ => {
                        match try!(self.read_i32::<BigEndian>()) {
                            -1 => (),
                            len => { try!(self.read_bytes(len as usize)); },
                        }
                        Cql::CqlUnknown
                    }
                };

                row.cols.push(col);
            }
            rows.push(row);
        }

        Ok(Rows {
            metadata: metadata,
            rows: rows,
        })
    }

    fn read_cql_response(&mut self) -> Result<Response> {
        let header_data = try!(self.read_bytes(8));

        let version = header_data[0];
        let flags = header_data[1];
        let stream = header_data[2] as i8;
        let opcode = opcode(header_data[3]);
        let length = ((header_data[4] as u32) << 24u32) +
            ((header_data[5] as u32) << 16u32) +
            ((header_data[6] as u32) << 8u32) +
            (header_data[7] as u32);

        let body_data = try!(self.read_bytes(length as usize));
        let mut reader = io::BufReader::new(body_data.as_slice());

        let body = match opcode {
            OpcodeResp::Ready => ResponseBody::Ready,
            OpcodeResp::Auth => {
                ResponseBody::Auth(try!(reader.read_cql_str()))
            }
            OpcodeResp::Error => {
                let code = try!(reader.read_u32::<BigEndian>());
                let msg = try!(reader.read_cql_str());
                ResponseBody::Error(code, msg)
            },
            OpcodeResp::Result => {
                let code = try!(reader.read_u32::<BigEndian>());
                match code {
                    0x0001 => {
                        ResponseBody::Void
                    },
                    0x0002 => {
                        ResponseBody::Rows(try!(reader.read_cql_rows()))
                    },
                    0x0003 => {
                        let msg = try!(reader.read_cql_str());
                        ResponseBody::Keyspace(msg)
                    },
                    0x0004 => {
                        let id = try!(reader.read_u8());
                        let metadata = try!(reader.read_cql_metadata());
                        ResponseBody::Prepared(id, metadata)
                    },
                    0x0005 => {
                        let change  = try!(reader.read_cql_str());
                        let keyspace = try!(reader.read_cql_str());
                        let table = try!(reader.read_cql_str());
                        ResponseBody::SchemaChange(change, keyspace, table)
                    },
                    _ => {
                        panic!("Unknown code for result: {}", code);
                    },
                }
            }
            _ => {
                panic!("Invalid response from server");
            },
        };

/*
        if reader.pos != length as usize {
            panic!("Data is not fully readed: specificatold_ion might be changed {} != {}",
                reader.pos, length);
        }
        */

        Ok(Response {
            version: version,
            flags: flags,
            stream: stream,
            opcode: opcode,
            body: body,
        })
    }
}

#[derive(Debug)]
struct Pair {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl CqlSerializable for Pair {
    fn serialize<T: Write>(&self, buf: &mut T) -> Result<()> {
        try!(buf.write_u16::<BigEndian>(self.key.len() as u16));
        try!(buf.write_all(self.key.as_slice()));
        try!(buf.write_u16::<BigEndian>(self.value.len() as u16));
        try!(buf.write_all(self.value.as_slice()));
        Ok(())
    }

    fn len(&self) -> usize {
        return 4 + self.key.len() + self.value.len();
    }
}

#[derive(Debug)]
pub struct StringMap {
    pairs: Vec<Pair>,
}

impl CqlSerializable for StringMap {
    fn serialize<T: Write>(&self, buf: &mut T) -> Result<()> {
        try!(buf.write_u16::<BigEndian>(self.pairs.len() as u16));
        for pair in self.pairs.iter() {
            try!(pair.serialize(buf));
        }
        Ok(())
    }

    fn len(&self) -> usize {
        let mut len = 2usize;
        for pair in self.pairs.iter() {
            len += pair.len();
        }
        len
    }
}

#[derive(Debug)]
struct CqlColMetadata {
    keyspace: Option<String>,
    table: Option<String>,
    col_name: String,
    col_type: ColumnType,
    col_type_name: ColumnType,
}

#[derive(Debug)]
pub struct Metadata {
    flags: u32,
    column_count: u32,
    keyspace: Option<String>,
    table: Option<String>,
    row_metadata: Vec<CqlColMetadata>,
}

#[derive(Clone, Debug)]
pub enum Cql {
    CqlString(Option<String>),

    Cqli32(Option<i32>),
    Cqli64(Option<i64>),

    CqlBlob(Option<Vec<u8>>),
    CqlBool(Option<bool>),

    CqlCounter(Option<u64>),

    Cqlf32(Option<f32>),
    Cqlf64(Option<f64>),

    CqlTimestamp(u64),
    CqlBigint(num::BigInt),

    CqlList(Option<Vec<Cql>>),

    CqlUnknown,
}

#[derive(Debug)]
pub struct Row {
    cols: Vec<Cql>,
    metadata: Rc<Metadata>,
}

impl Row {
    pub fn get_column(&self, col_name: &str) -> Option<Cql> {
        let mut i = 0;
        for metadata in self.metadata.row_metadata.iter() {
            if metadata.col_name == col_name {
                return Some(self.cols[i].clone());
            }
            i += 1;
        }
        None
    }
}

#[derive(Debug)]
pub struct Rows {
    metadata: Rc<Metadata>,
    rows: Vec<Row>,
}

#[derive(Debug)]
pub enum RequestBody {
    RequestStartup(StringMap),
    RequestCred(Vec<Vec<u8>>),
    RequestQuery(String, Consistency),
    RequestOptions,
}

#[derive(Debug)]
pub enum ResponseBody {
    Error(u32, String),
    Ready,
    Auth(String),

    Void,
    Rows(Rows),
    Keyspace(String),
    Prepared(u8, Metadata),
    SchemaChange(String, String, String),
}

#[derive(Debug)]
struct Request {
    version: u8,
    flags: u8,
    stream: i8,
    opcode: OpcodeReq,
    body: RequestBody,
}

#[derive(Debug)]
pub struct Response {
    version: u8,
    flags: u8,
    stream: i8,
    opcode: OpcodeResp,
    body: ResponseBody,
}

impl CqlSerializable for Request {
    fn serialize<T: Write>(&self, buf: &mut T) -> Result<()> {
        try!(buf.write_u8(self.version));
        try!(buf.write_u8(self.flags));
        try!(buf.write_i8(self.stream));
        try!(buf.write_u8(self.opcode.clone() as u8));
        try!(buf.write_u32::<BigEndian>((self.len()-8) as u32));

        match self.body {
            RequestBody::RequestStartup(ref map) => {
                map.serialize(buf)
            },
            RequestBody::RequestQuery(ref query_str, ref consistency) => {
                try!(buf.write_u32::<BigEndian>(query_str.len() as u32));
                try!(buf.write_all(query_str.as_bytes()));
                try!(buf.write_u16::<BigEndian>(consistency.clone() as u16));
                Ok(())
            },
            _ => panic!("not implemented request type"),
        }
    }
    fn len(&self) -> usize {
        8 + match self.body {
            RequestBody::RequestStartup(ref map) => {
                map.len()
            },
            RequestBody::RequestQuery(ref query_str, _) => {
                4 + query_str.len() + 2
            },
            _ => panic!("not implemented request type"),
        }
    }
}

fn startup() -> Request {
    let body = StringMap {
            pairs:vec![Pair{key: b"CQL_VERSION".to_owned(), value: b"3.0.0".to_owned()}],
        };
    return Request {
        version: CQL_VERSION,
        flags: 0x00,
        stream: 0x01,
        opcode: OpcodeReq::Startup,
        body: RequestBody::RequestStartup(body),
    };
}

fn auth(creds: Vec<Vec<u8>>) -> Request {
    return Request {
        version: CQL_VERSION,
        flags: 0x00,
        stream: 0x01,
        opcode: OpcodeReq::Cred,
        body: RequestBody::RequestCred(creds),
    };
}

fn options() -> Request {
    return Request {
        version: CQL_VERSION,
        flags: 0x00,
        stream: 0x01,
        opcode: OpcodeReq::Opts,
        body: RequestBody::RequestOptions,
    };
}

fn query(stream: i8, query_str: &str, con: Consistency) -> Request {
    return Request {
        version: CQL_VERSION,
        flags: 0x00,
        stream: stream,
        opcode: OpcodeReq::Query,
        body: RequestBody::RequestQuery(query_str.to_string(), con),
    };
}

pub struct Client {
    socket: TcpStream,
}

impl Client {
    pub fn query(&mut self, query_str: &str, con: Consistency) -> Result<Response> {
        let q = query(0, query_str, con);

        let mut writer = Vec::new();

        try!(q.serialize::<Vec<u8>>(&mut writer));
        try!(self.socket.write_all(writer.as_slice()));
        let resp = try!(self.socket.read_cql_response());
        Ok(resp)
    }
}

pub fn connect(addr: &str) -> Result<Client> {
    let mut socket = try!(TcpStream::connect(addr));
    let msg_startup = startup();

    let mut buf = Vec::new();
    try!(msg_startup.serialize::<Vec<u8>>(&mut buf));
    try!(socket.write_all(buf.as_slice()));

    let response = try!(socket.read_cql_response());
    match response.body {
        ResponseBody::Ready => {
            Ok(Client { socket: socket })
        },
        /*
        Auth(_) => {
            match(creds) {
                Some(cred) => {
                    let msg_auth = Auth(cred);
                    msg_auth.serialize::<net_tcp::TcpSocketBuf>(&buf);
                    let response = buf.read_cql_response();
                    match response.body {
                        Ready => result::Ok(Client { socket: buf }),
                        Error(_, ref msg) => {
                            result::Err(Error(~"Error", copy *msg))
                        }
                        _ => {
                            result::Err(Error(~"Error", ~"Server returned unknown message"))
                        },
                    }
                },
                None => {
                    result::Err(Error(~"Error", ~"Credential should be provided"))
                },
            }

        }
        */
        _ => panic!("invalid opcode: {}", response.opcode as u8)
    }
}

#[cfg(test)]
mod tests {
    use super::CqlReader;
    use test::Bencher;

    #[test]
    fn resp_ready() {
        let v = vec![129, 0, 0, 0, 0, 0, 0, 49, 0, 0, 36, 0, 0, 35, 67, 97, 110, 110, 111, 116, 32, 97, 100, 100, 32, 101, 120, 105, 115, 116, 105, 110, 103, 32, 107, 101, 121, 115, 112, 97, 99, 101, 32, 34, 114, 117, 115, 116, 34, 0, 4, 114, 117, 115, 116, 0, 0];
        let mut s = v.as_slice();
        let resp = s.read_cql_response();
        assert!(resp.is_ok())
    }

    #[test]
    fn resp_error() {
        let v = vec![129, 0, 0, 0, 0, 0, 0, 85, 0, 0, 36, 0, 0, 67, 67, 97, 110, 110, 111, 116, 32, 97, 100, 100, 32, 97, 108, 114, 101, 97, 100, 121, 32, 101, 120, 105, 115, 116, 105, 110, 103, 32, 99, 111, 108, 117, 109, 110, 32, 102, 97, 109, 105, 108, 121, 32, 34, 116, 101, 115, 116, 34, 32, 116, 111, 32, 107, 101, 121, 115, 112, 97, 99, 101, 32, 34, 114, 117, 115, 116, 34, 0, 4, 114, 117, 115, 116, 0, 4, 116, 101, 115, 116];
        let mut s = v.as_slice();
        let resp = s.read_cql_response();
        assert!(resp.is_ok())
    }

    #[test]
    fn resp_result() {
        let v = vec![129, 0, 0, 8, 0, 0, 0, 59, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 4, 114, 117, 115, 116, 0, 4, 116, 101, 115, 116, 0, 2, 105, 100, 0, 13, 0, 5, 118, 97, 108, 117, 101, 0, 8, 0, 0, 0, 1, 0, 0, 0, 4, 97, 115, 100, 102, 0, 0, 0, 4, 63, 158, 4, 25];
        let mut s = v.as_slice();
        let resp = s.read_cql_response();
        assert!(resp.is_ok())
    }

    #[bench]
    fn bench_decode(b: &mut Bencher) {
        let v = vec![129, 0, 0, 8, 0, 0, 0, 59, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 4, 114, 117, 115, 116, 0, 4, 116, 101, 115, 116, 0, 2, 105, 100, 0, 13, 0, 5, 118, 97, 108, 117, 101, 0, 8, 0, 0, 0, 1, 0, 0, 0, 4, 97, 115, 100, 102, 0, 0, 0, 4, 63, 158, 4, 25];
        b.bytes = v.len() as u64;
        b.iter(|| {
            let mut s = v.as_slice();
            let resp = s.read_cql_response();
            assert!(resp.is_ok())
        });
    }
}
