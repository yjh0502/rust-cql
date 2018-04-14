extern crate byteorder;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::intrinsics::transmute;
use std::io;
use std::io::Write;
use std::net::TcpStream;
use std::rc::Rc;
use std::string::FromUtf8Error;

pub static CQL_VERSION: u8 = 0x03;

#[derive(Clone, Copy, Debug)]
enum Opcode {
    // req
    Startup = 0x01,
    Cred = 0x04,
    Opts = 0x05,
    Query = 0x07,
    Prepare = 0x09,
    Register = 0x0B,

    // resp
    Error = 0x00,
    Ready = 0x02,
    Auth = 0x03,
    Supported = 0x06,
    Result = 0x08,
    Exec = 0x0A,
    Event = 0x0C,
}

fn opcode(val: u8) -> Opcode {
    use Opcode::*;
    match val {
        // req
        0x01 => Startup,
        0x04 => Cred,
        0x05 => Opts,
        0x07 => Query,
        0x09 => Prepare,
        0x0B => Register,

        // resp
        0x00 => Error,
        0x02 => Ready,
        0x03 => Auth,
        0x06 => Supported,
        0x08 => Result,
        0x0A => Exec,
        0x0C => Event,
        _ => Error,
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
    use Consistency::*;
    match val {
        0 => Any,
        1 => One,
        2 => Two,
        3 => Three,
        4 => Quorum,
        5 => All,
        6 => LocalQuorum,
        7 => EachQuorum,
        _ => Unknown,
    }
}

#[derive(Clone, Copy, Debug)]
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
    UDT = 0x0030,
    Tuple = 0x0031,
    Unknown = 0xffff,
}

fn column_type(val: u16) -> ColumnType {
    use ColumnType::*;

    match val {
        0x0000 => Custom,
        0x0001 => Ascii,
        0x0002 => Bigint,
        0x0003 => Blob,
        0x0004 => Boolean,
        0x0005 => Counter,
        0x0006 => Decimal,
        0x0007 => Double,
        0x0008 => Float,
        0x0009 => Int,
        0x000A => Text,
        0x000B => Timestamp,
        0x000C => UUID,
        0x000D => VarChar,
        0x000E => Varint,
        0x000F => TimeUUID,
        0x0010 => Inet,
        0x0020 => List,
        0x0021 => Map,
        0x0022 => Set,
        0x0030 => UDT,
        0x0031 => Tuple,
        _ => Unknown,
    }
}

#[derive(Debug)]
pub enum Error {
    Protocol,
    UnexpectedEOF,
    Io(io::Error),
    Utf8(FromUtf8Error),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Error::Utf8(err)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

trait CqlSerializable {
    fn len(&self) -> usize;
    fn serialize<T: io::Write>(&self, buf: &mut T) -> Result<()>;

    fn to_vec(&self) -> Result<Vec<u8>> {
        let mut s = Vec::with_capacity(self.len());
        self.serialize(&mut s)?;
        Ok(s)
    }
}

trait CqlReader: io::Read {
    fn read_full(&mut self, buf: &mut [u8]) -> Result<()> {
        let mut nread = 0usize;
        while nread < buf.len() {
            match self.read(&mut buf[nread..])? {
                0 => return Err(Error::UnexpectedEOF),
                n => nread += n,
            }
        }
        Ok(())
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        let mut vec = vec![0; len];
        self.read_full(vec.as_mut_slice())?;
        Ok(vec)
    }

    fn read_short(&mut self) -> Result<u16> {
        let val = self.read_u16::<BigEndian>()?;
        Ok(val)
    }

    fn read_int(&mut self) -> Result<i32> {
        let val = self.read_i32::<BigEndian>()?;
        Ok(val)
    }

    fn read_cql_str_len(&mut self, len: usize) -> Result<String> {
        let bytes = self.read_bytes(len)?;
        Ok(String::from_utf8(bytes)?)
    }

    fn read_cql_str(&mut self) -> Result<String> {
        let len = self.read_short()?;
        let bytes = self.read_bytes(usize::from(len))?;
        let s = String::from_utf8(bytes)?;
        Ok(s)
    }

    fn read_cql_string_list(&mut self) -> Result<Vec<String>> {
        let len = self.read_short()?;
        let mut v = Vec::with_capacity(usize::from(len));
        for _ in 0..len {
            v.push(self.read_cql_str()?);
        }
        Ok(v)
    }

    fn read_cql_string_multimap(&mut self) -> Result<StringMultiMap> {
        let len = self.read_short()?;
        let mut v = Vec::with_capacity(usize::from(len));
        for _ in 0..len {
            let key = self.read_cql_str()?;
            let values = self.read_cql_string_list()?;
            v.push((key, values));
        }
        Ok(v)
    }

    fn read_cql_col_type(&mut self) -> Result<CqlColDescr> {
        let type_key = column_type(self.read_short()?);
        let ty = match type_key {
            ColumnType::Custom => {
                let name = self.read_cql_str()?;
                CqlColDescr::Custom(name)
            }
            ColumnType::List => {
                let ty = self.read_cql_col_type()?;
                CqlColDescr::List(Box::new(ty))
            }
            ColumnType::Map => {
                let key_ty = self.read_cql_col_type()?;
                let val_ty = self.read_cql_col_type()?;
                CqlColDescr::Map(Box::new((key_ty, val_ty)))
            }
            ColumnType::Tuple => {
                let n = self.read_short()?;
                let mut ty_list = Vec::with_capacity(usize::from(n));
                for _ in 0..n {
                    ty_list.push(self.read_cql_col_type()?);
                }
                CqlColDescr::Tuple(ty_list)
            }
            ty => CqlColDescr::Single(ty),
        };
        Ok(ty)
    }

    fn read_cql_col_metadata(&mut self, flags: u32) -> Result<CqlColMetadata> {
        let (keyspace, table) = if flags == 0x0001 {
            (None, None)
        } else {
            let keyspace_str = self.read_cql_str()?;
            let table_str = self.read_cql_str()?;
            (Some(keyspace_str), Some(table_str))
        };
        let col_name = self.read_cql_str()?;
        let col_type = self.read_cql_col_type()?;

        Ok(CqlColMetadata {
            keyspace,
            table,
            col_name,
            col_type,
        })
    }

    fn read_cql_metadata(&mut self) -> Result<Metadata> {
        let flags = self.read_u32::<BigEndian>()?;
        let column_count = self.read_u32::<BigEndian>()?;
        let (keyspace, table) = if flags == 0x0001 {
            let keyspace_str = self.read_cql_str()?;
            let table_str = self.read_cql_str()?;
            (Some(keyspace_str), Some(table_str))
        } else {
            (None, None)
        };

        let mut row_metadata = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            row_metadata.push(self.read_cql_col_metadata(flags)?);
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
        let metadata = Rc::new(self.read_cql_metadata()?);
        let rows_count = self.read_u32::<BigEndian>()?;
        let col_count = metadata.row_metadata.len();

        let mut rows: Vec<Row> = Vec::with_capacity(rows_count as usize);
        for _ in 0..rows_count {
            let mut cols = Vec::with_capacity(col_count);

            for meta in metadata.row_metadata.iter() {
                cols.push(self.read_cql_col(&meta.col_type)?);
            }

            rows.push(Row {
                cols,
                metadata: metadata.clone(),
            });
        }

        Ok(Rows {
            metadata: metadata,
            rows: rows,
        })
    }

    fn read_cql_result(&mut self) -> Result<ResponseResult> {
        use ResponseResult::*;

        let code = self.read_u32::<BigEndian>()?;
        let res = match code {
            0x0001 => Void,
            0x0002 => Rows(self.read_cql_rows()?),
            0x0003 => {
                let msg = self.read_cql_str()?;
                Keyspace(msg)
            }
            0x0004 => {
                let id = self.read_u8()?;
                let metadata = self.read_cql_metadata()?;
                Prepared(id, metadata)
            }
            0x0005 => {
                let change_type = self.read_cql_str()?;
                let target = self.read_cql_str()?;
                let ks_name = self.read_cql_str()?;

                let name = match target.as_str() {
                    "KEYSPACE" => None,
                    "TABLE" | "TYPE" => {
                        let target_name = self.read_cql_str()?;
                        Some(target_name)
                    }
                    _ => {
                        return Err(Error::Protocol);
                    }
                };
                SchemaChange(change_type, target, ks_name, name)
            }
            _ => return Err(Error::Protocol),
        };
        Ok(res)
    }

    fn read_cql_body(&mut self, opcode: Opcode) -> Result<ResponseBody> {
        let body = match opcode {
            Opcode::Ready => ResponseBody::Ready,
            Opcode::Auth => ResponseBody::Auth(self.read_cql_str()?),
            Opcode::Error => {
                let code = self.read_u32::<BigEndian>()?;
                let msg = self.read_cql_str()?;

                match code {
                    0x2400 => {
                        let _ks = self.read_cql_str()?;
                        let _namespace = self.read_cql_str()?;
                    }
                    _ => (),
                }
                ResponseBody::Error(code, msg)
            }
            Opcode::Result => ResponseBody::Result(self.read_cql_result()?),
            Opcode::Supported => ResponseBody::Supported(self.read_cql_string_multimap()?),
            _ => {
                panic!("unknown response from server");
            }
        };
        Ok(body)
    }

    fn read_cql_response(&mut self) -> Result<Response> {
        let header_data = self.read_bytes(9)?;
        let mut header_reader = io::Cursor::new(header_data.as_slice());

        // 9-byte header
        let version = header_reader.read_u8()?;
        let flags = header_reader.read_u8()?;
        let stream = header_reader.read_i16::<BigEndian>()?;
        let opcode = opcode(header_reader.read_u8()?);
        let length = header_reader.read_u32::<BigEndian>()?;
        eprintln!("len: {:?}, opcode: {:?}", length, opcode);

        let body_data = self.read_bytes(length as usize)?;
        let mut reader = io::Cursor::new(body_data.as_slice());

        let body = reader.read_cql_body(opcode)?;
        eprintln!("body: {:?}", body);

        println!("{:?} {:?}", header_data, body_data);

        if reader.position() != length as u64 {
            eprintln!("short: {} != {}", reader.position(), length);
        }

        Ok(Response {
            header: FrameHeader {
                version,
                flags,
                stream,
                opcode,
            },
            body: body,
        })
    }

    fn read_cql_col_ty(&mut self, col_type: ColumnType, len: usize) -> Result<Value> {
        use ColumnType::*;
        use Value::*;

        eprintln!("ty: {:?}, len: {:?}", col_type, len);

        let col = match col_type {
            Ascii => CqlString(self.read_cql_str_len(len)?),
            Bigint => CqlBigint(match len {
                8 => self.read_i64::<BigEndian>()?,
                _len => return Err(Error::Protocol),
            }),
            Blob => CqlBlob(self.read_bytes(len)?),
            Boolean => CqlBool(match len {
                1 => self.read_u8()? != 0,
                _len => return Err(Error::Protocol),
            }),
            //TODO
            //Counter => ,
            //TODO
            //Decimal => ,
            Double => Cqlf64(unsafe {
                match len {
                    8 => transmute(self.read_u64::<BigEndian>()?),
                    _len => return Err(Error::Protocol),
                }
            }),
            Float => Cqlf32(unsafe {
                match len {
                    4 => transmute(self.read_u32::<BigEndian>()?),
                    _len => return Err(Error::Protocol),
                }
            }),
            Int => Cqli32(match len {
                4 => self.read_int()?,
                _len => return Err(Error::Protocol),
            }),
            Text => CqlString(self.read_cql_str_len(len)?),
            Timestamp => CqlTimestamp(match len {
                8 => self.read_i64::<BigEndian>()?,
                _len => return Err(Error::Protocol),
            }),
            UUID => CqlUUID(match len {
                16 => {
                    let mut v = [0u8; 16];
                    self.read_full(&mut v)?;
                    v
                }
                _len => return Err(Error::Protocol),
            }),
            VarChar => CqlString(self.read_cql_str_len(len)?),
            //TODO
            //Varint => ,
            TimeUUID => CqlTimeUUID(match len {
                16 => {
                    let mut v = [0u8; 16];
                    self.read_full(&mut v)?;
                    v
                }
                _len => return Err(Error::Protocol),
            }),
            Inet => CqlInet(match len {
                4 => {
                    let mut v = [0u8; 4];
                    self.read_full(&mut v)?;
                    std::net::IpAddr::V4(v.into())
                }
                16 => {
                    let mut v = [0u8; 16];
                    self.read_full(&mut v)?;
                    std::net::IpAddr::V6(v.into())
                }
                _len => return Err(Error::Protocol),
            }),
            Custom | List | Map | Set | UDT | Tuple => {
                unreachable!("non-singular type on read_cql_col_ty: {:?}", col_type);
            }
            _ => {
                self.read_bytes(len as usize)?;
                CqlUnknown
            }
        };
        Ok(col)
    }

    fn read_cql_col(&mut self, col_type: &CqlColDescr) -> Result<Value> {
        let len = match self.read_int()? {
            -1 => return Ok(Value::CqlNull),
            len => len as usize,
        };

        match *col_type {
            CqlColDescr::Custom(ref name) => {
                let data = self.read_bytes(len)?;
                Ok(Value::CqlCustom(name.clone(), data))
            }
            CqlColDescr::Single(ty) => self.read_cql_col_ty(ty, len),
            CqlColDescr::List(ref ty) => {
                let n = self.read_int()? as usize;
                let mut l = Vec::with_capacity(n);
                for _ in 0..n {
                    l.push(self.read_cql_col(ty)?);
                }
                Ok(Value::CqlList(l))
            }
            CqlColDescr::Map(ref ty_tup) => {
                let n = self.read_int()? as usize;
                let mut l = Vec::with_capacity(n);
                for _ in 0..n {
                    let key = self.read_cql_col(&ty_tup.0)?;
                    let val = self.read_cql_col(&ty_tup.1)?;
                    l.push((key, val));
                }
                Ok(Value::CqlMap(l))
            }
            CqlColDescr::Tuple(ref ty_list) => {
                let n = self.read_int()? as usize;
                let mut l = Vec::with_capacity(n);
                for _ in 0..n {
                    let mut row = Vec::with_capacity(ty_list.len());
                    for ty in ty_list.iter() {
                        row.push(self.read_cql_col(ty)?);
                    }
                    l.push(row)
                }
                Ok(Value::CqlTuple(l))
            }
        }
    }
}

impl<'a, T: io::Read> CqlReader for T {}

#[derive(Debug)]
struct Pair {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl CqlSerializable for Pair {
    fn serialize<T: io::Write>(&self, buf: &mut T) -> Result<()> {
        buf.write_u16::<BigEndian>(self.key.len() as u16)?;
        buf.write_all(self.key.as_slice())?;
        buf.write_u16::<BigEndian>(self.value.len() as u16)?;
        buf.write_all(self.value.as_slice())?;
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
    fn serialize<T: io::Write>(&self, buf: &mut T) -> Result<()> {
        buf.write_u16::<BigEndian>(self.pairs.len() as u16)?;
        for pair in self.pairs.iter() {
            pair.serialize(buf)?;
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
    col_type: CqlColDescr,
}

#[derive(Debug)]
enum CqlColDescr {
    Custom(String),
    Single(ColumnType),
    List(Box<CqlColDescr>),
    Map(Box<(CqlColDescr, CqlColDescr)>),
    //UDT,
    Tuple(Vec<CqlColDescr>),
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
pub enum Value {
    CqlCustom(String, Vec<u8>),

    CqlString(String),

    Cqli32(i32),
    Cqli64(i64),

    CqlBlob(Vec<u8>),
    CqlBool(bool),

    CqlCounter(u64),

    Cqlf32(f32),
    Cqlf64(f64),

    CqlTimestamp(i64),
    CqlUUID([u8; 16]),
    CqlTimeUUID([u8; 16]),
    CqlInet(std::net::IpAddr),
    CqlBigint(i64),

    CqlList(Vec<Value>),
    CqlMap(Vec<(Value, Value)>),
    CqlTuple(Vec<Vec<Value>>),

    CqlNull,
    CqlUnknown,
}

#[derive(Debug)]
pub struct Row {
    cols: Vec<Value>,
    metadata: Rc<Metadata>,
}

impl Row {
    pub fn get_column(&self, col_name: &str) -> Option<Value> {
        self.metadata
            .row_metadata
            .iter()
            .position(|m| m.col_name == col_name)
            .map(|i| self.cols[i].clone())
    }
}

#[derive(Debug)]
pub struct Rows {
    metadata: Rc<Metadata>,
    rows: Vec<Row>,
}

struct BodyStartup {
    body: StringMap,
}
impl CqlSerializable for BodyStartup {
    fn serialize<T: io::Write>(&self, buf: &mut T) -> Result<()> {
        self.body.serialize(buf)
    }

    fn len(&self) -> usize {
        self.body.len()
    }
}

struct BodyQuery {
    query: String,
    con: Consistency,
}
impl CqlSerializable for BodyQuery {
    fn serialize<T: io::Write>(&self, buf: &mut T) -> Result<()> {
        buf.write_u32::<BigEndian>(self.query.len() as u32)?;
        buf.write_all(self.query.as_bytes())?;

        buf.write_u16::<BigEndian>(self.con.clone() as u16)?;
        buf.write_u8(0)?;
        Ok(())
    }

    fn len(&self) -> usize {
        4 + self.query.len() + 3
    }
}

struct BodyEmpty;
impl CqlSerializable for BodyEmpty {
    fn serialize<T: io::Write>(&self, _buf: &mut T) -> Result<()> {
        Ok(())
    }

    fn len(&self) -> usize {
        0
    }
}

type StringMultiMap = Vec<(String, Vec<String>)>;

#[derive(Debug)]
pub enum ResponseBody {
    Error(u32, String),
    Ready,
    Auth(String),
    Supported(StringMultiMap),
    Result(ResponseResult),
}

#[derive(Debug)]
pub enum ResponseResult {
    Void,
    Rows(Rows),
    Keyspace(String),
    Prepared(u8, Metadata),
    SchemaChange(String, String, String, Option<String>),
}

#[derive(Debug)]
pub struct FrameHeader {
    version: u8,
    flags: u8,
    stream: i16,
    opcode: Opcode,
}
impl FrameHeader {
    fn new(stream: i16, opcode: Opcode) -> Self {
        Self {
            version: CQL_VERSION,
            flags: 0x00,
            stream,
            opcode,
        }
    }
}

#[derive(Debug)]
struct Request<B: CqlSerializable> {
    header: FrameHeader,
    body: B,
}

impl<B: CqlSerializable> CqlSerializable for Request<B> {
    fn serialize<T: io::Write>(&self, buf: &mut T) -> Result<()> {
        let header = &self.header;
        buf.write_u8(header.version)?;
        buf.write_u8(header.flags)?;
        buf.write_i16::<BigEndian>(header.stream)?;
        buf.write_u8(header.opcode.clone() as u8)?;

        buf.write_u32::<BigEndian>(self.body.len() as u32)?;
        self.body.serialize(buf)?;
        Ok(())
    }

    fn len(&self) -> usize {
        self.body.len() + 9
    }
}

#[derive(Debug)]
pub struct Response {
    header: FrameHeader,
    body: ResponseBody,
}

fn startup() -> Request<BodyStartup> {
    let body = StringMap {
        pairs: vec![Pair {
            key: b"CQL_VERSION".to_vec(),
            value: b"3.0.0".to_vec(),
        }],
    };
    return Request {
        header: FrameHeader::new(1, Opcode::Startup),
        body: BodyStartup { body },
    };
}

/*
#[allow(unused)]
fn auth(creds: Vec<Vec<u8>>) -> Request {
    return Request {
        header: FrameHeader::new(1, Opcode::Auth),
        body: RequestBody::RequestCred(creds),
    };
}
*/

#[allow(unused)]
fn options() -> Request<BodyEmpty> {
    return Request {
        header: FrameHeader::new(1, Opcode::Opts),
        body: BodyEmpty,
    };
}

fn query(stream: i16, query_str: &str, con: Consistency) -> Request<BodyQuery> {
    return Request {
        header: FrameHeader::new(stream, Opcode::Query),
        body: BodyQuery {
            query: query_str.to_owned(),
            con,
        },
    };
}

pub struct Client {
    socket: TcpStream,
}

impl Client {
    pub fn new(addr: &str) -> Result<Client> {
        let mut socket = TcpStream::connect(addr)?;
        let msg_startup = startup();

        let mut buf = Vec::new();
        msg_startup.serialize::<Vec<u8>>(&mut buf)?;
        socket.write_all(buf.as_slice())?;

        let response = socket.read_cql_response()?;
        match response.body {
            ResponseBody::Ready => Ok(Client { socket: socket }),
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
            _ => panic!("invalid opcode: {}", response.header.opcode as u8),
        }
    }

    pub fn options(&mut self) -> Result<Response> {
        let q = options();
        let msg = q.to_vec()?;

        self.socket.write_all(&msg)?;
        self.socket.read_cql_response()
    }

    pub fn query(&mut self, query_str: &str, con: Consistency) -> Result<Response> {
        let q = query(0, query_str, con);

        let mut writer = Vec::new();
        q.serialize::<Vec<u8>>(&mut writer)?;

        self.socket.write_all(writer.as_slice())?;
        self.socket.read_cql_response()
    }
}

#[cfg(test)]
mod tests {
    use super::CqlReader;

    #[test]
    fn resp_ready() {
        let v = vec![131, 0, 0, 1, 2, 0, 0, 0, 0];
        let resp = v.as_slice().read_cql_response();
        assert!(resp.is_ok())
    }

    #[test]
    fn resp_error() {
        let v = vec![
            131, 0, 0, 0, 0, 0, 0, 0, 49, 0, 0, 36, 0, 0, 35, 67, 97, 110, 110, 111, 116, 32, 97,
            100, 100, 32, 101, 120, 105, 115, 116, 105, 110, 103, 32, 107, 101, 121, 115, 112, 97,
            99, 101, 32, 34, 114, 117, 115, 116, 34, 0, 4, 114, 117, 115, 116, 0, 0,
        ];
        let resp = v.as_slice().read_cql_response();
        assert!(resp.is_ok())
    }

    #[test]
    fn resp_schema_change() {
        let v = vec![
            131, 0, 0, 0, 8, 0, 0, 0, 29, 0, 0, 0, 5, 0, 7, 67, 82, 69, 65, 84, 69, 68, 0, 8, 75,
            69, 89, 83, 80, 65, 67, 69, 0, 4, 114, 117, 115, 116,
        ];
        let resp = v.as_slice().read_cql_response();
        assert!(resp.is_ok())
    }

    #[test]
    fn resp_schema_change_table() {
        let v = vec![
            131, 0, 0, 0, 8, 0, 0, 0, 32, 0, 0, 0, 5, 0, 7, 67, 82, 69, 65, 84, 69, 68, 0, 5, 84,
            65, 66, 76, 69, 0, 4, 114, 117, 115, 116, 0, 4, 116, 101, 115, 116,
        ];
        let resp = v.as_slice().read_cql_response();
        assert!(resp.is_ok())
    }

    #[test]
    fn resp_result_void() {
        let v = vec![131, 0, 0, 0, 8, 0, 0, 0, 4, 0, 0, 0, 1];
        let resp = v.as_slice().read_cql_response();
        assert!(resp.is_ok())
    }

    #[test]
    fn resp_result_select() {
        let v = vec![
            131, 0, 0, 0, 8, 0, 0, 0, 59, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2, 0, 4, 114, 117, 115,
            116, 0, 4, 116, 101, 115, 116, 0, 2, 105, 100, 0, 13, 0, 5, 118, 97, 108, 117, 101, 0,
            8, 0, 0, 0, 1, 0, 0, 0, 4, 97, 115, 100, 102, 0, 0, 0, 4, 63, 158, 4, 25,
        ];
        let resp = v.as_slice().read_cql_response();
        assert!(resp.is_ok())
    }
}
