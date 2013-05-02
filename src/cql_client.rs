#[link(name="cql_client", 
    vers="0.0.1")];

#[crate_type="lib"];
#[warn(non_camel_case_types)];

extern mod std;

use core::io::{ReaderUtil, WriterUtil};

use std::{net_ip, net_tcp, uv_global_loop, bigint};
use std::io_util;

pub static CQL_VERSION:u8 = 0x01;

enum OpcodeRequest {
    //requests
    OpcodeStartup = 0x01,
    OpcodeCredentials = 0x04,
    OpcodeOptions = 0x05,
    OpcodeQuery = 0x07,
    OpcodePrepare = 0x09,
    OpcodeRegister = 0x0B,
}

enum OpcodeResponse {
    //responces
    OpcodeError = 0x00,
    OpcodeReady = 0x02,
    OpcodeAuthenticate = 0x03,
    OpcodeSupported = 0x06,
    OpcodeResult = 0x08,
    OpcodeExecute = 0x0A,
    OpcodeEvent = 0x0C,

    OpcodeUnknown
}

fn OpcodeResponse(val: u8) -> OpcodeResponse {
    match val {
        /* requests
        0x01 => OpcodeStartup,
        0x04 => OpcodeCredentials,
        0x05 => OpcodeOptions, 
        0x07 => OpcodeQuery,
        0x09 => OpcodePrepare,
        0x0B => OpcodeRegister,
        */

        0x00 => OpcodeError,
        0x02 => OpcodeReady,
        0x03 => OpcodeAuthenticate,
        0x06 => OpcodeSupported,
        0x08 => OpcodeResult,
        0x0A => OpcodeExecute,
        0x0C => OpcodeEvent,

        _ => OpcodeUnknown
    }
}

pub enum Consistency {
    ConsistencyAny = 0x0000,
    ConsistencyOne = 0x0001,
    ConsistencyTwo = 0x0002,
    ConsistencyThree = 0x0003,
    ConsistencyQuorum = 0x0004,
    ConsistencyAll = 0x0005,
    ConsistencyLocalQuorum = 0x0006,
    ConsistencyEachQuorum = 0x0007,
    ConsistencyUnknown,
}

pub fn Consistency(val: u16) -> Consistency {
    match val {
        0 => ConsistencyAny,
        1 => ConsistencyOne,
        2 => ConsistencyTwo,
        3 => ConsistencyThree,
        4 => ConsistencyQuorum,
        5 => ConsistencyAll,
        6 => ConsistencyLocalQuorum,
        7 => ConsistencyEachQuorum,
        _ => ConsistencyUnknown
    }
}

pub enum CqlColumnType {
    ColumnCustom = 0x0000,
    ColumnASCII = 0x0001,
    ColumnBigInt = 0x0002,
    ColumnBlob = 0x0003,
    ColumnBoolean = 0x0004,
    ColumnCounter = 0x0005,
    ColumnDecimal = 0x0006,
    ColumnDouble = 0x0007,
    ColumnFloat = 0x0008,
    ColumnInt = 0x0009,
    ColumnText = 0x000A,
    ColumnTimestamp = 0x000B,
    ColumnUUID = 0x000C,
    ColumnVarChar = 0x000D,
    ColumnVarint = 0x000E,
    ColumnTimeUUID = 0x000F,
    ColumnInet = 0x0010,
    ColumnList = 0x0020,
    ColumnMap = 0x0021,
    ColumnSet = 0x0022,
    ColumnUnknown,
}

fn CqlColumnType(val: u16) -> CqlColumnType {
    match val {
        0x0000 => ColumnCustom,
        0x0001 => ColumnASCII,
        0x0002 => ColumnBigInt,
        0x0003 => ColumnBlob,
        0x0004 => ColumnBoolean,
        0x0005 => ColumnCounter,
        0x0006 => ColumnDecimal,
        0x0007 => ColumnDouble,
        0x0008 => ColumnFloat,
        0x0009 => ColumnInt,
        0x000A => ColumnText,
        0x000B => ColumnTimestamp,
        0x000C => ColumnUUID,
        0x000D => ColumnVarChar,
        0x000E => ColumnVarint,
        0x000F => ColumnTimeUUID,
        0x0010 => ColumnInet,
        0x0020 => ColumnList,
        0x0021 => ColumnMap,
        0x0022 => ColumnSet,
        _ => ColumnUnknown
    }
}

struct CqlError {
    err_name: ~str,
    err_msg: ~str,
}

fn CqlError(name: ~str, msg: ~str) -> CqlError {
    return CqlError{err_name: name, err_msg: msg};
}

trait CqlSerializable {
    fn len(&self) -> uint;
    fn serialize<T: io::Writer>(&self, buf: &T);
}

trait CqlReader {
    fn read_cql_str(&self) -> ~str;
    fn read_cql_long_str(&self) -> Option<~str>;
    fn read_cql_rows(&self) -> CqlRows;

    fn read_cql_metadata(&self) -> CqlMetadata;
    fn read_cql_response(&self) -> CqlResponse;
}

impl<T: ReaderUtil> CqlReader for T {
    fn read_cql_str(&self) -> ~str {
        let len = self.read_be_u16() as uint;
        str::from_bytes(self.read_bytes(len))
    }

    fn read_cql_long_str(&self) -> Option<~str> {
        let len = self.read_be_i32() as uint;
        if(len == -1) {
            None
        } else {
            Some(str::from_bytes(self.read_bytes(len)))
        }
    }

    fn read_cql_metadata(&self) -> CqlMetadata {
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

        let mut row_metadata:~[CqlColMetadata] = ~[];
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
                    CqlColumnType(self.read_be_u16())
                } else {
                    ColumnUnknown
                };

            row_metadata.push(CqlColMetadata {
                keyspace: keyspace,
                table: table,
                col_name: col_name,
                col_type: CqlColumnType(type_key),
                col_type_name: type_name
            });
        }

        CqlMetadata {
            flags: flags,
            column_count: column_count,
            keyspace: keyspace,
            table: table,
            row_metadata: row_metadata,
        }
    }

    fn read_cql_rows(&self) -> CqlRows {
        let metadata = @self.read_cql_metadata();
        let rows_count = self.read_be_u32();

        io::println(fmt!("%?", metadata));

        let mut rows:~[CqlRow] = ~[];
        for u32::range(0, rows_count) |_| {
            let mut row = CqlRow{ cols: ~[], metadata: metadata };
            for metadata.row_metadata.each |meta| {
                let col = match meta.col_type {
                    ColumnASCII => CqlString(self.read_cql_long_str()),
                    ColumnVarChar => CqlString(self.read_cql_long_str()),
                    ColumnText => CqlString(self.read_cql_long_str()),

                    ColumnInt => Cqli32({
                        match self.read_be_i32() {
                            -1 => None,
                            4 => Some(self.read_be_i32()),
                            len => fail!(fmt!("Invalid length with i32: %?", len)),
                        }
                    }),
                    ColumnBigInt => Cqli64(Some(self.read_be_i64())),
                    ColumnFloat => Cqlf32(unsafe{
                        match self.read_be_i32() {
                            -1 => None,
                            4 => Some(cast::transmute(self.read_be_u32())),
                            len => fail!(fmt!("Invalid length with f32: %?", len)),
                        }
                    }),
                    ColumnDouble => Cqlf64(unsafe{
                        match self.read_be_i32() {
                            -1 => None,
                            4 => Some(cast::transmute(self.read_be_u64())),
                            len => fail!(fmt!("Invalid length with f64: %?", len)),
                        }
                    }),

                    ColumnList => CqlList({
                        match self.read_be_i32() {
                            -1 => None,
                            len => {
                                let data = self.read_bytes(len as uint);
                                io::println(fmt!("%?", data));
                                None
                            },
                        }
                    }),


   /*
                    ColumnCustom => ,
                    ColumnBlob => ,
                    ColumnBoolean => ,
                    ColumnCounter => ,
                    ColumnDecimal => ,
                    ColumnTimestamp => ,
                    ColumnUUID => ,
                    ColumnVarint => ,
                    ColumnTimeUUID => ,
                    ColumnInet => ,
                    ColumnList => ,
                    ColumnMap => ,
                    ColumnSet => ,
                    */
                    unknown => {
                        io::println(fmt!("Unknown type id: %?", unknown));
                        match self.read_be_i32() {
                            -1 => (),
                            len => { self.read_bytes(len as uint); },
                        }
                        CqlUnknown
                    }
                };

                row.cols.push(col);
            }
            rows.push(row);
        }

        CqlRows {
            metadata: metadata,
            rows: rows,
        }
    }

    fn read_cql_response(&self) -> CqlResponse {
        let header_data = self.read_bytes(8);

        let version = header_data[0];
        let flags = header_data[1];
        let stream = header_data[2] as i8;
        let opcode = OpcodeResponse(header_data[3]);
        let length = (header_data[4] as uint << 24) + 
            (header_data[5] as uint << 16) + 
            (header_data[6] as uint << 8) + 
            (header_data[7] as uint);

        let body_data = self.read_bytes(length);
        io::println(fmt!("%?", body_data));
        let reader = io_util::BufReader::new(body_data);

        let body = match opcode {
            OpcodeReady => ResponseReady,
            OpcodeAuthenticate => {
                ResponseAuth(reader.read_cql_str())
            }
            OpcodeError => {
                let code = reader.read_be_u32();
                let msg = reader.read_cql_str();
                ResponseError(code, msg)
            },
            OpcodeResult => {
                let code = reader.read_be_u32();
                match code {
                    0x0001 => {
                        ResultVoid
                    },
                    0x0002 => {
                        ResultRows(reader.read_cql_rows())
                    },
                    0x0003 => {
                        let msg = reader.read_cql_str();
                        ResultKeyspace(msg)
                    },
                    0x0004 => {
                        let id = reader.read_u8();
                        let metadata = reader.read_cql_metadata();
                        ResultPrepared(id, metadata)
                    },
                    0x0005 => {
                        let change  = reader.read_cql_str();
                        let keyspace = reader.read_cql_str();
                        let table = reader.read_cql_str();
                        ResultSchemaChange(change, keyspace, table)
                    },
                    _ => {
                        fail!(fmt!("Unknown code for result: %?", code));
                    },
                }
            }
            _ => {
                fail!(~"Invalid response from server");
            },//ResponseEmpty,
        };

        if reader.pos != length {
            debug!("Data is not fully readed: specification might be changed %? != %?", 
                reader.pos, length);
        }

        return CqlResponse {
            version: version,
            flags: flags,
            stream: stream,
            opcode: opcode,
            body: body,
        };
    }
}

struct CqlPair {
    key: ~str,
    value: ~str,
}

impl CqlSerializable for CqlPair {
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

struct CqlStringMap {
    pairs: ~[CqlPair],
}

impl CqlSerializable for CqlStringMap {
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

struct CqlColMetadata {
    keyspace: ~str,
    table: ~str,
    col_name: ~str,
    col_type: CqlColumnType,
    col_type_name: CqlColumnType,
}

struct CqlMetadata {
    flags: u32,
    column_count: u32,
    keyspace: ~str,
    table: ~str,
    row_metadata: ~[CqlColMetadata],
}

pub enum CqlColumn {
    CqlString(Option<~str>),

    Cqli32(Option<i32>),
    Cqli64(Option<i64>),

    CqlBlob(Option<~[u8]>),
    CqlBool(Option<bool>),

    CqlCounter(Option<u64>),

    Cqlf32(Option<f32>),
    Cqlf64(Option<f64>),

    CqlTimestamp(u64),
    CqlBigint(bigint::BigInt),

    CqlList(Option<~[CqlColumn]>),

    CqlUnknown,
}

pub struct CqlRow {
    cols: ~[CqlColumn],
    metadata: @CqlMetadata,
}

impl CqlRow {
    fn get_column(&self, col_name: ~str) -> Option<CqlColumn> {
        let mut i = 0;
        let len = self.metadata.row_metadata.len();
        while i < len {
            if self.metadata.row_metadata[i].col_name == col_name {
                return Some(copy self.cols[i]);
            }
            i += 1;
        }
        None
    }
}

pub struct CqlRows {
    metadata: @CqlMetadata,
    rows: ~[CqlRow],
}

pub enum CqlRequestBody {
    RequestStartup(CqlStringMap),
    RequestCred(~[~str]),
    RequestQuery(~str, Consistency),
    RequestOptions,
}

pub enum CqlResponseBody {
    ResponseError(u32, ~str),
    ResponseReady,
    ResponseAuth(~str),

    ResultVoid,
    ResultRows(CqlRows),
    ResultKeyspace(~str),
    ResultPrepared(u8, CqlMetadata),
    ResultSchemaChange(~str, ~str, ~str),
    ResultUnknown,

    ResponseEmpty,
}

struct CqlRequest {
    version: u8,
    flags: u8,
    stream: i8,
    opcode: OpcodeRequest,
    body: CqlRequestBody,
}

struct CqlResponse {
    version: u8,
    flags: u8,
    stream: i8,
    opcode: OpcodeResponse,
    body: CqlResponseBody,
}

impl CqlSerializable for CqlRequest {
    fn serialize<T: io::Writer>(&self, buf: &T) {
        buf.write_u8(self.version);
        buf.write_u8(self.flags);
        buf.write_i8(self.stream);
        buf.write_u8(self.opcode as u8);
        buf.write_be_u32((self.len()-8) as u32);

        match self.body {
            RequestStartup(ref map) => {
                map.serialize(buf)
            },
            RequestQuery(ref query_str, ref consistency) => {
                buf.write_be_u32(query_str.len() as u32);
                buf.write(str::to_bytes(*query_str));
                buf.write_be_u16(*consistency as u16);
            },
            _ => (),
        }
    }
    fn len(&self) -> uint {
        8 + match self.body {
            RequestStartup(ref map) => {
                map.len()
            },
            RequestQuery(ref query_str, _) => {
                4 + query_str.len() + 2
            },
            _ => {
                0
            }
        }
    }
}

fn Startup() -> CqlRequest {
    let body = CqlStringMap {
            pairs:~[CqlPair{key: ~"CQL_VERSION", value: ~"3.0.0"}],
        };
    return CqlRequest {
        version: CQL_VERSION,
        flags: 0x00,
        stream: 0x01,
        opcode: OpcodeStartup,
        body: RequestStartup(body),
    };
}

fn Auth(creds: ~[~str]) -> CqlRequest {
    return CqlRequest {
        version: CQL_VERSION,
        flags: 0x00,
        stream: 0x01,
        opcode: OpcodeOptions,
        body: RequestCred(creds),
    };
}

fn Options() -> CqlRequest {
    return CqlRequest {
        version: CQL_VERSION,
        flags: 0x00,
        stream: 0x01,
        opcode: OpcodeOptions,
        body: RequestOptions,
    };
}

fn Query(stream: i8, query_str: ~str, con: Consistency) -> CqlRequest {
    return CqlRequest {
        version: CQL_VERSION,
        flags: 0x00,
        stream: stream,
        opcode: OpcodeQuery,
        body: RequestQuery(query_str, con),
    };
}

pub struct CqlClient {
    socket: net_tcp::TcpSocketBuf,
}

impl CqlClient {
    fn query(&self, query_str: ~str, con: Consistency) -> CqlResponse {
        let q = Query(0x01, query_str, con);

        let writer = io::BytesWriter();
        
        q.serialize::<io::BytesWriter>(&writer);
        self.socket.write(writer.bytes);
        self.socket.read_cql_response()
    }
}

pub fn connect(ip: ~str, port: uint, creds:Option<~[~str]>) -> 
        result::Result<CqlClient, CqlError> {
    let task = @uv_global_loop::get();
    let addr = net_ip::v4::parse_addr(ip);

    let res = net_tcp::connect(addr, port, task);
    if(res.is_err()) {
        return result::Err(CqlError(~"Error", ~"Failed to connect to server"));
    }

    let socket = res.unwrap();
    let buf = net_tcp::socket_buf(socket);

    let msg_startup = Startup();
    msg_startup.serialize::<net_tcp::TcpSocketBuf>(&buf);

    let response = buf.read_cql_response();
    let opcode = copy response.opcode;
    match response.body {
        ResponseReady => {
            result::Ok(CqlClient { socket: buf })
        },
        ResponseAuth(_) => {
            match(creds) {
                Some(cred) => {
                    let msg_auth = Auth(cred);
                    msg_auth.serialize::<net_tcp::TcpSocketBuf>(&buf);
                    let response = buf.read_cql_response();
                    match response.body {
                        ResponseReady => result::Ok(CqlClient { socket: buf }),
                        ResponseError(_, ref msg) => {
                            result::Err(CqlError(~"Error", copy *msg))
                        }
                        _ => {
                            result::Err(CqlError(~"Error", ~"Server returned unknown message"))
                        },
                    }
                },
                None => {
                    result::Err(CqlError(~"Error", ~"Credential should be provided"))
                },
            }

        }
        _ => result::Err(CqlError(~"Error", fmt!("Invalid opcode: %?", opcode)))
    }
}
