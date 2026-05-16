use std::io;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use uuid::Uuid;

static PROTOCOL_VERSION: u8 = 0;
static MAGIC_BYTES: [u8; 4] = [0x72, 0x6B, 0x76, 0x73]; //rkvs

#[derive(Debug)]
pub enum TcpError {
    IoError(io::Error),
    WrongMagicBytes,
    UnsupportedVersion(Uuid),
    InvalidRequestType(Uuid),
    MissingValue(Uuid), // only for put
    InvalidKey(Uuid),
    InvalidValue(Uuid),
    UnknownFlags(Uuid),
    MalformedPayload(Uuid), // general errors
}

pub enum RequestType {
    Get,
    Put,
    Delete,
}

pub struct TcpHeaders {
    pub request_type: RequestType,
    pub correlation_id: Uuid,
    protocol_version: u8,
    flags: u16,
}

pub struct Payload {
    pub key: String,
    pub value: Option<String>,
}

pub struct TcpRequest {
    pub headers: TcpHeaders,
    pub payload: Payload,
}

fn verify_magic_bytes(buf: &[u8; 4]) -> bool {
    return *buf == MAGIC_BYTES;
}

fn parse_request_type(n: u8) -> Result<RequestType, ()> {
    match n {
        0 => Ok(RequestType::Delete),
        1 => Ok(RequestType::Get),
        2 => Ok(RequestType::Put),
        _ => Err(()),
    }
}

fn parse_headers(buf: &Vec<u8>) -> Result<TcpHeaders, TcpError> {
    if !verify_magic_bytes(&buf[0..4].try_into().unwrap()) {
        return Err(TcpError::WrongMagicBytes);
    }
    let correlation_id = Uuid::from_bytes(buf[4..12].try_into().unwrap());
    let version = buf[12];
    if version > PROTOCOL_VERSION {
        return Err(TcpError::UnsupportedVersion(correlation_id));
    }
    let request_type =
        parse_request_type(buf[13]).map_err(|_| TcpError::InvalidRequestType(correlation_id))?;
    let flags = u16::from_be_bytes(buf[14..16].try_into().unwrap());

    // TODO: optional headers
    Ok(TcpHeaders {
        request_type: request_type,
        correlation_id: correlation_id,
        protocol_version: version,
        flags: flags,
    })
}

fn parse_payload(
    buf: &Vec<u8>,
    request_type: &RequestType,
    correlation_id: &Uuid,
) -> Result<Payload, TcpError> {
    let payload_len = buf.len();
    let key_len_u32 = u32::from_be_bytes(buf[0..4].try_into().unwrap());
    let key_len = usize::try_from(key_len_u32).unwrap();
    let key = String::from_utf8(buf[4..key_len + 4].to_vec())
        .map_err(|_| TcpError::InvalidKey(*correlation_id))?;
    match request_type {
        RequestType::Put => {
            if key_len >= payload_len {
                Err(TcpError::MissingValue(*correlation_id))
            } else {
                let value_len_u32 =
                    u32::from_be_bytes(buf[key_len + 8..key_len + 12].try_into().unwrap());
                let value_len = usize::try_from(value_len_u32).unwrap();
                let value = String::from_utf8(buf[key_len + 12..key_len + 12 + value_len].to_vec())
                    .map_err(|_| TcpError::InvalidValue(*correlation_id))?;
                Ok(Payload {
                    key: key,
                    value: Some(value),
                })
            }
        }
        _ => {
            if key_len < payload_len {
                Err(TcpError::MalformedPayload(*correlation_id))
            } else {
                Ok(Payload {
                    key: key,
                    value: None,
                })
            }
        }
    }
}

async fn recv_headers(stream: &mut TcpStream, len: &u32) -> Result<Vec<u8>, TcpError> {
    let mut buf = Vec::<u8>::new();
    let header_len: usize = usize::try_from(*len).unwrap();
    buf.resize(header_len, 0);

    let _ = stream.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn recv_payload(stream: &mut TcpStream, len: &u32) -> Result<Vec<u8>, TcpError> {
    let mut buf = Vec::<u8>::new();
    let payload_len: usize = usize::try_from(*len).unwrap();
    buf.resize(payload_len, 0);

    let _ = stream.read_exact(&mut buf).await?;
    Ok(buf)
}

pub async fn recv_tcp_request(stream: &mut TcpStream) -> Result<TcpRequest, TcpError> {
    let header_len = stream.read_u32().await?;
    let headers = recv_headers(stream, &header_len)
        .await
        .and_then(|buf| parse_headers(&buf))?;
    let payload_len = stream.read_u32().await?;
    let payload = recv_payload(stream, &payload_len)
        .await
        .and_then(|buf| parse_payload(&buf, &headers.request_type, &headers.correlation_id))?;
    Ok(TcpRequest { headers, payload })
}

impl From<io::Error> for TcpError {
    fn from(value: io::Error) -> Self {
        TcpError::IoError(value)
    }
}

/* Test cases:
    1. Ok
        1. Different types
    2. First 4 bytes are not the magic bytes
    3. Wrong payload
        1. Put but no value
        2. Get or delete but given value
        3. Strings are malformed, i.e. no utf8 bytes
    4. Unsupported version
    5. Unknown request typw

*/
