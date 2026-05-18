use super::ServerError;
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

#[derive(Debug, PartialEq)]
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

pub struct TcpResponse {
    correlation_id: Uuid,
    response_code: u8,
    payload: Option<String>,
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
    let correlation_id = Uuid::from_bytes(buf[4..20].try_into().unwrap());
    let version = buf[20];
    if version > PROTOCOL_VERSION {
        return Err(TcpError::UnsupportedVersion(correlation_id));
    }
    let request_type =
        parse_request_type(buf[21]).map_err(|_| TcpError::InvalidRequestType(correlation_id))?;
    let flags = u16::from_be_bytes(buf[22..24].try_into().unwrap());

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
            if key_len >= payload_len - 4 {
                Err(TcpError::MissingValue(*correlation_id))
            } else {
                let value_len_u32 =
                    u32::from_be_bytes(buf[key_len + 4..key_len + 8].try_into().unwrap());
                let value_len = usize::try_from(value_len_u32).unwrap();
                let value = String::from_utf8(buf[key_len + 8..key_len + 8 + value_len].to_vec())
                    .map_err(|_| TcpError::InvalidValue(*correlation_id))?;
                Ok(Payload {
                    key: key,
                    value: Some(value),
                })
            }
        }
        _ => {
            if key_len < payload_len - 4 {
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

impl TcpResponse {
    pub fn from_error(correlation_id: &Uuid, error: &ServerError) -> Self {
        TcpResponse {
            correlation_id: *correlation_id,
            response_code: error.to_rc(),
            payload: None,
        }
    }

    pub fn from(correlation_id: &Uuid, value: Option<String>) -> Self {
        TcpResponse {
            correlation_id: *correlation_id,
            response_code: 0,
            payload: value,
        }
    }

    pub fn len(&self) -> usize {
        match &self.payload {
            Some(str) => str.len() + 5,
            None => 5,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::<u8>::new();
        buf.resize(self.len(), 0);
        buf[0..4].copy_from_slice(self.correlation_id.as_bytes());
        buf[4] = self.response_code;
        match &self.payload {
            None => buf,
            Some(str) => {
                buf[5..].copy_from_slice(str.as_bytes());
                buf
            }
        }
    }
}

impl TcpHeaders {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.resize(self.len(), 0);
        buf[0..16].copy_from_slice(&self.correlation_id.to_bytes_le());
        buf[16] = self.protocol_version;
        buf[17] = match self.request_type {
            RequestType::Delete => 0,
            RequestType::Get => 1,
            RequestType::Put => 2,
        };
        buf[18..20].copy_from_slice(&self.flags.to_be_bytes());
        buf
    }

    pub fn len(&self) -> usize {
        20 // correlation id: 16, type: 1, flags: 2, version: 1
    }
}

impl Payload {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::<u8>::new();
        let key_len = self.key.len();
        match &self.value {
            Some(value) => buf.resize(key_len + 8 + value.len(), 0),
            None => buf.resize(key_len + 4, 0),
        }
        let key_len_u32: u32 = key_len.try_into().unwrap();
        buf[0..4].copy_from_slice(&key_len_u32.to_be_bytes());
        buf[4..key_len + 4].copy_from_slice(self.key.as_bytes());
        match &self.value {
            Some(value) => {
                let value_len: u32 = value.len().try_into().unwrap();
                buf[key_len + 4..key_len + 8].copy_from_slice(&value_len.to_be_bytes());
                buf[key_len + 8..].copy_from_slice(value.as_bytes());
            }
            None => (),
        }
        buf
    }
}

#[cfg(test)]
mod tests {

    use crate::tcp_protocol::TcpError;

    use super::{RequestType, TcpHeaders, parse_headers};
    use uuid::Uuid;
    #[test]
    fn parse_headers_ok() {
        let headers_write = TcpHeaders {
            correlation_id: Uuid::from_u128(1),
            request_type: RequestType::Delete,
            protocol_version: 0,
            flags: 0,
        };
        let mut buf = Vec::<u8>::new();
        buf.resize(headers_write.len() + 4, 0);
        buf[0..4].copy_from_slice(&super::MAGIC_BYTES);
        buf[4..].copy_from_slice(&headers_write.to_bytes());
        let headers_read = parse_headers(&buf);
        assert!(headers_read.is_ok());
        assert_eq!(headers_read.as_ref().unwrap().correlation_id.as_u128(), 1);
        assert_eq!(headers_read.as_ref().unwrap().flags, 0);
        assert_eq!(headers_read.as_ref().unwrap().protocol_version, 0);
        assert_eq!(
            headers_read.as_ref().unwrap().request_type,
            RequestType::Delete
        );
    }

    #[test]
    fn parse_headers_wrong_magic_bytes() {
        use super::TcpError;
        let headers_write = TcpHeaders {
            correlation_id: Uuid::from_u128(1),
            request_type: RequestType::Delete,
            protocol_version: 0,
            flags: 0,
        };
        let mut buf = Vec::<u8>::new();
        buf.resize(headers_write.len() + 4, 0);
        buf[0..4].copy_from_slice(&super::MAGIC_BYTES);
        buf[0] += 1;
        buf[4..].copy_from_slice(&headers_write.to_bytes());
        let headers_read = parse_headers(&buf);
        assert!(matches!(headers_read, Err(TcpError::WrongMagicBytes)));
    }

    use super::{Payload, parse_payload};

    #[test]
    fn parse_payload_delete_ok() {
        let payload_write = Payload {
            key: String::from("key"),
            value: None,
        };
        let buf = payload_write.to_bytes();
        let correlation_id = Uuid::from_u128(20);
        let payload = parse_payload(&buf, &RequestType::Delete, &correlation_id);
        assert!(payload.is_ok());
        assert_eq!(payload_write.key, payload.as_ref().unwrap().key);
        assert!(payload.as_ref().unwrap().value.is_none());
    }

    #[test]
    fn parse_payload_get_ok() {
        let payload_write = Payload {
            key: String::from("key"),
            value: None,
        };
        let buf = payload_write.to_bytes();
        let correlation_id = Uuid::from_u128(20);
        let payload = parse_payload(&buf, &RequestType::Get, &correlation_id);
        assert!(payload.is_ok());
        assert_eq!(payload_write.key, payload.as_ref().unwrap().key);
        assert!(payload.as_ref().unwrap().value.is_none());
    }

    #[test]
    fn parse_payload_put_ok() {
        let payload_write = Payload {
            key: String::from("key"),
            value: Some(String::from("value")),
        };
        let buf = payload_write.to_bytes();
        let correlation_id = Uuid::from_u128(20);
        let payload = parse_payload(&buf, &RequestType::Put, &correlation_id);
        assert!(payload.is_ok());
        assert_eq!(payload_write.key, payload.as_ref().unwrap().key);
        assert!(payload.as_ref().unwrap().value.is_some());
        assert_eq!(
            payload_write.value.as_ref().unwrap(),
            payload.as_ref().unwrap().value.as_ref().unwrap()
        );
    }

    #[test]
    fn parse_payload_put_no_value() {
        let payload_write = Payload {
            key: String::from("key"),
            value: None,
        };
        let buf = payload_write.to_bytes();
        let correlation_id = Uuid::from_u128(20);
        let payload = parse_payload(&buf, &RequestType::Put, &correlation_id);
        assert!(matches!(payload, Err(TcpError::MissingValue(_))));
    }

    #[test]
    fn parse_payload_get_with_value() {
        let payload_write = Payload {
            key: String::from("key"),
            value: Some(String::from("value")),
        };
        let buf = payload_write.to_bytes();
        let correlation_id = Uuid::from_u128(20);
        let payload = parse_payload(&buf, &RequestType::Get, &correlation_id);
        assert!(matches!(payload, Err(TcpError::MalformedPayload(_))));
    }

    #[test]
    fn parse_payload_delete_with_value() {
        let payload_write = Payload {
            key: String::from("key"),
            value: Some(String::from("value")),
        };
        let buf = payload_write.to_bytes();
        let correlation_id = Uuid::from_u128(20);
        let payload = parse_payload(&buf, &RequestType::Delete, &correlation_id);
        assert!(matches!(payload, Err(TcpError::MalformedPayload(_))));
    }
}

/* Test cases:
    1. Ok
        1. Headers
        2. Payload Different types
    2. First 4 bytes are not the magic bytes -> done
    3. Wrong payload
        1. Put but no value -> done
        2. Get or delete but given value
        3. Strings are malformed, i.e. no utf8 bytes
    4. Unsupported version
    5. Unknown request typw

*/
