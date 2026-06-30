use crate::storage_engine::StorageEngineError;
use std::io;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

static PROTOCOL_VERSION: u8 = 0;
static MAGIC_BYTES: [u8; 4] = [0x72, 0x6B, 0x76, 0x73]; //rkvs

/*
TODO:
1. Probably need a separate error for timeout when receiving header/payload and for idle connection timeout
2. Test serialization of new tcp errors and of storage engine errors

*/

#[derive(Debug)]
pub enum TcpError {
    Connection(ConnectionError),
    Parse(ParseError),
}

#[derive(Debug)]
pub enum ParseError {
    UnsupportedVersion(Uuid),
    InvalidRequestType(Uuid),
    MissingValue(Uuid), // only for put
    InvalidKey(Uuid),
    InvalidValue(Uuid),
    UnknownFlags(Uuid),
    MalformedPayload(Uuid), // general errors
}

#[derive(Debug)]
pub enum ConnectionError {
    IoError(io::Error),
    WrongMagicBytes,
    TimedOut,
}

#[derive(Debug, PartialEq)]
pub enum RequestType {
    Get,
    Put,
    Delete,
}

#[derive(Debug, PartialEq)]
pub struct TcpHeaders {
    pub request_type: RequestType,
    pub correlation_id: Uuid,
    pub protocol_version: u8,
    pub flags: u16,
}

#[derive(Debug, PartialEq)]
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
        return Err(TcpError::from(ConnectionError::WrongMagicBytes));
    }
    let correlation_id = Uuid::from_bytes(buf[4..20].try_into().unwrap());
    let version = buf[20];
    if version > PROTOCOL_VERSION {
        return Err(TcpError::from(ParseError::UnsupportedVersion(
            correlation_id,
        )));
    }
    let request_type =
        parse_request_type(buf[21]).map_err(|_| ParseError::InvalidRequestType(correlation_id))?;
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
        .map_err(|_| ParseError::InvalidKey(*correlation_id))?;
    match request_type {
        RequestType::Put => {
            if key_len >= payload_len - 4 {
                Err(TcpError::from(ParseError::MissingValue(*correlation_id)))
            } else {
                let value_len_u32 =
                    u32::from_be_bytes(buf[key_len + 4..key_len + 8].try_into().unwrap());
                let value_len = usize::try_from(value_len_u32).unwrap();
                let value = String::from_utf8(buf[key_len + 8..key_len + 8 + value_len].to_vec())
                    .map_err(|_| ParseError::InvalidValue(*correlation_id))?;
                Ok(Payload {
                    key: key,
                    value: Some(value),
                })
            }
        }
        _ => {
            if key_len < payload_len - 4 {
                Err(TcpError::from(ParseError::MalformedPayload(
                    *correlation_id,
                )))
            } else {
                Ok(Payload {
                    key: key,
                    value: None,
                })
            }
        }
    }
}

async fn recv_headers<T>(reader: &mut T, len: &u32) -> Result<Vec<u8>, TcpError>
where
    T: AsyncReadExt + Unpin,
{
    let mut buf = Vec::<u8>::new();
    let header_len: usize = usize::try_from(*len).unwrap();
    buf.resize(header_len, 0);
    let _ = reader.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn recv_payload<T>(reader: &mut T, len: &u32) -> Result<Vec<u8>, TcpError>
where
    T: AsyncReadExt + Unpin,
{
    let mut buf = Vec::<u8>::new();
    let payload_len: usize = usize::try_from(*len).unwrap();
    buf.resize(payload_len, 0);

    let _ = reader.read_exact(&mut buf).await?;
    Ok(buf)
}

pub async fn recv_tcp_request<T>(stream: &mut T, header_len: u32) -> Result<TcpRequest, TcpError>
where
    T: AsyncReadExt + Unpin,
{
    let headers = recv_headers(stream, &header_len)
        .await
        .and_then(|buf| parse_headers(&buf))?;
    let payload_len = stream.read_u32().await?;
    let payload = recv_payload(stream, &payload_len)
        .await
        .and_then(|buf| parse_payload(&buf, &headers.request_type, &headers.correlation_id))?;
    Ok(TcpRequest { headers, payload })
}

impl From<ParseError> for TcpError {
    fn from(value: ParseError) -> Self {
        TcpError::Parse(value)
    }
}

impl From<ConnectionError> for TcpError {
    fn from(value: ConnectionError) -> Self {
        TcpError::Connection(value)
    }
}

impl From<io::Error> for TcpError {
    fn from(value: io::Error) -> Self {
        TcpError::Connection(ConnectionError::IoError(value))
    }
}

impl TcpRequest {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.resize(8, 0);
        let header_len: u32 = <usize as TryInto<u32>>::try_into(self.headers.len()).unwrap() + 4; // account for magic bytes
        bytes[0..4].copy_from_slice(&header_len.to_be_bytes());
        bytes[4..8].copy_from_slice(&MAGIC_BYTES);
        bytes.append(&mut self.headers.to_bytes().to_vec());
        let payload_len: u32 = self.payload.len().try_into().unwrap();
        bytes.append(&mut payload_len.to_be_bytes().to_vec());
        bytes.append(&mut self.payload.to_bytes().to_vec());
        bytes
    }
}

impl TcpResponse {
    pub fn from_internal_error(correlation_id: &Uuid, error: &StorageEngineError) -> Self {
        TcpResponse {
            correlation_id: *correlation_id,
            response_code: error.to_rc(),
            payload: None,
        }
    }

    pub fn from_tcp_parse_error(error: ParseError) -> Self {
        let correlation_id = error.extract_correlation_id();
        let e = TcpError::from(error);
        TcpResponse {
            correlation_id: correlation_id,
            response_code: e.to_rc(),
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

    pub fn create_close_notification() -> Self {
        let error = TcpError::from(ConnectionError::TimedOut);
        TcpResponse {
            correlation_id: uuid::Uuid::nil(),
            response_code: error.to_rc(),
            payload: None,
        }
    }

    pub fn len(&self) -> usize {
        match &self.payload {
            Some(str) => str.len() + 17,
            None => 17,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::<u8>::new();
        buf.resize(self.len(), 0);
        buf[0..16].copy_from_slice(self.correlation_id.as_bytes());
        buf[16] = self.response_code;
        match &self.payload {
            None => buf,
            Some(str) => {
                buf[17..].copy_from_slice(str.as_bytes());
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
    pub fn len(&self) -> usize {
        match &self.value {
            Some(value) => 8 + self.key.len() + value.len(),
            None => 4 + self.key.len(),
        }
    }
}

impl TcpError {
    pub fn to_rc(&self) -> u8 {
        match self {
            Self::Parse(e) => match e {
                ParseError::InvalidKey(_) => 4,
                ParseError::InvalidValue(_) => 5,
                ParseError::MissingValue(_) => 6,
                ParseError::MalformedPayload(_) => 7,
                ParseError::InvalidRequestType(_) => 8,
                ParseError::UnknownFlags(_) => 9,
                ParseError::UnsupportedVersion(_) => 10,
            },
            Self::Connection(e) => match e {
                ConnectionError::IoError(_) => 255,
                ConnectionError::TimedOut => 11,
                ConnectionError::WrongMagicBytes => 255,
            },
        }
    }
}

impl ParseError {
    pub fn extract_correlation_id(&self) -> Uuid {
        match self {
            ParseError::InvalidKey(c)
            | ParseError::InvalidRequestType(c)
            | ParseError::InvalidValue(c)
            | ParseError::MalformedPayload(c)
            | ParseError::MissingValue(c)
            | ParseError::UnknownFlags(c)
            | ParseError::UnsupportedVersion(c) => c.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ConnectionError, MAGIC_BYTES, ParseError, Payload, RequestType, TcpError, TcpHeaders,
        TcpRequest, parse_headers, parse_payload,
    };
    use uuid::Uuid;

    #[test]
    fn parse_headers_ok_delete() {
        let headers_write = TcpHeaders {
            correlation_id: Uuid::from_u128(1),
            request_type: RequestType::Delete,
            protocol_version: 0,
            flags: 0,
        };
        let mut buf = Vec::<u8>::new();
        buf.resize(headers_write.len() + 4, 0);
        buf[0..4].copy_from_slice(&MAGIC_BYTES);
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
    fn parse_headers_ok_get() {
        let headers_write = TcpHeaders {
            correlation_id: Uuid::from_u128(64),
            request_type: RequestType::Get,
            protocol_version: 0,
            flags: 0,
        };
        let mut buf = Vec::<u8>::new();
        buf.resize(headers_write.len() + 4, 0);
        buf[0..4].copy_from_slice(&MAGIC_BYTES);
        buf[4..].copy_from_slice(&headers_write.to_bytes());
        let headers_read = parse_headers(&buf);
        assert!(headers_read.is_ok());
        assert_eq!(headers_read.as_ref().unwrap().correlation_id.as_u128(), 64);
        assert_eq!(headers_read.as_ref().unwrap().flags, 0);
        assert_eq!(headers_read.as_ref().unwrap().protocol_version, 0);
        assert_eq!(
            headers_read.as_ref().unwrap().request_type,
            RequestType::Get
        );
    }

    #[test]
    fn parse_headers_ok_put() {
        let headers_write = TcpHeaders {
            correlation_id: Uuid::from_u128(1024),
            request_type: RequestType::Put,
            protocol_version: 0,
            flags: 0,
        };
        let mut buf = Vec::<u8>::new();
        buf.resize(headers_write.len() + 4, 0);
        buf[0..4].copy_from_slice(&MAGIC_BYTES);
        buf[4..].copy_from_slice(&headers_write.to_bytes());
        let headers_read = parse_headers(&buf);
        assert!(headers_read.is_ok());
        assert_eq!(
            headers_read.as_ref().unwrap().correlation_id.as_u128(),
            1024
        );
        assert_eq!(headers_read.as_ref().unwrap().flags, 0);
        assert_eq!(headers_read.as_ref().unwrap().protocol_version, 0);
        assert_eq!(
            headers_read.as_ref().unwrap().request_type,
            RequestType::Put
        );
    }

    #[test]
    fn parse_headers_wrong_version() {
        let headers_write = TcpHeaders {
            correlation_id: Uuid::from_u128(1),
            request_type: RequestType::Delete,
            protocol_version: 1,
            flags: 0,
        };
        let mut buf = Vec::<u8>::new();
        buf.resize(headers_write.len() + 4, 0);
        buf[0..4].copy_from_slice(&MAGIC_BYTES);
        buf[4..].copy_from_slice(&headers_write.to_bytes());
        let headers_read = parse_headers(&buf);
        assert!(matches!(
            headers_read,
            Err(TcpError::Parse(ParseError::UnsupportedVersion(_)))
        ))
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
        buf[0..4].copy_from_slice(&MAGIC_BYTES);
        buf[0] += 1;
        buf[4..].copy_from_slice(&headers_write.to_bytes());
        let headers_read = parse_headers(&buf);
        assert!(matches!(
            headers_read,
            Err(TcpError::Connection(ConnectionError::WrongMagicBytes))
        ));
    }

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
        assert!(matches!(
            payload,
            Err(TcpError::Parse(ParseError::MissingValue(_)))
        ));
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
        assert!(matches!(
            payload,
            Err(TcpError::Parse(ParseError::MalformedPayload(_)))
        ));
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
        assert!(matches!(
            payload,
            Err(TcpError::Parse(ParseError::MalformedPayload(_)))
        ));
    }

    fn parse_tcp_request(bytes: &Vec<u8>) -> Result<TcpRequest, TcpError> {
        let header_len = u32::from_be_bytes(*bytes[0..4].as_array().unwrap());
        let header_len_usize = usize::try_from(header_len).unwrap();
        let headers = parse_headers(&bytes[4..4 + header_len_usize].to_vec())?;
        let payload = parse_payload(
            &bytes[8 + header_len_usize..].to_vec(),
            &headers.request_type,
            &headers.correlation_id,
        )?;
        Ok(TcpRequest {
            headers: headers,
            payload: payload,
        })
    }

    #[test]
    fn serialize_request_no_value() {
        let headers = TcpHeaders {
            correlation_id: Uuid::from_u128(1024),
            request_type: RequestType::Get,
            protocol_version: 0,
            flags: 0,
        };
        let payload = Payload {
            key: String::from("key"),
            value: None,
        };
        let request = TcpRequest {
            headers: headers,
            payload: payload,
        };
        let bytes = request.to_bytes();
        let request_read = parse_tcp_request(&bytes);
        assert!(matches!(request_read, Ok(_)));
        assert_eq!(request_read.as_ref().unwrap().headers, request.headers);
        assert_eq!(request_read.as_ref().unwrap().payload, request.payload);
    }
}

/* Test cases:
    1. Ok
        1. Headers -> done
        2. Payload Different types -> done
    2. First 4 bytes are not the magic bytes -> done
    3. Wrong payload
        1. Put but no value -> done
        2. Get or delete but given value -> done
        3. Strings are malformed, i.e. no utf8 bytes
    4. Unsupported version -> done
    5. Unknown request type
    6. payload too large
    7. Response:
        1. length correct with and without payload
        2. Correct (de)serialization
*/
