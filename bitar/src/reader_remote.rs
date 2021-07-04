use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use core::pin::Pin;
use core::task::{Context, Poll};
use futures_util::{ready, stream::Stream, StreamExt};
use reqwest::{RequestBuilder, Url};
use std::time::Duration;

use crate::{
    http_range_request,
    reader::{ReadAt, Reader},
};

#[derive(Debug)]
pub enum ReaderRemoteError {
    UnexpectedEnd,
    RequestNotClonable,
    Http(reqwest::Error),
}
impl std::error::Error for ReaderRemoteError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ReaderRemoteError::Http(err) => Some(err),
            ReaderRemoteError::UnexpectedEnd | ReaderRemoteError::RequestNotClonable => None,
        }
    }
}
impl std::fmt::Display for ReaderRemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnexpectedEnd => write!(f, "unexpected end"),
            Self::RequestNotClonable => write!(f, "request is not clonable"),
            Self::Http(_) => write!(f, "http error"),
        }
    }
}
impl From<reqwest::Error> for ReaderRemoteError {
    fn from(e: reqwest::Error) -> Self {
        Self::Http(e)
    }
}

/// Read archive from remote location.
pub struct ReaderRemote {
    request_builder: RequestBuilder,
    retry_count: u32,
    retry_delay: Duration,
}

impl ReaderRemote {
    /// Create a remote archive reader using RequestBuilder for the http request.
    pub fn from_request(request_builder: RequestBuilder) -> Self {
        Self {
            request_builder,
            retry_count: 0,
            retry_delay: Duration::from_secs(0),
        }
    }
    /// Create a remote archive reader using an URL and default parameters for the request.
    pub fn from_url(url: Url) -> Self {
        Self::from_request(reqwest::Client::new().get(url))
    }
    /// Set number of times to retry on failure
    ///
    /// The reader will try to reconnect and continue download from where the failure occured.
    /// Any progress made so far should not be lost.
    pub fn retries(mut self, retry_count: u32) -> Self {
        self.retry_count = retry_count;
        self
    }
    /// Set a delay between attempts to reconnect to the remote server.
    ///
    /// On failure the reader will wait for the given time before trying to reconnect.
    pub fn retry_delay(mut self, retry_delay: Duration) -> Self {
        self.retry_delay = retry_delay;
        self
    }
    fn read_chunk_stream(
        &mut self,
        chunks: Vec<ReadAt>,
    ) -> impl Stream<Item = Result<Bytes, ReaderRemoteError>> + '_ {
        ChunkReader {
            request_builder: &self.request_builder,
            chunk_buf: BytesMut::new(),
            chunk_index: 0,
            num_adjacent_reads: 0,
            chunks,
            retry_count: self.retry_count,
            retry_delay: self.retry_delay,
            request: None,
        }
    }
}

struct ChunkReader<'a> {
    request_builder: &'a RequestBuilder,
    chunk_buf: BytesMut,
    chunks: Vec<ReadAt>,
    chunk_index: usize,
    num_adjacent_reads: usize,
    retry_count: u32,
    retry_delay: Duration,
    request: Option<http_range_request::Builder>,
}

impl<'a> ChunkReader<'a>
where
    Self: Unpin,
{
    fn poll_read(&mut self, cx: &mut Context) -> Poll<Option<Result<Bytes, ReaderRemoteError>>> {
        loop {
            let chunks = &self.chunks[self.chunk_index..];
            if chunks.is_empty() {
                // No more chunks to fetch.
                return Poll::Ready(None);
            }
            let next = &chunks[0];
            if self.chunk_buf.len() >= next.size {
                self.chunk_index += 1;
                let chunk = self.chunk_buf.split_to(next.size).freeze();
                self.num_adjacent_reads -= 1;
                if self.num_adjacent_reads == 0 {
                    // Time to make another request.
                    self.request = None;
                }
                return Poll::Ready(Some(Ok(chunk)));
            }
            if self.request.is_none() {
                // Create a new range request.
                let request_builder = self
                    .request_builder
                    .try_clone()
                    .ok_or(ReaderRemoteError::RequestNotClonable)?;

                self.num_adjacent_reads = Self::adjacent_reads(chunks);
                let last_adjacent = &chunks[self.num_adjacent_reads - 1];
                let total_size = last_adjacent.end_offset() - next.offset;
                self.chunk_buf.clear();
                self.request = Some(
                    http_range_request::Builder::new(request_builder, next.offset, total_size)
                        .retry(self.retry_count, self.retry_delay),
                );
            };

            // Poll for chunks.
            let request = self.request.as_mut().unwrap();
            match ready!(request.poll_next_unpin(cx)) {
                Some(Ok(chunk)) => {
                    self.chunk_buf.extend(chunk);
                }
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                None => return Poll::Ready(Some(Err(ReaderRemoteError::UnexpectedEnd))),
            }
        }
    }
    fn adjacent_reads(chunks: &[ReadAt]) -> usize {
        chunks
            .windows(2)
            .take_while(|p| (p[0].offset + p[0].size as u64) == p[1].offset)
            .count()
            + 1
    }
}

impl<'a> Stream for ChunkReader<'a> {
    type Item = Result<Bytes, ReaderRemoteError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        self.poll_read(cx)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let chunks_left = self.chunks.len() - self.chunk_index;
        (chunks_left, Some(chunks_left))
    }
}

#[async_trait]
impl Reader for ReaderRemote {
    type Error = ReaderRemoteError;
    async fn read_at(&mut self, offset: u64, size: usize) -> Result<Bytes, ReaderRemoteError> {
        let request = http_range_request::Builder::new(
            self.request_builder
                .try_clone()
                .ok_or(ReaderRemoteError::RequestNotClonable)?,
            offset,
            size as u64,
        )
        .retry(self.retry_count, self.retry_delay);

        let mut res = request.single().await?;
        if res.len() >= size {
            // Truncate the response if bigger than requested size
            Ok(res.split_to(size))
        } else if res.len() < size {
            Err(ReaderRemoteError::UnexpectedEnd)
        } else {
            Ok(res)
        }
    }
    fn read_chunks<'a>(
        &'a mut self,
        chunks: Vec<ReadAt>,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes, ReaderRemoteError>> + Send + 'a>> {
        Box::pin(self.read_chunk_stream(chunks))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyper::service::{make_service_fn, service_fn};

    async fn new_server(listener: std::net::TcpListener, data: Vec<u8>) {
        hyper::Server::from_tcp(listener)
            .unwrap()
            .serve(make_service_fn(move |_conn| {
                let data = data.clone();
                async move {
                    Ok::<_, std::convert::Infallible>(service_fn(move |req| {
                        // Only respond with the requested range of bytes
                        let range = req
                            .headers()
                            .get("range")
                            .expect("range header")
                            .to_str()
                            .unwrap()[6..]
                            .split('-')
                            .map(|s| s.parse::<u64>().unwrap())
                            .collect::<Vec<u64>>();
                        let start = range[0] as usize;
                        let end = std::cmp::min(range[1] as usize + 1, data.len());
                        let data = data[start..end].to_vec();
                        async move {
                            Ok::<_, hyper::Error>(hyper::Response::new(hyper::Body::from(data)))
                        }
                    }))
                }
            }))
            .await
            .unwrap();
    }
    fn new_reader(port: u16) -> ReaderRemote {
        ReaderRemote::from_url(Url::parse(&format!("http://127.0.0.1:{}", port)).unwrap())
    }
    fn new_listener() -> (std::net::TcpListener, u16) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, port)
    }

    #[test]
    fn one_adjacent_reads() {
        let chunks = vec![ReadAt::new(0, 1), ReadAt::new(10, 1)];
        assert_eq!(ChunkReader::adjacent_reads(&chunks[..]), 1);
    }

    #[test]
    fn two_adjacent_reads() {
        let chunks = vec![ReadAt::new(0, 1), ReadAt::new(1, 3), ReadAt::new(10, 3)];
        assert_eq!(ChunkReader::adjacent_reads(&chunks[..]), 2);
    }

    #[test]
    fn multiple_adjacent_reads() {
        let chunks = vec![
            ReadAt::new(0, 1),
            ReadAt::new(1, 3),
            ReadAt::new(4, 3),
            ReadAt::new(7, 3),
            ReadAt::new(50, 3),
        ];
        assert_eq!(ChunkReader::adjacent_reads(&chunks[..]), 4);
    }

    #[test]
    fn builder() {
        let reader = ReaderRemote::from_url(Url::parse("http://localhost/file").unwrap())
            .retries(3)
            .retry_delay(Duration::from_secs(10));
        assert_eq!(reader.retry_delay, Duration::from_secs(10));
        assert_eq!(reader.retry_count, 3);
        let request = reader.request_builder.build().unwrap();
        assert_eq!(request.url(), &Url::parse("http://localhost/file").unwrap());
        assert_eq!(request.method(), reqwest::Method::GET);
    }
    #[tokio::test]
    async fn read_single() {
        let expect = vec![1, 2, 3, 4, 5, 6];
        let (listener, port) = new_listener();
        let server = new_server(listener, expect.clone());
        let mut reader = new_reader(port);
        let read = reader.read_at(0, expect.len());
        tokio::select! {
            _ = server => panic!("server ended"),
            data = read => assert_eq!(data.unwrap(), expect),
        };
    }
    #[tokio::test]
    async fn read_single_offset() {
        let expect = vec![1, 2, 3, 4, 5, 6];
        let (listener, port) = new_listener();
        let server = new_server(listener, expect.clone());
        let mut reader = new_reader(port);
        let read = reader.read_at(1, expect.len() - 1);
        tokio::select! {
            _ = server => panic!("server ended"),
            data = read => assert_eq!(&data.unwrap()[..], &expect[1..]),
        };
    }
    #[tokio::test]
    async fn read_single_zero() {
        let (listener, port) = new_listener();
        let server = new_server(listener, vec![1, 2, 3, 4, 5, 6]);
        let mut reader = new_reader(port);
        let read = reader.read_at(1, 0);
        tokio::select! {
            _ = server => panic!("server ended"),
            data = read => assert_eq!(&data.unwrap()[..], &[]),
        };
    }
    #[tokio::test]
    async fn unexpected_end() {
        let (listener, port) = new_listener();
        let server = new_server(listener, vec![1, 2, 3, 4, 5, 6]);
        let mut reader = new_reader(port);
        let read = reader.read_at(0, 10);
        tokio::select! {
            _ = server => panic!("server ended"),
            data = read => match data.unwrap_err() { ReaderRemoteError::UnexpectedEnd => {} err=> panic!("{}", err) },
        };
    }
    #[tokio::test]
    async fn read_chunks() {
        let expect = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];
        let (listener, port) = new_listener();
        let server = new_server(listener, expect.clone());
        let mut reader = new_reader(port);
        let chunks = vec![
            ReadAt { offset: 0, size: 6 },
            ReadAt {
                offset: 6,
                size: 10,
            },
            ReadAt {
                offset: 16,
                size: 4,
            },
        ];
        let stream = reader.read_chunks(chunks).map(|v| v.expect("item"));
        tokio::select! {
            _ = server => panic!("server ended"),
            chunks = stream.collect::<Vec<Bytes>>() => assert_eq!(chunks, vec![
                Bytes::from(vec![1, 2, 3, 4, 5, 6]), Bytes::from(vec![7, 8, 9, 10, 11, 12, 13, 14, 15, 16]), Bytes::from(vec![17, 18, 19, 20]),
            ]),
        };
    }
    #[tokio::test]
    async fn read_chunks_with_offset() {
        let expect = vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
        ];
        let (listener, port) = new_listener();
        let server = new_server(listener, expect.clone());
        let mut reader = new_reader(port);
        let chunks = vec![
            ReadAt { offset: 2, size: 4 },
            ReadAt {
                offset: 6,
                size: 10,
            },
            ReadAt {
                offset: 16,
                size: 4,
            },
        ];
        let stream = reader.read_chunks(chunks).map(|v| v.expect("item"));
        tokio::select! {
            _ = server => panic!("server ended"),
            chunks = stream.collect::<Vec<Bytes>>() => assert_eq!(chunks, vec![
                Bytes::from(vec![3, 4, 5, 6]), Bytes::from(vec![7, 8, 9, 10, 11, 12, 13, 14, 15, 16]), Bytes::from(vec![17, 18, 19, 20]),
            ]),
        };
    }
    #[tokio::test]
    async fn connection_timeout() {
        let (listener, port) = new_listener();
        let _server = new_server(listener, vec![]);
        let mut reader = ReaderRemote::from_request(
            reqwest::Client::new()
                .get(Url::parse(&format!("http://127.0.0.1:{}", port)).unwrap())
                .timeout(Duration::from_millis(5)),
        );
        let chunks = vec![ReadAt { offset: 0, size: 1 }];
        match reader.read_chunks(chunks).next().await {
            Some(Err(ReaderRemoteError::Http(reqwest::Error { .. }))) => {}
            _ => panic!("unexpected result"),
        };
    }
}
