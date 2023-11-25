use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
};

pub struct Reader {
    inner: File,
}

impl Seek for Reader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl Read for Reader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl Reader {
    pub fn pos(&mut self) -> std::io::Result<u64> {
        self.inner.stream_position()
    }

    pub fn new(file: File) -> Self {
        Self { inner: file }
    }
}

pub struct Writer {
    inner: File,
}

impl Write for Writer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for Writer {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl Writer {
    pub fn pos(&mut self) -> std::io::Result<u64> {
        self.inner.stream_position()
    }

    pub fn new(file: File) -> Self {
        Self { inner: file }
    }
}
