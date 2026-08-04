use std::io;

pub(crate) trait PositionalFileIo {
    fn read_at_position(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;
    fn write_at_position(&self, buf: &[u8], offset: u64) -> io::Result<usize>;
}

#[cfg(any(unix, target_os = "wasi"))]
impl PositionalFileIo for cap_std::fs::File {
    fn read_at_position(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        cap_std::fs::FileExt::read_at(self, buf, offset)
    }

    fn write_at_position(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        cap_std::fs::FileExt::write_at(self, buf, offset)
    }
}

#[cfg(windows)]
impl PositionalFileIo for cap_std::fs::File {
    fn read_at_position(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        cap_std::fs::FileExt::seek_read(self, buf, offset)
    }

    fn write_at_position(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        cap_std::fs::FileExt::seek_write(self, buf, offset)
    }
}
