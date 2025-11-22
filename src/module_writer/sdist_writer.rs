use std::io;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context as _;
use anyhow::Result;
use flate2::Compression;
use flate2::write::GzEncoder;
use fs_err as fs;
use normpath::PathExt as _;

use crate::Metadata24;

use super::ModuleWriterInternal;
use super::default_permission;

/// A deterministic, arbitrary, non-zero timestamp that use used as `mtime`
/// of headers when writing sdists.
///
/// This value, copied from the tar crate, corresponds to _Jul 23, 2006_,
/// which is the date of the first commit for what would become Rust.
///
/// This value is used instead of unix epoch 0 because some tools do not handle
/// the 0 value properly (See rust-lang/cargo#9512).
const SDIST_DETERMINISTIC_TIMESTAMP: u64 = 1153704088;

/// Creates a .tar.gz archive containing the source distribution
pub struct SDistWriter {
    tar: tar::Builder<GzEncoder<Vec<u8>>>,
    path: PathBuf,
    mtime: u64,
}

impl ModuleWriterInternal for SDistWriter {
    fn add_bytes(
        &mut self,
        target: impl AsRef<Path>,
        _source: Option<&Path>,
        mut data: impl Read,
        executable: bool,
    ) -> Result<()> {
        let target = target.as_ref();

        let mut buffer = Vec::new();
        data.read_to_end(&mut buffer)
            .with_context(|| format!("Failed to read data into buffer for {}", target.display()))?;

        let mut header = tar::Header::new_gnu();
        header.set_entry_type(tar::EntryType::Regular);
        header.set_size(buffer.len() as u64);
        header.set_mode(default_permission(executable));
        header.set_mtime(self.mtime);
        self.tar
            .append_data(&mut header, target, buffer.as_slice())
            .with_context(|| {
                format!(
                    "Failed to add {} bytes to sdist as {}",
                    buffer.len(),
                    target.display()
                )
            })?;
        Ok(())
    }
}

impl SDistWriter {
    /// Create a source distribution .tar.gz which can be subsequently expanded
    pub fn new(
        wheel_dir: impl AsRef<Path>,
        metadata24: &Metadata24,
        mtime_override: Option<u64>,
    ) -> Result<Self, io::Error> {
        let path = wheel_dir
            .as_ref()
            .normalize()?
            .join(format!(
                "{}-{}.tar.gz",
                &metadata24.get_distribution_escaped(),
                &metadata24.get_version_escaped()
            ))
            .into_path_buf();

        let enc = GzEncoder::new(Vec::new(), Compression::default());
        let mut tar = tar::Builder::new(enc);
        tar.mode(tar::HeaderMode::Deterministic);

        Ok(Self {
            tar,
            path,
            mtime: mtime_override.unwrap_or(SDIST_DETERMINISTIC_TIMESTAMP),
        })
    }

    /// Finished the .tar.gz archive
    pub fn finish(self) -> Result<PathBuf, io::Error> {
        let archive = self.tar.into_inner()?;
        fs::write(&self.path, archive.finish()?)?;
        Ok(self.path)
    }
}
