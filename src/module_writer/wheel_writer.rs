use std::collections::BTreeMap;
use std::io;
use std::io::Read;
use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context as _;
use anyhow::Result;
use fs_err::File;
use tracing::debug;
use zip::ZipWriter;
use zip::write::SimpleFileOptions;

use crate::Metadata24;

use super::ModuleWriterInternal;
use super::default_permission;
use super::util::StreamSha256;

/// A glorified zip builder, mostly useful for writing the record file of a wheel
pub struct WheelWriter {
    zip: ZipWriter<File>,
    record: BTreeMap<PathBuf, (String, usize)>,
    file_options: SimpleFileOptions,
}

impl ModuleWriterInternal for WheelWriter {
    fn add_bytes(
        &mut self,
        target: impl AsRef<Path>,
        _source: Option<&Path>,
        mut data: impl Read,
        executable: bool,
    ) -> Result<()> {
        let target = target.as_ref();

        let options = self
            .file_options
            .unix_permissions(default_permission(executable));
        self.zip.start_file_from_path(target, options)?;
        let mut writer = StreamSha256::new(&mut self.zip);

        io::copy(&mut data, &mut writer)
            .with_context(|| format!("Failed to write to zip archive for {target:?}"))?;

        let (hash, length) = writer.finalize()?;
        self.record.insert(target.to_path_buf(), (hash, length));

        Ok(())
    }
}

impl WheelWriter {
    /// Create a new wheel file which can be subsequently expanded
    ///
    /// Adds the .dist-info directory and the METADATA file in it
    pub fn new(
        tag: &str,
        wheel_dir: &Path,
        metadata24: &Metadata24,
        file_options: SimpleFileOptions,
    ) -> Result<WheelWriter, io::Error> {
        let wheel_path = wheel_dir.join(format!(
            "{}-{}-{}.whl",
            metadata24.get_distribution_escaped(),
            metadata24.get_version_escaped(),
            tag
        ));

        let file = File::create(wheel_path)?;

        let builder = WheelWriter {
            zip: ZipWriter::new(file),
            record: BTreeMap::new(),
            file_options,
        };
        Ok(builder)
    }

    /// Creates the record file and finishes the zip
    pub fn finish(mut self, metadata24: &Metadata24) -> Result<PathBuf, io::Error> {
        let options = self
            .file_options
            .unix_permissions(default_permission(false));
        let record_filename = metadata24.get_dist_info_dir().join("RECORD");
        debug!("Adding {}", record_filename.display());
        self.zip.start_file_from_path(&record_filename, options)?;

        for (filename, (hash, len)) in self.record {
            let filename = filename.to_string_lossy();
            writeln!(self.zip, "{filename},sha256={hash},{len}")?;
        }
        // Write the record for the RECORD file itself
        writeln!(self.zip, "{},,", record_filename.display())?;

        let file = self.zip.finish()?;
        Ok(file.into_path())
    }
}

#[cfg(test)]
mod tests {
    use pep440_rs::Version;
    use tempfile::TempDir;

    use crate::CompressionMethod;
    use crate::CompressionOptions;
    use crate::Metadata24;

    use super::WheelWriter;

    #[test]
    fn wheel_writer_no_compression() -> Result<(), Box<dyn std::error::Error>> {
        let metadata = Metadata24::new("dummy".to_string(), Version::new([1, 0]));
        let tmp_dir = TempDir::new()?;
        let compression_options = CompressionOptions {
            compression_method: CompressionMethod::Stored,
            ..Default::default()
        };

        let writer = WheelWriter::new(
            "no compression",
            tmp_dir.path(),
            &metadata,
            compression_options.get_file_options(),
        )?;

        writer.finish(&metadata)?;
        tmp_dir.close()?;

        Ok(())
    }
}
