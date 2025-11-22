use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::collections::btree_map::VacantEntry;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Result;
use anyhow::bail;
use ignore::overrides::Override;
use same_file::is_same_file;
use tracing::debug;

use crate::Metadata24;
use crate::PathWriter;
use crate::SDistWriter;
use crate::WheelWriter;
use crate::write_dist_info;

use super::ModuleWriter;
use super::ModuleWriterInternal;

/// This is a 'virtual' [ModuleWriter] that defers writing the files into the archive
/// until the end. It provides 3 primary features:
/// 1. Policy enforcement for included files (e.g. handles exclusions, duplicate tracking)
/// 2. Writes files to archive ordered by the target path for reproducibility
/// 3. Orders `.dist-info/` files at the end of the archive as per recommendation in PEP-427
pub struct VirtualWriter<W> {
    inner: W,
    entries: BTreeMap<PathBuf, ArchiveSource>,
    excludes: Override,
}

impl<W> VirtualWriter<W>
where
    W: ModuleWriterInternal,
{
    /// Constructs a virtual writer that wraps around the given [ModuleWriterInternal]
    /// and uses the provided exclusion filter.
    pub fn new(inner: W, excludes: Override) -> Self {
        Self {
            inner,
            entries: BTreeMap::new(),
            excludes,
        }
    }

    /// Takes all the files that have been tracked and writes them to the underlying archive,
    /// and returns the archive when finished
    fn finalize(mut self) -> Result<W> {
        // PEP-427 recommends that `.dist-info` is placed physically at the end of the zip file
        let (dist_info_entries, other_entries): (Vec<_>, Vec<_>) = self
            .entries
            .into_iter()
            .partition(|(path, _)| match path.components().next() {
                Some(component) => component
                    .as_os_str()
                    .to_string_lossy()
                    .ends_with(".dist-info"),
                None => false,
            });

        for collection in [other_entries, dist_info_entries] {
            for (target, source) in collection {
                match source {
                    ArchiveSource::Generated(source, data, executable) => self.inner.add_bytes(
                        target,
                        source.as_deref(),
                        data.as_slice(),
                        executable,
                    )?,
                    ArchiveSource::File(source, executable) => {
                        self.inner.add_file(target, source, executable)?
                    }
                }
            }
        }

        Ok(self.inner)
    }

    fn get_entry(
        &mut self,
        target: PathBuf,
        source: Option<&Path>,
    ) -> Result<Option<VacantEntry<'_, PathBuf, ArchiveSource>>> {
        // First, check excludes to see if the file is allowed
        if let Some(source) = source {
            if self.exclude(source) {
                debug!("Excluding source file {source:?}");
                return Ok(None);
            }
        }
        if self.exclude(&target) {
            debug!("Excluding target file {target:?}");
            return Ok(None);
        }

        // Then check for duplicates
        let occupied = match self.entries.entry(target.clone()) {
            Entry::Vacant(entry) => return Ok(Some(entry)),
            Entry::Occupied(entry) => entry,
        };
        match (occupied.get().path(), source) {
            (None, None) => {
                bail!(
                    "Generated file {} was already added, can't add it again",
                    target.display()
                );
            }
            (Some(previous_source), None) => {
                bail!(
                    "File {} was already added from {}, can't overwrite with generated file",
                    target.display(),
                    previous_source.display()
                )
            }
            (None, Some(source)) => {
                bail!(
                    "Generated file {} was already added, can't overwrite it with {}",
                    target.display(),
                    source.display()
                );
            }
            (Some(previous_source), Some(source)) => {
                if is_same_file(source, previous_source).unwrap_or(false) {
                    // Ignore identical duplicate files
                    Ok(None)
                } else {
                    bail!(
                        "File {} was already added from {}, can't add it from {}",
                        target.display(),
                        previous_source.display(),
                        source.display()
                    );
                }
            }
        }
    }

    fn exclude(&self, path: impl AsRef<Path>) -> bool {
        self.excludes.matched(path.as_ref(), false).is_whitelist()
    }
}

impl VirtualWriter<PathWriter> {
    /// Closes this writer by writing out the files to disk, returning the [PathWriter]
    pub fn finalize_path(self) -> Result<PathWriter> {
        self.finalize()
    }
}

impl VirtualWriter<SDistWriter> {
    /// Closes this writer by closing the tar file and writing it to disk, returning
    /// the path to the written tar file
    pub fn finalize_sdist(self) -> Result<PathBuf> {
        let path = self.finalize()?.finish()?;
        Ok(path)
    }
}

impl VirtualWriter<WheelWriter> {
    /// Closes this writer by writing the .dist-info and closing the zip file, returning
    /// the path to the written zip file
    pub fn finalize_wheel(
        mut self,
        pyproject_dir: &Path,
        metadata24: &Metadata24,
        tags: &[String],
    ) -> Result<PathBuf> {
        write_dist_info(&mut self, pyproject_dir, metadata24, tags)?;
        let inner = self.finalize()?;
        let res = inner.finish(metadata24)?;
        Ok(res)
    }
}

#[cfg(test)]
impl VirtualWriter<super::tests::MockWriter> {
    /// Closes this writer by writing the files to the MockWriter, returning the
    /// MockWriter
    pub fn finalize_mock(self) -> Result<super::tests::MockWriter> {
        self.finalize()
    }
}

impl<W> ModuleWriter for VirtualWriter<W>
where
    W: ModuleWriterInternal,
{
    fn add_bytes(
        &mut self,
        target: impl AsRef<Path>,
        source: Option<&Path>,
        data: Vec<u8>,
        executable: bool,
    ) -> Result<()> {
        let target = target.as_ref().to_path_buf();
        let source = source.map(|p| p.to_path_buf());
        let source = ArchiveSource::Generated(source, data, executable);

        let entry = self.get_entry(target, source.path())?;
        let Some(entry) = entry else {
            // Ignore duplicate files.
            return Ok(());
        };

        entry.insert(source);
        Ok(())
    }

    fn add_file(
        &mut self,
        target: impl AsRef<Path>,
        source: impl AsRef<Path>,
        executable: bool,
    ) -> Result<()> {
        let target = target.as_ref().to_path_buf();
        let source = source.as_ref().to_path_buf();
        let source = ArchiveSource::File(source, executable);

        let entry = self.get_entry(target, source.path())?;
        let Some(entry) = entry else {
            // Ignore duplicate files.
            return Ok(());
        };

        entry.insert(source);
        Ok(())
    }

    fn add_empty_file(&mut self, target: impl AsRef<Path>) -> Result<()> {
        self.add_bytes(target, None, Vec::new(), false)
    }
}

pub enum ArchiveSource {
    Generated(Option<PathBuf>, Vec<u8>, bool),
    File(PathBuf, bool),
}

impl ArchiveSource {
    fn path(&self) -> Option<&Path> {
        match self {
            Self::Generated(opt, _, _) => opt.as_deref(),
            Self::File(path, _) => Some(path),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use anyhow::Result;
    use ignore::overrides::Override;
    use ignore::overrides::OverrideBuilder;
    use insta::assert_snapshot;
    use tempfile::TempDir;

    use crate::ModuleWriter;
    use crate::module_writer::tests::MockWriter;

    use super::VirtualWriter;

    #[test]
    fn virtual_writer_no_excludes() -> Result<()> {
        let mut writer = VirtualWriter::new(MockWriter::default(), Override::empty());

        assert!(writer.entries.is_empty());
        assert!(writer.inner.files.is_empty());

        writer.add_bytes("test", Some(Path::new("test")), Vec::new(), true)?;

        assert_eq!(writer.entries.len(), 1);
        assert!(writer.inner.files.is_empty());

        let writer = writer.finalize()?;

        assert_eq!(writer.files.len(), 1);
        Ok(())
    }

    #[test]
    fn virtual_writer_excludes() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let mut excludes = OverrideBuilder::new(&tmp_dir);
        excludes.add("test*")?;
        excludes.add("!test2*")?;
        let mut writer = VirtualWriter::new(MockWriter::default(), excludes.build()?);

        writer.add_bytes("test1", Some(Path::new("test1")), Vec::new(), true)?;
        writer.add_bytes("test3", Some(Path::new("test3")), Vec::new(), true)?;

        assert!(writer.entries.is_empty());
        assert!(writer.inner.files.is_empty());

        writer.add_bytes("test215", Some(Path::new("test2")), Vec::new(), true)?;
        writer.add_bytes("test214", Some(Path::new("test2")), Vec::new(), true)?;
        writer.add_bytes("test213", Some(Path::new("test2")), Vec::new(), true)?;
        writer.add_bytes("test212", Some(Path::new("test2")), Vec::new(), true)?;

        assert_eq!(writer.entries.len(), 4);
        assert!(writer.inner.files.is_empty());

        writer.add_bytes("yes", Some(Path::new("yes")), Vec::new(), true)?;

        assert_eq!(writer.entries.len(), 5);
        assert!(writer.inner.files.is_empty());

        let writer = writer.finalize()?;
        assert_eq!(writer.files.len(), 5);
        tmp_dir.close()?;

        assert_snapshot!(writer.files.join("\n").replace("\\", "/"), @r"
        test212
        test213
        test214
        test215
        yes
        ");
        Ok(())
    }
}
