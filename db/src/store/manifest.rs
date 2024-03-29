use std::{fs, io::ErrorKind, path::PathBuf, usize};
use anyhow::{anyhow, bail, Result};
use prost::Message;
use tokio::fs::{create_dir_all, File, metadata, OpenOptions, read_dir, remove_file, rename,};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, SeekFrom};
use crate::error::Error;
use crate::store::meta::VersionEdit;


const CURRENT_FILE_NAME: &str = "CURRENT";
const MANIFEST_FILE_NAME: &str = "MANIFEST";
const TEMPLE_SUFFIX: &str = "tmpdb";
const MAX_MANIFEST_SIZE: u64 = 128 << 20; // 128 MiB

pub(crate) struct Manifest {
    base: PathBuf,
    base_dir: Option<File>,
    max_file_size: u64,
    next_file_id: u32,

    current_file_num: Option<u32>,
    current_writer: Option<ManifestWriter>,
}

struct ManifestWriter {
    current_file_size: u64,
    current_writer: File,
}

impl Manifest {
    // Open manifest in specified folder.
    // it will reopen manifest by find CURRENT and do some cleanup.
    pub(crate) async fn open(base: impl Into<PathBuf>) -> Result<Self> {
        let base = base.into();

        let mut manifest = Self {
            base,
            base_dir: None,
            max_file_size: MAX_MANIFEST_SIZE,
            next_file_id: 0,
            current_file_num: Default::default(),
            current_writer: None,
        };
        manifest.create_base_dir_if_not_exist().await?;
        manifest.current_file_num = manifest.load_current().await?;
        // 清理过期文件
        // TODO: 清理到异步任务中
        manifest.cleanup_obsolete_files().await?;
        Ok(manifest)
    }

    async fn create_base_dir_if_not_exist(&self) -> Result<()> {
        match create_dir_all(&self.base).await {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                let raw_metadata = fs::metadata(&self.base)?;

                if !raw_metadata.is_dir() {
                    panic!("base dir is not a dir")
                }
            }
            Err(err) => panic!("open base dir fail, {}", err),
        }
        Ok(())
    }

    pub(super) fn reset_next_file_id(&mut self, next_id: u32) {
        self.next_file_id = next_id;
    }

    pub(crate) fn next_file_id(&mut self) -> u32 {
        let id = self.next_file_id;
        self.next_file_id += 1;
        id
    }

    pub(crate) fn now(&self) -> u32 {
        self.next_file_id
    }

    /// 将一个新的 version_edit 写入到 manifest 文件中，然后在文件大小超过 max_file_size 时进行文件滚动。在文件滚动时，需要传递一个 version_snapshot 参数以获取当前的快照
    pub(crate) async fn record_version_edit(
        &mut self,
        ve: VersionEdit,
        version_snapshot: impl FnOnce() -> VersionEdit,
    ) -> Result<()> {
        let mut current = self.current_writer.take();
        let mut file_num = self.current_file_num.as_ref().unwrap_or(&0).to_owned();

        let rolled_path = if current.is_none()
            || current.as_ref().unwrap().current_file_size > self.max_file_size
        {

            file_num += 1;
            let path = self
                .base
                .join(format!("{}_{}", MANIFEST_FILE_NAME, file_num));
            let current_writer = File::create(&path).await?;
            current = Some(ManifestWriter {
                current_file_size: 0,
                current_writer,
            });
            Some(path)
        } else {
            None
        };

        let mut current = current.unwrap();
        let written = if rolled_path.is_some() {  // 说明需要进行滚动了
            // TODO: remove new created file when write fail.
            let base_snapshot = version_snapshot();
            // 先写入快照版本
            let base_written = VersionEditEncoder(base_snapshot)
                .encode(&mut current.current_writer)
                .await?;
            // 再写具体数据
            match VersionEditEncoder(ve)
                .encode(&mut current.current_writer)
                .await
            {
                Ok(record_written) => base_written + record_written,
                Err(err) => {
                    // 因为是滚动下一个文件
                    remove_file(rolled_path.as_ref().unwrap()).await?;
                    return Err(err);
                }
            }
        } else {
            // 继续写入
            VersionEditEncoder(ve)
                .encode(&mut current.current_writer)
                .await?
        } as u64;

        if rolled_path.is_some() {
            current
                .current_writer
                .sync_all()
                .await
                .expect("sync new manifest file fail");
            self.set_current(file_num).await?;
            // TODO: notify cleaner previous manifest + size, so it can be delete when need.
            self.current_file_num = Some(file_num);
        } else {
            current
                .current_writer
                .sync_all()
                .await
                .expect("sync manifest data fail");
        }

        current.current_file_size += written;

        self.current_writer = Some(current);

        Ok(())
    }

    // List current versions.
    // the caller can recovery Versions by apply each version_edits.
    pub(crate) async fn list_versions(&self) -> Result<Vec<VersionEdit>> {
        Ok(if let Some(current_file) = self.current_file_num {
            let path = self
                .base
                .join(format!("{}_{}", MANIFEST_FILE_NAME, current_file));
            let reader = File::open(path).await?;
            let mut decoder = VersionEditDecoder::new(reader);
            let mut ves = Vec::new();
            while let Some(ve) = decoder.next_record().await.map_err(|_| Error::Corrupted)? {
                ves.push(ve)
            }
            ves
        } else {
            vec![]
        })
    }

    async fn load_current(&self) -> Result<Option<u32 /* file_num */>> {
        // 查看当前数据 它使用的是文件计数
        let mut curr_file_reader = match File::open(self.base.join(CURRENT_FILE_NAME))
            .await
        {
            Ok(f) => f,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(_) => panic!("read current meet error"),
        };
        let mut file_num_bytes = vec![0u8; core::mem::size_of::<u32>()];
        curr_file_reader
            .read_exact(&mut file_num_bytes)
            .await?;
        let file_num = u32::from_le_bytes(
            file_num_bytes[0..core::mem::size_of::<u32>()]
                .try_into()
                .map_err(|_| Error::Corrupted)?,
        );
        Ok(Some(file_num))
    }

    async fn set_current(&self, file_num: u32) -> Result<()> {
        //先建一个临时文件， 之后再修改名称
        let tmp_path = self
            .base
            .join(format!("curr.{}.{}", file_num, TEMPLE_SUFFIX));

        {
            let mut tmp_file = File::create(&tmp_path).await?;
            tmp_file.write_all(&file_num.to_le_bytes()).await?;
            tmp_file
                .sync_all()
                .await
                .expect("sync tmp current file fail");
        }

        match rename(&tmp_path, self.base.join(CURRENT_FILE_NAME))
            .await
        {
            Ok(_) => Ok(()),
            Err(_err) => {
                let _ = remove_file(&tmp_path).await?;
                // TODO: throw right error.
                Err(Error::Corrupted)
            }
        }?;
        Ok(())
    }

    async fn cleanup_obsolete_files(&self) -> Result<()> {
        fn is_obsolete_manifest(file_name: &str, curr_file_num: Option<u32>) -> bool {
            let file_num_str = file_name.trim_start_matches(&format!("{}_", MANIFEST_FILE_NAME));
            if let Ok(file_num) = file_num_str.parse::<u32>() {
                if let Some(curr_file_num) = curr_file_num {
                    if file_num < curr_file_num {
                        return true;
                    }
                } else {
                    return true;
                }
            }
            false
        }

        let mut wait_remove_paths = Vec::new();
        let mut dir = read_dir(&self.base).await?;
        while let Some(path) = dir.next_entry().await? {
            let file_path = path.path();
            if let Some(ext) = file_path.extension() {
                if ext.to_str().unwrap() == TEMPLE_SUFFIX {
                    wait_remove_paths.push(file_path.to_owned());
                    continue;
                }
            }
            if is_obsolete_manifest(
                file_path.file_name().unwrap().to_str().unwrap(),
                self.current_file_num,
            ) {
                wait_remove_paths.push(file_path.to_owned());
            }
        }

        for path in wait_remove_paths {
            remove_file(path).await?;
        }

        Ok(())
    }

}

struct VersionEditEncoder(VersionEdit);

impl VersionEditEncoder {
    async fn encode(&self, w: &mut File) -> Result<usize> {
        let bytes = self.0.encode_to_vec();
        w.write_all(&bytes.len().to_le_bytes()).await?;
        w.write_all(&bytes).await?;
        Ok(bytes.len() + core::mem::size_of::<u64>())
    }
}

struct VersionEditDecoder {
    reader: File,
    offset: u64,
}

impl VersionEditDecoder {
    fn new(reader: File) -> Self {
        Self { reader, offset: 0 }
    }

    async fn next_record(&mut self) -> Result<Option<VersionEdit>> {
        let mut offset = self.offset;
        let len = {
            let mut len_bytes = vec![0u8; core::mem::size_of::<u64>()];
            self.reader.seek(SeekFrom::Start(offset)).await?;
            match self
                .reader
                .read_exact(&mut len_bytes)
                .await
            {
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => return Ok(None),
                e @ Err(_) => e?,
                _ => {0}
            };
            u64::from_le_bytes(
                len_bytes[0..core::mem::size_of::<u64>()]
                    .try_into()
                    .map_err(|_| Error::Corrupted)?,
            )
        };
        offset += (core::mem::size_of::<u64>() as u64);
        let ve = {
            let mut ve_bytes = vec![0u8; len as usize];
            self.reader.seek(SeekFrom::Start(offset)).await?;
            self.reader
                .read_exact(&mut ve_bytes)
                .await?;
            // error
            println!("{:?}", ve_bytes);
            VersionEdit::decode(ve_bytes.as_slice()).map_err(|_| Error::Corrupted)?
        };
        self.offset = offset + len;
        Ok(Some(ve))
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use super::*;
    use crate::store::meta::{NewFile, StreamEdit};

    #[inline]
    fn new_files(ids: Vec<u32>) -> Vec<NewFile> {
        ids.into_iter().map(Into::into).collect()
    }

    #[tokio::test]
    async fn test_cleanup_when_restart() {
        let base = tempdir::TempDir::new("curr_test_restart").unwrap();

        let version_snapshot = VersionEdit::default;
        {
            let mut manifest = Manifest::open(base.as_ref()).await.unwrap();
            manifest.max_file_size = 1;

            manifest
                .record_version_edit(
                    VersionEdit {
                        file_stream: Some(StreamEdit {
                            new_files: new_files(vec![2, 3]),
                            deleted_files: vec![1],
                        }),
                    },
                    version_snapshot,
                )
                .await
                .unwrap();

            manifest
                .record_version_edit(
                    VersionEdit {
                        file_stream: Some(StreamEdit {
                            new_files: new_files(vec![2, 3]),
                            deleted_files: vec![1],
                        }),
                    },
                    version_snapshot,
                )
                .await
                .unwrap();
            manifest
                .record_version_edit(
                    VersionEdit {
                        file_stream: Some(StreamEdit {
                            new_files: new_files(vec![2, 3]),
                            deleted_files: vec![1],
                        }),
                    },
                    version_snapshot,
                )
                .await
                .unwrap();

            let mut files = 0;
            let mut files = fs::read_dir(&base)
                .expect("open base dir fail")
                .into_iter()
                .count();
            assert_eq!(files, 4); // 3 data + 1 current
        }
        {
            let _ = Manifest::open(base.as_ref()).await.unwrap();

            let files = fs::read_dir(&base)
                .expect("open base dir fail")
                .into_iter()
                .count();
            assert_eq!(files, 2);
        }
    }

    #[tokio::test]
    async fn test_roll_manifest() {

        let base = tempdir::TempDir::new("curr_test_roll").unwrap();

        let ver = std::sync::Arc::new(std::sync::Mutex::new(VersionEdit {
            file_stream: Some(StreamEdit {
                new_files: vec![],
                deleted_files: vec![],
            }),
        }));

        let ve_snapshot = || {
            let ver = ver.lock().unwrap();
            ver.to_owned()
        };

        let mock_apply = |ve: &VersionEdit| {
            let mut ver = ver.lock().unwrap();
            let edit = ver.file_stream.as_mut().unwrap();
            edit.new_files
                .extend_from_slice(&ve.file_stream.as_ref().unwrap().new_files);
            edit.new_files.retain(|f| {
                !ve.file_stream
                    .as_ref()
                    .unwrap()
                    .deleted_files
                    .iter()
                    .any(|d| *d == f.id)
            })
        };

        {
            let mut manifest = Manifest::open( base.as_ref()).await.unwrap();
            manifest.max_file_size = 100; // set a small threshold value to trigger roll
            assert_eq!(manifest.current_file_num, None);

            let ve = VersionEdit {
                file_stream: Some(StreamEdit {
                    new_files: new_files(vec![0]),
                    deleted_files: vec![],
                }),
            };
            manifest
                .record_version_edit(ve.to_owned(), ve_snapshot)
                .await
                .unwrap();
            mock_apply(&ve);
            assert_eq!(manifest.current_file_num, Some(1));


            for i in 1..43u32 {
                let r = i.saturating_sub(10u32);
                let ve = VersionEdit {
                    file_stream: Some(StreamEdit {
                        new_files: new_files(vec![i]),
                        deleted_files: vec![r],
                    }),
                };
                manifest
                    .record_version_edit(ve.to_owned(), ve_snapshot)
                    .await
                    .unwrap();
                mock_apply(&ve);
            }
            assert_eq!(manifest.current_file_num, Some(36));
        }

        {
            let mut manifest2 = Manifest::open( base.as_ref()).await.unwrap();
            let versions = manifest2.list_versions().await.unwrap();
            assert_eq!(manifest2.current_file_num, Some(36));

            let mut recover_ver = VersionEdit {
                file_stream: Some(StreamEdit::default()),
            };
            for ve in versions {
                let recover_ver = recover_ver.file_stream.as_mut().unwrap();
                let ve = ve.file_stream.as_ref().unwrap();
                recover_ver.new_files.extend_from_slice(&ve.new_files);
                recover_ver
                    .new_files
                    .retain(|f| !ve.deleted_files.iter().any(|d| *d == f.id));
            }

            assert_eq!(recover_ver.file_stream.as_ref().unwrap().new_files, {
                let ver = ver.lock().unwrap();
                ver.to_owned().file_stream.unwrap().new_files
            });

            let ve = VersionEdit {
                file_stream: Some(StreamEdit {
                    new_files: new_files(vec![1]),
                    deleted_files: vec![],
                }),
            };
            manifest2
                .record_version_edit(ve.to_owned(), ve_snapshot)
                .await
                .unwrap(); // first write after reopen trigger roll.
            assert_eq!(manifest2.current_file_num, Some(37));
        }
    }

    #[tokio::test]
    async fn test_mantain_current() {
        let version_snapshot = VersionEdit::default;

        let base = tempdir::TempDir::new("curr_test2").unwrap();

        {
            let mut manifest = Manifest::open( base.as_ref()).await.unwrap();
            manifest
                .record_version_edit(
                    VersionEdit {
                        file_stream: Some(StreamEdit {
                            new_files: new_files(vec![2, 3]),
                            deleted_files: vec![1],
                        }),
                    },
                    version_snapshot,
                )
                .await
                .unwrap();
            manifest
                .record_version_edit(
                    VersionEdit {
                        file_stream: Some(StreamEdit {
                            new_files: new_files(vec![4]),
                            deleted_files: vec![],
                        }),
                    },
                    version_snapshot,
                )
                .await
                .unwrap();
            manifest
                .record_version_edit(
                    VersionEdit {
                        file_stream: Some(StreamEdit {
                            new_files: new_files(vec![5]),
                            deleted_files: vec![],
                        }),
                    },
                    version_snapshot,
                )
                .await
                .unwrap();
        }

        {
            let manifest2 = Manifest::open( base.as_ref()).await.unwrap();
            let versions = manifest2.list_versions().await.unwrap();
            assert_eq!(versions.len(), 4);
        }
    }
}
