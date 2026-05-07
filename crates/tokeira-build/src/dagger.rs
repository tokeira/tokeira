use std::path::Path;

use crate::BuildError;

pub trait DaggerClient: Send + Sync {
    fn host_directory<'client>(
        &'client self,
        path: &Path,
    ) -> Result<Box<dyn DirectoryRef<'client> + 'client>, BuildError>;

    /// Load a directory with gitignore-style exclude/include filters.
    ///
    /// Pipelines that upload the workspace root MUST use this to exclude
    /// Rust `target/`, `.git/`, and other large directories the builder
    /// does not need. Passing empty slices is equivalent to `host_directory`.
    ///
    /// The default implementation falls back to unfiltered `host_directory`
    /// so mocks that don't care about filtering still work. Production
    /// implementations SHOULD override to honour the patterns.
    fn host_directory_filtered<'client>(
        &'client self,
        path: &Path,
        _exclude: &[&str],
        _include: &[&str],
    ) -> Result<Box<dyn DirectoryRef<'client> + 'client>, BuildError> {
        self.host_directory(path)
    }

    fn container_from<'client>(
        &'client self,
        image: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError>;
    fn set_secret<'client>(
        &'client self,
        name: &str,
        value: &str,
    ) -> Result<Box<dyn SecretRef + 'client>, BuildError>;
}

pub trait ContainerRef<'client>: Send + Sync {
    fn clone_ref(&self) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError>;

    fn with_exec(
        self: Box<Self>,
        args: &[&str],
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError>;
    fn with_env(
        self: Box<Self>,
        key: &str,
        value: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError>;
    fn with_workdir(
        self: Box<Self>,
        path: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError>;
    fn with_directory(
        self: Box<Self>,
        path: &str,
        dir: &dyn DirectoryRef<'client>,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError>;
    fn with_file(
        self: Box<Self>,
        path: &str,
        file: &dyn FileRef<'client>,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError>;
    fn with_entrypoint(
        self: Box<Self>,
        args: &[&str],
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError>;
    fn with_user(
        self: Box<Self>,
        user: &str,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError>;
    fn with_registry_auth(
        self: Box<Self>,
        registry: &str,
        user: &str,
        secret: &dyn SecretRef,
    ) -> Result<Box<dyn ContainerRef<'client> + 'client>, BuildError>;
    fn file(&self, path: &str) -> Result<Box<dyn FileRef<'client> + 'client>, BuildError>;
    fn export_image(&self, tag: &str) -> Result<(), BuildError>;
    fn publish(&self, remote_ref: &str) -> Result<String, BuildError>;

    #[doc(hidden)]
    fn as_dagger_container(&self) -> Option<&dagger_client::Container<'client>> {
        None
    }
}

pub trait DirectoryRef<'client>: Send + Sync {
    fn file(&self, name: &str) -> Result<Box<dyn FileRef<'client> + 'client>, BuildError>;

    #[doc(hidden)]
    fn as_dagger_directory(&self) -> Option<&dagger_client::Directory<'client>> {
        None
    }
}

pub trait FileRef<'client>: Send + Sync {
    #[doc(hidden)]
    fn as_dagger_file(&self) -> Option<&dagger_client::File<'client>> {
        None
    }
}

pub trait SecretRef: Send + Sync {
    #[doc(hidden)]
    fn as_dagger_secret(&self) -> Option<&dagger_client::SecretId> {
        None
    }
}
